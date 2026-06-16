package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
)

// ────────────── Lua 脚本（原子化桶操作）──────────────

// luaBucketTransfer 原子执行 ZADD to + ZREM from
// KEYS[1]=from, KEYS[2]=to, ARGV[1]=id, ARGV[2]=score
// 返回 1=成功转移, 0=目标已有该元素未转移
const luaBucketTransfer = `
local reply = redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
if reply == 0 then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`

// luaBucketTransferWithAckCleanup 原子执行 ZADD to + ZREM from + ZREM ack_dl
// KEYS[1]=from, KEYS[2]=to, KEYS[3]=ack_deadline, ARGV[1]=id, ARGV[2]=score
const luaBucketTransferWithAckCleanup = `
local reply = redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
if reply == 0 then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`

// luaUnackToError 原子执行 ZADD error + ZREM unack + ZREM ack_dl
// KEYS[1]=unack, KEYS[2]=error, KEYS[3]=ack_deadline, ARGV[1]=id, ARGV[2]=score
const luaUnackToError = `
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`

// luaGetTaskAndDelayToUnack 原子执行 GET task_key + ZADD unack + ZREM delay + ZADD ack_dl
// KEYS[1]=task_key, KEYS[2]=delay_bucket, KEYS[3]=unack_bucket, KEYS[4]=ack_deadline_bucket
// ARGV[1]=id, ARGV[2]=score, ARGV[3]=ack_deadline
// 返回: {data} 成功, nil 失败（任务不存在）
// 将 ack_deadline ZADD 纳入原子操作，消除 callback 中 SetAckDeadline 与桶迁移之间的竞态窗口
const luaGetTaskAndDelayToUnack = `
local data = redis.call('GET', KEYS[1])
if not data then return nil end
local moved = redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
if moved == 0 then return nil end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])
return data
`

// luaCleanExpiredBucket 清理桶中超期的元素
// KEYS[1]=bucket, ARGV[1]=cutoff（Unix 时间戳，清理 score < cutoff 的元素）
const luaCleanExpiredBucket = `
return redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
`

// RedisBackend implements Backend using Redis sorted sets
type RedisBackend struct {
	pool *redis.Pool
}

// NewRedisBackend creates a RedisBackend with the given address
func NewRedisBackend(address string) *RedisBackend {
	dial := func() (redis.Conn, error) {
		return redis.DialURL(address,
			redis.DialConnectTimeout(RedisConnectTimeout),
			redis.DialReadTimeout(RedisReadTimeout),
			redis.DialWriteTimeout(RedisWriteTimeout))
	}
	p := &redis.Pool{
		MaxIdle:     RedisPoolMaxIdle,
		MaxActive:   RedisPoolMaxIdle * 2,
		IdleTimeout: RedisPoolIdleTimeout,
		Dial:        dial,
	}
	return &RedisBackend{pool: p}
}

func (b *RedisBackend) Init() error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("PING")
	return err
}

func (b *RedisBackend) Close() {
	b.pool.Close()
}

// CreateTask 使用 pipeline 减少网络往返
func (b *RedisBackend) CreateTask(task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	c := b.pool.Get()
	defer c.Close()
	// pipeline: SET + ZADD 合并
	if err := c.Send("SET", TaskKey(task.ID), String(data), "EX", TaskTTL); err != nil {
		return err
	}
	if err := c.Send("ZADD", DelayBucket, task.ExecuteTime, task.ID); err != nil {
		return err
	}
	if err := c.Flush(); err != nil {
		return err
	}
	// 接收 pipeline 响应，确保 Redis 服务端错误不被静默忽略
	if _, err := c.Receive(); err != nil {
		return err
	}
	_, err = c.Receive()
	return err
}

func (b *RedisBackend) GetTask(id string) (*Task, error) {
	c := b.pool.Get()
	defer c.Close()
	data, err := redis.String(c.Do("GET", TaskKey(id)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	var task Task
	err = json.Unmarshal(Slice(data), &task)
	return &task, err
}

func (b *RedisBackend) UpdateTask(task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	c := b.pool.Get()
	defer c.Close()
	// 使用固定 TaskTTL，避免读-改-写竞态
	_, err = c.Do("SET", TaskKey(task.ID), String(data), "EX", TaskTTL)
	return err
}

// DeleteTask 使用 pipeline 批量删除所有桶中的引用
func (b *RedisBackend) DeleteTask(id string) error {
	c := b.pool.Get()
	defer c.Close()
	if err := c.Send("DEL", TaskKey(id)); err != nil {
		return err
	}
	if err := c.Send("ZREM", DelayBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", UnackBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", ErrorBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", AckDeadlineBucket, id); err != nil {
		return err
	}
	if err := c.Flush(); err != nil {
		return err
	}
	// 接收 5 条 pipeline 响应
	var firstErr error
	for i := 0; i < 5; i++ {
		if _, err := c.Receive(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *RedisBackend) GetReadyIDs(bucket string, begin, end int64) ([]string, error) {
	c := b.pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("ZRANGEBYSCORE", bucket, begin, end, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

// GetAckTimeoutIDs 返回 ACK deadline 已过的任务 ID
func (b *RedisBackend) GetAckTimeoutIDs(now int64) ([]string, error) {
	c := b.pool.Get()
	defer c.Close()
	// 查找 ack_deadline 在 [-inf, now] 范围内的任务
	return redis.Strings(c.Do("ZRANGEBYSCORE", AckDeadlineBucket, "-inf", now, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

func (b *RedisBackend) DelayToUnack(id string, score int64) (bool, error) {
	return b.bucketTransferLua(DelayBucket, UnackBucket, id, score)
}

func (b *RedisBackend) UnackToDelay(id string, score int64) (bool, error) {
	return b.bucketTransferWithAckCleanupLua(UnackBucket, DelayBucket, id, score)
}

func (b *RedisBackend) ErrorToDelay(id string, score int64) (bool, error) {
	return b.bucketTransferLua(ErrorBucket, DelayBucket, id, score)
}

func (b *RedisBackend) UnackToError(id string, score int64) error {
	c := b.pool.Get()
	defer c.Close()
	script := redis.NewScript(3, luaUnackToError)
	_, err := redis.Int(script.Do(c, UnackBucket, ErrorBucket, AckDeadlineBucket, id, score))
	return err
}

// DelayToUnackWithData 原子获取任务数据并从 delay 移到 unack，同时注册 ACK deadline。
// ackDeadline 被纳入 Lua 原子操作，消除 callback 中桶迁移与 SetAckDeadline 之间的崩溃丢任务窗口。
func (b *RedisBackend) DelayToUnackWithData(id string, score int64, ackDeadline int64) (*Task, error) {
	c := b.pool.Get()
	defer c.Close()
	script := redis.NewScript(4, luaGetTaskAndDelayToUnack)
	data, err := redis.Bytes(script.Do(c, TaskKey(id), DelayBucket, UnackBucket, AckDeadlineBucket, id, score, ackDeadline))
	if err != nil {
		if err == redis.ErrNil {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// AckTask 确认任务成功，删除任务及所有桶中的引用
func (b *RedisBackend) AckTask(id string) error {
	c := b.pool.Get()
	defer c.Close()
	if err := c.Send("DEL", TaskKey(id)); err != nil {
		return err
	}
	if err := c.Send("ZREM", DelayBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", UnackBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", ErrorBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", AckDeadlineBucket, id); err != nil {
		return err
	}
	if err := c.Flush(); err != nil {
		return err
	}
	// 接收 5 条 pipeline 响应
	var firstErr error
	for i := 0; i < 5; i++ {
		if _, err := c.Receive(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *RedisBackend) bucketTransferLua(from, to, id string, score int64) (bool, error) {
	c := b.pool.Get()
	defer c.Close()
	script := redis.NewScript(2, luaBucketTransfer)
	reply, err := redis.Int(script.Do(c, from, to, id, score))
	if err != nil {
		return false, err
	}
	return reply == 1, nil
}

func (b *RedisBackend) bucketTransferWithAckCleanupLua(from, to, id string, score int64) (bool, error) {
	c := b.pool.Get()
	defer c.Close()
	script := redis.NewScript(3, luaBucketTransferWithAckCleanup)
	reply, err := redis.Int(script.Do(c, from, to, AckDeadlineBucket, id, score))
	if err != nil {
		return false, err
	}
	return reply == 1, nil
}

// CleanExpiredBuckets 清理所有桶中超过 TTL 的过期数据，防止桶泄漏
func (b *RedisBackend) CleanExpiredBuckets() {
	c := b.pool.Get()
	defer c.Close()
	cutoff := time.Now().Add(-time.Duration(TaskTTL) * time.Second).Unix()
	for _, bucket := range []string{DelayBucket, UnackBucket, ErrorBucket, AckDeadlineBucket} {
		script := redis.NewScript(1, luaCleanExpiredBucket)
		count, err := redis.Int64(script.Do(c, bucket, cutoff))
		if err == nil && count > 0 {
			log.Infof("cleaned %d expired entries from %s", count, bucket)
		}
	}
}

// ────────────── MetricsProvider 实现 ──────────────

// ZCard 返回指定 ZSet 的成员数量
func (b *RedisBackend) ZCard(key string) (int64, error) {
	c := b.pool.Get()
	defer c.Close()
	return redis.Int64(c.Do("ZCARD", key))
}

// Ping 检查 Redis 连接可达性
func (b *RedisBackend) Ping() error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("PING")
	return err
}

// SetAckDeadline 将任务 ID 添加到 ACK deadline 跟踪集合（在 DelayToUnack 成功后调用）
func (b *RedisBackend) SetAckDeadline(id string, deadline int64) error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("ZADD", AckDeadlineBucket, deadline, id)
	return err
}
