package queue

import (
	"encoding/json"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

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
	if err := c.Send("SET", task.ID, String(data), "EX", TaskTTL); err != nil {
		return err
	}
	if err := c.Send("ZADD", DelayBucket, task.ExecuteTime, task.ID); err != nil {
		return err
	}
	return c.Flush()
}

func (b *RedisBackend) GetTask(id string) (*Task, error) {
	c := b.pool.Get()
	defer c.Close()
	data, err := redis.String(c.Do("GET", id))
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
	ttl, err := redis.Int(c.Do("TTL", task.ID))
	if err != nil {
		return err
	}
	_, err = c.Do("SET", task.ID, String(data), "EX", ttl)
	return err
}

// DeleteTask 使用 pipeline 批量删除所有桶中的引用
func (b *RedisBackend) DeleteTask(id string) error {
	c := b.pool.Get()
	defer c.Close()
	if err := c.Send("DEL", id); err != nil {
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
	return c.Flush()
}

func (b *RedisBackend) GetReadyIDs(bucket string, begin, end int64) ([]string, error) {
	c := b.pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("ZRANGEBYSCORE", bucket, begin, end, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

// GetAckTimeoutIDs 返回 unack 桶中 ack_deadline 已过的任务 ID
// 使用独立的 sorted set "later_ack_deadline" 来跟踪 ACK 超时
const AckDeadlineBucket = "later_ack_deadline"

func (b *RedisBackend) GetAckTimeoutIDs(now int64) ([]string, error) {
	c := b.pool.Get()
	defer c.Close()
	// 查找 ack_deadline 在 [-inf, now] 范围内的任务
	return redis.Strings(c.Do("ZRANGEBYSCORE", AckDeadlineBucket, "-inf", now, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

func (b *RedisBackend) DelayToUnack(id string, score int64) (bool, error) {
	return b.bucketTransfer(DelayBucket, UnackBucket, id, score)
}

func (b *RedisBackend) UnackToDelay(id string, score int64) (bool, error) {
	return b.bucketTransferWithAckCleanup(UnackBucket, DelayBucket, id, score)
}

func (b *RedisBackend) ErrorToDelay(id string, score int64) (bool, error) {
	return b.bucketTransfer(ErrorBucket, DelayBucket, id, score)
}

func (b *RedisBackend) UnackToError(id string, score int64) error {
	c := b.pool.Get()
	defer c.Close()
	if err := c.Send("ZADD", ErrorBucket, score, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", UnackBucket, id); err != nil {
		return err
	}
	if err := c.Send("ZREM", AckDeadlineBucket, id); err != nil {
		return err
	}
	return c.Flush()
}

// AckTask 确认任务成功，删除任务及所有桶中的引用
func (b *RedisBackend) AckTask(id string) error {
	c := b.pool.Get()
	defer c.Close()
	if err := c.Send("DEL", id); err != nil {
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
	return c.Flush()
}

func (b *RedisBackend) bucketTransfer(from, to, id string, score int64) (bool, error) {
	c := b.pool.Get()
	defer c.Close()
	reply, err := redis.Int(c.Do("ZADD", to, score, id))
	if err != nil {
		return false, err
	}
	if reply == 0 {
		return false, nil
	}
	_, err = c.Do("ZREM", from, id)
	return true, err
}

// bucketTransferWithAckCleanup 在 UnackToDelay 时同时清理 ACK deadline 跟踪
func (b *RedisBackend) bucketTransferWithAckCleanup(from, to, id string, score int64) (bool, error) {
	c := b.pool.Get()
	defer c.Close()
	reply, err := redis.Int(c.Do("ZADD", to, score, id))
	if err != nil {
		return false, err
	}
	if reply == 0 {
		return false, nil
	}
	if err := c.Send("ZREM", from, id); err != nil {
		return true, err
	}
	if err := c.Send("ZREM", AckDeadlineBucket, id); err != nil {
		return true, err
	}
	return true, c.Flush()
}

// SetAckDeadline 将任务 ID 添加到 ACK deadline 跟踪集合（在 DelayToUnack 成功后调用）
func (b *RedisBackend) SetAckDeadline(id string, deadline int64) error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("ZADD", AckDeadlineBucket, deadline, id)
	return err
}
