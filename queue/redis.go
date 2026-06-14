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

func (b *RedisBackend) CreateTask(task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	c := b.pool.Get()
	defer c.Close()
	_, err = c.Do("SET", task.ID, String(data), "EX", TaskTTL)
	if err != nil {
		return err
	}
	_, err = c.Do("ZADD", DelayBucket, task.ExecuteTime, task.ID)
	return err
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

func (b *RedisBackend) DeleteTask(id string) error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("DEL", id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", DelayBucket, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", UnackBucket, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", ErrorBucket, id)
	return err
}

func (b *RedisBackend) GetReadyIDs(bucket string, begin, end int64) ([]string, error) {
	c := b.pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("ZRANGEBYSCORE", bucket, begin, end, "LIMIT", "0", fmt.Sprintf("%v", ZrangeCount)))
}

func (b *RedisBackend) DelayToUnack(id string, score int64) (bool, error) {
	return b.bucketTransfer(DelayBucket, UnackBucket, id, score)
}

func (b *RedisBackend) UnackToDelay(id string, score int64) (bool, error) {
	return b.bucketTransfer(UnackBucket, DelayBucket, id, score)
}

func (b *RedisBackend) ErrorToDelay(id string, score int64) (bool, error) {
	return b.bucketTransfer(ErrorBucket, DelayBucket, id, score)
}

func (b *RedisBackend) UnackToError(id string, score int64) error {
	c := b.pool.Get()
	defer c.Close()
	_, err := c.Do("ZADD", ErrorBucket, score, id)
	if err != nil {
		return err
	}
	_, err = c.Do("ZREM", UnackBucket, id)
	return err
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
