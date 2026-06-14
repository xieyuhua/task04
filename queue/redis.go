package queue

import (
	"github.com/garyburd/redigo/redis"
)

var pool *redis.Pool

func InitRedis(address string) error {
	dial := func() (redis.Conn, error) {
		return redis.DialURL(address,
			redis.DialConnectTimeout(RedisConnectTimeout),
			redis.DialReadTimeout(RedisReadTimeout),
			redis.DialWriteTimeout(RedisWriteTimeout))
	}
	pool = &redis.Pool{
		MaxIdle:     RedisPoolMaxIdle,
		MaxActive:   RedisPoolMaxIdle * 2,
		IdleTimeout: RedisPoolIdleTimeout,
		Dial:        dial,
	}
	return nil
}
