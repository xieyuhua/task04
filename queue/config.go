package queue

import (
	"time"
)

var (
	RedisConnectTimeout  = 50 * time.Millisecond
	RedisReadTimeout     = 50 * time.Millisecond
	RedisWriteTimeout    = 100 * time.Millisecond
	RedisPoolMaxIdle     = 200
	RedisPoolIdleTimeout = 3 * time.Minute
)

var (
	TaskTTL       = 24 * 3600
	ZrangeCount   = 100
	RetryInterval = 10 //second

	DelayWorkerInterval = 100 * time.Millisecond
	UnackWorkerInterval = 1000 * time.Millisecond
	ErrorWorkerInterval = 1000 * time.Millisecond
	AckCheckInterval    = 500 * time.Millisecond
)

var (
	CallbackTTR         = 3 * time.Second  //time to run
	AckTimeout          = 30 * time.Second // ACK 超时时间，超时未确认则重入延迟队列
	MaxIdleConnsPerHost = 10
	MaxIdleConns        = 1024
	IdleConnTimeout     = time.Minute * 5

	WorkerPoolSize = 64 // 并发 worker 池大小
	BatchSize      = 50 // 每次批量拉取任务数
)
