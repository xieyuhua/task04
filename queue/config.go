package queue

import (
	"net/http"
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

// RuntimeConfig 运行时可通过命令行调整的配置项
type RuntimeConfig struct {
	WorkerPoolSize   int           // 并发 worker 池大小
	BatchSize        int           // 每次批量拉取任务数（对应 ZrangeCount）
	CallbackTTR      time.Duration // 单次回调超时
	AckTimeout       time.Duration // ACK 确认超时
	MaxRetryDelayCap int64         // 指数退避延迟上限（秒），0 表示使用默认值 86400
}

// ApplyConfig 应用运行时配置，必须在 RunWorker 之前调用。
func ApplyConfig(cfg RuntimeConfig) {
	if cfg.WorkerPoolSize > 0 {
		WorkerPoolSize = cfg.WorkerPoolSize
	}
	if cfg.BatchSize > 0 {
		BatchSize = cfg.BatchSize
		ZrangeCount = cfg.BatchSize
	}
	if cfg.CallbackTTR > 0 {
		CallbackTTR = cfg.CallbackTTR
	}
	if cfg.AckTimeout > 0 {
		AckTimeout = cfg.AckTimeout
	}
	if cfg.MaxRetryDelayCap > 0 {
		MaxRetryDelayCap = cfg.MaxRetryDelayCap
	}

	// 重建 HTTP 客户端，使 CallbackTTR 和连接池参数生效
	httpClient = &http.Client{
		Timeout: CallbackTTR,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnsPerHost,
			MaxIdleConns:        MaxIdleConns,
			IdleConnTimeout:     IdleConnTimeout,
		},
	}
}

// GetQueueLengths 获取各桶中的任务数量（用于 metrics/监控）
func GetQueueLengths(b Backend) map[string]int64 {
	result := map[string]int64{
		"delay":           0,
		"unack":           0,
		"error":           0,
		"ack_deadline":    0,
	}
	if b == nil {
		return result
	}
	// 尝试通过 GetQueueLen 接口获取（如果 backend 实现了扩展接口）
	if ext, ok := b.(MetricsProvider); ok {
		for _, key := range []string{DelayBucket, UnackBucket, ErrorBucket, AckDeadlineBucket} {
			count, err := ext.ZCard(key)
			if err == nil {
				switch key {
				case DelayBucket:
					result["delay"] = count
				case UnackBucket:
					result["unack"] = count
				case ErrorBucket:
					result["error"] = count
				case AckDeadlineBucket:
					result["ack_deadline"] = count
				}
			}
		}
	}
	return result
}

// MetricsProvider 是 Backend 的可选扩展接口，提供监控指标
type MetricsProvider interface {
	ZCard(key string) (int64, error)
	Ping() error
}
