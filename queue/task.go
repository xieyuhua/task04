package queue

import "encoding/json"

// RetryStrategy defines how retry delay is calculated
type RetryStrategy int

const (
	// RetryFixed uses a constant delay between retries
	RetryFixed RetryStrategy = iota
	// RetryExponential uses 2^n * interval delay (2, 4, 8, 16...)
	RetryExponential
)

// TaskAckStatus represents the ack state of a task
type TaskAckStatus int

const (
	AckPending    TaskAckStatus = iota // 等待确认
	AckConfirmed                       // 已确认成功
	AckExpired                         // 确认超时，需重试
)

// Task is the task to execute
type Task struct {
	// ID is a global unique id
	ID string
	// Topic use to classify tasks
	Topic string
	// ExecuteTime is the time to deliver
	ExecuteTime int64
	// MaxRetry is max deliver retry times
	MaxRetry int
	// HasRetry is the current retry times
	HasRetry int
	// RetryStrategy: 0=fixed, 1=exponential
	RetryStrategy RetryStrategy `json:"retry_strategy"`
	// RetryInterval is the base retry interval in seconds
	// For fixed strategy: delay = RetryInterval every time
	// For exponential strategy: delay = 2^HasRetry * RetryInterval (2,4,8,16...)
	RetryInterval int64 `json:"retry_interval"`
	// Callback is the deliver address
	Callback string
	// Content is the task content to deliver
	Content string
	// CreatTime is the time task created
	CreatTime int64
	// AckDeadline is the unix timestamp by which the task must be acked
	AckDeadline int64 `json:"ack_deadline"`
	// AckStatus: 0=pending, 1=success, 2=timeout
	AckStatus TaskAckStatus `json:"ack_status"`
}

// Redis Key 命名规范: later:{category}:{name}
// - later:task:{id}     → 任务数据 (String)
// - later:bucket:delay  → 延迟桶 (ZSet)
// - later:bucket:unack  → 未确认桶 (ZSet)
// - later:bucket:error  → 错误桶 (ZSet)
// - later:bucket:ack_dl → ACK 截止桶 (ZSet)
const (
	// Redis key 命名空间前缀
	KeyPrefix = "later"

	// 桶名称（有序集合）
	DelayBucket      = "later:bucket:delay"
	UnackBucket      = "later:bucket:unack"
	ErrorBucket      = "later:bucket:error"
	AckDeadlineBucket = "later:bucket:ack_dl"
)

// TaskKey 返回任务数据在 Redis 中的存储 key
func TaskKey(id string) string {
	return "later:task:" + id
}

// MaxRetryDelayCap 指数退避延迟上限（秒），默认 86400（24 小时）
var MaxRetryDelayCap int64 = 86400

// NextRetryDelay returns the delay in seconds before the next retry
func (t *Task) NextRetryDelay() int64 {
	switch t.RetryStrategy {
	case RetryExponential:
		// 2, 4, 8, 16... * RetryInterval，上限 MaxRetryDelayCap
		delay := int64(1<<uint(t.HasRetry)) * t.RetryInterval
		if delay > MaxRetryDelayCap {
			return MaxRetryDelayCap
		}
		return delay
	default: // RetryFixed
		return t.RetryInterval
	}
}

// MarshalTask serializes a task to JSON bytes
func MarshalTask(task *Task) ([]byte, error) {
	return json.Marshal(task)
}

// UnmarshalTask deserializes a task from JSON bytes
func UnmarshalTask(data []byte) (*Task, error) {
	var task Task
	err := json.Unmarshal(data, &task)
	return &task, err
}
