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
}

const (
	DelayBucket = "later_delay"
	UnackBucket = "later_unack"
	ErrorBucket = "later_error"
)

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
