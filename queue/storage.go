package queue

import (
	"errors"
	"sync"
	"sync/atomic"
)

// ErrTaskNotFound is returned when a task does not exist
var ErrTaskNotFound = errors.New("task not found")

// backend is the active storage backend (Redis or RabbitMQ)
var backend Backend

// shutdownFlag 优雅关闭标记：1 表示正在关闭
var shutdownFlag int32

// stopOnce 确保 stop channel 只关闭一次
var stopOnce sync.Once

// workerStopCh 通知所有后台 worker 退出
var workerStopCh = make(chan struct{})

// IsShuttingDown 检查是否正在关闭
func IsShuttingDown() bool {
	return atomic.LoadInt32(&shutdownFlag) != 0
}

// SetShuttingDown 设置关闭标记
func SetShuttingDown() {
	atomic.StoreInt32(&shutdownFlag, 1)
}

// InitBackend initializes the global backend and calls Init()
func InitBackend(b Backend) error {
	if err := b.Init(); err != nil {
		return err
	}
	backend = b
	return nil
}

// CloseBackend closes the active backend
func CloseBackend() {
	if backend != nil {
		backend.Close()
	}
}

// Shutdown 优雅关闭：通知 worker 停止 → 关闭 taskChan → worker 协程排空后自然退出
func Shutdown() {
	SetShuttingDown()
	stopOnce.Do(func() {
		close(workerStopCh)
	})
	if taskChan != nil {
		close(taskChan)
	}
}

// Implementations include Redis and RabbitMQ.
type Backend interface {
	// Init initializes the backend connection/resources
	Init() error
	// Close releases backend resources
	Close()

	// Task CRUD
	CreateTask(task *Task) error
	GetTask(id string) (*Task, error)
	UpdateTask(task *Task) error
	DeleteTask(id string) error

	// Bucket operations for the delay queue state machine
	GetReadyIDs(bucket string, begin, end int64) ([]string, error)
	DelayToUnack(id string, score int64) (bool, error)
	UnackToDelay(id string, score int64) (bool, error)
	ErrorToDelay(id string, score int64) (bool, error)
	UnackToError(id string, score int64) error

	// ACK mechanism
	// AckTask marks a task as successfully acknowledged and removes it
	AckTask(id string) error
	// GetAckTimeoutIDs returns task IDs whose ack deadline has passed
	GetAckTimeoutIDs(now int64) ([]string, error)
	// SetAckDeadline registers the ack deadline for a task (called after DelayToUnack)
	SetAckDeadline(id string, deadline int64) error
}
