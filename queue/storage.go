package queue

import "errors"

// ErrTaskNotFound is returned when a task does not exist
var ErrTaskNotFound = errors.New("task not found")

// backend is the active storage backend (Redis or RabbitMQ)
var backend Backend

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
