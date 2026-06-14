package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

// RabbitMQBackend implements Backend using RabbitMQ with DLX for delay
type RabbitMQBackend struct {
	url     string
	conn    *amqp.Connection
	mu      sync.Mutex
	taskMap sync.Map // 并发安全的任务存储
	ackMap  sync.Map // task ack deadline tracking: id -> int64
	errMap  sync.Map // task error retry time tracking: id -> int64
	delayCh *amqp.Channel
	unackCh *amqp.Channel
	errorCh *amqp.Channel
	// 统计计数器
	taskCount int64
}

// RabbitMQ queue/exchange names
const (
	delayExchange   = "later.delay.exchange"
	delayQueue      = "later.delay.queue"
	delayRoutingKey = "later.delay"

	unackExchange   = "later.unack.exchange"
	unackQueue      = "later.unack.queue"
	unackRoutingKey = "later.unack"

	errorExchange   = "later.error.exchange"
	errorQueue      = "later.error.queue"
	errorRoutingKey = "later.error"
)

// NewRabbitMQBackend creates a RabbitMQBackend with the given AMQP URL
func NewRabbitMQBackend(url string) *RabbitMQBackend {
	return &RabbitMQBackend{
		url: url,
	}
}

func (b *RabbitMQBackend) Init() error {
	var err error
	b.conn, err = amqp.Dial(b.url)
	if err != nil {
		return err
	}

	b.delayCh, err = b.setupChannel(delayExchange, "direct", delayQueue, delayRoutingKey, unackExchange, unackRoutingKey)
	if err != nil {
		return err
	}

	b.unackCh, err = b.setupChannel(unackExchange, "direct", unackQueue, unackRoutingKey, delayExchange, delayRoutingKey)
	if err != nil {
		return err
	}

	b.errorCh, err = b.setupChannel(errorExchange, "direct", errorQueue, errorRoutingKey, delayExchange, delayRoutingKey)
	if err != nil {
		return err
	}

	return nil
}

func (b *RabbitMQBackend) setupChannel(exchange, exchangeType, queue, routingKey, dlxExchange, dlxRoutingKey string) (*amqp.Channel, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(exchange, exchangeType, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    dlxExchange,
		"x-dead-letter-routing-key": dlxRoutingKey,
	}
	_, err = ch.QueueDeclare(queue, true, false, false, false, args)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(queue, routingKey, exchange, false, nil)
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (b *RabbitMQBackend) Close() {
	if b.delayCh != nil {
		b.delayCh.Close()
	}
	if b.unackCh != nil {
		b.unackCh.Close()
	}
	if b.errorCh != nil {
		b.errorCh.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

func (b *RabbitMQBackend) CreateTask(task *Task) error {
	b.taskMap.Store(task.ID, task)
	atomic.AddInt64(&b.taskCount, 1)

	delay := task.ExecuteTime - time.Now().Unix()
	if delay < 0 {
		delay = 0
	}

	body, err := json.Marshal(task.ID)
	if err != nil {
		return err
	}

	return b.delayCh.Publish(delayExchange, delayRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Expiration:   formatMs(delay),
	})
}

func (b *RabbitMQBackend) GetTask(id string) (*Task, error) {
	val, ok := b.taskMap.Load(id)
	if !ok {
		return nil, ErrTaskNotFound
	}
	return val.(*Task), nil
}

func (b *RabbitMQBackend) UpdateTask(task *Task) error {
	b.taskMap.Store(task.ID, task)
	return nil
}

func (b *RabbitMQBackend) DeleteTask(id string) error {
	b.taskMap.Delete(id)
	b.ackMap.Delete(id)
	b.errMap.Delete(id)
	atomic.AddInt64(&b.taskCount, -1)
	return nil
}

// GetReadyIDs scans the in-memory maps to find ready task IDs.
// For RabbitMQ backend, bucket semantics are simulated with ackMap/errMap.
func (b *RabbitMQBackend) GetReadyIDs(bucket string, begin, end int64) ([]string, error) {
	var ids []string
	switch bucket {
	case UnackBucket:
		b.ackMap.Range(func(key, value interface{}) bool {
			deadline := value.(int64)
			if deadline >= begin && deadline <= end {
				ids = append(ids, key.(string))
			}
			return true
		})
	case ErrorBucket:
		b.errMap.Range(func(key, value interface{}) bool {
			retryTime := value.(int64)
			if retryTime >= begin && retryTime <= end {
				ids = append(ids, key.(string))
			}
			return true
		})
	}
	return ids, nil
}

func (b *RabbitMQBackend) DelayToUnack(id string, score int64) (bool, error) {
	if _, ok := b.taskMap.Load(id); !ok {
		return false, nil
	}
	b.ackMap.Store(id, score)
	return true, nil
}

func (b *RabbitMQBackend) UnackToDelay(id string, score int64) (bool, error) {
	if _, ok := b.ackMap.Load(id); !ok {
		return false, nil
	}
	b.ackMap.Delete(id)

	body, err := json.Marshal(id)
	if err != nil {
		return true, err
	}
	b.mu.Lock()
	err = b.delayCh.Publish(delayExchange, delayRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	b.mu.Unlock()
	return true, err
}

func (b *RabbitMQBackend) ErrorToDelay(id string, score int64) (bool, error) {
	if _, ok := b.errMap.Load(id); !ok {
		return false, nil
	}
	b.errMap.Delete(id)

	delay := score - time.Now().Unix()
	if delay < 0 {
		delay = 0
	}

	body, err := json.Marshal(id)
	if err != nil {
		return true, err
	}
	b.mu.Lock()
	err = b.delayCh.Publish(delayExchange, delayRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Expiration:   formatMs(delay),
	})
	b.mu.Unlock()
	return true, err
}

func (b *RabbitMQBackend) UnackToError(id string, score int64) error {
	b.ackMap.Delete(id)
	b.errMap.Store(id, score)

	body, err := json.Marshal(id)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.errorCh.Publish(errorExchange, errorRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Expiration:   formatMs(score - time.Now().Unix()),
	})
}

// AckTask 确认任务成功，删除任务及所有跟踪信息
func (b *RabbitMQBackend) AckTask(id string) error {
	b.taskMap.Delete(id)
	b.ackMap.Delete(id)
	b.errMap.Delete(id)
	atomic.AddInt64(&b.taskCount, -1)
	return nil
}

// GetAckTimeoutIDs 返回 ackMap 中 deadline 已过的任务 ID
func (b *RabbitMQBackend) GetAckTimeoutIDs(now int64) ([]string, error) {
	var ids []string
	b.ackMap.Range(func(key, value interface{}) bool {
		deadline := value.(int64)
		if deadline <= now {
			ids = append(ids, key.(string))
		}
		return true
	})
	return ids, nil
}

// SetAckDeadline 设置任务的 ACK 截止时间
func (b *RabbitMQBackend) SetAckDeadline(id string, deadline int64) error {
	b.ackMap.Store(id, deadline)
	return nil
}

// ConsumeDelayQueue starts consuming from the delay queue (RabbitMQ DLX mechanism).
// Messages arrive when the per-message TTL expires, meaning the task is ready.
func (b *RabbitMQBackend) ConsumeDelayQueue() {
	msgs, err := b.delayCh.Consume(delayQueue, "", false, false, false, false, nil)
	if err != nil {
		log.WithError(err).Fatal("failed to consume delay queue")
	}
	for msg := range msgs {
		var taskID string
		if err := json.Unmarshal(msg.Body, &taskID); err != nil {
			log.WithError(err).Error("unmarshal delay message fail")
			msg.Nack(false, false)
			continue
		}
		select {
		case taskChan <- taskID:
			msg.Ack(false)
		default:
			// worker 池满，拒绝并重新入队
			msg.Nack(false, true)
			log.Warn("worker pool full, requeue message: ", taskID)
		}
	}
}

func formatMs(seconds int64) string {
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d000", seconds)
}
