package queue

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

// RabbitMQBackend implements Backend using RabbitMQ with DLX for delay
type RabbitMQBackend struct {
	url      string
	conn     *amqp.Connection
	mu       sync.Mutex
	taskMap  map[string]*Task       // in-memory task store (RabbitMQ has no KV)
	ackMap   map[string]int64       // task ack deadline tracking
	errMap   map[string]int64       // task error retry time tracking
	delayCh  *amqp.Channel
	unackCh  *amqp.Channel
	errorCh  *amqp.Channel
}

// RabbitMQ queue/exchange names
const (
	delayExchange    = "later.delay.exchange"
	delayQueue       = "later.delay.queue"
	delayRoutingKey  = "later.delay"

	unackExchange    = "later.unack.exchange"
	unackQueue       = "later.unack.queue"
	unackRoutingKey  = "later.unack"

	errorExchange    = "later.error.exchange"
	errorQueue       = "later.error.queue"
	errorRoutingKey  = "later.error"
)

// NewRabbitMQBackend creates a RabbitMQBackend with the given AMQP URL
func NewRabbitMQBackend(url string) *RabbitMQBackend {
	return &RabbitMQBackend{
		url:     url,
		taskMap: make(map[string]*Task),
		ackMap:  make(map[string]int64),
		errMap:  make(map[string]int64),
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
	b.mu.Lock()
	b.taskMap[task.ID] = task
	b.mu.Unlock()

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
	b.mu.Lock()
	defer b.mu.Unlock()
	task, ok := b.taskMap[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return task, nil
}

func (b *RabbitMQBackend) UpdateTask(task *Task) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.taskMap[task.ID] = task
	return nil
}

func (b *RabbitMQBackend) DeleteTask(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.taskMap, id)
	delete(b.ackMap, id)
	delete(b.errMap, id)
	return nil
}

// GetReadyIDs scans the in-memory maps to find ready task IDs.
// For RabbitMQ backend, bucket semantics are simulated with ackMap/errMap.
func (b *RabbitMQBackend) GetReadyIDs(bucket string, begin, end int64) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ids []string
	switch bucket {
	case UnackBucket:
		for id, deadline := range b.ackMap {
			if deadline >= begin && deadline <= end {
				ids = append(ids, id)
			}
		}
	case ErrorBucket:
		for id, retryTime := range b.errMap {
			if retryTime >= begin && retryTime <= end {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

func (b *RabbitMQBackend) DelayToUnack(id string, score int64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.taskMap[id]; !ok {
		return false, nil
	}
	b.ackMap[id] = score
	return true, nil
}

func (b *RabbitMQBackend) UnackToDelay(id string, score int64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.ackMap[id]; !ok {
		return false, nil
	}
	delete(b.ackMap, id)

	body, err := json.Marshal(id)
	if err != nil {
		return true, err
	}
	err = b.delayCh.Publish(delayExchange, delayRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	return true, err
}

func (b *RabbitMQBackend) ErrorToDelay(id string, score int64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.errMap[id]; !ok {
		return false, nil
	}
	delete(b.errMap, id)

	delay := score - time.Now().Unix()
	if delay < 0 {
		delay = 0
	}

	body, err := json.Marshal(id)
	if err != nil {
		return true, err
	}
	err = b.delayCh.Publish(delayExchange, delayRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Expiration:   formatMs(delay),
	})
	return true, err
}

func (b *RabbitMQBackend) UnackToError(id string, score int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.ackMap, id)
	b.errMap[id] = score

	body, err := json.Marshal(id)
	if err != nil {
		return err
	}
	return b.errorCh.Publish(errorExchange, errorRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
		Expiration:   formatMs(score - time.Now().Unix()),
	})
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
		go callback(taskID)
		msg.Ack(false)
	}
}

func formatMs(seconds int64) string {
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d000", seconds)
}
