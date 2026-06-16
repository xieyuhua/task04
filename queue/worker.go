package queue

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// taskChan 是任务分发通道，worker 池从中消费
var taskChan chan string

// once 确保 worker 池只初始化一次
var once sync.Once

// RunWorker starts all background workers
func RunWorker() {
	once.Do(func() {
		taskChan = make(chan string, WorkerPoolSize*2)
		// 启动 worker 池
		for i := 0; i < WorkerPoolSize; i++ {
			go worker()
		}
	})
	go delayWorker()
	go unackWorker()
	go errorWorker()
	go ackCheckWorker()
}

// worker 从 taskChan 消费任务并执行回调
func worker() {
	for id := range taskChan {
		callback(id)
	}
}

func delayWorker() {
	// If backend supports push-based delay (e.g. RabbitMQ DLX), skip polling
	if rb, ok := backend.(*RabbitMQBackend); ok {
		rb.ConsumeDelayQueue()
		return
	}
	ticker := time.NewTicker(DelayWorkerInterval)
	for range ticker.C {
		begin := time.Now().Add(-time.Duration(TaskTTL) * time.Second).Unix()
		end := time.Now().Add(-CallbackTTR).Unix()
		ids, err := backend.GetReadyIDs(DelayBucket, begin, end)
		if err != nil {
			log.WithError(err).Error("get tasks fail")
			continue
		}
		for _, id := range ids {
			select {
			case taskChan <- id:
			default:
				// worker 池已满，跳过本轮，避免阻塞
				log.Warn("worker pool full, skip task: ", id)
			}
		}
	}
}

func unackWorker() {
	ticker := time.NewTicker(UnackWorkerInterval)
	for range ticker.C {
		begin := time.Now().Add(-time.Duration(TaskTTL)).Unix()
		end := time.Now().Unix()
		ids, err := backend.GetReadyIDs(UnackBucket, begin, end)
		if err != nil {
			log.WithError(err).Error("get unack tasks fail")
			continue
		}
		for _, id := range ids {
			if _, err := backend.UnackToDelay(id, time.Now().Unix()); err != nil {
				log.WithError(err).WithField("id", id).Error("unack to delay fail")
			}
		}
	}
}

func errorWorker() {
	ticker := time.NewTicker(ErrorWorkerInterval)
	for range ticker.C {
		begin := time.Now().Add(-time.Duration(TaskTTL)).Unix()
		end := time.Now().Unix()
		ids, err := backend.GetReadyIDs(ErrorBucket, begin, end)
		if err != nil {
			log.WithError(err).Error("get error tasks fail")
			continue
		}
		for _, id := range ids {
			if _, err := backend.ErrorToDelay(id, time.Now().Unix()); err != nil {
				log.WithError(err).WithField("id", id).Error("error to delay fail")
			}
		}
	}
}

// ackCheckWorker 定期检查 unack 桶中超时未确认的任务，将其重入延迟队列
func ackCheckWorker() {
	ticker := time.NewTicker(AckCheckInterval)
	for range ticker.C {
		now := time.Now().Unix()
		ids, err := backend.GetAckTimeoutIDs(now)
		if err != nil {
			log.WithError(err).Error("get ack timeout tasks fail")
			continue
		}
		for _, id := range ids {
			task, err := backend.GetTask(id)
			if err != nil {
				if err == ErrTaskNotFound {
					_ = backend.DeleteTask(id)
				}
				continue
			}
			task.HasRetry++
			if task.HasRetry > task.MaxRetry {
				_ = backend.DeleteTask(id)
				log.WithField("id", id).Warn("ack timeout, max retry exceeded, discard task")
				continue
			}
			task.AckStatus = AckExpired
			_ = backend.UpdateTask(task)
			score := time.Now().Unix() + task.NextRetryDelay()
			if _, err := backend.UnackToDelay(id, score); err != nil {
				log.WithError(err).WithField("id", id).Error("ack timeout, unack to delay fail")
			} else {
				log.WithField("id", id).Info("ack timeout, requeue task")
			}
		}
	}
}

func callback(id string) {
	task, err := backend.GetTask(id)
	if err != nil {
		if err == ErrTaskNotFound {
			_ = backend.DeleteTask(id)
		}
		return
	}
	got, err := backend.DelayToUnack(id, time.Now().Unix())
	if err != nil {
		log.WithError(err).Error("transfer from delay to unack fail")
		return
	}
	if !got {
		return
	}

	// 设置 ACK 截止时间，将任务状态标记为等待确认
	task.AckDeadline = time.Now().Add(AckTimeout).Unix()
	task.AckStatus = AckPending
	_ = backend.UpdateTask(task)
	// 在后端注册 ACK 截止时间，用于超时检测
	_ = backend.SetAckDeadline(id, task.AckDeadline)

	code, err := post(task)
	if err != nil {
		goto retry
	}
	if code == CodeSuccess || code == CodeSuccess200 || code == CodeSuccess0 {
		// 回调 HTTP 成功（code=100/200/0），自动 ACK 确认
		_ = backend.AckTask(id)
		log.WithField("id", id).Infof("auto ack success, code=%d", code)
		return
	}
	log.Errorf("backend fail, code is %v", code)

retry:
	task.HasRetry++
	if task.HasRetry > task.MaxRetry {
		_ = backend.DeleteTask(id)
		return
	}
	task.AckStatus = AckExpired
	_ = backend.UpdateTask(task)
	score := time.Now().Unix() + task.NextRetryDelay()
	err = backend.UnackToError(id, score)
	if err != nil {
		log.WithError(err).Error("transfer from unack to error bucket fail")
		return
	}
}
