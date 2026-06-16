package queue

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// taskChan 是任务分发通道，worker 池从中消费
var taskChan chan string

// RunWorker starts all background workers.
// 会读取 storage.go 中的 workerStopCh 用于优雅关闭。
func RunWorker() {
	// 确保只初始化一次
	if taskChan == nil {
		taskChan = make(chan string, WorkerPoolSize*2)
		for i := 0; i < WorkerPoolSize; i++ {
			go worker()
		}
	}
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
	defer ticker.Stop()

	cleanTicker := time.NewTicker(10 * time.Minute)
	defer cleanTicker.Stop()

	for {
		select {
		case <-workerStopCh:
			return
		case <-ticker.C:
			// 使用 0（-inf 语义）作为下限，确保积压任务不会因"等太久"而被跳过
			end := time.Now().Add(-CallbackTTR).Unix()
			ids, err := backend.GetReadyIDs(DelayBucket, 0, end)
			if err != nil {
				log.WithError(err).Error("get tasks fail")
				continue
			}
			for _, id := range ids {
				select {
				case taskChan <- id:
				default:
					log.Warn("worker pool full, skip task: ", id)
				}
			}
		case <-cleanTicker.C:
			if rb, ok := backend.(*RedisBackend); ok {
				rb.CleanExpiredBuckets()
			}
		}
	}
}

func unackWorker() {
	ticker := time.NewTicker(UnackWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-workerStopCh:
			return
		case <-ticker.C:
			end := time.Now().Unix()
			ids, err := backend.GetReadyIDs(UnackBucket, 0, end)
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
}

func errorWorker() {
	ticker := time.NewTicker(ErrorWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-workerStopCh:
			return
		case <-ticker.C:
			end := time.Now().Unix()
			ids, err := backend.GetReadyIDs(ErrorBucket, 0, end)
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
}

// ackCheckWorker 定期检查 ack_deadline 桶中超时未确认的任务，将其重入延迟队列
func ackCheckWorker() {
	ticker := time.NewTicker(AckCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-workerStopCh:
			return
		case <-ticker.C:
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
				if err := backend.UpdateTask(task); err != nil {
					log.WithError(err).WithField("id", id).Error("update task ack timeout fail")
				}
				score := time.Now().Unix() + task.NextRetryDelay()
				if _, err := backend.UnackToDelay(id, score); err != nil {
					log.WithError(err).WithField("id", id).Error("ack timeout, unack to delay fail")
				} else {
					log.WithField("id", id).Info("ack timeout, requeue task")
				}
			}
		}
	}
}

// ────────────── callback 流程（无 goto 重构版）──────────────

func callback(id string) {
	startTime := time.Now()

	task := fetchAndPrepareTask(id)
	if task == nil {
		return
	}

	code, err := post(task)
	duration := time.Since(startTime).Milliseconds()
	if err != nil {
		log.WithField("id", id).WithField("duration_ms", duration).Error("http post fail")
		retryTask(task, id)
		return
	}
	if code == CodeSuccess || code == CodeSuccess200 || code == CodeSuccess0 {
		_ = backend.AckTask(id)
		log.WithField("id", id).WithField("duration_ms", duration).Infof("auto ack success, code=%d", code)
		return
	}
	log.WithField("id", id).WithField("duration_ms", duration).Errorf("backend fail, code=%d", code)
	retryTask(task, id)
}

// fetchAndPrepareTask 获取任务并完成桶迁移和 ACK 准备。
// Redis 后端：使用 Lua 原子操作一次性完成 GET + delay→unack + ZADD ack_deadline。
// 非 Redis 后端：分步执行 GetTask + DelayToUnack + SetAckDeadline。
func fetchAndPrepareTask(id string) *Task {
	if rb, ok := backend.(*RedisBackend); ok {
		ackDeadline := time.Now().Add(AckTimeout).Unix()
		task, err := rb.DelayToUnackWithData(id, time.Now().Unix(), ackDeadline)
		if err != nil {
			if err == ErrTaskNotFound {
				_ = backend.DeleteTask(id)
			}
			return nil
		}
		// Lua 已原子完成桶迁移 + ack_deadline 注册，此处只需更新任务数据
		task.AckDeadline = ackDeadline
		task.AckStatus = AckPending
		if err := backend.UpdateTask(task); err != nil {
			log.WithError(err).WithField("id", id).Error("update task ack deadline fail")
		}
		return task
	}

	// 非 Redis 后端路径
	task, err := backend.GetTask(id)
	if err != nil {
		if err == ErrTaskNotFound {
			_ = backend.DeleteTask(id)
		}
		return nil
	}
	got, err := backend.DelayToUnack(id, time.Now().Unix())
	if err != nil {
		log.WithError(err).Error("transfer from delay to unack fail")
		return nil
	}
	if !got {
		return nil
	}

	task.AckDeadline = time.Now().Add(AckTimeout).Unix()
	task.AckStatus = AckPending
	if err := backend.UpdateTask(task); err != nil {
		log.WithError(err).WithField("id", id).Error("update task ack deadline fail")
	}
	if err := backend.SetAckDeadline(id, task.AckDeadline); err != nil {
		log.WithError(err).WithField("id", id).Error("set ack deadline fail")
	}
	return task
}

// retryTask 处理任务重试：递增重试次数，超过上限则删除，否则移入 error 桶等待重试。
func retryTask(task *Task, id string) {
	task.HasRetry++
	if task.HasRetry > task.MaxRetry {
		_ = backend.DeleteTask(id)
		return
	}
	task.AckStatus = AckExpired
	if err := backend.UpdateTask(task); err != nil {
		log.WithError(err).WithField("id", id).Error("update task retry fail")
	}
	score := time.Now().Unix() + task.NextRetryDelay()
	if err := backend.UnackToError(id, score); err != nil {
		log.WithError(err).Error("transfer from unack to error bucket fail")
	}
}
