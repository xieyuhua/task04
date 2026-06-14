package queue

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// RunWorker starts all background workers
func RunWorker() {
	go delayWorker()
	go unackWorker()
	go errorWorker()
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
			go callback(id)
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

func callback(id string) {
	task, err := backend.GetTask(id)
	if err != nil {
		if err == ErrTaskNotFound {
			if err = backend.DeleteTask(id); err != nil {
				log.WithError(err).Error("delete task fail")
			}
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
	code, err := post(task)
	if err != nil {
		goto retry
	}
	if code == CodeSuccess {
		if err = backend.DeleteTask(id); err != nil {
			log.WithError(err).Error("delete task fail")
		}
		return
	}
	log.Errorf("backend fail, code is %v", code)

retry:
	task.HasRetry++
	if task.HasRetry > task.MaxRetry {
		if err = backend.DeleteTask(id); err != nil {
			log.WithError(err).Error("delete task fail")
		}
		return
	}
	err = backend.UpdateTask(task)
	if err != nil {
		log.WithError(err).Error("update task fail")
	}
	score := time.Now().Unix() + task.NextRetryDelay()
	err = backend.UnackToError(id, score)
	if err != nil {
		log.WithError(err).Error("transfer from unack to error bucket fail")
		return
	}
}
