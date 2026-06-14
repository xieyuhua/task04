package queue

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

const maxBodySize = 1 << 20 // 1MB

func ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/create", createHandler)
	mux.HandleFunc("/delete", deleteHandler)
	mux.HandleFunc("/query", queryHandler)
	mux.HandleFunc("/ack", ackHandler)
	server := http.Server{Addr: addr, Handler: mux}
	return server.ListenAndServe()
}

type createRequest struct {
	Topic string `json:"topic"`
	// Delay is the number of seconds that should elapse before the task execute
	Delay    int64  `json:"delay"`
	Retry    int    `json:"retry"`
	// RetryStrategy: "fixed" or "exponential", default "exponential"
	RetryStrategy string `json:"retry_strategy"`
	// RetryInterval is the base retry interval in seconds, default 10
	RetryInterval int64  `json:"retry_interval"`
	Callback      string `json:"callback"`
	Content       string `json:"content"`
}

type createResponse struct {
	ID string `json:"id"`
}

func createHandler(w http.ResponseWriter, r *http.Request) {
	var request createRequest
	if ok := decode(w, r, &request); !ok {
		return
	}

	// default retry strategy
	strategy := RetryExponential
	if request.RetryStrategy == "fixed" {
		strategy = RetryFixed
	}
	retryInterval := request.RetryInterval
	if retryInterval <= 0 {
		retryInterval = int64(RetryInterval)
	}

	task := &Task{
		ID:            uuid.New(),
		Topic:         request.Topic,
		ExecuteTime:   time.Now().Unix() + request.Delay,
		MaxRetry:      request.Retry,
		RetryStrategy: strategy,
		RetryInterval: retryInterval,
		Callback:      request.Callback,
		Content:       request.Content,
		CreatTime:     time.Now().Unix(),
	}
	err := backend.CreateTask(task)
	if err != nil {
		log.WithError(err).Error("create task fail")
		w.WriteHeader(500)
		return
	}
	response := createResponse{ID: task.ID}
	write(w, response)
}

type deleteRequest struct {
	ID string `json:"id"`
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	var request deleteRequest
	if ok := decode(w, r, &request); !ok {
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}
	err := backend.DeleteTask(request.ID)
	if err != nil {
		log.WithError(err).Error("delete task fail")
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
}

type queryRequest struct {
	ID string `json:"id"`
}

type queryResponse struct {
	ID            string `json:"id"`
	Topic         string `json:"topic"`
	ExecuteTime   int64  `json:"execute_time"`
	MaxRetry      int    `json:"max_retry"`
	HasRetry      int    `json:"has_retry"`
	RetryStrategy int    `json:"retry_strategy"`
	RetryInterval int64  `json:"retry_interval"`
	Callback      string `json:"callback"`
	Content       string `json:"content"`
	CreatTime     int64  `json:"creat_time"`
	AckStatus     int    `json:"ack_status"`
	AckDeadline   int64  `json:"ack_deadline"`
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	var request queryRequest
	if ok := decode(w, r, &request); !ok {
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}
	task, err := backend.GetTask(request.ID)
	if err != nil {
		if err == ErrTaskNotFound {
			w.WriteHeader(404)
			return
		}
		log.WithError(err).Error("get task fail")
		w.WriteHeader(500)
		return
	}
	response := queryResponse{
		ID:            task.ID,
		Topic:         task.Topic,
		ExecuteTime:   task.ExecuteTime,
		MaxRetry:      task.MaxRetry,
		HasRetry:      task.HasRetry,
		RetryStrategy: int(task.RetryStrategy),
		RetryInterval: task.RetryInterval,
		Callback:      task.Callback,
		Content:       task.Content,
		CreatTime:     task.CreatTime,
		AckStatus:     int(task.AckStatus),
		AckDeadline:   task.AckDeadline,
	}
	write(w, response)
}

// ackRequest 是 ACK 请求
type ackRequest struct {
	ID string `json:"id"`
}

// ackHandler 处理任务确认请求
// 调用方在成功处理任务后，需要调用此接口确认任务
// 超时未确认的任务将被重新投递
func ackHandler(w http.ResponseWriter, r *http.Request) {
	var request ackRequest
	if ok := decode(w, r, &request); !ok {
		return
	}
	if request.ID == "" {
		w.WriteHeader(400)
		return
	}

	// 先检查任务是否存在
	task, err := backend.GetTask(request.ID)
	if err != nil {
		if err == ErrTaskNotFound {
			w.WriteHeader(404)
			return
		}
		log.WithError(err).Error("get task for ack fail")
		w.WriteHeader(500)
		return
	}

	// 检查任务状态，只有等待确认的任务才能 ACK
	if task.AckStatus != AckPending {
		w.WriteHeader(409) // Conflict: 任务状态不允许 ACK
		return
	}

	err = backend.AckTask(request.ID)
	if err != nil {
		log.WithError(err).Error("ack task fail")
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
}

func decode(w http.ResponseWriter, r *http.Request, obj interface{}) bool {
	if r.Method != http.MethodPost {
		w.WriteHeader(400)
		return false
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
	if err != nil {
		log.WithError(err).Error("io read from frontend fail")
		w.WriteHeader(500)
		return false
	}
	err = json.Unmarshal(data, obj)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		w.WriteHeader(400)
		return false
	}
	return true
}

func write(w http.ResponseWriter, obj interface{}) {
	respData, err := json.Marshal(obj)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		w.WriteHeader(500)
		return
	}
	w.Write(respData)
}
