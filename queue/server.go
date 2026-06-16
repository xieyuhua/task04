package queue

import (
	"encoding/json"
	"fmt"
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
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/metrics", metricsHandler)
	server := http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
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

	// ── 参数校验 ──
	if request.Callback == "" {
		http.Error(w, "callback URL is required", 400)
		return
	}
	if request.Delay < 0 {
		http.Error(w, "delay must be >= 0", 400)
		return
	}
	if request.Delay > 86400*30 {
		http.Error(w, "delay too large (max 30 days)", 400)
		return
	}
	if request.Retry < 0 || request.Retry > 100 {
		http.Error(w, "retry must be 0-100", 400)
		return
	}
	if len(request.Content) > 1<<20 { // 1MB 上限
		http.Error(w, "content too large (max 1MB)", 400)
		return
	}
	if request.RetryStrategy != "" && request.RetryStrategy != "fixed" && request.RetryStrategy != "exponential" {
		http.Error(w, "retry_strategy must be 'fixed' or 'exponential'", 400)
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

// ────────────── 健康检查 & 监控 ──────────────

// healthHandler 返回服务健康状态
func healthHandler(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	if backend == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(503)
		json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "reason": "no backend"})
		return
	}
	if mp, ok := backend.(MetricsProvider); ok {
		if err := mp.Ping(); err != nil {
			status = "unhealthy"
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(503)
			json.NewEncoder(w).Encode(map[string]string{"status": status, "reason": err.Error()})
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]string{"status": status, "shutting_down": fmt.Sprintf("%t", IsShuttingDown())})
}

// metricsHandler 返回各桶任务数量统计
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if backend == nil {
		w.WriteHeader(503)
		return
	}
	metrics := GetQueueLengths(backend)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(metrics)
}
