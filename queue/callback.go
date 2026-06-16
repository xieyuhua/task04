package queue

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

var httpClient = &http.Client{
	Timeout: CallbackTTR,
	Transport: &http.Transport{
		MaxIdleConnsPerHost: MaxIdleConnsPerHost,
		MaxIdleConns:        MaxIdleConns,
		IdleConnTimeout:     IdleConnTimeout,
	},
}

type callbackRequest struct {
	ID      string `json:"id"`
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

type callbackResponse struct {
	Code int `json:"code"`
}

const (
	CodeSuccess        = 100
	CodeSuccess200     = 200
	CodeSuccess0       = 0
	CodeTooManyRequest = 101
)

func post(task *Task) (int, error) {
	startTime := time.Now()

	request := callbackRequest{
		ID:      task.ID,
		Topic:   task.Topic,
		Content: task.Content,
	}
	data, err := json.Marshal(request)
	if err != nil {
		log.WithError(err).Error("json marshal fail")
		return 0, err
	}

	content := bytes.NewBuffer(data)
	resp, err := httpClient.Post(task.Callback, "application/json", content)
	if err != nil {
		log.WithError(err).Error("http post fail")
		return 0, err
	}
	defer resp.Body.Close()

	result, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16)) // 限制 64KB，防止 OOM
	duration := time.Since(startTime).Milliseconds()
	log.Debugf("task.id %s => http_status=%d duration_ms=%d result=%s", task.ID, resp.StatusCode, duration, result)
	if err != nil {
		log.WithError(err).Error("io read from backend fail")
		return 0, err
	}
	var response callbackResponse
	err = json.Unmarshal(result, &response)
	if err != nil {
		log.WithError(err).Error("json unmarshal fail")
		return 0, err
	}
	
	
	return response.Code, nil
}
