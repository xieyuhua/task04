package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"httpqueue/queue"
	log "github.com/sirupsen/logrus"
)

var (
	redisURL    = flag.String("redis", "redis://:147258369@127.0.0.1:6379/3", "redis://:[password]@[host]:[port]/[database]")
	rabbitmqURL = flag.String("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", "rabbitmq address")
	backendType = flag.String("backend", "redis", "storage backend: redis or rabbitmq")
	address     = flag.String("address", ":2345", "serve listen address")
	logDir      = flag.String("logdir", "", "log file directory, empty means stdout only")
	logMaxDays  = flag.Int("logdays", 7, "max days to retain log files")
	workerPool  = flag.Int("pool", 64, "concurrent worker pool size")
	batchSize   = flag.Int("batch", 50, "batch task fetch count per poll")
	callbackTTR   = flag.Int("ctt", 3, "callback http timeout in seconds")
	ackTimeout    = flag.Int("acktimeout", 30, "ack timeout in seconds, unacked tasks will be requeued")
	maxRetryCap   = flag.Int64("maxretrycap", 86400, "max retry delay cap in seconds for exponential backoff")
	logLevel      = flag.String("loglevel", "info", "log level: panic, fatal, error, warn, info, debug, trace")
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
}

func main() {
	flag.Parse()

	// 设置日志级别
	lvl, parseErr := log.ParseLevel(*logLevel)
	if parseErr != nil {
		fmt.Fprintf(os.Stderr, "invalid log level: %s, use debug/info/warn/error\n", *logLevel)
		os.Exit(1)
	}
	log.SetLevel(lvl)

	// 初始化按天分割的日志文件
	queue.InitLogger(*logDir, *logMaxDays)

	// 应用运行时参数
	queue.ApplyConfig(queue.RuntimeConfig{
		WorkerPoolSize:   *workerPool,
		BatchSize:        *batchSize,
		CallbackTTR:      time.Duration(*callbackTTR) * time.Second,
		AckTimeout:       time.Duration(*ackTimeout) * time.Second,
		MaxRetryDelayCap: *maxRetryCap,
	})

	var err error
	switch *backendType {
	case "redis":
		err = queue.InitBackend(queue.NewRedisBackend(*redisURL))
	case "rabbitmq":
		err = queue.InitBackend(queue.NewRabbitMQBackend(*rabbitmqURL))
	default:
		fmt.Fprintf(os.Stderr, "unknown backend: %s, use redis or rabbitmq\n", *backendType)
		os.Exit(1)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer queue.CloseBackend()

	// 启动后台 worker
	queue.RunWorker()

	// 监听系统信号，实现优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// HTTP server 在 goroutine 中运行
	go func() {
		log.Infof("server listen on :%v [backend=%s]", *address, *backendType)
		if err := queue.ListenAndServe(*address); err != nil {
			log.Fatal(err)
		}
	}()

	// 等待关闭信号
	sig := <-sigCh
	log.Infof("received signal %v, shutting down gracefully...", sig)

	// 1. 标记关闭，停止接收新任务
	queue.Shutdown()
	log.Info("waiting for workers to drain...")

	// 2. 等待进行中的任务完成（按 CallbackTTR × 2 估算最大等待时间）
	waitTime := time.Duration(*callbackTTR) * time.Second * 2
	if waitTime < 10*time.Second {
		waitTime = 10 * time.Second
	}
	log.Infof("draining for up to %v...", waitTime)
	time.Sleep(waitTime)

	log.Info("shutdown complete")
}
