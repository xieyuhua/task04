package main

import (
	"flag"
	"fmt"
	"os"

	"httpqueue/queue"
	log "github.com/sirupsen/logrus"
)

var (
	redisURL    = flag.String("redis", "redis://127.0.0.1:6379/0", "redis address")
	rabbitmqURL = flag.String("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", "rabbitmq address")
	backendType = flag.String("backend", "redis", "storage backend: redis or rabbitmq")
	address     = flag.String("address", ":8080", "serve listen address")
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	flag.Parse()
}

func main() {
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

	queue.RunWorker()
	log.Infof("server listen on :%v [backend=%s]", *address, *backendType)
	err = queue.ListenAndServe(*address)
	if err != nil {
		log.Fatal(err)
	}
}
