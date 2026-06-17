package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

type HelloPayload struct {
	Message string `json:"message"`
}

func HandleHelloTask(ctx context.Context, task *asynq.Task) error {
	var payload HelloPayload
	//time.Sleep(1 * time.Millisecond)
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	fmt.Printf("Processing task: %s\n", payload.Message)

	return nil
}

func main() {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
		asynq.Config{
			Concurrency: 5,
			//DelayedTaskCheckInterval: time.Second,
		},
	)

	mux := asynq.NewServeMux()

	mux.HandleFunc("demo:hello", HandleHelloTask)

	log.Println("Worker started...")

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
