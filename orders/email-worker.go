package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

type EmailPayload struct {
	OrderID int
	To      string
	Subject string
}

func HandleEmail(ctx context.Context, t *asynq.Task) error {

	var p EmailPayload
	json.Unmarshal(t.Payload(), &p)

	log.Printf("[EMAIL] %s -> %s", p.Subject, p.To)

	return nil
}

func main() {

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 2,
			Queues: map[string]int{
				"email": 1,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("email:order_received", HandleEmail)

	log.Println("Email worker started")

	log.Fatal(srv.Run(mux))
}
