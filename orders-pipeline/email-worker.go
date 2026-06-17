package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

const (
	QueueOrdering = "ordering"
	QueueEmail    = "email"
)

type EmailPayload struct {
	OrderID int    `json:"order_id"`
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type StepCompleted struct {
	OrderID int    `json:"order_id"`
	Step    string `json:"step"`
}

var client = asynq.NewClient(asynq.RedisClientOpt{
	Addr: "127.0.0.1:6379",
})

func enqueue(taskType string, payload any) {

	b, _ := json.Marshal(payload)

	_, err := client.Enqueue(
		asynq.NewTask(taskType, b),
		asynq.Queue(QueueOrdering),
	)

	if err != nil {
		log.Println(err)
	}
}

func HandleEmail(ctx context.Context, t *asynq.Task) error {

	var p EmailPayload

	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}

	log.Printf("[EMAIL] %s -> %s", p.Subject, p.To)

	// Only notify Ordering after the first email.
	if t.Type() == "email:order_received" {

		enqueue(
			"ordering:step_completed",
			StepCompleted{
				OrderID: p.OrderID,
				Step:    "email",
			},
		)
	}

	return nil
}

func main() {

	defer client.Close()

	server := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
		asynq.Config{
			Concurrency: 2,
			Queues: map[string]int{
				QueueEmail: 1,
			},
		},
	)

	mux := asynq.NewServeMux()

	mux.HandleFunc("email:order_received", HandleEmail)
	mux.HandleFunc("email:order_on_the_way", HandleEmail)

	log.Println("Email worker started")

	log.Fatal(server.Run(mux))
}
