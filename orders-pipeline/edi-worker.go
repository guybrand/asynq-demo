package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

const (
	QueueOrdering = "ordering"
	QueueEDI      = "edi"
)

type EDIPayload struct {
	OrderID int    `json:"order_id"`
	Product string `json:"product"`
	Vendor  string `json:"vendor"`
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

func HandleEDI(ctx context.Context, t *asynq.Task) error {

	var p EDIPayload

	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}

	log.Printf("[EDI] Ordering %s from %s",
		p.Product,
		p.Vendor)

	time.Sleep(2 * time.Second)

	log.Printf("[EDI] Finished %s", p.Product)

	step := "phone"

	if p.Product == "Screen Protector" {
		step = "protector"
	}

	enqueue(
		"ordering:step_completed",
		StepCompleted{
			OrderID: p.OrderID,
			Step:    step,
		},
	)

	return nil
}

func main() {

	defer client.Close()

	server := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
		asynq.Config{
			Concurrency: 1,
			Queues: map[string]int{
				QueueEDI: 1,
			},
		},
	)

	mux := asynq.NewServeMux()

	mux.HandleFunc("edi:create_order", HandleEDI)

	log.Println("EDI worker started")

	log.Fatal(server.Run(mux))
}
