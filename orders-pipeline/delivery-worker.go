package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

const (
	QueueOrdering = "ordering"
	QueueDelivery = "delivery"
)

type DeliveryPayload struct {
	OrderID   int    `json:"order_id"`
	Carrier   string `json:"carrier"`
	Recipient string `json:"recipient"`
	Address   string `json:"address"`
}

type StepCompleted struct {
	OrderID int    `json:"order_id"`
	Step    string `json:"step"`
}

var client = asynq.NewClient(asynq.RedisClientOpt{
	Addr: "127.0.0.1:6379",
})

var retries int

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

func HandleDelivery(ctx context.Context, t *asynq.Task) error {

	var p DeliveryPayload

	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return err
	}

	log.Printf(
		"[DELIVERY] Shipment for %s, %s created with %s",
		p.Recipient,
		p.Address,
		p.Carrier,
	)

	enqueue(
		"ordering:step_completed",
		StepCompleted{
			OrderID: p.OrderID,
			Step:    "delivery",
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
			Concurrency: 2,
			Queues: map[string]int{
				QueueDelivery: 1,
			},
		},
	)

	mux := asynq.NewServeMux()

	mux.HandleFunc("delivery:create", HandleDelivery)

	log.Println("Delivery worker started")

	log.Fatal(server.Run(mux))
}
