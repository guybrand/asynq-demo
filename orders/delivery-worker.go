package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

type DeliveryPayload struct {
	OrderID   int
	Carrier   string
	Recipient string
	Address   string
}

func HandleDelivery(ctx context.Context, t *asynq.Task) error {

	var p DeliveryPayload
	json.Unmarshal(t.Payload(), &p)

	log.Printf(
		"[DELIVERY] Shipment for %s, %s created with %s",
		p.Recipient,
		p.Address,
		p.Carrier,
	)

	return nil
}

func main() {

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 2,
			Queues: map[string]int{
				"delivery": 1,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("delivery:create", HandleDelivery)

	log.Println("Delivery worker started")

	log.Fatal(srv.Run(mux))
}
