package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

type EDIPayload struct {
	OrderID int
	Vendor  string
	Product string
}

func HandleEDI(ctx context.Context, t *asynq.Task) error {

	var p EDIPayload
	json.Unmarshal(t.Payload(), &p)

	log.Printf("[EDI] Ordering %s from %s",
		p.Product,
		p.Vendor,
	)

	time.Sleep(2 * time.Second)

	log.Printf("[EDI] Finished %s", p.Product)

	return nil
}

func main() {

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 1,
			Queues: map[string]int{
				"edi": 1,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("edi:create_order", HandleEDI)

	log.Println("EDI worker started")

	log.Fatal(srv.Run(mux))
}
