package main

import (
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

func enqueue(client *asynq.Client, queue, taskType string, payload any) {
	b, _ := json.Marshal(payload)

	_, err := client.Enqueue(
		asynq.NewTask(taskType, b),
		asynq.Queue(queue),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Queued %-22s -> queue=%s", taskType, queue)
}

type EmailPayload struct {
	OrderID int
	To      string
	Subject string
}

type EDIPayload struct {
	OrderID int
	Vendor  string
	Product string
}

type DeliveryPayload struct {
	OrderID   int
	Carrier   string
	Recipient string
	Address   string
}

func main() {

	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()

	orderID := 1001

	enqueue(client,
		"email",
		"email:order_received",
		EmailPayload{
			OrderID: orderID,
			To:      "Sher.lock@doyle.com",
			Subject: "We received your order!",
		},
	)

	enqueue(client,
		"edi",
		"edi:create_order",
		EDIPayload{
			OrderID: orderID,
			Vendor:  "BestPhones.com",
			Product: "Phone",
		},
	)

	enqueue(client,
		"edi",
		"edi:create_order",
		EDIPayload{
			OrderID: orderID,
			Vendor:  "Gadget Accessories LTD",
			Product: "Screen Protector",
		},
	)

	enqueue(client,
		"delivery",
		"delivery:create",
		DeliveryPayload{
			OrderID:   orderID,
			Carrier:   "DHL",
			Recipient: "S.Holmes",
			Address:   "221B Baker Street, London",
		},
	)

	log.Println("Done.")
}
