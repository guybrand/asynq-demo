package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/hibiken/asynq"
)

const (
	QueueOrdering = "ordering"
	QueueEmail    = "email"
	QueueEDI      = "edi"
	QueueDelivery = "delivery"
)

type EmailPayload struct {
	OrderID int    `json:"order_id"`
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type EDIPayload struct {
	OrderID int    `json:"order_id"`
	Product string `json:"product"`
	Vendor  string `json:"vendor"`
}

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

type OrderStatus struct {
	EmailReceived bool
	PhoneOrdered  bool
	ProtectorSent bool //TODO: list of items/vendors...
	DeliveryReady bool
	OnTheWaySent  bool
}

var (
	orders = map[int]*OrderStatus{}
	mu     sync.Mutex
)

func enqueue(client *asynq.Client, queue, taskType string, payload any) {

	b, _ := json.Marshal(payload)

	_, err := client.Enqueue(
		asynq.NewTask(taskType, b),
		asynq.Queue(queue),
	)

	if err != nil {
		log.Printf("enqueue failed: %v", err)
		return
	}

	log.Printf("Queued %-25s -> %s", taskType, queue)
}

func submitOrder(client *asynq.Client) {

	orderID := 1001

	mu.Lock()
	orders[orderID] = &OrderStatus{}
	mu.Unlock()

	log.Printf("[ORDERING] Order %d submitted", orderID)

	enqueue(client,
		QueueEmail,
		"email:order_received",
		EmailPayload{
			OrderID: orderID,
			To:      "Sher.lock@doyle.com",
			Subject: "We received your order!",
		},
	)

	enqueue(client,
		QueueEDI,
		"edi:create_order",
		EDIPayload{
			OrderID: orderID,
			Product: "Phone",
			Vendor:  "BestPhones.com",
		},
	)

	enqueue(client,
		QueueEDI,
		"edi:create_order",
		EDIPayload{
			OrderID: orderID,
			Product: "Screen Protector",
			Vendor:  "Gadget Accessories LTD",
		},
	)

	enqueue(client,
		QueueDelivery,
		"delivery:create",
		DeliveryPayload{
			OrderID:   orderID,
			Carrier:   "DHL",
			Recipient: "S.Holmes",
			Address:   "221B Baker Street, London",
		},
	)
}

func HandleStepCompleted(client *asynq.Client) func(context.Context, *asynq.Task) error {

	return func(ctx context.Context, t *asynq.Task) error {

		var step StepCompleted

		if err := json.Unmarshal(t.Payload(), &step); err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()

		order := orders[step.OrderID]
		if order == nil {
			log.Printf("Unknown order %d", step.OrderID)
			return nil
		}

		log.Printf("[ORDERING] Step completed: %s", step.Step)

		switch step.Step {

		case "email":
			order.EmailReceived = true

		case "phone":
			order.PhoneOrdered = true

		case "protector":
			order.ProtectorSent = true

		case "delivery":
			order.DeliveryReady = true
		}

		if order.EmailReceived &&
			order.PhoneOrdered &&
			order.ProtectorSent &&
			order.DeliveryReady &&
			!order.OnTheWaySent {

			log.Println("[ORDERING] All steps completed!")

			order.OnTheWaySent = true

			enqueue(client,
				QueueEmail,
				"email:order_on_the_way",
				EmailPayload{
					OrderID: step.OrderID,
					To:      "Sher.lock@doyle.com",
					Subject: "Your order is on the way!",
				},
			)
		}

		return nil
	}
}

func main() {

	client := asynq.NewClient(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
	)
	defer client.Close()

	// Simulate a customer placing an order.
	submitOrder(client)

	server := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
		asynq.Config{
			Concurrency: 2,
			Queues: map[string]int{
				QueueOrdering: 1,
			},
		},
	)

	mux := asynq.NewServeMux()

	mux.HandleFunc(
		"ordering:step_completed",
		HandleStepCompleted(client),
	)

	log.Println("[ORDERING] Service started")

	if err := server.Run(mux); err != nil {
		log.Fatal(err)
	}
}
