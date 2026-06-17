package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

type HelloPayload struct {
	Message string `json:"message"`
}

func main() {
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr: "127.0.0.1:6379",
	})
	defer client.Close()
	sayHello(client)
}

func sayHello(client *asynq.Client) {
	payload, err := json.Marshal(HelloPayload{
		Message: "Hello from Asynq!",
	})
	if err != nil {
		log.Fatal(err)
	}

	task := asynq.NewTask("demo:hello", payload)

	info, err := client.Enqueue(task)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Task enqueued:\n")
	fmt.Printf("ID: %s\n", info.ID)
	fmt.Printf("Queue: %s\n", info.Queue)
	fmt.Printf("Type: %s\n", task.Type())
}
