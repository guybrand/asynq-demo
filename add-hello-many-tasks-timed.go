package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

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
	for i := 0; i < 99; i++ {
		go sayHello(client, i)
	}
	time.Sleep(time.Second) //so goroutine would complete with no waitGrouping...
}

func sayHello(client *asynq.Client, id int) {
	payload, err := json.Marshal(HelloPayload{
		Message: "Hello " + strconv.Itoa(id),
	})
	if err != nil {
		log.Fatal(err)
	}

	task := asynq.NewTask("demo:hello", payload)

	runAt := time.Now().Add(time.Minute * time.Duration(id))
	info, err := client.Enqueue(
		task,
		asynq.ProcessAt(runAt),
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Task enqueued:\n")
	fmt.Printf("ID: %s\n", info.ID)
	fmt.Printf("Queue: %s\n", info.Queue)
	fmt.Printf("Type: %s\n", task.Type())
}
