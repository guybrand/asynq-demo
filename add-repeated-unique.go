package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	//set up all jobs for next 24 hours in rounded hours
	for i := 0; i < 24; i++ {
		sched := time.Now().Add(time.Duration(i) * time.Hour).Truncate(time.Hour)
		sayHelloOnTime(client, sched)
	}
}

func sayHelloOnTime(client *asynq.Client, runAt time.Time) {

	payload, err := json.Marshal(HelloPayload{
		Message: "Hello " + runAt.Format(time.DateTime),
	})
	if err != nil {
		log.Fatal(err)
	}
	task := asynq.NewTask("demo:hello", payload)

	taskID := fmt.Sprintf("hourlyFor:%s", runAt.Format("2006010215"))
	info, err := client.Enqueue(
		task,
		asynq.ProcessAt(runAt),
		asynq.TaskID(taskID),
	)

	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			fmt.Printf("taskID %s is already scheduled ", taskID)
		} else {
			fmt.Printf("Enqueue error: %s\n", err)
		}
	} else {

		fmt.Printf("ID: %s\n", info.ID)
		fmt.Printf("Queue: %s\n", info.Queue)
		fmt.Printf("Type: %s\n", task.Type())
	}

}
