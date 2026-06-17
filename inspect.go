package main

import (
	"fmt"
	"log"

	"github.com/hibiken/asynq"
)

func main() {
	inspector := asynq.NewInspector(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
	)

	taskType := "demo:hello"

	tasks, err := inspector.ListScheduledTasks("default", asynq.PageSize(100))
	if err != nil {
		log.Fatal(err)
	}

	for _, t := range tasks {
		if t.Type == taskType {
			fmt.Printf(
				"ID=%s Type=%s State=%s Payload=%s\n",
				t.ID,
				t.Type,
				t.State,
				string(t.Payload),
			)
		}
	}
}
