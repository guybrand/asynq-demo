package main

import (
	"fmt"
	"log"
	"os"

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
			if len(os.Args) > 1 && t.ID == os.Args[1] {
				if err := inspector.DeleteTask("default", t.ID); err != nil {
					fmt.Printf("Error deleting task %s, %s\n", t.ID, err)
				} else {
					fmt.Printf("Deleted task %s\n", t.ID)
				}
			}
		}
	}
}
