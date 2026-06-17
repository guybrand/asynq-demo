package main

import (
	"encoding/json"
	"log"

	"github.com/hibiken/asynq"
)

type HelloPayload struct {
	Message string `json:"message"`
}

func main() {
	scheduler := asynq.NewScheduler(
		asynq.RedisClientOpt{
			Addr: "127.0.0.1:6379",
		},
		&asynq.SchedulerOpts{
			Location: nil, // use local timezone
		},
	)

	sayHelloRepeatedly(scheduler)
}

func sayHelloRepeatedly(scheduler *asynq.Scheduler) {
	payload, err := json.Marshal(HelloPayload{
		Message: "5 Sec Tick from Asynq!",
	})
	if err != nil {
		log.Fatal(err)
	}

	task := asynq.NewTask("demo:hello", payload)

	entryID, err := scheduler.Register(
		"@every 5s",
		task,
	)
	if err != nil {
		log.Fatalf("failed to register periodic task: %v", err)
	}

	log.Printf("Registered periodic task. EntryID=%s", entryID)
	log.Println("Task will be enqueued every 5 seconds")

	if err := scheduler.Run(); err != nil {
		log.Fatalf("scheduler stopped: %v", err)
	}
}
