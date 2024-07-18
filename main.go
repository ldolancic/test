package main

import (
	"arkis_test/database"
	"arkis_test/processor"
	"arkis_test/queue"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()
	ctxWithCancel, cancel := context.WithCancel(ctx)

	var inputQueues []processor.Queue
	var outputQueues []processor.Queue

	inputQueues = append(inputQueues, createQueue("input-A"), createQueue("input-B"))
	outputQueues = append(outputQueues, createQueue("output-A"), createQueue("output-B"))

	log.Info("Application is ready to run")

	var workers sync.WaitGroup
	for i, inputQ := range inputQueues {
		outputQ := outputQueues[i]
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := processor.New(inputQ, outputQ, database.D{}).Run(ctxWithCancel)
			if err != nil {
				log.WithError(err)
			}
		}()
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	<-signalCh
	// doing graceful shutdown of the service, waiting for all the workers (processors) to finish their work

	log.Info("Stopping the application")
	cancel()
	workers.Wait()
	log.Info("Application workers are done, exiting")
}

func createQueue(queueName string) processor.Queue {
	inputQueue, err := queue.New(os.Getenv("RABBITMQ_URL"), queueName)
	if err != nil {
		log.WithError(err).Panic(
			fmt.Sprintf("Cannot create queue %s", queueName),
		)
	}
	return inputQueue
}
