package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

func ConnectConsumer(brokersURL []string) (sarama.Consumer, error) {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokersURL, config)

	if err != nil {
		return nil, err
	}

	return conn, err
}

func runConsumer() {
	worker, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		//		fmt.Printf("Error %v\n", err)
		panic(err)
	}

	consumer, err := worker.ConsumePartition("comments", 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	messageCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				messageCount++
				fmt.Printf("Received message count %d: | Topic(%s) | Message(%s) ", messageCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interuption detected...")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Printf("Processed (%d) messages", messageCount)

	if err := worker.Close(); err != nil {
		panic(err)
	}
}
