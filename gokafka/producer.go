package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func createComment(c *fiber.Ctx) {
	comment := new(Comment)

	if err := c.BodyParser(comment); err != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
	}
	commentBytes, err := json.Marshal(comment)
	PushCommentToQueue("comments", commentBytes)

	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": comment,
	})

	if err != nil {
		c.JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	con, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return con, nil
}

func PushCommentToQueue(topic string, message []byte) error {

	brokersURL := []string{"localhost:9092"}

	producer, err := ConnectProducer(brokersURL)

	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic (%s)/ partition (%d)/ offset (%d)\n ", topic, partition, offset)

	return nil
}

func runProducer() {
	app := fiber.New()
	api := app.Group("/app").Group("/v1")
	api.Post("/comment", createComment)
	app.Listen(":3000")
}
