package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sapaad/print-microservice/kafka"
	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println(err)
		log.Fatal("Error loading .env file")
	}
}

func main() {
	kafkaClient := kafka.Client{}
	kafkaClient.Connect()

	// Trap SIGTERM
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		// Ctrl + C trap
		fmt.Println("Closing consumer and producer...")
		kafkaClient.Producer.Close()
		kafkaClient.Consumer.Close()
		os.Exit(1)
	}()

	go kafkaClient.ShowErrors()
	go kafkaClient.ShowNotifications()
	defer kafkaClient.Producer.Close()
	defer kafkaClient.Consumer.Close()

	fmt.Println("Listening to messages...")
	for message := range kafkaClient.Consumer.Messages() {
		if message != nil {
			go processMessage(message, kafkaClient)
		}
	}
}

func processMessage(msg *sarama.ConsumerMessage, kc kafka.Client) {
	message := kafka.Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Topic:     msg.Topic,
		Value:     string(msg.Value),
	}

	fmt.Printf(
		"Message Received:\nTopic: %s\nPartition: %d\nOffset: %d\n",
		message.Topic, message.Partition, message.Offset)
}
