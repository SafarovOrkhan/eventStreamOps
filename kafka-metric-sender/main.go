package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	address := "157.180.115.157:9092"
	topicName := "mainTopic"
	min := 10
	max := 30
	fmt.Println("Connecting to:", address)
	// Create connection once
	conn, err := kafkago.DialLeader(context.Background(), "tcp", address, topicName, 0)
	if err != nil {
		log.Fatal("failed to connect to Kafka:", err)
	}
	defer conn.Close()

	for {
		log.Println("Generating value...")
		value := rand.Intn(max-min) + min
		message := fmt.Sprintf(`{"cpu": %v}`, value)

		log.Println("Sending message:", message)
		_, err := conn.WriteMessages(
			kafkago.Message{
				Value: []byte(message),
			},
		)
		if err != nil {
			log.Println("Failed to write message:", err)
		} else {
			log.Println("Message sent successfully.")
		}

		time.Sleep(10 * time.Second)
	}
}
