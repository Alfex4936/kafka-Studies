package main

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	// get kafka writer using environment variables.
	kafkaURL := "localhost:9092"
	topic := "first-topic"
	kafkaWriter := getKafkaWriter(kafkaURL, topic)

	defer kafkaWriter.Close()

	// Send messages.
	for _, word := range []string{"Welcome", "gokafka"} {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", 1)),
			Value: []byte(word),
		}

		err := kafkaWriter.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		}
	}
}
