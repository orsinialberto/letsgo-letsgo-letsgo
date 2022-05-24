package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

type customer struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func main() {
	router := gin.Default()

	router.POST("/customers", postCustomers)

	router.Run("localhost:8080")
}

func postCustomers(c *gin.Context) {
	var newCustomer customer

	// Call BindJSON to bind the received JSON to newCustomer.
	if err := c.BindJSON(&newCustomer); err != nil {
		return
	}

	log.Printf("creating customer %s", newCustomer.Email)

	produce(newCustomer)

	c.IndentedJSON(http.StatusCreated, newCustomer)
}

func produce(newCustomer customer) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's .Events channel is used.
	deliveryChan := make(chan kafka.Event)
	topic := "demo"

	message, err := json.Marshal(newCustomer)

	if err != nil {
		fmt.Println("Can't serislize", newCustomer)
	}

	// todo, gestire errore
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
		Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}
