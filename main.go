package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

type customer struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

var (
	topic         = "demo"
	brokerAddress = "localhost:9092"
	group         = "demo-group"
)

func main() {
	router := gin.Default()

	router.POST("/customers", postCustomers)

	go consume()

	router.Run("localhost:8080")
}

func postCustomers(c *gin.Context) {

	var newCustomer customer

	err := c.BindJSON(&newCustomer)
	if err != nil {
		fmt.Println("can't convert request body", c)
		return
	}

	log.Printf("creating customer %s", newCustomer.Email)

	go produce(newCustomer)

	c.IndentedJSON(http.StatusCreated, newCustomer)
}

func produce(newCustomer customer) {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerAddress})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("created producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	message, err := json.Marshal(newCustomer)
	if err != nil {
		fmt.Println("can't serislize", newCustomer)
		return
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, deliveryChan)
	if err != nil {
		fmt.Println("can't send message to topic")
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}

func consume() {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe topic: %s\n", err)
		os.Exit(1)
	}

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var message customer
				json.Unmarshal(e.Value, &message)
				fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, message)
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
	os.Exit(1)
}
