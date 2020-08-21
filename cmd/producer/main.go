package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	flag.Parse()

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	c := newDataCollector(brokerList)

	t := time.NewTicker(5 * time.Second)

	for range t.C {
		err := c.SendMessages([]*sarama.ProducerMessage{
			{
				Topic: "mytopic",
				Value: sarama.StringEncoder("something"),
			},
			{
				Topic: "mysecondtopic",
				Value: sarama.StringEncoder("something_another"),
			},
		})

		if err != nil {
			log.Printf("Failed to send message: %s", err)
		} else {
			log.Print("messages sent")
		}
	}
}

func newDataCollector(brokerList []string) sarama.SyncProducer {

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Metadata.Retry.Backoff = 2 * time.Second

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}
