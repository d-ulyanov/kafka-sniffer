package stream

import (
	"bufio"
	"io"
	"log"

	"github.com/d-ulyanov/kafka-sniffer/kafka"
	"github.com/d-ulyanov/kafka-sniffer/metrics"

	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

// KafkaStreamFactory implements tcpassembly.StreamFactory
type KafkaStreamFactory struct {
	metricsStorage *metrics.Storage
}

func NewKafkaStreamFactory(metricsStorage *metrics.Storage) *KafkaStreamFactory {
	return &KafkaStreamFactory{metricsStorage: metricsStorage}
}

func (h *KafkaStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &KafkaStream{
		net:            net,
		transport:      transport,
		r:              tcpreader.NewReaderStream(),
		metricsStorage: h.metricsStorage,
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

// KafkaStream will handle the actual decoding of http requests.
type KafkaStream struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
	metricsStorage *metrics.Storage
}

func (h *KafkaStream) run() {
	log.Printf("%s:%s -> %s:%s", h.net.Src(), h.transport.Src(), h.net.Dst(), h.transport.Dst())
	log.Printf("%s:%s -> %s:%s", h.net.Dst(), h.transport.Dst(), h.net.Src(), h.transport.Src())

	buf := bufio.NewReaderSize(&h.r, 2<<15) // 65k

	for {
		req, _, err := kafka.DecodeRequest(buf)
		if err == io.EOF {
			log.Println("got EOF - stop reading from stream")
			return
		}

		if err != nil {
			// important! Need to reset buffer if some error occur
			buf.Reset(&h.r)
			log.Printf("unable to read request to Broker - skipping stream: %s\n", err)
			continue
		}

		// increase count of received requests
		h.metricsStorage.IncReceivedTotal()

		log.Printf("got request, key: %d, version: %d, correlationID: %d, clientID: %s\n", req.Key, req.Version, req.CorrelationID, req.ClientID)

		switch body := req.Body.(type) {
		case *kafka.ProduceRequest:
			for _, topic := range body.ExtractTopics() {
				log.Printf("client %s:%s wrote to topic %s", h.net.Src(), h.transport.Src(), topic)

				// add producer and topic relation info into metric
				h.metricsStorage.AddProducerTopicRelationInfo(h.net.Src().String(), topic)
			}
		case *kafka.FetchRequest:
			for _, topic := range body.ExtractTopics() {
				h.metricsStorage.AddConsumerTopicRelationInfo(h.net.Src().String(), topic)
			}
		}
	}
}
