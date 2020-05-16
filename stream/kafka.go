package stream

import (
	"bufio"
	"fmt"
	"github.com/d-ulyanov/kafka-sniffer/kafka"
	"github.com/google/gopacket"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"io"
	"log"
	"sync"
)

var (
	streamPath = map[string]map[uint32]string{}
	pathLock   sync.RWMutex
)

// KafkaStreamFactory implements tcpassembly.StreamFactory
type KafkaStreamFactory struct{}

func (h *KafkaStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	s := &KafkaStream{
		net:       net,
		transport: transport,
		r:         tcpreader.NewReaderStream(),
	}

	go s.run() // Important... we must guarantee that data from the reader stream is read.

	return &s.r
}

// KafkaStream will handle the actual decoding of http requests.
type KafkaStream struct {
	net, transport gopacket.Flow
	r              tcpreader.ReaderStream
}

func (h *KafkaStream) run() {
	net := fmt.Sprintf("%s:%s -> %s:%s", h.net.Src(), h.transport.Src(), h.net.Dst(), h.transport.Dst())
	revNet := fmt.Sprintf("%s:%s -> %s:%s", h.net.Dst(), h.transport.Dst(), h.net.Src(), h.transport.Src())

	log.Println(net)
	log.Println(revNet)

	buf := bufio.NewReaderSize(&h.r, 2 << 15) // 65k

	for {
		req, _, err := kafka.DecodeRequest(buf)
		if err == io.EOF {
			log.Printf("got EOF - stop reading from stream")
			return
		}

		if err != nil {
			log.Printf("unable to read request to Broker - skipping stream: %s\n", err)
			continue
		}


		log.Printf("got request, key: %d, version: %d, correlationID: %d, clientID: %s\n", req.Key, req.Version, req.CorrelationID, req.ClientID)

		if produceReq, ok := req.Body.(*kafka.ProduceRequest); ok {
			for _, topic := range produceReq.ExtractTopics() {
				log.Printf("client %s:%s wrote to topic %s", h.net.Src(), h.transport.Src(), topic)
			}

		}
	}
}
