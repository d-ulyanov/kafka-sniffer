package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/d-ulyanov/kafka-sniffer/metrics"
	"github.com/d-ulyanov/kafka-sniffer/stream"

	"github.com/google/gopacket"
	"github.com/google/gopacket/examples/util"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/tcpassembly"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "net/http/pprof"
)

const (
	defaultListenAddr = ":9870"
	defaultExpireTime = 5 * time.Minute
)

var (
	iface      = flag.String("i", "eth0", "Interface to get packets from")
	dstport    = flag.Uint("p", 9092, "Kafka broker port") // todo: use -f tcp and dst port 9092
	snaplen    = flag.Int("s", 16<<10, "SnapLen for pcap packet capture")
	filter     = flag.String("f", "tcp", "BPF filter for pcap")
	verbose    = flag.Bool("v", false, "Logs every packet in great detail")
	listenAddr = flag.String("addr", defaultListenAddr, "Address on which sniffer listen the requests")
	expireTime = flag.Duration("metrics.expire-time", defaultExpireTime, "Expiration time of metric.")
)

func main() {
	defer util.Run()()
	log.Printf("starting capture on interface %q", *iface)

	// run telemetry
	go runTelemetry()

	// Set up pcap packet capture
	handle, err := pcap.OpenLive(*iface, int32(*snaplen), true, pcap.BlockForever)
	if err != nil {
		panic(err)
	}

	if err := handle.SetBPFFilter(*filter); err != nil {
		panic(err)
	}

	// init metrics storage
	metricsStorage := metrics.NewStorage(prometheus.DefaultRegisterer, *expireTime)

	// Set up assembly
	streamPool := tcpassembly.NewStreamPool(stream.NewKafkaStreamFactory(metricsStorage, *verbose))
	assembler := tcpassembly.NewAssembler(streamPool)

	// Auto-flushing connection state to get packets
	// without waiting SYN
	assembler.MaxBufferedPagesTotal = 1000
	assembler.MaxBufferedPagesPerConnection = 1

	log.Println("reading in packets")

	// Read in packets, pass to assembler.
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()
	ticker := time.Tick(time.Minute)

	for {
		select {
		case packet := <-packets:
			if *verbose {
				log.Println(packet)
			}

			if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
				if *verbose {
					log.Println("Unusable packet")
				}
				continue
			}

			tcp := packet.TransportLayer().(*layers.TCP)

			// todo: remove it (because port filter is in BFP)
			if tcp.DstPort != layers.TCPPort(*dstport) {
				if *verbose {
					log.Println("Unusable dst port:" + tcp.DstPort.String())
				}

				continue
			}

			assembler.AssembleWithTimestamp(packet.NetworkLayer().NetworkFlow(), tcp, packet.Metadata().Timestamp)

		case <-ticker:
			// Every minute, flush connections that haven't seen activity in the past 2 minutes.
			assembler.FlushOlderThan(time.Now().Add(time.Minute * -2))
			log.Println("---- FLUSHING ----")
		}
	}
}

func runTelemetry() {
	fmt.Printf("serving metrics on %s\n", *listenAddr)

	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(*listenAddr, nil); err != nil {
		panic(err)
	}
}
