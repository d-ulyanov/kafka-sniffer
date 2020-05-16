package main

import (
	"flag"
	"log"
	"time"

	"github.com/d-ulyanov/kafka-sniffer/stream"

	"github.com/google/gopacket"
	"github.com/google/gopacket/examples/util"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/tcpassembly"
)

var iface = flag.String("i", "eth0", "Interface to get packets from")
var dstport = flag.Uint("p", 9092, "Kafka broker port")
var snaplen = flag.Int("s", 16<<10, "SnapLen for pcap packet capture")
var filter = flag.String("f", "tcp", "BPF filter for pcap")
var verbose = flag.Bool("v", false, "Logs every packet in great detail")

func main() {
	defer util.Run()()
	log.Printf("starting capture on interface %q", *iface)

	// Set up pcap packet capture
	handle, err := pcap.OpenLive(*iface, int32(*snaplen), true, pcap.BlockForever)
	if err != nil {
		panic(err)
	}

	if err := handle.SetBPFFilter(*filter); err != nil {
		panic(err)
	}

	// Set up assembly
	streamPool := tcpassembly.NewStreamPool(&stream.KafkaStreamFactory{})
	assembler := tcpassembly.NewAssembler(streamPool)

	// Limit memory usage by auto-flushing connection state if we get over 100K
	// packets in memory, or over 1000 for a single stream.
	assembler.MaxBufferedPagesTotal = 100000
	assembler.MaxBufferedPagesPerConnection = 1000

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
