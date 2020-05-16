# Kafka sniffer

It's made for detection how producers and topics related 
(usually, it's not possible to know if auth is disabled on Kafka cluster).

Kafka protocol: https://kafka.apache.org/protocol


Example:

```
// Run simple producer who writes to topic "mytopic"
go run cmd/producer/main.go -brokers 127.0.0.1:9092

// Run sniffer on net iface (loopback or usually, eth0)
go run cmd/sniffer/main.go -i=lo0
```

Example output:

```
2020/05/16 16:25:49 got request, key: 0, version: 0, correlationID: 132, clientID: sarama
2020/05/16 16:25:49 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:25:54 got request, key: 0, version: 0, correlationID: 133, clientID: sarama
2020/05/16 16:25:54 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:25:59 got request, key: 0, version: 0, correlationID: 134, clientID: sarama
2020/05/16 16:25:59 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:26:04 got request, key: 0, version: 0, correlationID: 135, clientID: sarama
2020/05/16 16:26:04 client 127.0.0.1:60423 wrote to topic mytopic
2020/05/16 16:26:05 got EOF - stop reading from stream
```

## Run as docker container

```
docker build . -t kafka-sniffer
docker run --rm --network host kafka-sniffer:latest -i lo0
```