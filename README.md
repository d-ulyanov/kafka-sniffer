# Kafka sniffer

Initially, this tool was build for internal usage: we had a shared Kafka cluster with thousands topics and hundreds clients (without any auth) and wanted to migrate all this stuff to the new cluster. For this reason, we needed to know what mircoservices are still connected to the old cluster and what topics this service uses.

This sniffer can:
- detect active connections to Kafka Broker and can say who is producer and who is consumer
- detect topics to which producers trying to write / consumers trying to read
- expose IPs, request kind and topic as Prometheus metrics

Kafka protocol: https://kafka.apache.org/protocol

Example:

```
// Run simple producer who writes to topic "mytopic"
go run cmd/producer/main.go -brokers 127.0.0.1:9092

// Run sniffer on net iface (loopback or usually, eth0)
go run cmd/sniffer/main.go -i=lo0

// OR with debug info:
go run cmd/sniffer/main.go -i=lo0 -assembly_debug_log=false
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

## Run as a Docker container

```
docker build . -t kafka-sniffer
docker run --rm --network host kafka-sniffer:latest -i lo0
```

## Run Kafka in docker (bitnami Kafka + Zookeeper)
```
docker-compose ./etc/docker-compose.yml up
```

## Run Kafka in minicube (Strimzi Kafka Operator)

```yaml
kubectl create namespace kafka
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml -n kafka 
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka
```

Send & receive messages
```yaml
kubectl -n kafka run kafka-producer -ti --image=strimzi/kafka:0.17.0-kafka-2.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --broker-list my-cluster-kafka-bootstrap:9092 --topic my-topic
kubectl -n kafka run kafka-consumer -ti --image=strimzi/kafka:0.17.0-kafka-2.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic my-topic --from-beginning
```

Port-forwarding for local development:
```yaml
kubectl port-forward service/my-cluster-kafka-brokers 9092
```

And probably you'll need to add this row to `/etc/hosts`
```yaml
127.0.0.1   my-cluster-kafka-0.my-cluster-kafka-brokers.kafka.svc
```
