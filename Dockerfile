FROM golang:1.14.3-stretch

RUN apt-get update && apt-get -y install libpcap-dev

RUN mkdir -p /go/src/github.com/d-ulyanov/kafka-sniffer

WORKDIR /go/src/github.com/d-ulyanov/kafka-sniffer
COPY . .

RUN go get -d -v ./... && go build -o kafka_sniffer -v cmd/sniffer/main.go

ENTRYPOINT ["/go/src/github.com/d-ulyanov/kafka-sniffer/kafka_sniffer"]
