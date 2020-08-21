package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	RequestsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "typed_requests_total",
		Help: "Total requests to kafka by type",
	}, []string{"client_ip", "request_type"})

	ProducerBatchLen = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "producer_batch_length",
		Help: "Length of producer request batch to kafka",
	}, []string{"client_ip"})

	ProducerBatchSize = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "producer_batch_size",
		Help: "Total size of a batch in producer request to kafka",
	}, []string{"client_ip"})

	BlocksRequested = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "blocks_requested",
		Help: "Total size of a batch in producer request to kafka",
	}, []string{"client_ip"})
)

func init() {
	prometheus.MustRegister(RequestsCount, ProducerBatchLen, ProducerBatchSize, BlocksRequested)
}

type ClientMetricsSender interface {
	SendClientMetrics(srcHost string)
}
