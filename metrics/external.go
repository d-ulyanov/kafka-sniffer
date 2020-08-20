package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ProducerRequestsCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name: "producer_requests_total",
		Help: "Total producer requests to kafka",
	}, []string{"client_ip"})

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
)

func init() {
	prometheus.MustRegister(ProducerRequestsCount, ProducerBatchLen, ProducerBatchSize)
}
