package metrics

import (
	"github.com/d-ulyanov/kafka-sniffer/version"

	"github.com/prometheus/client_golang/prometheus"
)

var buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "build_info",
	Help:      "Kafka sniffer build info",
}, []string{"version", "revision", "branch"})

func init() {
	prometheus.MustRegister(buildInfo)

	buildInfo.WithLabelValues(version.Version, version.Revision, version.Branch)
}
