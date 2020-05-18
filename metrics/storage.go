package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "sniffer"

type Storage struct {
	registerer prometheus.Registerer

	producerTopicRelationInfo *prometheus.GaugeVec
}

// @todo add refreshing metric value functionality
func NewStorage(registerer prometheus.Registerer) *Storage {
	var s = &Storage{
		registerer: registerer,
		producerTopicRelationInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "producer_topic_relation_info",
			Help:      "Relation information between producer and topic",
		}, []string{"producer", "topic"}),
	}

	s.registerer.MustRegister(s.producerTopicRelationInfo)

	return s
}

func (s Storage) SetProducerTopicRelationInfoValue(producer, topic string) {
	s.producerTopicRelationInfo.WithLabelValues(producer, topic).Set(float64(1))
}
