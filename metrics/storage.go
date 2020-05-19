package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "sniffer"

	defaultTimeExpiration = 5 * time.Minute
)

type Storage struct {
	registerer prometheus.Registerer

	producerTopicRelationInfo *metric
	consumerTopicRelationInfo *metric
}

func NewStorage(registerer prometheus.Registerer) *Storage {
	var s = &Storage{
		registerer: registerer,
		producerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "producer_topic_relation_info",
			Help:      "Relation information between producer and topic",
		}, []string{"producer", "topic"})),
		consumerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumer_topic_relation_info",
			Help:      "Relation information between consumer and topic",
		}, []string{"consumer", "topic"})),
	}

	s.registerer.MustRegister(
		s.producerTopicRelationInfo.promMetric,
		s.consumerTopicRelationInfo.promMetric,
	)

	go s.producerTopicRelationInfo.runExpiration()
	go s.consumerTopicRelationInfo.runExpiration()

	return s
}

func (s *Storage) SetProducerTopicRelationInfoValue(producer, topic string) {
	s.producerTopicRelationInfo.update(producer, topic)
}

func (s *Storage) SetConsumerTopicRelationInfoValue(consumer, topic string) {
	s.consumerTopicRelationInfo.update(consumer, topic)
}

type metric struct {
	promMetric *prometheus.GaugeVec
	expCh      chan []string

	mux       sync.Mutex
	relations map[string]*relation
}

func newMetric(promMetric *prometheus.GaugeVec) *metric {
	return &metric{
		promMetric: promMetric,
		relations:  make(map[string]*relation),
		expCh:      make(chan []string),
	}
}

func (m *metric) update(labels ...string) {
	m.promMetric.WithLabelValues(labels...).Set(float64(1))

	m.mux.Lock()
	if r, ok := m.relations[genLabelKey(labels...)]; ok {
		r.refresh()
	} else {
		m.relations[genLabelKey(labels...)] = newRelation(labels, m.expCh)
	}
	m.mux.Unlock()
}

func (m *metric) runExpiration() {
	for {
		select {
		case labels := <-m.expCh:
			m.promMetric.DeleteLabelValues(labels...)

			// remove relation
			m.mux.Lock()
			delete(m.relations, genLabelKey(labels...))
			m.mux.Unlock()
		}
	}
}

type relation struct {
	labels []string
	expCh  chan []string

	mux   sync.Mutex
	timer *time.Timer
}

func newRelation(labels []string, expCh chan []string) *relation {
	var rel = relation{
		labels: labels,
		expCh:  expCh,
	}

	go rel.run()

	return &rel
}

func (c *relation) run() {
	c.refresh()

	for {
		select {
		case <-c.timer.C:
			c.expCh <- c.labels
			return
		}
	}
}

func (c *relation) refresh() {
	c.mux.Lock()
	if c.timer == nil {
		c.timer = time.NewTimer(defaultTimeExpiration)
	} else {
		c.timer.Reset(defaultTimeExpiration)
	}
	c.mux.Unlock()
}

func genLabelKey(labels ...string) string {
	return strings.Join(labels, "_")
}
