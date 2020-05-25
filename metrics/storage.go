package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace      = "kafka_sniffer"
	hourExpireTime = time.Hour
)

// Storage contains prometheus metrics that have expiration time. When expiration time is succeeded,
// metric with specific labels will be removed from storage. It is needed to keep only fresh producer,
// topic and consumer relations.
type Storage struct {
	producerTopicRelationInfo *metric
	consumerTopicRelationInfo *metric
	activeConnectionsTotal    *metric
}

func NewStorage(registerer prometheus.Registerer, expireTime time.Duration) *Storage {
	var s = &Storage{
		producerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "producer_topic_relation_info",
			Help:      "Relation information between producer and topic",
		}, []string{"producer", "topic"}), expireTime),
		consumerTopicRelationInfo: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "consumer_topic_relation_info",
			Help:      "Relation information between consumer and topic",
		}, []string{"consumer", "topic"}), expireTime),
		activeConnectionsTotal: newMetric(prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_connections_total",
			Help:      "Contains total count of active connections",
		}, []string{"client_ip"}), hourExpireTime),
	}

	registerer.MustRegister(
		s.producerTopicRelationInfo.promMetric,
		s.consumerTopicRelationInfo.promMetric,
		s.activeConnectionsTotal.promMetric,
	)

	return s
}

func (s *Storage) AddProducerTopicRelationInfo(producer, topic string) {
	s.producerTopicRelationInfo.set(producer, topic)
}

func (s *Storage) AddConsumerTopicRelationInfo(consumer, topic string) {
	s.consumerTopicRelationInfo.set(consumer, topic)
}

func (s *Storage) AddActiveConnectionsTotal(clientIP string) {
	s.activeConnectionsTotal.inc(clientIP)
}

// metric contains expiration functionality
type metric struct {
	promMetric *prometheus.GaugeVec
	expireTime time.Duration

	expCh chan []string

	mux       sync.Mutex
	relations map[string]*relation
}

func newMetric(promMetric *prometheus.GaugeVec, expireTime time.Duration) *metric {
	m := &metric{
		promMetric: promMetric,
		expireTime: expireTime,

		relations: make(map[string]*relation),
		expCh:     make(chan []string),
	}

	go m.runExpiration()

	return m
}

func (m *metric) set(labels ...string) {
	m.promMetric.WithLabelValues(labels...).Set(float64(1))

	m.update(labels...)
}

func (m *metric) inc(labels ...string) {
	m.promMetric.WithLabelValues(labels...).Inc()

	m.update(labels...)
}

// update updates relations or creates new one
func (m *metric) update(labels ...string) {
	m.mux.Lock()
	if r, ok := m.relations[genLabelKey(labels...)]; ok {
		r.refresh()
	} else {
		m.relations[genLabelKey(labels...)] = newRelation(m.expireTime, labels, m.expCh)
	}
	m.mux.Unlock()
}

// runExpiration removes metric by specific label values and removes relation
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

// relation contains metric labels and expiration time
type relation struct {
	expireTime time.Duration

	labels []string
	expCh  chan []string

	mux   sync.Mutex
	timer *time.Timer
}

func newRelation(expireTime time.Duration, labels []string, expCh chan []string) *relation {
	var rel = relation{
		expireTime: expireTime,
		labels:     labels,
		expCh:      expCh,
	}

	go rel.run()

	return &rel
}

// run runs expiration with specific timer
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

// refresh resets timer or create new one
func (c *relation) refresh() {
	c.mux.Lock()
	if c.timer == nil {
		c.timer = time.NewTimer(c.expireTime)
	} else {
		c.timer.Reset(c.expireTime)
	}
	c.mux.Unlock()
}

func genLabelKey(labels ...string) string {
	return strings.Join(labels, "_")
}
