package kafka

import (
	"github.com/d-ulyanov/kafka-sniffer/metrics"
)

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

// ProduceRequest is a type of request in kafka
type ProduceRequest struct {
	TransactionalID *string
	RequiredAcks    RequiredAcks
	Timeout         int32
	Version         int16 // v1 requires Kafka 0.9, v2 requires Kafka 0.10, v3 requires Kafka 0.11
	records         map[string]map[int32]Records
}

// Decode decodes kafka produce request from packet
func (r *ProduceRequest) Decode(pd PacketDecoder, version int16) error {
	r.Version = version

	if version >= 3 {
		id, err := pd.getNullableString()
		if err != nil {
			return err
		}
		r.TransactionalID = id
	}
	requiredAcks, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.RequiredAcks = RequiredAcks(requiredAcks)
	if r.Timeout, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}

	r.records = make(map[string]map[int32]Records)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.records[topic] = make(map[int32]Records)

		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			size, err := pd.getInt32()
			if err != nil {
				return err
			}

			// rewind decoder to size
			recordsDecoder, err := pd.getSubset(int(size))
			if err != nil {
				return err
			}
			var records Records
			if err := records.decode(recordsDecoder); err != nil {
				return err
			}
			r.records[topic][partition] = records
		}
	}

	return nil
}

func (r *ProduceRequest) key() int16 {
	return 0
}

func (r *ProduceRequest) version() int16 {
	return r.Version
}

// ExtractTopics returns topics list
func (r *ProduceRequest) ExtractTopics() []string {
	out := make([]string, 0, len(r.records))

	for topic := range r.records {
		out = append(out, topic)
	}

	return out
}

// RecordsLen retrieves total size in bytes of all records in message
func (r *ProduceRequest) RecordsLen() (recordsLen int) {
	for _, partition := range r.records {
		for _, record := range partition {
			switch record.recordsType {
			case legacyRecords:
				recordsLen += len(record.MsgSet.Messages)
			case defaultRecords:
				recordsLen += len(record.RecordBatch.Records)
			}
		}
	}
	return
}

// RecordsSize retrieves total number of records in batch
func (r *ProduceRequest) RecordsSize() (recordsSize int) {
	for _, partition := range r.records {
		for _, record := range partition {
			switch record.recordsType {
			case legacyRecords:
				for _, msg := range record.MsgSet.Messages {
					recordsSize += msg.Msg.compressedSize
				}
			case defaultRecords:
				recordsSize += record.RecordBatch.recordsLen
			}
		}
	}
	return
}

// CollectClientMetrics collects metrics associated with client
func (r *ProduceRequest) CollectClientMetrics(srcHost string) {
	metrics.RequestsCount.WithLabelValues(srcHost, "produce").Inc()

	batchSize := r.RecordsSize()
	metrics.ProducerBatchSize.WithLabelValues(srcHost).Add(float64(batchSize))

	batchLen := r.RecordsLen()
	metrics.ProducerBatchLen.WithLabelValues(srcHost).Add(float64(batchLen))
}

func (r *ProduceRequest) requiredVersion() Version {
	switch r.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_11_0_0
	case 7:
		return V2_1_0_0
	default:
		return MinVersion
	}
}
