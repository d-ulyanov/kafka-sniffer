package kafka

import (
	"fmt"
)

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message. Length() is provided as an
// optimization, and must return the same as len() on the result of Encode().
type Encoder interface {
	Encode() ([]byte, error)
	Length() int
}

// make strings and byte slices encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that they can be used
// as the Key or Value in a ProducerMessage.
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

func (s StringEncoder) Length() int {
	return len(s)
}

// ByteEncoder implements the Encoder interface for Go byte slices so that they can be used
// as the Key or Value in a ProducerMessage.
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

func (b ByteEncoder) Length() int {
	return len(b)
}

// KafkaVersion instances represent versions of the upstream Kafka broker.
type KafkaVersion struct {
	// it's a struct rather than just typing the array directly to make it opaque and stop people
	// generating their own arbitrary versions
	version [4]uint
}

func newKafkaVersion(major, minor, veryMinor, patch uint) KafkaVersion {
	return KafkaVersion{
		version: [4]uint{major, minor, veryMinor, patch},
	}
}

// IsAtLeast return true if and only if the version it is called on is
// greater than or equal to the version passed in:
//    V1.IsAtLeast(V2) // false
//    V2.IsAtLeast(V1) // true
func (v KafkaVersion) IsAtLeast(other KafkaVersion) bool {
	for i := range v.version {
		if v.version[i] > other.version[i] {
			return true
		} else if v.version[i] < other.version[i] {
			return false
		}
	}
	return true
}

// Effective constants defining the supported kafka versions.
var (
	V0_8_2_0  = newKafkaVersion(0, 8, 2, 0)
	V0_9_0_0  = newKafkaVersion(0, 9, 0, 0)
	V0_10_0_0 = newKafkaVersion(0, 10, 0, 0)
	V0_10_1_0 = newKafkaVersion(0, 10, 1, 0)
	V0_11_0_0 = newKafkaVersion(0, 11, 0, 0)
	V1_0_0_0  = newKafkaVersion(1, 0, 0, 0)
	V1_1_0_0  = newKafkaVersion(1, 1, 0, 0)
	V2_0_0_0  = newKafkaVersion(2, 0, 0, 0)
	V2_1_0_0  = newKafkaVersion(2, 1, 0, 0)
	V2_3_0_0  = newKafkaVersion(2, 3, 0, 0)
	V2_4_0_0  = newKafkaVersion(2, 4, 0, 0)

	MinVersion = V0_8_2_0
	MaxVersion = V2_4_0_0
)

func (v KafkaVersion) String() string {
	if v.version[0] == 0 {
		return fmt.Sprintf("0.%d.%d.%d", v.version[1], v.version[2], v.version[3])
	}

	return fmt.Sprintf("%d.%d.%d", v.version[0], v.version[1], v.version[2])
}
