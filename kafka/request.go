package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/d-ulyanov/kafka-sniffer/metrics"
)

var (
	// MaxRequestSize is the maximum size (in bytes) of any Request
	MaxRequestSize int32 = 100 * 1024 * 1024
)

// ProtocolBody represents body of kafka request
type ProtocolBody interface {
	versionedDecoder
	metrics.ClientMetricsCollector
	key() int16
	version() int16
	requiredVersion() Version
}

// Request is a kafka request
type Request struct {
	// Key is a Kafka api key - it defines kind of request (why it called api key?)
	// List of api keys see here: https://kafka.apache.org/protocol#protocol_api_keys
	Key int16

	// Version is a Kafka broker version
	Version int16

	// Is request body length
	BodyLength int32

	CorrelationID int32

	ClientID string

	Body ProtocolBody

	UsePreparedKeyVersion bool
}

// Decode decodes request from packet
func (r *Request) Decode(pd PacketDecoder) (err error) {
	if !r.UsePreparedKeyVersion {
		r.Key, err = pd.getInt16() // +2 bytes
		if err != nil {
			return err
		}
	}

	if !r.UsePreparedKeyVersion {
		r.Version, err = pd.getInt16() // +2 bytes
		if err != nil {
			return err
		}
	}

	r.CorrelationID, err = pd.getInt32() // +4 bytes
	if err != nil {
		return err
	}

	r.ClientID, err = pd.getString() // +2 + len(r.ClientID) bytes
	if err != nil {
		return err
	}

	body := allocateBody(r.Key, r.Version)

	// If  we can't (don't want) to unmarshal request structure - we need to discard the rest bytes
	if body == nil {
		// discard 10 bytes + clientID length
		pd.discard(int(r.BodyLength) - 10 - len(r.ClientID))

		// Skip Body decoding for now
		return nil
	}

	r.Body = body
	if r.Body == nil {
		return PacketDecodingError{fmt.Sprintf("unknown Request key (%d)", r.Key)}
	}

	return r.Body.Decode(pd, r.Version)
}

// DecodeLength decodes length from packet
func DecodeLength(encoded []byte) int32 {
	return int32(binary.BigEndian.Uint32(encoded[:4]))
}

// DecodeKey decodes key from packet. For terminology see kafka reference
func DecodeKey(encoded []byte) int16 {
	return int16(binary.BigEndian.Uint16(encoded[4:6]))
}

// DecodeVersion descodes version from packet
func DecodeVersion(encoded []byte) int16 {
	return int16(binary.BigEndian.Uint16(encoded[6:]))
}

// DecodeRequest decodes request from packets delivered by reader
func DecodeRequest(r io.Reader) (*Request, int, error) {
	var (
		needReadBytes = 8
		readBytes     = make([]byte, needReadBytes)
	)
	/// read bytes to decode length, key, version
	if _, err := io.ReadFull(r, readBytes); err != nil {
		return nil, needReadBytes, err
	}
	if len(readBytes) != needReadBytes {
		return nil, len(readBytes), errors.New("could define length, key, version")
	}

	// length - (key(2 bytes) + version(2 bytes))
	length := DecodeLength(readBytes) - 4
	key := DecodeKey(readBytes)
	version := DecodeVersion(readBytes)

	// check request type
	if protocol := allocateBody(key, version); protocol == nil {
		return nil, int(length), PacketDecodingError{fmt.Sprintf("unsupported protocol with key: %d", key)}
	}

	// check request size
	if length <= 4 || length > MaxRequestSize {
		return nil, int(length), PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	// read full request
	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, int(length), err
	}

	bytesRead := needReadBytes + len(encodedReq)
	req := &Request{
		BodyLength:            length,
		Key:                   key,
		Version:               version,
		UsePreparedKeyVersion: true,
	}

	// decode request
	if err := Decode(encodedReq, req); err != nil {
		return nil, bytesRead, err
	}

	return req, bytesRead, nil
}

func allocateBody(key, version int16) ProtocolBody {
	switch key {
	case 0:
		return &ProduceRequest{}
	case 1:
		return &FetchRequest{Version: version}
	}
	return nil
}
