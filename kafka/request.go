package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
)

var (
	// MaxRequestSize is the maximum size (in bytes) of any Request
	MaxRequestSize int32 = 100 * 1024 * 1024
)

type ProtocolBody interface {
	versionedDecoder
	key() int16
	version() int16
	requiredVersion() KafkaVersion
}

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
}

func (r *Request) Decode(pd PacketDecoder) (err error) {
	r.Key, err = pd.getInt16() // +2 bytes
	if err != nil {
		return err
	}

	r.Version, err = pd.getInt16() // +2 bytes
	if err != nil {
		return err
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

func DecodeRequest(r io.Reader) (*Request, int, error) {
	var (
		bytesRead   int
		lengthBytes = make([]byte, 4)
	)

	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(lengthBytes)
	length := int32(binary.BigEndian.Uint32(lengthBytes))

	if length <= 4 || length > MaxRequestSize {
		return nil, bytesRead, PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(encodedReq)

	req := &Request{
		BodyLength: length,
	}

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
		//case 2:
		//	return &OffsetRequest{Version: version}
		//case 3:
		//	return &MetadataRequest{}
		//case 8:
		//	return &OffsetCommitRequest{Version: version}
		//case 9:
		//	return &OffsetFetchRequest{}
		//case 10:
		//	return &FindCoordinatorRequest{}
		//case 11:
		//	return &JoinGroupRequest{}
		//case 12:
		//	return &HeartbeatRequest{}
		//case 13:
		//	return &LeaveGroupRequest{}
		//case 14:
		//	return &SyncGroupRequest{}
		//case 15:
		//	return &DescribeGroupsRequest{}
		//case 16:
		//	return &ListGroupsRequest{}
		//case 17:
		//	return &SaslHandshakeRequest{}
		//case 18:
		//	return &ApiVersionsRequest{}
		//case 19:
		//	return &CreateTopicsRequest{}
		//case 20:
		//	return &DeleteTopicsRequest{}
		//case 21:
		//	return &DeleteRecordsRequest{}
		//case 22:
		//	return &InitProducerIDRequest{}
		//case 24:
		//	return &AddPartitionsToTxnRequest{}
		//case 25:
		//	return &AddOffsetsToTxnRequest{}
		//case 26:
		//	return &EndTxnRequest{}
		//case 28:
		//	return &TxnOffsetCommitRequest{}
		//case 29:
		//	return &DescribeAclsRequest{}
		//case 30:
		//	return &CreateAclsRequest{}
		//case 31:
		//	return &DeleteAclsRequest{}
		//case 32:
		//	return &DescribeConfigsRequest{}
		//case 33:
		//	return &AlterConfigsRequest{}
		//case 35:
		//	return &DescribeLogDirsRequest{}
		//case 36:
		//	return &SaslAuthenticateRequest{}
		//case 37:
		//	return &CreatePartitionsRequest{}
		//case 42:
		//	return &DeleteGroupsRequest{}
	}
	return nil
}
