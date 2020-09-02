package kafka

import "time"

// Timestamp is a structure representing UNIX timestamp
type Timestamp struct {
	*time.Time
}

// Decode decodes timestamp from packet
func (t Timestamp) Decode(pd PacketDecoder) error {
	millis, err := pd.getInt64()
	if err != nil {
		return err
	}

	// negative timestamps are invalid, in these cases we should return
	// a zero time
	timestamp := time.Time{}
	if millis >= 0 {
		timestamp = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
	}

	*t.Time = timestamp
	return nil
}
