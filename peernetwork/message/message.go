package message

import (
	"encoding/gob"
	"github.com/golang/snappy/snappy"
	"io"
	"time"
)

//Message ...
type Message struct {
	Action    string
	Key       string
	Value     []byte
	Timestamp int64
	Ttl       int64
}

//NewMessage returns a message object through the interface
func NewMessage(action string, key string, value string, ttl int64) *Message {

	/*var b bytes.Buffer
	w := snappy.NewWriter(&b)
	w.Write([]byte(value))
	compressedValue := b.Bytes()
	*/

	compressedValue, _ := snappy.Encode([]byte{}, []byte(value))

	timestamp := time.Now().Unix()

	return &Message{
		Action:     action,
		Key:       key,
		Value:     compressedValue,
		Timestamp: timestamp,
		Ttl:       ttl,
	}
}

//SetMessage sets message object from data
func SetMessage(action string, key string, compressedValue []byte, timestamp int64, ttl int64) *Message {

	return &Message{
		Action:     action,
		Key:       key,
		Value:     compressedValue,
		Timestamp: timestamp,
		Ttl:       ttl,
	}
}

//GetValue gets value payload of the message
func (m *Message) GetValue() *string {
	dst, _ := snappy.Decode([]byte{}, m.Value)
	value := string(dst)
	return &value
}

//WriteGob this writes the object to io.Writer interface
func (m *Message) WriteGob(conn io.Writer) error {
	encoder := gob.NewEncoder(conn)
	err := encoder.Encode(m)
	return err
}
