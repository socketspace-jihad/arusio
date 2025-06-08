package message

type Message struct {
	ID            uint32
	TopicLength   uint32
	Topic         []byte
	PayloadLength uint64
	Payload       []byte
}
