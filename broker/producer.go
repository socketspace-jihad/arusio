package broker

import (
	"errors"
	"log"
)

var (
	ProducerToTopic = map[*Producer][]*Topic{}
)

func RegisterProducertoTopic(p *Producer, topic string) error {
	if _, ok := TopicPool[topic]; !ok {
		return errors.New("topic not found")
	}
	ProducerToTopic[p] = []*Topic{TopicPool[topic]}
	return nil
}

type Producer struct {
	ID         uint
	connection *Connection
}

func NewProducer(conn *Connection) *Producer {
	return &Producer{
		connection: conn,
	}
}

func (p *Producer) Send(data []byte) {
	for _, topics := range ProducerToTopic {
		for _, t := range topics {
			go func(t *Topic) {
				log.Print("sending data to partition ", t.partitions[t.nextPartition].ID, string(data[8:]))
				t.partitions[t.nextPartition].Data <- data
				t.nextPartition = (t.nextPartition + 1) % len(t.partitions)
			}(t)
		}
	}
}
