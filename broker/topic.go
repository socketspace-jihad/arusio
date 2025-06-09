package broker

import (
	"errors"
	"sync"
)

type topicPool struct {
	data map[string]*topic
	sync.Mutex
}

var (
	pool = topicPool{
		data:  make(map[string]*topic),
		Mutex: sync.Mutex{},
	}
)

func RegisterTopic(name string, numPartition uint) error {
	if _, ok := pool.data[name]; ok {
		return errors.New("topic name has been already registerd, change topic name")
	}
	pool.Lock()
	pool.data[name] = NewTopic(name, numPartition)
	pool.Unlock()
	return nil
}

func RegisterProducertoTopic(conn *Connection, topicName string) error {
	if _, ok := pool.data[topicName]; !ok {
		return errors.New("topic not found")
	}
	return pool.data[topicName].registerNewProducer(conn)
}

type Partition struct {
	ID          uint
	Data        chan []byte
	Connections []*Connection
}

func NewPartition(bufferCap uint32) Partition {
	return Partition{
		Data: make(chan []byte, bufferCap),
	}
}

type topic struct {
	name      string
	partition [1024]Partition
	ConsumerGroup
	Producer []*Connection
}

func NewTopic(name string, numPartition uint) *topic {
	part := make([]Partition, numPartition)
	for i := 0; i < int(numPartition); i++ {
		part[i] = NewPartition(1024)
	}
	return &topic{
		name:      name,
		partition: [1024]Partition{},
	}
}

func (t *topic) registerNewProducer(conn *Connection) error {
	t.Producer = append(t.Producer, conn)
	return nil
}

func (t *topic) rebalance() error {
	return nil
}

func (t *topic) DecreasePartition(num uint) error {
	return nil
}

func (t *topic) IncreasePartition(num int) error {
	return nil
}
