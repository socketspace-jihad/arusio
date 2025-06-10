package broker

import (
	"errors"
	"sync"

	"github.com/rs/zerolog/log"
)

type topicPool struct {
	data map[string]*topic
	sync.Mutex
}

var (
	errTopicNotFound = errors.New("topic not found")
	pool             = topicPool{
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

func RegisterProducertoTopic(conn *Connection, topicName string) (*topic, error) {
	if _, ok := pool.data[topicName]; !ok {
		return nil, errTopicNotFound
	}
	return pool.data[topicName].registerNewProducer(conn), nil
}

func RegisterConsumertoTopic(cg *ConsumerGroup, topicName string) (*topic, error) {
	if _, ok := pool.data[topicName]; !ok {
		return nil, errTopicNotFound
	}
	return pool.data[topicName].registerNewConsumer(cg), nil
}

type Partition struct {
	ID             uint
	Data           chan []byte
	errChan        chan error
	doneChan       chan struct{}
	ConsumerGroups map[string]*ConsumerGroup
}

func NewPartition(bufferCap uint32, ID uint) Partition {
	return Partition{
		ID:             ID,
		Data:           make(chan []byte, bufferCap),
		ConsumerGroups: make(map[string]*ConsumerGroup),
		errChan:        make(chan error),
		doneChan:       make(chan struct{}),
	}
}

func (p *Partition) registerNewConsumer(cg *ConsumerGroup) error {
	p.ConsumerGroups[cg.name] = cg
	go func(partition *Partition) {
		for payload := range partition.Data {
			for _, cg := range partition.ConsumerGroups {
				log.Print("sending ", string(payload), " from ", p.ID, " listener ", cg)
				_, err := cg.connections[0].Conn.Write(payload)
				if err != nil {
					log.Err(err).Msg("error when sending message")
				}
			}
		}
		for {
			select {
			case payload := <-partition.Data:
				for _, cg := range partition.ConsumerGroups {
					log.Print("sending ", string(payload), " from ", p.ID, " listener ", cg)
					_, err := cg.connections[0].Conn.Write(payload)
					if err != nil {
						log.Err(err).Msg("error when sending message")
					}
				}
			case <-p.doneChan:
				log.Print("end of the data")
			}
		}
	}(p)
	return nil
}

func (p *Partition) registerExistingConsumer(cg *ConsumerGroup) error {
	if _, ok := p.ConsumerGroups[cg.name]; !ok {
		p.ConsumerGroups[cg.name] = cg
	} else {
		p.ConsumerGroups[cg.name].connections = append(p.ConsumerGroups[cg.name].connections, cg.connections...)
	}
	return nil
}

func (p *Partition) send(data []byte) {
	p.Data <- data
}

type topic struct {
	name          string
	partition     []Partition
	Producer      []*Connection
	nextPartition int
	cgCounter     map[string]int
	sync.Mutex
}

func (t *topic) Publish(payload []byte) error {
	t.Mutex.Lock()
	go t.partition[t.nextPartition].send(payload)
	t.nextPartition = (t.nextPartition + 1) % len(t.partition)
	defer t.Mutex.Unlock()
	return nil
}

func NewTopic(name string, numPartition uint) *topic {
	part := make([]Partition, numPartition)
	for i := 0; i < int(numPartition); i++ {
		part[i] = NewPartition(1024, uint(i+1))
	}
	return &topic{
		name:      name,
		partition: part,
		Producer:  make([]*Connection, 1024),
		cgCounter: make(map[string]int),
	}
}

func (t *topic) registerNewConsumer(cg *ConsumerGroup) *topic {
	if _, ok := t.cgCounter[cg.name]; !ok {
		t.cgCounter[cg.name] = 0
	}
	t.cgCounter[cg.name]++
	if t.cgCounter[cg.name] > len(t.partition) {
		log.Warn().Msg("number of consumer instance exceed the topic partition number, some instance would be idle")
	} else {
		t.rebalance(cg)
	}
	return t
}

func (t *topic) registerNewProducer(conn *Connection) *topic {
	t.Producer = append(t.Producer, conn)
	return t
}

func (t *topic) rebalance(cg *ConsumerGroup) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	log.Print("rebalance executed")
	numConsumer := t.cgCounter[cg.name]
	partitionLength := len(t.partition)
	occurance := partitionLength / numConsumer

	if numConsumer == 1 {
		log.Print("num consumer 1 ", numConsumer)
		for i := 0; i < partitionLength; i++ {
			t.partition[i].registerNewConsumer(cg)
		}
		return
	}
	i := 0
	counter := 0
	consumer := 1
	for i < partitionLength {
		candidate := t.partition[i]
		// for j := i; j < occurance; j++ {
		// 	log.Print("re-registering ", candidate.ConsumerGroups[cg.name])
		// 	log.Print("num consumer ", numConsumer, " partitionLength ", partitionLength, " occurance ", occurance)
		// 	t.partition[j].registerNewConsumer(candidate.ConsumerGroups[cg.name])
		// 	counter++
		// 	i++
		// }
		log.Print("detect re-register")
		if consumer == numConsumer {
			break
		}

		for j := 0; j < occurance; j++ {
			log.Print("re-registering ", candidate.ConsumerGroups[cg.name])
			t.partition[i].registerNewConsumer(candidate.ConsumerGroups[cg.name])
			i++
			counter++
		}
		consumer++
	}
	log.Print("counter num", counter)
	for counter < partitionLength {
		log.Print("registring new node ", cg)
		t.partition[counter].registerNewConsumer(cg)
		counter++
	}
}

func (t *topic) DecreasePartition(num uint) error {
	return nil
}

func (t *topic) IncreasePartition(num int) error {
	return nil
}

func init() {
	RegisterTopic("topic-1", 3)
}
