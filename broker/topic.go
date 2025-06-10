package broker

import (
	"errors"
	"io"
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
	ID              uint
	Data            chan []byte
	errChan         chan error
	doneChan        chan struct{}
	killingConsumer chan *ConsumerGroup
	ConsumerGroups  map[string]*ConsumerGroup
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

func (p *Partition) registerNewConsumer(t *topic, cg *ConsumerGroup) error {
	log.Print("registering consumer", cg)
	p.ConsumerGroups[cg.name] = cg
	t.cgConnectionBoundCounter[cg][cg.connections[0]]++
	go func(t *topic, partition *Partition) {
		for {
			select {
			case payload := <-partition.Data:
				for _, cg := range partition.ConsumerGroups {
					_, err := cg.connections[0].Conn.Write(payload)
					if err != nil {
						log.Err(err).Msg("error when sending message")
					}
				}
			case <-p.doneChan:
				for _, cg := range partition.ConsumerGroups {
					for _, connection := range cg.connections {
						connection.Conn.Close()
					}
					delete(partition.ConsumerGroups, cg.name)
				}
				return
			case err := <-p.errChan:
				log.Err(err)
			case cg := <-p.killingConsumer:
				t.Mutex.Lock()
				delete(p.ConsumerGroups, cg.name)
				delete(t.cgConnectionBoundCounter[cg], cg.connections[0])
				t.cgCounter[cg.name]--
				t.Mutex.Unlock()
			}
		}
	}(t, p)
	for _, consumer := range p.ConsumerGroups {
		go func(partition *Partition, consumer *ConsumerGroup) {
			_, err := io.ReadAll(consumer.connections[0].Conn)
			if err != nil {
				p.errChan <- err
			}
			log.Print("killling the consumer")
			p.killingConsumer <- consumer
			for _, connection := range consumer.connections {
				connection.Conn.Close()
			}
		}(p, consumer)
	}
	return nil
}

func (p *Partition) send(data []byte) {
	p.Data <- data
}

type topic struct {
	name                     string
	partition                []Partition
	Producer                 []*Connection
	nextPartition            int
	cgCounter                map[string]int
	cgConnectionBoundCounter map[*ConsumerGroup]map[*Connection]int
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
		name:                     name,
		partition:                part,
		Producer:                 make([]*Connection, 1024),
		cgCounter:                make(map[string]int),
		cgConnectionBoundCounter: make(map[*ConsumerGroup]map[*Connection]int),
	}
}

func (t *topic) registerNewConsumer(cg *ConsumerGroup) *topic {
	if _, ok := t.cgCounter[cg.name]; !ok {
		t.cgCounter[cg.name] = 0
	}
	if _, ok := t.cgConnectionBoundCounter[cg]; !ok {
		t.cgConnectionBoundCounter[cg] = map[*Connection]int{}
		t.cgConnectionBoundCounter[cg][cg.connections[0]] = 0
	}
	t.cgConnectionBoundCounter[cg][cg.connections[0]]++
	t.cgCounter[cg.name]++
	if t.cgCounter[cg.name] > len(t.partition) {
		log.Info().Msg("number of consumer instance exceed the topic partition number, instance cancelled to join the cluster")
		for _, connection := range cg.connections {
			connection.Conn.Close()
		}
		return nil
	}
	t.rebalance(cg)
	return t
}

func (t *topic) registerNewProducer(conn *Connection) *topic {
	t.Producer = append(t.Producer, conn)
	return t
}

func (t *topic) replaceConnection(cg *ConsumerGroup, partition *Partition) {
	numConsumer := t.cgCounter[cg.name]
	partitionLength := len(t.partition)

	if numConsumer == 1 {
		log.Print("num consumer 1 ", numConsumer)
		for i := 0; i < partitionLength; i++ {
			t.partition[i].registerNewConsumer(t, cg)
		}
		return
	}

	var minnConn *Connection
	i := 999999
	for _, mapConnection := range t.cgConnectionBoundCounter {
		for connection, counter := range mapConnection {
			if counter < i {
				minnConn = connection
			}
		}
	}
	log.Print("replacing the consumer with ", minnConn)
	t.cgConnectionBoundCounter[cg][minnConn]++
	partition.ConsumerGroups[cg.name].connections = []*Connection{minnConn}
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
			t.partition[i].registerNewConsumer(t, cg)
		}
		return
	}
	i := 0
	counter := 0
	consumer := 1
	for i < partitionLength {
		candidate := t.partition[i]
		if consumer == numConsumer {
			break
		}

		for j := 0; j < occurance; j++ {
			t.partition[i].registerNewConsumer(t, candidate.ConsumerGroups[cg.name])
			i++
			counter++
		}
		consumer++
	}
	for counter < partitionLength {
		t.partition[counter].registerNewConsumer(t, cg)
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
