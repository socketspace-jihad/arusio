package broker

import (
	"io"
	"sync"

	"github.com/rs/zerolog/log"
)

type ConsumerGroup struct {
	consumers   []*Consumer
	partitions  []*Partition
	assignments map[*Partition]*Consumer
	mu          sync.Mutex
}

func NewConsumerGroup(partitions []*Partition) *ConsumerGroup {
	return &ConsumerGroup{
		consumers:   make([]*Consumer, 0),
		partitions:  partitions,
		assignments: make(map[*Partition]*Consumer),
		mu:          sync.Mutex{},
	}
}

func (cg *ConsumerGroup) AddConsumer(c *Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.consumers = append(cg.consumers, c)
	cg.rebalance()
}

func (cg *ConsumerGroup) RemoveConsumer(c *Consumer) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	newList := make([]*Consumer, 0, len(cg.consumers)-1)
	for _, cons := range cg.consumers {
		if cons != c {
			newList = append(newList, cons)
		} else {
			cons.closeConsumer <- struct{}{}
		}
	}
	cg.consumers = newList
	cg.rebalance()
}

func (cg *ConsumerGroup) rebalance() {
	if len(cg.consumers) == 0 {
		return
	}
	for i, partition := range cg.partitions {
		consumer := cg.consumers[i%len(cg.consumers)]
		cg.assignments[partition] = consumer
		consumer.assignPartition <- partition
	}
}

func (cg *ConsumerGroup) MonitorConsumer() {
	for _, consumer := range cg.consumers {
		go func(consumer *Consumer) {
			_, err := io.ReadAll(consumer.connections.conn)
			if err != nil {
				log.Err(err)
			}
			cg.RemoveConsumer(consumer)
			cg.rebalance()
		}(consumer)
	}
}
