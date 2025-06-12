package broker

import (
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
		log.Info().Str("consumer_id", consumer.ID).Uint("partition_id", partition.ID).Msg("(re)assign  consumer to partition")
		if cg.assignments[partition] == nil {
			cg.assignments[partition] = consumer
		}
		if cg.assignments[partition] != consumer {
			cg.assignments[partition].quitPartition[partition.ID] <- struct{}{}
		}
		cg.assignments[partition] = consumer
		cg.assignments[partition].assignPartition <- partition
	}
	log.Print("current listener", cg.assignments)
	log.Print("finish rebalancing")
}
