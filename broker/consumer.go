package broker

import (
	"io"
	"sync"

	"github.com/alitto/pond/v2"
	"github.com/rs/zerolog/log"
)

type Consumer struct {
	ID              string
	connections     *Connection
	assignPartition chan *Partition
	quitPartition   map[uint]chan struct{}
	closeConsumer   chan struct{}
	rebalanceSignal chan struct{}
	Worker          pond.Pool
	mu              sync.Mutex
}

func (c *Consumer) Consume() {
	for {
		select {
		case partition := <-c.assignPartition:
			c.mu.Lock()
			c.quitPartition[partition.ID] = make(chan struct{})
			c.mu.Unlock()
			go func(c *Consumer, partition *Partition) {
				for {
					select {
					case data := <-partition.Data:
						_, err := c.connections.conn.Write(data)
						if err != nil {
							if err != io.EOF {
								log.Err(err)
							}
						}
					case <-c.quitPartition[partition.ID]:
						log.Info().Str("consumer_id", c.ID).Uint("partition_id", partition.ID).Msg("consumer stop listening to several partitions due to rebalancing")
						return
					}
				}
			}(c, partition)
		case <-c.closeConsumer:
			c.connections.conn.Close()
			close(c.assignPartition)
			for _, ch := range c.quitPartition {
				close(ch)
			}
			close(c.closeConsumer)
			return
		}
	}
}

func NewConsumer(id string, conn *Connection) *Consumer {
	return &Consumer{
		ID:              id,
		connections:     conn,
		assignPartition: make(chan *Partition, 1000),
		quitPartition:   make(map[uint]chan struct{}),
		closeConsumer:   make(chan struct{}),
		Worker:          pond.NewPool(100, pond.WithQueueSize(100)),
		mu:              sync.Mutex{},
		rebalanceSignal: make(chan struct{}),
	}
}

func (c *Consumer) MonitorConsumer(cg *ConsumerGroup) {
	_, err := io.ReadAll(c.connections.conn)
	if err != nil {
		log.Err(err)
	}
	cg.RemoveConsumer(c)
}
