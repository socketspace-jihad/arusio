package broker

import (
	"encoding/binary"
	"sync"

	"github.com/alitto/pond/v2"
)

type Consumer struct {
	ID              string
	connections     *Connection
	assignPartition chan *Partition
	quitPartition   map[uint]chan struct{}
	closeConsumer   chan struct{}
	Worker          pond.Pool
	mu              sync.Mutex
}

func (c *Consumer) Consume() {
	for {
		select {
		case partition := <-c.assignPartition:
			go func() {
				c.mu.Lock()
				c.quitPartition[partition.ID] = make(chan struct{})
				c.mu.Unlock()
				for {
					select {
					case data := <-partition.Data:
						msgBuff := make([]byte, 8+len(data))
						binary.BigEndian.PutUint64(msgBuff[:8], uint64(len(data)))
						copy(msgBuff[8:], data)
						c.connections.conn.Write(msgBuff)
					case <-c.quitPartition[partition.ID]:
						return
					}
				}
			}()
		case <-c.closeConsumer:
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
	}
}
