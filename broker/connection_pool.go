package broker

import "sync"

type ConnectionPool struct {
	consumerGroups map[string]*ConsumerGroup
	mtx            sync.Mutex
}

func NewConnectionPool(conn *Connection) *ConnectionPool {
	return &ConnectionPool{
		consumerGroups: make(map[string]*ConsumerGroup),
		mtx:            sync.Mutex{},
	}
}

func (cp *ConnectionPool) add(conn *Connection, consumerGroup string) {
	if _, ok := cp.consumerGroups[consumerGroup]; !ok {
		cp.consumerGroups[consumerGroup] = NewConsumerGroup()
	}
	cp.consumerGroups[consumerGroup].add(conn)
}
