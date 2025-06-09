package broker

import "sync"

type ConnectionPool struct {
	consumerGroups map[string]*ConsumerGroup
	mtx            sync.Mutex
	topicName      string
}

func NewConnectionPool(conn *Connection, topicName string) *ConnectionPool {
	return &ConnectionPool{
		consumerGroups: make(map[string]*ConsumerGroup),
		mtx:            sync.Mutex{},
		topicName:      topicName,
	}
}

func (cp *ConnectionPool) add(conn *Connection, consumerGroup string, connPool *ConnectionPool) {
	if _, ok := cp.consumerGroups[consumerGroup]; !ok {
		cp.consumerGroups[consumerGroup] = NewConsumerGroup(consumerGroup)
	}
	cp.consumerGroups[consumerGroup].add(conn, connPool)
}
