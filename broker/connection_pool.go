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
