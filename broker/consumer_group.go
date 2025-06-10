package broker

import (
	"errors"
	"net"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	errNoConnectionsAvailable error = errors.New("no connection available")
)

type ConsumerGroup struct {
	connections []*Connection
	mtx         sync.Mutex
	targetIdx   int
	name        string
	*ConnectionPool
}

func NewConsumerGroup(name string, connection net.Conn) *ConsumerGroup {
	return &ConsumerGroup{
		connections: []*Connection{NewConnection(connection)},
		mtx:         sync.Mutex{},
		name:        name,
	}
}

func (cg *ConsumerGroup) send(data []byte) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()
	if len(cg.connections) == 0 {
		log.Err(errNoConnectionsAvailable)
		return errNoConnectionsAvailable
	}
	conn := cg.connections[cg.targetIdx]

	var err error
	_, err = conn.Conn.Write(data)
	if err != nil {
		conn.Conn.Close()
		cg.connections = append(cg.connections[:cg.targetIdx], cg.connections[cg.targetIdx+1:]...)
		if len(cg.connections) == 0 {
			delete(consumerPool[cg.ConnectionPool.topicName].consumerGroups, cg.name)
			cg.targetIdx = 0
			return err
		}
	}
	cg.targetIdx = (cg.targetIdx + 1) % len(cg.connections)
	return err
}
