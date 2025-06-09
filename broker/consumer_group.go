package broker

import (
	"errors"
	"io"
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

func NewConsumerGroup(name string) *ConsumerGroup {
	return &ConsumerGroup{
		connections: []*Connection{},
		mtx:         sync.Mutex{},
		name:        name,
	}
}

func (cg *ConsumerGroup) add(conn *Connection, connPool *ConnectionPool) {
	cg.mtx.Lock()
	cg.connections = append(cg.connections, conn)
	go func(cg *ConsumerGroup) {
		_, err := io.ReadAll(conn.Reader)
		cg.mtx.Lock()
		defer cg.mtx.Unlock()
		if err != nil {
			log.Err(err).Msg("error when reading heartbeat from client ..")
		}
		conn.Conn.Close()
		cg.connections = append(cg.connections[:cg.targetIdx], cg.connections[cg.targetIdx+1:]...)
		if len(cg.connections) == 0 {
			delete(consumerPool[connPool.topicName].consumerGroups, cg.name)
			cg.targetIdx = 0
			return
		}
		cg.targetIdx = (cg.targetIdx + 1) % len(cg.connections)
	}(cg)
	cg.mtx.Unlock()
}

func (cg *ConsumerGroup) send(data []byte) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()
	if len(cg.connections) == 0 {
		log.Print("No connections available")
		return errNoConnectionsAvailable
	}
	conn := cg.connections[cg.targetIdx]

	var err error
	_, err = conn.Conn.Write(data)
	if err != nil {
		log.Print(err)
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
