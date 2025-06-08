package broker

import (
	"errors"
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
}

func NewConsumerGroup() *ConsumerGroup {
	return &ConsumerGroup{
		connections: []*Connection{},
		mtx:         sync.Mutex{},
	}
}

func (cg *ConsumerGroup) add(conn *Connection) {
	cg.mtx.Lock()
	cg.connections = append(cg.connections, conn)
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

	_, err = conn.conn.Write(data)
	if err != nil {
		log.Print(err)
		conn.conn.Close()
		cg.connections = append(cg.connections[:cg.targetIdx], cg.connections[cg.targetIdx+1:]...)
	}
	cg.targetIdx = (cg.targetIdx + 1) % len(cg.connections)
	return err
}
