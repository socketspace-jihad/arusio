package broker

import (
	"errors"

	"github.com/socketspace-jihad/arusio/message"
)

var (
	NUM_WORKER      = 5
	brokerEventPool = make(chan *message.Message, 1024)
	consumerPool    = make(map[string]*ConnectionPool)
)

func getConsumers(data string) (*ConnectionPool, error) {
	if _, ok := consumerPool[data]; !ok {
		return nil, errors.New("no consumers is listening to this event")
	}
	return consumerPool[data], nil
}

func registerConsumers(topic string, consumerGroup string, conn *Connection) {
	if _, ok := consumerPool[topic]; !ok {
		consumerPool[topic] = NewConnectionPool(conn)
		consumerPool[topic].add(conn, consumerGroup)
		return
	}
	if consumerPool[topic] == nil {
		consumerPool[topic] = NewConnectionPool(conn)
		consumerPool[topic].add(conn, consumerGroup)
		return
	}
	if _, ok := consumerPool[topic].consumerGroups[consumerGroup]; !ok {
		consumerPool[topic].consumerGroups[consumerGroup] = NewConsumerGroup()
	}
	consumerPool[topic].consumerGroups[consumerGroup].add(conn)
}

func Publish(data *message.Message) error {
	brokerEventPool <- data
	return nil
}
