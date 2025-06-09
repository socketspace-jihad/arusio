package broker

import (
	"errors"

	"github.com/socketspace-jihad/arusio/message"
)

var (
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
		consumerPool[topic] = NewConnectionPool(conn, topic)
		consumerPool[topic].add(conn, consumerGroup, consumerPool[topic])
		return
	}
	if consumerPool[topic] == nil {
		consumerPool[topic] = NewConnectionPool(conn, topic)
		consumerPool[topic].add(conn, consumerGroup, consumerPool[topic])
		return
	}
	if _, ok := consumerPool[topic].consumerGroups[consumerGroup]; !ok {
		consumerPool[topic].consumerGroups[consumerGroup] = NewConsumerGroup(consumerGroup)
	}
	consumerPool[topic].consumerGroups[consumerGroup].add(conn, consumerPool[topic])
}

func Publish(data *message.Message) error {
	brokerEventPool <- data
	return nil
}
