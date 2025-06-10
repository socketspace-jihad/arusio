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

func Publish(data *message.Message) error {
	brokerEventPool <- data
	return nil
}
