package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/rs/zerolog/log"
)

var (
	errReadingBuff error = errors.New("error reading buff")
	NUM_WORKER           = 50
)

func worker(id int) {
	for event := range brokerEventPool {
		consumer, err := getConsumers(string(event.Topic))
		if err != nil {
			log.Err(err).Int("worker_id", id).Msg("error getting consumer")
			continue
		}
		data := make([]byte, 8+len(event.Payload))
		binary.BigEndian.PutUint64(data, event.PayloadLength)
		for _, cg := range consumer.consumerGroups {
			go func(cg *ConsumerGroup, payload []byte) {
				copy(data[8:], payload)
				err := cg.send(data)
				for err != nil {
					switch err {
					case errNoConnectionsAvailable:
						log.Print("err no connections available")
						break
					default:
						log.Print("error to specific connection, retrying to another node")
						copy(data[8:], payload)
						err = cg.send(data)
					}
				}
			}(cg, event.Payload)
		}
	}
}

func handle(conn net.Conn) {
	var lengthBuf [4]byte

	if _, err := io.ReadFull(conn, lengthBuf[:]); err != nil {
		log.Err(err).Msg("error reading buf length")
		return
	}
	topicLen := binary.BigEndian.Uint32(lengthBuf[:])
	if topicLen == 0 || topicLen > 1<<20 {
		log.Err(errReadingBuff).Msg("error reading topic length")
		return
	}

	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topic); err != nil {
		log.Err(err).Msg("error reading topic")
		return
	}

	if _, err := io.ReadFull(conn, lengthBuf[:]); err != nil {
		log.Err(err).Msg("error reading topic")
		return
	}
	groupLen := binary.BigEndian.Uint32(lengthBuf[:])
	if groupLen == 0 || groupLen > 1<<20 {
		log.Err(errReadingBuff).Msg("error reading consumer group length")
		return
	}

	consumerGroup := make([]byte, groupLen)
	if _, err := io.ReadFull(conn, consumerGroup); err != nil {
		log.Err(err).Msg("error reading consumer group")
		return
	}

	log.Info().Str("consumer_group", string(consumerGroup)).Str("topic", string(topic)).Msg("registering new connections")

	registerConsumers(string(topic), string(consumerGroup), NewConnection(conn))
}

func Serve(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for i := 0; i < NUM_WORKER; i++ {
		go worker(i)
	}
	log.Info().Str("addr", addr).Msg("broker for consuming data is listening on port")
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		log.Info().Str("addr", conn.RemoteAddr().String()).Msg("incoming connection from")
		go handle(conn)
	}
}
