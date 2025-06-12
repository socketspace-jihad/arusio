package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/rs/zerolog/log"
)

var (
	errReadingBuff    error = errors.New("error reading buff")
	NUM_WORKER              = 50
	ConsumerGroupPool       = map[string]*ConsumerGroup{}
)

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
	if _, ok := TopicPool[string(topic)]; !ok {
		log.Err(errors.New("topic not found"))
		return
	}

	consumerGroupStr := string(consumerGroup)

	_, ok := ConsumerGroupPool[consumerGroupStr]
	if !ok {
		ConsumerGroupPool[consumerGroupStr] = NewConsumerGroup(TopicPool[string(topic)].partitions)
	}
	c := NewConsumer(conn.RemoteAddr().String(), NewConnection(conn))
	go c.Consume()
	go c.MonitorConsumer(ConsumerGroupPool[consumerGroupStr])
	ConsumerGroupPool[consumerGroupStr].AddConsumer(c)
}

func Serve(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
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
