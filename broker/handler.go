package broker

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/rs/zerolog/log"
)

func worker(id int) {
	for event := range brokerEventPool {
		consumer, err := getConsumers(string(event.Topic))
		if err != nil {
			log.Info().Msg(err)
			continue
		}
		data := make([]byte, 8+len(event.Payload))
		binary.BigEndian.PutUint64(data, event.PayloadLength)
		for _, cg := range consumer.consumerGroups {
			go func(cg *ConsumerGroup, payload []byte) {
				copy(data[8:], payload)
				err := cg.send(data)
				for err != errNoConnectionsAvailable && err != nil {
					copy(data[8:], payload)
					err = cg.send(data)
				}
			}(cg, event.Payload)
		}
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	var lengthBuf [4]byte

	// Baca panjang topic (4 byte)
	if _, err := io.ReadFull(conn, lengthBuf[:]); err != nil {
		log.Info().Msg("read topic length:", err)
		return
	}
	topicLen := binary.BigEndian.Uint32(lengthBuf[:])
	if topicLen == 0 || topicLen > 1<<20 {
		log.Info().Msg("invalid topic length:", topicLen)
		return
	}

	// Reuse buffer (stack) jika memungkinkan
	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topic); err != nil {
		log.Info().Msg("read topic:", err)
		return
	}

	// Baca panjang consumerGroup (4 byte)
	if _, err := io.ReadFull(conn, lengthBuf[:]); err != nil {
		log.Info().Msg("read consumer group length:", err)
		return
	}
	groupLen := binary.BigEndian.Uint32(lengthBuf[:])
	if groupLen == 0 || groupLen > 1<<20 {
		log.Info().Msg("invalid group length:", groupLen)
		return
	}

	consumerGroup := make([]byte, groupLen)
	if _, err := io.ReadFull(conn, consumerGroup); err != nil {
		log.Info().Msg("read consumer group:", err)
		return
	}

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
	log.Info().Msg("Consumer pipe is listening at", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		log.Info().Msg("connection coming from ", conn.RemoteAddr())
		go handle(conn)
	}
}
