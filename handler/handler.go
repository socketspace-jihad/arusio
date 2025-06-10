package handler

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

	"github.com/rs/zerolog/log"

	"github.com/socketspace-jihad/arusio/broker"
)

var (
	connectionPool          = make(chan *broker.Connection, 1024)
	NUM_WORKER              = 5
	errReadLengthBuff error = errors.New("error read length buffer")
)

func handle(conn *broker.Connection, id int) {
	defer conn.Conn.Close()

	var (
		topicLenBuf   [4]byte
		payloadLenBuf [8]byte
	)
	if _, err := io.ReadFull(conn.Reader, topicLenBuf[:]); err != nil {
		log.Err(errReadLengthBuff)
		return
	}
	topicLen := binary.BigEndian.Uint32(topicLenBuf[:])

	// Guard against unreasonable topic length
	if topicLen == 0 || topicLen > 1<<20 {
		log.Err(errReadLengthBuff)
		return
	}

	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(conn.Reader, topic); err != nil {
		log.Err(errReadLengthBuff)
		return
	}

	log.Print("producer registered to topic ", string(topic))
	topicConnection, err := broker.RegisterProducertoTopic(broker.NewConnection(conn.Conn), string(topic))
	if err != nil {
		log.Err(err).Msg("error when registering producer to topic")
	}

	for {
		if _, err := io.ReadFull(conn.Reader, payloadLenBuf[:]); err != nil {
			log.Err(errReadLengthBuff)
			return
		}
		payloadLen := binary.BigEndian.Uint64(payloadLenBuf[:])

		if payloadLen == 0 || payloadLen > 1<<30 {
			log.Err(errReadLengthBuff)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(conn.Reader, payload); err != nil {
			log.Err(errReadLengthBuff)
			return
		}

		data := make([]byte, 8+payloadLen)
		copy(data[:8], payloadLenBuf[:])
		copy(data[8:], payload)
		if err := topicConnection.Publish(data); err != nil {
			log.Err(err).Int("worker_id", id).Msg("error when publishing message")
		}
	}
}

func worker(id int) {
	for conn := range connectionPool {
		go handle(conn, id)
	}
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
	log.Info().Str("addr", addr).Msg("publisher is listening on")
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		connectionPool <- broker.NewConnection(conn)
	}
}
