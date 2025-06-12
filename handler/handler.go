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

func handle(conn net.Conn) {
	defer conn.Close()

	var (
		topicLenBuf   [4]byte
		payloadLenBuf [8]byte
	)
	if _, err := io.ReadFull(conn, topicLenBuf[:]); err != nil {
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
	if _, err := io.ReadFull(conn, topic); err != nil {
		log.Err(errReadLengthBuff)
		return
	}

	p := broker.NewProducer(broker.NewConnection(conn))
	err := broker.RegisterProducertoTopic(p, string(topic))
	if err != nil {
		log.Err(err).Msg("error when registering producer to topic")
	}

	for {
		if _, err := io.ReadFull(conn, payloadLenBuf[:]); err != nil {
			log.Err(errReadLengthBuff)
			return
		}
		payloadLen := binary.BigEndian.Uint64(payloadLenBuf[:])
		log.Print("length of payload ", payloadLen)

		if payloadLen == 0 || payloadLen > 1<<30 {
			log.Err(errReadLengthBuff)
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			log.Err(errReadLengthBuff)
			return
		}
		log.Print("payload ", string(payload), " with bytes ", payload)

		data := make([]byte, 8+payloadLen)
		copy(data[:8], payloadLenBuf[:])
		copy(data[8:], payload)
		log.Print("sending data to consumer", data)
		p.Send(data)
	}
}

func Serve(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	log.Info().Str("addr", addr).Msg("publisher is listening on")
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		go handle(conn)
	}
}
