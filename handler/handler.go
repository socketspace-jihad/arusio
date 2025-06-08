package handler

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/rs/zerolog/log"

	"github.com/socketspace-jihad/arusio/broker"
	"github.com/socketspace-jihad/arusio/message"
)

var (
	connectionPool = make(chan *Connection, 1024)
	NUM_WORKER     = 5
)

func handle(conn *Connection, id int) {
	defer conn.conn.Close()

	var (
		topicLenBuf   [4]byte
		payloadLenBuf [8]byte
	)

	for {
		// Read topic length (4 bytes)
		if _, err := io.ReadFull(conn.reader, topicLenBuf[:]); err != nil {
			log.Print("read topic length:", err)
			return
		}
		topicLen := binary.BigEndian.Uint32(topicLenBuf[:])

		// Guard against unreasonable topic length
		if topicLen == 0 || topicLen > 1<<20 {
			log.Print("invalid topic length")
			return
		}

		topic := make([]byte, topicLen)
		if _, err := io.ReadFull(conn.reader, topic); err != nil {
			log.Print("read topic:", err)
			return
		}

		if _, err := io.ReadFull(conn.reader, payloadLenBuf[:]); err != nil {
			log.Print("read payload length:", err)
			return
		}
		payloadLen := binary.BigEndian.Uint64(payloadLenBuf[:])

		if payloadLen == 0 || payloadLen > 1<<30 {
			log.Print("invalid payload length")
			return
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(conn.reader, payload); err != nil {
			log.Print("read payload:", err)
			return
		}

		msg := &message.Message{
			Topic:         topic,
			Payload:       payload,
			TopicLength:   topicLen,
			PayloadLength: payloadLen,
		}
		if err := broker.Publish(msg); err != nil {
			log.Printf("publish error: %v (worker id: %d)", err, id)
			return
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
	log.Print("Listening at", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		connectionPool <- NewConnection(conn)
	}
}
