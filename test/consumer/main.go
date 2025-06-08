package main

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/rs/zerolog/log"
)

func main() {
	conn, err := net.Dial("tcp", ":3031")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	topic := []byte("topic-1")
	topicLengthBytes := make([]byte, 4)
	topicLength := uint32(len(topic))
	binary.BigEndian.PutUint32(topicLengthBytes, topicLength)

	consumerGroup := []byte("group.svc-b")
	consumerGroupLengthBytes := make([]byte, 4)
	consumerGroupLength := uint32(len(consumerGroup))
	binary.BigEndian.PutUint32(consumerGroupLengthBytes, consumerGroupLength)

	data := make([]byte, 4+topicLength+4+consumerGroupLength)
	copy(data[:4], topicLengthBytes)
	copy(data[4:4+topicLength], topic)
	copy(data[4+topicLength:4+topicLength+4], consumerGroupLengthBytes)
	copy(data[4+topicLength+4:], consumerGroup)
	log.Print("sending", string(data))

	_, err = conn.Write(data)
	if err != nil {
		panic(err)
	}
	log.Print("consuming data from topic-1")
	for {
		payload := make([]byte, 8)
		_, err := io.ReadFull(conn, payload)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}

		payloadLength := binary.BigEndian.Uint64(payload)
		data := make([]byte, payloadLength)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			log.Fatal().Msg(err.Error())
		}
		if string(data) != "" {
			log.Print(string(data))
		}
	}
}
