package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/socketspace-jihad/arusio/message"
)

func main() {

	conn, err := net.Dial("tcp", ":3030")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	log.Print("Arusio - Blazingly fast messaging queue system (producer console)")
	log.Print("--------")

	topic := []byte("topic-1")
	topicLength := len(topic)

	msgBuff := make([]byte, 4+topicLength)
	binary.BigEndian.PutUint32(msgBuff[:4], uint32(topicLength))
	copy(msgBuff[4:4+topicLength], topic)
	_, err = conn.Write(msgBuff)
	log.Print("Sending to register topic")

	for {
		fmt.Print("->")
		text, _ := reader.ReadBytes('\n')

		msg := &message.Message{
			Topic:         topic,
			Payload:       text,
			TopicLength:   uint32(topicLength),
			PayloadLength: uint64(len(text)),
		}
		log.Print("sending", msg)
		msgBuff := make([]byte, 8+len(msg.Payload))
		binary.BigEndian.PutUint64(msgBuff[:8], uint64(len(text)))
		copy(msgBuff[8:], msg.Payload)
		_, err = conn.Write(msgBuff)

		if err != nil {
			panic(err)
		}
	}
}
