package broker

import (
	"bufio"
	"io"
	"net"
)

type Connection struct {
	conn     net.Conn
	reader   io.Reader
	doneChan chan struct{}
	open     bool
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		open:     true,
		doneChan: make(chan struct{}),
	}
}
