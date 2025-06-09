package broker

import (
	"bufio"
	"io"
	"net"
)

type Connection struct {
	Conn     net.Conn
	Reader   io.Reader
	doneChan chan struct{}
	open     bool
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		Conn:     conn,
		Reader:   bufio.NewReader(conn),
		open:     true,
		doneChan: make(chan struct{}),
	}
}
