package broker

import (
	"bufio"
	"net"
)

type Connection struct {
	ID     uint
	conn   net.Conn
	reader bufio.Reader
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:   conn,
		reader: *bufio.NewReader(conn),
	}
}
