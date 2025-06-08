package handler

import (
	"bufio"
	"io"
	"net"
)

type Connection struct {
	conn   net.Conn
	reader io.Reader
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
}
