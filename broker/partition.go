package broker

import "github.com/alitto/pond/v2"

type Partition struct {
	ID     uint
	Data   chan []byte
	Worker pond.Pool
}

func NewPartition(id uint, bufferSize int) *Partition {
	return &Partition{
		ID:     id,
		Data:   make(chan []byte, bufferSize),
		Worker: pond.NewPool(1000),
	}
}
