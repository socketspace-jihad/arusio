package broker

import "github.com/alitto/pond/v2"

var (
	TopicPool = map[string]*Topic{}
)

type Topic struct {
	ID            uint
	name          string
	partitions    []*Partition
	nextPartition int
	Worker        pond.Pool
}

func NewTopic(id uint, name string, numPartition int) *Topic {
	partitions := []*Partition{}
	for i := 0; i < 3; i++ {
		partitions = append(partitions, NewPartition(uint(i), 1000))
	}
	return &Topic{
		ID:         id,
		name:       name,
		partitions: partitions,
		Worker:     pond.NewPool(100),
	}
}

func init() {
	TopicPool["topic-1"] = NewTopic(1, "topic-1", 3)
}
