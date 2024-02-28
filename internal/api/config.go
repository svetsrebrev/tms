package api

import (
	"math/rand"

	"github.com/svetsrebrev/tms/internal/utils"
)

const defaulrPartition = 0

type Config struct {
	// API service host:port
	ListenAddress string

	// Database nodes
	ScyllaNodes []string

	// Database keyspace
	Keyspace string

	// Partitions to schedule tasks on
	SchedulePartitiones []int32
}

// TODO: return error if loading congiguration fails. For now just use defaults
func LoadConfig() *Config {
	return &Config{
		ListenAddress:       utils.GetEnvOrDefaultStr("API_HOST", ":3369"),
		ScyllaNodes:         utils.GetEnvOrDefaultArray("SCYLLA_NODES", "localhost:19042", ","),
		Keyspace:            utils.GetEnvOrDefaultStr("KEYSPACE", "tms"),
		SchedulePartitiones: utils.CollectAll(utils.GetEnvOrDefaultIntArray("SCHEDULE_PARTITIONES", "1,2,3", ","), func(i int) int32 { return int32(i) }),
	}
}

func (c *Config) GetSchedulePartition() int32 {
	partitiones := len(c.SchedulePartitiones)
	if partitiones < 1 {
		return defaulrPartition
	}
	return c.SchedulePartitiones[rand.Int31n(int32(partitiones))]
}
