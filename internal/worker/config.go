package worker

import (
	"math/rand"
	"runtime"

	"github.com/svetsrebrev/tms/internal/utils"
)

const defaulrPartition = 0

type Config struct {
	// Kakfa brockers list
	KafkaBrokers []string

	// Kafka tasks topics
	TaskTopics []string

	// Workers consumer group
	ConsumersGroup string

	// Database nodes
	ScyllaNodes []string

	// Database keyspace
	Keyspace string

	// Partitions to re-schedule recurring tasks on
	SchedulePartitiones []int32

	// Max concurrent tasks execution per worker node
	ConcurentTasks int

	// Worker heartbeat interval in seconds
	HeartBeatIntervalSec int

	// If a worker did not heartbeat in WorkerTimeoutSec interval it is declared abandoned and he's tasks are rescheduled
	WorkerTimeoutSec int
}

// TODO: return error if loading congiguration fails. For now just use defaults
func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:   utils.GetEnvOrDefaultArray("KAFKA_BROKERS", "localhost:9092", ","),
		TaskTopics:     utils.GetEnvOrDefaultArray("TASKS_TOPICS", "tms", ","),
		ConsumersGroup: utils.GetEnvOrDefaultStr("CONSUMERS_GROUP", "workers"),

		ScyllaNodes:         utils.GetEnvOrDefaultArray("SCYLLA_NODES", "localhost:19042", ","),
		Keyspace:            utils.GetEnvOrDefaultStr("KEYSPACE", "tms"),
		SchedulePartitiones: utils.CollectAll(utils.GetEnvOrDefaultIntArray("SCHEDULE_PARTITIONES", "1,2,3", ","), func(i int) int32 { return int32(i) }),

		ConcurentTasks: utils.GetEnvOrDefaultInt("CONCURENT_TASKS", 8*runtime.NumCPU()),

		HeartBeatIntervalSec: utils.GetEnvOrDefaultInt("HEART_BEAT_INTERVAL_SEC", 3),
		WorkerTimeoutSec:     utils.GetEnvOrDefaultInt("WORKER_TIMEOUT_SEC", 9),
	}
}

func (c *Config) GetSchedulePartition() int32 {
	partitiones := len(c.SchedulePartitiones)
	if partitiones < 1 {
		return defaulrPartition
	}
	return c.SchedulePartitiones[rand.Int31n(int32(partitiones))]
}
