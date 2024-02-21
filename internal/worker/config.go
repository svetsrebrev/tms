package worker

import (
	"runtime"

	"github.com/svetsrebrev/tms/internal/utils"
)

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

		ScyllaNodes: utils.GetEnvOrDefaultArray("SCYLLA_NODES", "localhost:19042", ","),
		Keyspace:    utils.GetEnvOrDefaultStr("KEYSPACE", "tms"),

		ConcurentTasks: utils.GetEnvOrDefaultInt("CONCURENT_TASKS", 8*runtime.NumCPU()),

		HeartBeatIntervalSec: utils.GetEnvOrDefaultInt("HEART_BEAT_INTERVAL_SEC", 3),
		WorkerTimeoutSec:     utils.GetEnvOrDefaultInt("WORKER_TIMEOUT_SEC", 9),
	}
}
