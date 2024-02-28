package scheduling

import "github.com/svetsrebrev/tms/internal/utils"

type Config struct {
	// Kakfa brockers list
	KafkaBrokers []string

	// Kafka topic name for tasks
	TaskTopic string

	// Database nodes
	ScyllaNodes []string

	// Database keyspace
	Keyspace string

	// Interval in seconds to check if a task should be scheduled for execution
	CheckTasksScheduleSec int

	// Maximum number of tasks to pool and schedule for execution
	MaxTasksPoll int

	// Which scheduling partitions this instance of scheduler service should consume
	SchedulePartitiones []int32

	// Interval in seconds to check if workers are still alive
	CheckWorkersSec int

	// If a worker did not heartbeat in WorkerTimeoutSec interval it is declared abandoned and he's tasks are rescheduled
	WorkerTimeoutSec int
}

// TODO: return error if loading congiguration fails. For now just use defaults
func LoadConfig() *Config {
	return &Config{
		KafkaBrokers: utils.GetEnvOrDefaultArray("KAFKA_BROKERS", "localhost:9092", ","),
		TaskTopic:    utils.GetEnvOrDefaultStr("TASKS_TOPIC", "tms"),

		ScyllaNodes: utils.GetEnvOrDefaultArray("SCYLLA_NODES", "localhost:19042", ","),
		Keyspace:    utils.GetEnvOrDefaultStr("KEYSPACE", "tms"),

		MaxTasksPoll:          utils.GetEnvOrDefaultInt("MAX_POLL_RECORDS", 10000),
		CheckTasksScheduleSec: utils.GetEnvOrDefaultInt("CHECK_TASKS_SCHEDULE_SEC", 1),
		SchedulePartitiones:   utils.CollectAll(utils.GetEnvOrDefaultIntArray("SCHEDULE_PARTITIONES", "1,2,3", ","), func(i int) int32 { return int32(i) }),

		CheckWorkersSec:  utils.GetEnvOrDefaultInt("CHECK_WORKERS_SEC", 5),
		WorkerTimeoutSec: utils.GetEnvOrDefaultInt("WORKER_TIMEOUT_SEC", 15),
	}
}
