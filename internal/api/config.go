package api

import "github.com/svetsrebrev/tms/internal/utils"

type Config struct {
	// API service host:port
	ListenAddress string

	// Database nodes
	ScyllaNodes []string

	// Database keyspace
	Keyspace string
}

// TODO: return error if loading congiguration fails. For now just use defaults
func LoadConfig() *Config {
	return &Config{
		ListenAddress: utils.GetEnvOrDefaultStr("API_HOST", ":3369"),
		ScyllaNodes:   utils.GetEnvOrDefaultArray("SCYLLA_NODES", "localhost:19042", ","),
		Keyspace:      utils.GetEnvOrDefaultStr("KEYSPACE", "tms"),
	}
}
