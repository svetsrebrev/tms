package main

import (
	"github.com/rs/zerolog/log"
	"github.com/svetsrebrev/tms/internal/api"
	"github.com/svetsrebrev/tms/internal/store"
)

func main() {
	cfg := api.LoadConfig()

	store, err := store.NewScyllaStore(cfg.ScyllaNodes, cfg.Keyspace)
	if err != nil {
		log.Error().Err(err).Msg("Unable to create Scylla store")
		return
	}
	defer store.Close()

	// Run API service
	err = api.Run(store, cfg)
	log.Error().Err(err).Msgf("API proces failed")
}
