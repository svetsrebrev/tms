package main

import (
	"context"

	"github.com/svetsrebrev/tms/cmd/common"
	"github.com/svetsrebrev/tms/internal/queue"
	"github.com/svetsrebrev/tms/internal/scheduling"
	"github.com/svetsrebrev/tms/internal/store"
	"github.com/svetsrebrev/tms/internal/utils"
)

func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	done := common.RunUntilCancelled(ctx, "Scheduler service", runService)
	kill := common.TrackInterupts(cancelFunc, done)

	select {
	case <-done:
	case <-kill:
	}
}

func runService(ctx context.Context) error {
	cfg := scheduling.LoadConfig()

	producer, err := queue.NewKafkaProducer(cfg.KafkaBrokers, cfg.TaskTopic)
	if err != nil {
		utils.LogIfNotCancelled(err, "Unable to create Kafka producer")
		return err
	}
	defer producer.Close()

	store, err := store.NewScyllaStore(cfg.ScyllaNodes, cfg.Keyspace)
	if err != nil {
		utils.LogIfNotCancelled(err, "Unable to create Scylla store")
		return err
	}
	defer store.Close()

	// Run Scheduler service
	srv := scheduling.NewScheduler(producer, store, cfg)
	return srv.Run(ctx)
}
