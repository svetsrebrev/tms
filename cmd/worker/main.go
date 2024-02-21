package main

import (
	"context"

	"github.com/svetsrebrev/tms/cmd/common"
	"github.com/svetsrebrev/tms/internal/queue"
	"github.com/svetsrebrev/tms/internal/store"
	"github.com/svetsrebrev/tms/internal/utils"
	"github.com/svetsrebrev/tms/internal/worker"
)

func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	done := common.RunUntilCancelled(ctx, "Worker process", runService)
	kill := common.TrackInterupts(cancelFunc, done)

	select {
	case <-done:
	case <-kill:
	}
}

func runService(ctx context.Context) error {
	cfg := worker.LoadConfig()

	consumer, err := queue.NewKafkaConsumer(cfg.KafkaBrokers, cfg.TaskTopics, cfg.ConsumersGroup)
	if err != nil {
		utils.LogIfNotCancelled(err, "Unable to create kafka consumer")
		return err
	}
	defer consumer.Close()

	store, err := store.NewScyllaStore(cfg.ScyllaNodes, cfg.Keyspace)
	if err != nil {
		utils.LogIfNotCancelled(err, "Unable to create Scylla store")
		return err
	}
	defer store.Close()

	// Run Worker process
	srv := worker.NewWorker(consumer, store, cfg)
	return srv.Run(ctx)
}
