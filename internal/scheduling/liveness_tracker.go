package scheduling

import (
	"context"
	"time"

	"github.com/svetsrebrev/tms/internal/queue"
	"github.com/svetsrebrev/tms/internal/store"
	"github.com/svetsrebrev/tms/internal/store/models"
	"github.com/svetsrebrev/tms/internal/utils"
)

// LivenessTracker checks for inactive (crashed) workers and re-schedule the tasks assigned to these workers
type LivenessTracker struct {
	store    store.Store
	producer queue.Producer
	cfg      *Config
}

func NewLivenessTracker(producer queue.Producer, store store.Store, cfg *Config) *LivenessTracker {
	return &LivenessTracker{
		store:    store,
		producer: producer,
		cfg:      cfg,
	}
}

// Run checks workers heartbeat and reschedule crashed worker's tasks
func (lt *LivenessTracker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(time.Duration(lt.cfg.CheckWorkersSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := lt.checkAbandonedWorkers(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (lt *LivenessTracker) checkAbandonedWorkers(ctx context.Context) error {
	const maxAbandonedWorkers = 100
	for {
		// 1. Get list of inactive (dead) workers
		lastHartbeat := time.Now().UTC().Add(-time.Duration(lt.cfg.WorkerTimeoutSec) * time.Second)
		workers, err := lt.store.GetInactiveWorkers(ctx, lastHartbeat, maxAbandonedWorkers)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to check abandoned workers")
			return err
		}
		if len(workers) == 0 {
			return nil
		}

		// 2. Get the tasks assigned to inactive workers
		workerIds := utils.CollectAll(workers, func(w *models.WorkersStruct) string { return w.Id })
		tasks, err := lt.store.GetWorkersAssignedTasks(ctx, workerIds)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to get workers assigned tasks")
			return err
		}

		// 3. Re-enqueue tasks owned by inactive workers
		err = lt.producer.Produce(ctx, tasks.TaskIds())
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to get workers assigned tasks")
			return err
		}

		// 4. Mark the tasks status as pending for execution
		err = lt.store.ReenqueueWorkerTasks(ctx, tasks)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to mark tasks as restarted")
			return err
		}

		workersToDelete := []*models.WorkersStruct{}
		for _, worker := range workers {
			if time.Since(worker.LastHeartbeat) > 10*time.Minute {
				workersToDelete = append(workersToDelete, worker)
			}
		}

		if len(workersToDelete) > 0 {
			err = lt.store.DeleteWorkers(ctx, workers)
			if err != nil {
				utils.LogIfNotCancelled(err, "Unable to delete abandoned workers records")
			}
		}
	}
}
