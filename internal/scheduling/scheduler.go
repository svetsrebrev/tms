package scheduling

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/svetsrebrev/tms/internal/queue"
	"github.com/svetsrebrev/tms/internal/store"
)

// Scheduler checks tasks that should be started and schedules them for asynchronous execution in future
type Scheduler struct {
	store    store.Store
	producer queue.Producer
	cfg      *Config
}

func NewScheduler(producer queue.Producer, store store.Store, cfg *Config) *Scheduler {
	return &Scheduler{
		store:    store,
		producer: producer,
		cfg:      cfg,
	}
}

// Run checks tasks start-time and schedule them for execution
func (s *Scheduler) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(time.Duration(s.cfg.CheckTasksScheduleSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.checkReadyTasks(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Scheduler) checkReadyTasks(ctx context.Context) error {
	for {
		tasks, err := s.store.GetReadyTasks(ctx, s.cfg.MaxTasksPoll)
		if err != nil {
			log.Error().Err(err).Msg("Unable to retrieve 'Scheduled' tasks")
			return err
		}
		if len(tasks) == 0 {
			return nil
		}

		err = s.store.SetPendingState(ctx, tasks)
		if err != nil {
			log.Error().Err(err).Msg("Unable to set 'Pending' state")
			return err
		}

		err = s.producer.Produce(ctx, tasks.TaskIds())
		if err != nil {
			log.Error().Err(err).Msg("Unable to produce tasks")
			return err
		}

		err = s.store.ConfirmPendingState(ctx, tasks)
		if err != nil {
			log.Error().Err(err).Msg("Unable to confirm 'Pending' state")
			return err
		}
	}
}
