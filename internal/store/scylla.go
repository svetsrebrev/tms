package store

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/svetsrebrev/tms/internal/store/models"
	"github.com/svetsrebrev/tms/internal/utils"
)

const (
	maxScheduledDays = 2
)

type Scylla struct {
	db       *gocqlx.Session
	pastDays int
}

func NewScyllaStore(hosts []string, keyspace string) (*Scylla, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 5, Min: 1 * time.Second, Max: 16 * time.Second}

	db, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return nil, err
	}

	return &Scylla{db: &db, pastDays: maxScheduledDays}, nil
}

func (s *Scylla) Close() error {
	s.db.Close()
	return nil
}

func (s *Scylla) ScheduleNewTask(ctx context.Context, command string, startAt time.Time, recurring string) (string, error) {
	id := uuid.New().String()
	task := models.TasksStruct{
		Id:        id,
		StartAt:   startAt,
		Recurring: recurring,
		Command:   command,
		Status:    int32(utils.Scheduled),
	}
	err := s.scheduleTask(ctx, &task)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (s *Scylla) scheduleTask(ctx context.Context, task *models.TasksStruct) error {
	query := s.db.Query(models.Tasks.Insert()).BindStruct(task).WithContext(ctx)
	if err := query.ExecRelease(); err != nil {
		return err
	}

	st := models.TasksByScheduleStruct{
		When:         task.StartAt,
		ScheduledFor: task.StartAt,
		TaskId:       task.Id,
	}
	query = s.db.Query(models.TasksBySchedule.Insert()).BindStruct(st).WithContext(ctx)
	return query.ExecRelease()
}

func (s *Scylla) GetAllTasks(ctx context.Context) ([]*models.TasksStruct, error) {
	selector := qb.Select(models.Tasks.Name())
	query := s.db.Query(selector.ToCql()).WithContext(ctx)
	tasks := []*models.TasksStruct{}
	if err := query.SelectRelease(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (s *Scylla) GetTask(ctx context.Context, taskId string) (*models.TasksStruct, error) {
	query := s.db.Query(models.Tasks.Get()).WithContext(ctx)
	query.Bind(taskId)
	task := &models.TasksStruct{}
	err := query.GetRelease(task)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			err = errors.Join(err, ErrTaskNotFound)
		}
		return nil, err
	}
	return task, nil
}

func (s *Scylla) GetReadyTasks(ctx context.Context, max int) (ScheduledTasks, error) {
	return s.getSchedules(ctx, max, s.pastDays)
}

func (s *Scylla) SetPendingState(ctx context.Context, tasks ScheduledTasks) error {
	return s.setStatus(ctx, tasks.TaskIds(), utils.Pending)
}

func (s *Scylla) ConfirmPendingState(ctx context.Context, tasks ScheduledTasks) error {
	return s.deleteSchedules(ctx, tasks)
}

func (s *Scylla) SetRunningState(ctx context.Context, owner string, taskIds ...string) error {
	err := s.setTasksOwner(ctx, owner, taskIds...)
	if err != nil {
		return err
	}

	return s.setStatus(ctx, taskIds, utils.Running)
}

func (s *Scylla) SetFinishedState(ctx context.Context, owner string, taskIds ...string) error {
	err := s.setStatus(ctx, taskIds, utils.Finished)
	if err != nil {
		return err
	}

	return s.deleteTasksForOwner(ctx, owner, taskIds...)
}

func (s *Scylla) ReScheduleTask(ctx context.Context, currentOwner string, task *models.TasksStruct, startAt time.Time) error {
	task.StartAt = startAt
	task.Status = int32(utils.Scheduled)

	err := s.scheduleTask(ctx, task)
	if err != nil {
		return err
	}

	return s.deleteTasksForOwner(ctx, currentOwner, task.Id)
}

func (s *Scylla) DiscardTasks(ctx context.Context, owner string, taskIds ...string) error {
	return s.deleteTasksForOwner(ctx, owner, taskIds...)
}

func (s *Scylla) GetWorkersAssignedTasks(ctx context.Context, workers []string) (WorkerTasks, error) {
	selector := qb.Select(models.TasksByWorker.Name()).Where(
		qb.In("worker_id"),
	)
	query := s.db.Query(selector.ToCql()).WithContext(ctx)
	query.BindMap(qb.M{"worker_id": workers})

	tasks := []*models.TasksByWorkerStruct{}
	if err := query.SelectRelease(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (s *Scylla) ReenqueueWorkerTasks(ctx context.Context, tasks WorkerTasks) error {
	err := s.setStatus(ctx, tasks.TaskIds(), utils.Pending)
	if err != nil {
		return err
	}

	return s.deleteWorkerTasks(ctx, tasks)
}

func (s *Scylla) HeartBeat(ctx context.Context, worker string) error {
	batch := s.db.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(`DELETE FROM workers WHERE id = ? AND last_heartbeat < currentTimestamp();`, worker)
	batch.Query(`INSERT INTO workers(id, last_heartbeat) VALUES (?, currentTimestamp());`, worker)
	return s.db.Session.ExecuteBatch(batch)
}

func (s *Scylla) GetInactiveWorkers(ctx context.Context, hartbeatBefore time.Time, max int) ([]*models.WorkersStruct, error) {
	selector := qb.Select(models.Workers.Name()).Where(
		qb.Lt("last_heartbeat"),
	)

	query := s.db.Query(selector.Limit(uint(max)).ToCql()).WithContext(ctx)
	query.BindMap(qb.M{"last_heartbeat": hartbeatBefore})

	workers := []*models.WorkersStruct{}
	if err := query.SelectRelease(&workers); err != nil {
		return nil, err
	}
	return workers, nil
}

func (s *Scylla) DeleteWorkers(ctx context.Context, workers []*models.WorkersStruct) error {
	err := utils.ForEachConcurrently(workers, -1, true,

		func(worker *models.WorkersStruct) error {
			query := s.db.Query(models.Workers.Delete()).BindStruct(worker).WithContext(ctx)
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}

func (s *Scylla) getSchedules(ctx context.Context, max int, pastDays int) (ScheduledTasks, error) {
	result := ScheduledTasks{}

	now := time.Now().UTC()
	selector := qb.Select(models.TasksBySchedule.Name()).Columns("*").Where(
		qb.Eq("when"),
		qb.Lt("scheduled_for"),
	)
	for ; max > 0 && pastDays >= 0; pastDays-- {
		date := now.AddDate(0, 0, -pastDays).Format(time.DateOnly)

		query := s.db.Query(selector.Limit(uint(max)).ToCql()).WithContext(ctx)
		query.BindMap(qb.M{"when": date, "scheduled_for": now})

		batch := ScheduledTasks{}
		if err := query.SelectRelease(&batch); err != nil {
			return nil, err
		}
		result = append(result, batch...)
		max -= len(batch)
	}

	return result, nil
}

func (s *Scylla) deleteSchedules(ctx context.Context, tasks ScheduledTasks) error {

	err := utils.ForEachConcurrently(tasks, -1, false,

		func(task *models.TasksByScheduleStruct) error {
			query := s.db.Query(models.TasksBySchedule.Delete()).BindStruct(task).WithContext(ctx)
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}

func (s *Scylla) setTasksOwner(ctx context.Context, owner string, taskIds ...string) error {
	err := utils.ForEachConcurrently(taskIds, -1, false,

		func(taskId string) error {
			val := models.TasksByWorkerStruct{
				WorkerId: owner,
				TaskId:   taskId,
			}
			query := s.db.Query(models.TasksByWorker.Insert()).BindStruct(val).WithContext(ctx)
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}

func (s *Scylla) deleteTasksForOwner(ctx context.Context, owner string, taskIds ...string) error {
	err := utils.ForEachConcurrently(taskIds, -1, false,

		func(taskId string) error {
			val := models.TasksByWorkerStruct{
				WorkerId: owner,
				TaskId:   taskId,
			}
			query := s.db.Query(models.TasksByWorker.Delete()).BindStruct(val).WithContext(ctx)
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}

func (s *Scylla) deleteWorkerTasks(ctx context.Context, tasks []*models.TasksByWorkerStruct) error {
	err := utils.ForEachConcurrently(tasks, -1, false,

		func(task *models.TasksByWorkerStruct) error {
			query := s.db.Query(models.TasksByWorker.Delete()).BindStruct(task).WithContext(ctx)
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}

func (s *Scylla) setStatus(ctx context.Context, taskIds []string, status int) error {

	err := utils.ForEachConcurrently(taskIds, -1, false,

		func(id string) error {
			query := s.db.Query(models.Tasks.Update("status")).WithContext(ctx)
			query.BindMap(qb.M{"id": id, "status": status})
			if err := query.ExecRelease(); err != nil {
				return err
			}

			return nil
		},
	)

	return err
}
