package store

import (
	"context"
	"errors"
	"time"

	"github.com/svetsrebrev/tms/internal/store/models"
	"github.com/svetsrebrev/tms/internal/utils"
)

var (
	ErrTaskNotFound = errors.New("task not found")
)

type Store interface {
	ScheduleNewTask(ctx context.Context, command string, startAt time.Time, recurring string) (string, error)

	GetAllTasks(ctx context.Context) ([]*models.TasksStruct, error)
	GetTask(ctx context.Context, taskId string) (*models.TasksStruct, error)

	GetReadyTasks(ctx context.Context, max int) (ScheduledTasks, error)

	SetPendingState(ctx context.Context, tasks ScheduledTasks) error
	ConfirmPendingState(ctx context.Context, tasks ScheduledTasks) error

	SetRunningState(ctx context.Context, owner string, taskIds ...string) error
	SetFinishedState(ctx context.Context, owner string, taskIds ...string) error
	ReScheduleTask(ctx context.Context, currentOwner string, task *models.TasksStruct, startAt time.Time) error

	GetWorkersAssignedTasks(ctx context.Context, workers []string) (WorkerTasks, error)
	ReenqueueWorkerTasks(ctx context.Context, taskIds WorkerTasks) error

	DiscardTasks(ctx context.Context, owner string, taskIds ...string) error

	HeartBeat(ctx context.Context, worker string) error
	GetInactiveWorkers(ctx context.Context, hartbeatBefore time.Time, max int) ([]*models.WorkersStruct, error)
	DeleteWorkers(ctx context.Context, workers []*models.WorkersStruct) error
}

type ScheduledTasks []*models.TasksByScheduleStruct

func (st ScheduledTasks) TaskIds() []string {
	return utils.CollectAll(st, func(a *models.TasksByScheduleStruct) string { return a.TaskId })
}

type WorkerTasks []*models.TasksByWorkerStruct

func (wt WorkerTasks) TaskIds() []string {
	return utils.CollectAll(wt, func(a *models.TasksByWorkerStruct) string { return a.TaskId })
}
