// Code generated by "gocqlx/cmd/schemagen"; DO NOT EDIT.

package models

import (
	"github.com/scylladb/gocqlx/v2/table"
	"time"
)

// Table models.
var (
	Tasks = table.New(table.Metadata{
		Name: "tasks",
		Columns: []string{
			"command",
			"id",
			"recurring",
			"start_at",
			"status",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{},
	})

	TasksBySchedule = table.New(table.Metadata{
		Name: "tasks_by_schedule",
		Columns: []string{
			"partition",
			"scheduled_for",
			"task_id",
		},
		PartKey: []string{
			"partition",
		},
		SortKey: []string{
			"scheduled_for",
			"task_id",
		},
	})

	TasksByWorker = table.New(table.Metadata{
		Name: "tasks_by_worker",
		Columns: []string{
			"task_id",
			"worker_id",
		},
		PartKey: []string{
			"worker_id",
		},
		SortKey: []string{
			"task_id",
		},
	})

	Workers = table.New(table.Metadata{
		Name: "workers",
		Columns: []string{
			"id",
			"last_heartbeat",
		},
		PartKey: []string{
			"id",
		},
		SortKey: []string{
			"last_heartbeat",
		},
	})
)

type TasksStruct struct {
	Command   string
	Id        string
	Recurring string
	StartAt   time.Time
	Status    int32
}
type TasksByScheduleStruct struct {
	Partition    int32
	ScheduledFor time.Time
	TaskId       string
}
type TasksByWorkerStruct struct {
	TaskId   string
	WorkerId string
}
type WorkersStruct struct {
	Id            string
	LastHeartbeat time.Time
}
