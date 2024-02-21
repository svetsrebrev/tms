package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"github.com/svetsrebrev/tms/internal/store"
	"github.com/svetsrebrev/tms/internal/store/models"
	"github.com/svetsrebrev/tms/internal/utils"
)

func Run(store store.Store, cfg *Config) error {
	srv := &server{store: store, echo: echo.New()}
	srv.setupHandlers()

	return srv.echo.Start(cfg.ListenAddress)
}

type server struct {
	echo  *echo.Echo
	store store.Store
}

func (s *server) setupHandlers() {
	//
	// TODO: Midlewares for metrics/tracing/loging, skip for now.

	s.echo.POST("/tasks", s.scheduleTask)
	s.echo.GET("/tasks", s.listTasks)
	s.echo.GET("/tasks/:id", s.getTask)
}

func (s *server) scheduleTask(c echo.Context) error {
	task, err := parseTask(c)
	if err != nil {
		return AsHttpErrOrWrap(err, http.StatusBadRequest, "Invalid payload")
	}

	// Schedule task for execution
	id, err := s.store.ScheduleNewTask(c.Request().Context(), task.Command, task.StartAt, task.Recurring)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to schedule task")
		return AsHttpErrOrWrap(err, http.StatusInternalServerError, "The request can not be handled at the moment")
	}
	task.Id = id
	task.Status = utils.GetStatusString(utils.Scheduled)

	return c.JSON(http.StatusOK, task)
}

// NOTE: Sample endpoint exposed for demo/testing.
// For production the API should take into account expected usage pattern and allow filtering/paging of tasks.
func (s *server) listTasks(c echo.Context) error {
	dbTasks, err := s.store.GetAllTasks(c.Request().Context())
	if err != nil {
		log.Warn().Err(err).Msg("Failed to retrieve all tasks")
		return AsHttpErrOrWrap(err, http.StatusInternalServerError, "The request can not be handled at the moment")
	}

	tasks := utils.CollectAll(dbTasks, taskFromDbTask)
	return c.JSON(http.StatusOK, tasks)
}

func (s *server) getTask(c echo.Context) error {
	taskId := c.Param("id")
	dbTasks, err := s.store.GetTask(c.Request().Context(), taskId)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Task not found")
		}
		log.Warn().Err(err).Msgf("Failed to retrieve task %s", taskId)
		return AsHttpErrOrWrap(err, http.StatusInternalServerError, "The request can not be handled at the moment")
	}

	return c.JSON(http.StatusOK, taskFromDbTask(dbTasks))
}

func parseTask(c echo.Context) (*Task, error) {
	// Simple validation for now
	task := &Task{}
	err := c.Bind(task)
	if err != nil {
		return nil, AsHttpErrOrWrap(err, http.StatusBadRequest, "Invalid payload")
	}
	if task.Command == "" {
		return nil, AsHttpErrOrWrap(err, http.StatusBadRequest, "Attribute 'command' is required")
	}
	_, err = time.ParseDuration(task.Command)
	if err != nil {
		return nil, AsHttpErrOrWrap(err, http.StatusBadRequest, "Attribute 'command' should be a duration string (3s)")
	}
	if task.Recurring != "" {
		_, err := time.ParseDuration(task.Recurring)
		if err != nil {
			return nil, AsHttpErrOrWrap(err, http.StatusBadRequest, "Attribute 'recurring' should be a duration string (3s)")
		}
	}
	if task.StartAt.IsZero() {
		task.StartAt = time.Now()
	}
	task.StartAt = task.StartAt.UTC()

	return task, nil
}

func taskFromDbTask(t *models.TasksStruct) *Task {
	return &Task{
		Id:        t.Id,
		Status:    utils.GetStatusString(int(t.Status)),
		StartAt:   t.StartAt,
		Recurring: t.Recurring,
		Command:   t.Command,
	}
}
