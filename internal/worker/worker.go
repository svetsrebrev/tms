package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/svetsrebrev/tms/internal/queue"
	"github.com/svetsrebrev/tms/internal/store"
	"github.com/svetsrebrev/tms/internal/store/models"
	"github.com/svetsrebrev/tms/internal/utils"
)

var (
	ErrUnableToHeartbeat = errors.New("unable to heartbeat")
)

// Worker pools pending tasks from the queue and execute them asynchronously.
type Worker struct {
	workerId string
	consumer queue.Consumer
	store    store.Store
	cfg      *Config
}

func NewWorker(consumer queue.Consumer, store store.Store, cfg *Config) *Worker {
	return &Worker{workerId: uuid.New().String(), consumer: consumer, store: store, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancelFunc := context.WithCancelCause(ctx)
	err := w.startHeartBeat(ctx, cancelFunc)
	if err != nil {
		return err
	}

	err = w.run(ctx)
	cancelFunc(err) // Stop HeartBeat
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return err
}

// startHeartBeat starts a goroutine that sends heartbeats for current worker every cfg.HeartBeatIntervalSec seconds.
// If a heartbeat can not be send for cfg.WorkerTimeoutSec seconds (due to communication error or for some other reason)
// the context is cancelled to signal that the worker should exit
func (w *Worker) startHeartBeat(ctx context.Context, cancelFunc context.CancelCauseFunc) error {
	err := w.store.HeartBeat(ctx, w.workerId)
	if err != nil {
		return err
	}

	timeoutInterval := time.Duration(w.cfg.WorkerTimeoutSec) * time.Second
	lastHeartBeat := time.Now()
	go func() {
		ticker := time.NewTicker(time.Duration(w.cfg.HeartBeatIntervalSec) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err = w.store.HeartBeat(ctx, w.workerId)
				if err == nil {
					lastHeartBeat = time.Now()
				}

				if time.Since(lastHeartBeat) > timeoutInterval {
					log.Error().Msgf("Unable to heartbeat for %v, exiting ...", time.Since(lastHeartBeat))
					cancelFunc(ErrUnableToHeartbeat)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

type taskResult struct {
	taskId string
	err    error
}

func (w *Worker) run(ctx context.Context) error {
	dispatchChan := make(chan string, w.cfg.ConcurentTasks)
	processedChan := make(chan taskResult, w.cfg.ConcurentTasks)

	processorWg := &sync.WaitGroup{}
	processorWg.Add(w.cfg.ConcurentTasks)
	for i := 0; i < w.cfg.ConcurentTasks; i++ {
		go w.processLoop(ctx, dispatchChan, processedChan, processorWg)
	}

	err := w.dispatchLoop(ctx, dispatchChan, processedChan)

	close(dispatchChan)
	processorWg.Wait()

	return err

}

// processLoop reads from dispatchChan, process the task and writes the result to processedChan
func (w *Worker) processLoop(ctx context.Context, dispatchChan <-chan string, processedChan chan<- taskResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for taskId := range dispatchChan {
		err := w.processTask(ctx, taskId)
		processedChan <- taskResult{taskId: taskId, err: err}
	}
}

// dispatchLoop reads from the queue, mark the task as running (owned by current processor) and
// sends the task to dispatchChan for async processing
func (w *Worker) dispatchLoop(ctx context.Context, dispatchChan chan<- string, processedChan <-chan taskResult) error {
	availableProcessors := w.cfg.ConcurentTasks
	for {
		taskIds, err := w.consumer.Consume(ctx, availableProcessors)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to consume tasks")
			return err
		}

		err = w.store.SetRunningState(ctx, w.workerId, taskIds...)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to set 'Running' state")
			return err
		}

		err = w.consumer.CommitConsumed(ctx)
		if err != nil {
			utils.LogIfNotCancelled(err, "Unable to commit consumed offset")
			return err
		}

		for _, taskId := range taskIds {
			dispatchChan <- taskId
		}
		availableProcessors -= len(taskIds)

		if availableProcessors < 1 {
			select {
			case res := <-processedChan:
				if res.err != nil {
					return res.err
				}
				availableProcessors++
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		for _, res := range utils.ConsumeChannel(processedChan) {
			if res.err != nil {
				return res.err
			}
			availableProcessors++
		}
	}
}

func (w *Worker) processTask(ctx context.Context, taskId string) error {
	task, err := w.store.GetTask(ctx, taskId)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			log.Warn().Err(err).Msgf("Task (%s) not available anymore, skipping", taskId)
			err = w.store.DiscardTasks(ctx, w.workerId, taskId)
			if err != nil {
				utils.LogIfNotCancelled(err, fmt.Sprintf("Unable to discard task id: %s", taskId))
			}
		}
		return err
	}

	w.runTask(ctx, task)

	var nextRun time.Time
	if task.Recurring != "" {
		recurring, err := time.ParseDuration(task.Recurring)
		if err != nil {
			log.Error().Err(err).Msgf("Unable to parse task (%s) recuring param, skipping", task.Id)
		} else {
			nextRun = time.Now().UTC().Add(recurring)
		}
	}

	if nextRun.IsZero() {
		err := w.store.SetFinishedState(ctx, w.workerId, task.Id)
		if err != nil {
			utils.LogIfNotCancelled(err, fmt.Sprintf("Unable to set 'Finished' state for task id: %s", task.Id))
			return err
		}
	} else {
		err := w.store.ReScheduleTask(context.Background(), w.workerId, w.cfg.GetSchedulePartition(), task, nextRun)
		if err != nil {
			utils.LogIfNotCancelled(err, fmt.Sprintf("Unable to reshedule task: %s", task.Id))
			return err
		}
	}
	return nil
}

func (w *Worker) runTask(ctx context.Context, task *models.TasksStruct) {
	execTime, err := time.ParseDuration(task.Command)
	if err != nil {
		execTime = 1 * time.Second
	}

	fmt.Printf("Running %s for %v\n", task.Id, execTime)
	time.Sleep(execTime)
}
