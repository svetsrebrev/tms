package queue

import (
	"context"
)

// Producer provides interface for sending tasks for asynchronous future execution
type Producer interface {
	Produce(ctx context.Context, tasks []string) error
}

// Consumer provides interface for workers to retrieve pending tasks and check-point it's current progress
type Consumer interface {
	Consume(ctx context.Context, max int) ([]string, error)
	CommitConsumed(ctx context.Context) error
}
