package common

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

func RunUntilCancelled(ctx context.Context, serviceName string, action func(ctx context.Context) error) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		const restartInterval = 3 * time.Second
		for {
			log.Info().Msgf("Starting %s", serviceName)

			err := action(ctx)

			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Msgf("%s failed, restarting in %v", serviceName, restartInterval)
			}

			select {
			case <-time.After(restartInterval):
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return done
}

func TrackInterupts(cancelScheduler context.CancelFunc, done <-chan struct{}) <-chan struct{} {
	kill := make(chan struct{})

	go func() {
		interupts := make(chan os.Signal, 2)
		signal.Notify(interupts, os.Interrupt)

		select {
		case <-done:
			return
		case <-interupts:
			log.Info().Msg("Starting graceful exit ....")
			cancelScheduler()
		}

		select {
		case <-done:
			return
		case <-interupts:
			log.Warn().Msgf("Second interrupt signal, quitting without waiting for graceful exit")
			close(kill)
		}
	}()

	return kill
}

func ConcatErrMessages(err error) string {
	if err == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(err.Error())
	for nextErr := errors.Unwrap(err); nextErr != nil; nextErr = errors.Unwrap(nextErr) {
		sb.WriteString(" :: ")
		sb.WriteString(nextErr.Error())
	}
	return sb.String()
}
