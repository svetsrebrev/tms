package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
)

func ForEachConcurrently[T any](items []T, maxConcurrency int, continueOnError bool, action func(item T) error) error {
	if len(items) == 0 {
		return nil
	}
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU() * 8
	}
	if maxConcurrency > len(items) {
		maxConcurrency = len(items)
	}

	channel := make(chan T, maxConcurrency)
	wg := &sync.WaitGroup{}
	wg.Add(maxConcurrency)

	var lastError atomic.Pointer[error]
	for i := 0; i < maxConcurrency; i++ {
		go func() {
			defer wg.Done()

			for item := range channel {
				if !continueOnError && lastError.Load() != nil {
					break
				}

				err := action(item)
				if err != nil {
					lastError.Store(&err)
					if !continueOnError {
						break
					}
				}
			}

			// Drain the channel
			for range channel {
			}
		}()
	}

	for _, item := range items {
		channel <- item
	}

	close(channel)
	wg.Wait()
	lerror := lastError.Load()
	if lerror != nil {
		return *lerror
	}
	return nil
}

func ConsumeChannel[T any](ch <-chan T) []T {
	result := []T{}
loop:
	for {
		select {
		case val, ok := <-ch:
			if ok {
				result = append(result, val)
			} else {
				break loop
			}
		default:
			break loop
		}
	}

	return result
}
