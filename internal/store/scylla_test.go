package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestScheduleTasks(t *testing.T) {
	s := getStore()
	defer s.db.Close()
	ctx := context.Background()

	concurency := 10
	wg := &sync.WaitGroup{}
	wg.Add(concurency)
	for i := 0; i < concurency; i++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 1000; i++ {
				_, err := s.ScheduleNewTask(ctx, "0s", time.Now().UTC(), "")
				checkError(err)
			}
		}()
	}

	wg.Wait()

}

func getStore() *Scylla {
	s, err := NewScyllaStore([]string{"localhost:19042"}, "tms")
	checkError(err)
	return s
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
