package api

import "time"

type Task struct {
	Id        string    `json:"id"`
	Status    string    `json:"status"`
	StartAt   time.Time `json:"start_at"`
	Recurring string    `json:"recurring"`
	Command   string    `json:"command"`
}
