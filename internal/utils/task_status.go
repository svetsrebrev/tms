package utils

const (
	Scheduled int = 0
	Pending   int = 1
	Running   int = 2
	Finished  int = 3

	ScheduledStr string = "SCHEDULED"
	PendingStr   string = "PENDING"
	RunningStr   string = "RUNNING"
	FinishedStr  string = "FINISHED"
	UnknownStr   string = "UNKNOWN"
)

var statusMap = map[int]string{
	Scheduled: ScheduledStr,
	Pending:   PendingStr,
	Running:   RunningStr,
	Finished:  FinishedStr,
}

func GetStatusString(status int) string {
	if str, ok := statusMap[status]; ok {
		return str
	}
	return UnknownStr
}
