package jobhistory

import (
	"encoding/json"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

// EventBus is the optional NATS bus for publishing job state change events.
// Set this before calling Finalize/Transition to enable real-time event publishing.
var EventBus *natsbus.Bus

// JobEvent is published to NATS when a job changes state.
type JobEvent struct {
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	PrevStatus string `json:"prev_status,omitempty"`
	Error      string `json:"error,omitempty"`
	Timestamp  string `json:"timestamp"`
	NodeID     string `json:"node_id,omitempty"`
}

// PublishEvent publishes a job state change event to NATS if a bus is configured.
func PublishEvent(jobID, status, prevStatus, errMsg string) {
	if EventBus == nil {
		return
	}
	evt := JobEvent{
		JobID:      jobID,
		Status:     status,
		PrevStatus: prevStatus,
		Error:      errMsg,
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		return
	}
	// Fire-and-forget publish to topic
	_ = EventBus.Publish("pipedpeer.jobs.status."+jobID, data)
}
