package console

import "encoding/json"

// Kernel states reported in status frames.
const (
	stateStarting = "starting"
	stateIdle     = "idle"
	stateBusy     = "busy"
	stateDead     = "dead"
)

// Execution outcomes reported in done frames.
const (
	statusOK      = "ok"
	statusError   = "error"
	statusAborted = "aborted"
)

// Execution targets accepted in execute frames.
const (
	targetLocal   = "local"
	targetCluster = "cluster"
)

// clientFrame is any frame a browser sends over the kernel WebSocket.
type clientFrame struct {
	Type   string `json:"type"`
	CellID string `json:"cell_id"`
	Code   string `json:"code"`
	Target string `json:"target"`
	ReqID  string `json:"req_id"`
	Cursor *int   `json:"cursor"`
}

type statusFrame struct {
	Type           string   `json:"type"`
	State          string   `json:"state"`
	Python         string   `json:"python"`
	Interception   bool     `json:"interception"`
	ExecutionCount int      `json:"execution_count"`
	Queued         []string `json:"queued"`
	Running        string   `json:"running"`
}

type streamFrame struct {
	Type   string `json:"type"`
	CellID string `json:"cell_id"`
	Name   string `json:"name"`
	Text   string `json:"text"`
}

type displayFrame struct {
	Type      string          `json:"type"`
	CellID    string          `json:"cell_id"`
	Data      json.RawMessage `json:"data"`
	DisplayID string          `json:"display_id,omitempty"`
}

type resultFrame struct {
	Type           string          `json:"type"`
	CellID         string          `json:"cell_id"`
	ExecutionCount int             `json:"execution_count"`
	Data           json.RawMessage `json:"data"`
}

type errorFrame struct {
	Type      string   `json:"type"`
	CellID    string   `json:"cell_id"`
	Ename     string   `json:"ename"`
	Evalue    string   `json:"evalue"`
	Traceback []string `json:"traceback"`
}

type doneFrame struct {
	Type           string `json:"type"`
	CellID         string `json:"cell_id"`
	ExecutionCount int    `json:"execution_count"`
	Status         string `json:"status"`
	DurationMs     int64  `json:"duration_ms"`
}

type completeReplyFrame struct {
	Type        string   `json:"type"`
	ReqID       string   `json:"req_id"`
	Matches     []string `json:"matches"`
	CursorStart int      `json:"cursor_start"`
	CursorEnd   int      `json:"cursor_end"`
}

type varsReplyFrame struct {
	Type  string          `json:"type"`
	ReqID string          `json:"req_id"`
	Vars  json.RawMessage `json:"vars"`
}

type fatalFrame struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// driverCommand is one line written to the kernel driver's control pipe.
type driverCommand struct {
	Type           string `json:"type"`
	ID             string `json:"id,omitempty"`
	Code           string `json:"code,omitempty"`
	ExecutionCount int    `json:"execution_count,omitempty"`
	ReqID          string `json:"req_id,omitempty"`
	Cursor         *int   `json:"cursor,omitempty"`
}

// driverEvent is one line read from the kernel driver's event pipe.
type driverEvent struct {
	Type           string          `json:"type"`
	ID             string          `json:"id"`
	Python         string          `json:"python"`
	Name           string          `json:"name"`
	Text           string          `json:"text"`
	Data           json.RawMessage `json:"data"`
	DisplayID      string          `json:"display_id"`
	ExecutionCount int             `json:"execution_count"`
	Ename          string          `json:"ename"`
	Evalue         string          `json:"evalue"`
	Traceback      []string        `json:"traceback"`
	Status         string          `json:"status"`
	ReqID          string          `json:"req_id"`
	Matches        []string        `json:"matches"`
	CursorStart    int             `json:"cursor_start"`
	CursorEnd      int             `json:"cursor_end"`
	Vars           json.RawMessage `json:"vars"`
}
