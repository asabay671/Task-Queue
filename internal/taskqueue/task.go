package taskqueue

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusPending  Status = "pending" 
	StatusRunning  Status = "running"  
	StatusRetrying Status = "retrying" 
	StatusDone     Status = "done"    
	StatusFailed   Status = "failed"   
)

type Task struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}
