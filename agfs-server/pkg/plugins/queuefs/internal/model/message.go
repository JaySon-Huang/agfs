package model

import "time"

// QueueMessage is the logical message payload exchanged with queue backends.
type QueueMessage struct {
	ID        string    `json:"id"`
	Data      string    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}
