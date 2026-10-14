package taskqueue

import (
	"errors"
	"sync"
)

var (
	ErrQueueFull   = errors.New("queue is full")
	ErrQueueClosed = errors.New("queue is closed")
)

type Queue struct {
	mu     sync.Mutex
	closed bool
	ch     chan string
}

func NewQueue(size int) *Queue {
	return &Queue{ch: make(chan string, size)}
}

func (q *Queue) Enqueue(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	select {
	case q.ch <- id:
		return nil
	default:
		return ErrQueueFull
	}
}

func (q *Queue) C() <-chan string { return q.ch }

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ch)
	}
}
