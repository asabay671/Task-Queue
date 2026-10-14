package taskqueue

import (
	"errors"
	"testing"
)

func TestQueue_FullClosedAndDrain(t *testing.T) {
	q := NewQueue(2)

	if err := q.Enqueue("a"); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := q.Enqueue("b"); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}
	if err := q.Enqueue("c"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("enqueue c: want ErrQueueFull, got %v", err)
	}

	q.Close()
	q.Close()

	if err := q.Enqueue("d"); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("enqueue after close: want ErrQueueClosed, got %v", err)
	}

	var got []string
	for id := range q.C() {
		got = append(got, id)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("drained %v, want [a b]", got)
	}
}


