package taskqueue

import (
	"testing"
	"time"
)

func TestStore_CreateGetCopy(t *testing.T) {
	s := NewStore()
	s.Create(&Task{ID: "1", Type: "x", Status: StatusPending})

	got, ok := s.Get("1")
	if !ok || got.Type != "x" {
		t.Fatalf("Get: ok=%v task=%+v", ok, got)
	}

	got.Status = StatusDone
	again, _ := s.Get("1")
	if again.Status != StatusPending {
		t.Fatalf("store was mutated through a copy: %s", again.Status)
	}

	if _, ok := s.Get("missing"); ok {
		t.Fatal("Get(missing) must return ok=false")
	}
}

func TestStore_Update(t *testing.T) {
	s := NewStore()
	old := time.Now().Add(-time.Hour)
	s.Create(&Task{ID: "1", Status: StatusPending, UpdatedAt: old})

	updated, ok := s.Update("1", func(t *Task) { t.Status = StatusRunning; t.Attempts++ })
	if !ok || updated.Status != StatusRunning || updated.Attempts != 1 {
		t.Fatalf("Update: ok=%v task=%+v", ok, updated)
	}
	if !updated.UpdatedAt.After(old) {
		t.Fatal("UpdatedAt was not refreshed")
	}

	if _, ok := s.Update("missing", func(*Task) {}); ok {
		t.Fatal("Update(missing) must return ok=false")
	}
}

func TestStore_Delete(t *testing.T) {
	s := NewStore()
	s.Create(&Task{ID: "1"})
	s.Delete("1")
	if _, ok := s.Get("1"); ok {
		t.Fatal("task still present after Delete")
	}
}
