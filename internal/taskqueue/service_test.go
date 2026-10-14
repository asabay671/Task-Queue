package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestService(t *testing.T, cfg Config) *Service {
	t.Helper()
	if cfg.Workers == 0 {
		cfg.Workers = 2
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 16
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BaseDelay == 0 {
		cfg.BaseDelay = 5 * time.Millisecond
	}
	if cfg.MaxDelay == 0 {
		cfg.MaxDelay = 50 * time.Millisecond
	}
	if cfg.TaskTimeout == 0 {
		cfg.TaskTimeout = 2 * time.Second
	}
	svc := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = svc.Shutdown(ctx)
	})
	return svc
}

func waitStatus(t *testing.T, svc *Service, id string, want Status) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, ok := svc.Get(id); ok && got.Status == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := svc.Get(id)
	t.Fatalf("task %s: want status %q, got %q (last_error=%q)", id, want, got.Status, got.LastError)
	return Task{}
}

func mustSubmit(t *testing.T, svc *Service, typ string, maxAttempts int) Task {
	t.Helper()
	task, err := svc.Submit(typ, nil, maxAttempts)
	if err != nil {
		t.Fatalf("Submit(%s): %v", typ, err)
	}
	return task
}

func TestSubmit_UnknownType(t *testing.T) {
	svc := newTestService(t, Config{})
	svc.Start()
	if _, err := svc.Submit("nope", nil, 0); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("want ErrUnknownType, got %v", err)
	}
}

func TestTask_Succeeds(t *testing.T) {
	svc := newTestService(t, Config{})
	svc.Register("ok", func(context.Context, json.RawMessage) error { return nil })
	svc.Start()

	task := mustSubmit(t, svc, "ok", 0)
	if task.Status != StatusPending || task.MaxAttempts != 3 {
		t.Fatalf("unexpected initial task: %+v", task)
	}

	done := waitStatus(t, svc, task.ID, StatusDone)
	if done.Attempts != 1 || done.LastError != "" {
		t.Fatalf("unexpected final task: %+v", done)
	}
}

func TestRetry_ThenSuccess(t *testing.T) {
	svc := newTestService(t, Config{})
	var calls atomic.Int32
	svc.Register("flaky", func(context.Context, json.RawMessage) error {
		if calls.Add(1) < 3 {
			return errors.New("boom")
		}
		return nil
	})
	svc.Start()

	task := mustSubmit(t, svc, "flaky", 3)
	done := waitStatus(t, svc, task.ID, StatusDone)
	if done.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3/3", done.Attempts, calls.Load())
	}
}

func TestRetry_ExhaustedMarksFailed(t *testing.T) {
	svc := newTestService(t, Config{})
	var calls atomic.Int32
	svc.Register("bad", func(context.Context, json.RawMessage) error {
		calls.Add(1)
		return errors.New("always broken")
	})
	svc.Start()

	task := mustSubmit(t, svc, "bad", 2)
	failed := waitStatus(t, svc, task.ID, StatusFailed)
	if failed.Attempts != 2 || calls.Load() != 2 {
		t.Fatalf("attempts=%d calls=%d, want 2/2", failed.Attempts, calls.Load())
	}
	if !strings.Contains(failed.LastError, "always broken") {
		t.Fatalf("last_error = %q", failed.LastError)
	}
}

func TestPanic_IsRecoveredAndFails(t *testing.T) {
	svc := newTestService(t, Config{})
	svc.Register("panic", func(context.Context, json.RawMessage) error { panic("oops") })
	svc.Start()

	task := mustSubmit(t, svc, "panic", 1)
	failed := waitStatus(t, svc, task.ID, StatusFailed)
	if !strings.Contains(failed.LastError, "panic: oops") {
		t.Fatalf("last_error = %q", failed.LastError)
	}
}

func TestTask_Timeout(t *testing.T) {
	svc := newTestService(t, Config{TaskTimeout: 20 * time.Millisecond})
	svc.Register("hang", func(ctx context.Context, _ json.RawMessage) error {
		<-ctx.Done()
		return ctx.Err()
	})
	svc.Start()

	task := mustSubmit(t, svc, "hang", 1)
	failed := waitStatus(t, svc, task.ID, StatusFailed)
	if !strings.Contains(failed.LastError, "deadline exceeded") {
		t.Fatalf("last_error = %q", failed.LastError)
	}
}

func TestConcurrency_LimitedByWorkers(t *testing.T) {
	const workers = 2
	svc := newTestService(t, Config{Workers: workers, QueueSize: 10})

	var cur, peak atomic.Int32
	svc.Register("work", func(context.Context, json.RawMessage) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		return nil
	})
	svc.Start()

	ids := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		ids = append(ids, mustSubmit(t, svc, "work", 0).ID)
	}
	for _, id := range ids {
		waitStatus(t, svc, id, StatusDone)
	}

	if got := peak.Load(); got > workers {
		t.Fatalf("peak concurrency %d exceeds limit %d", got, workers)
	}
	if got := peak.Load(); got < workers {
		t.Fatalf("peak concurrency %d, expected workers to run in parallel (%d)", got, workers)
	}
}

func TestSubmit_QueueFull(t *testing.T) {
	svc := newTestService(t, Config{Workers: 1, QueueSize: 1})

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	svc.Register("block", func(ctx context.Context, _ json.RawMessage) error {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	svc.Start()

	first := mustSubmit(t, svc, "block", 0)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first task did not start")
	}

	second := mustSubmit(t, svc, "block", 0)
	if _, err := svc.Submit("block", nil, 0); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}

	close(release)
	waitStatus(t, svc, first.ID, StatusDone)
	waitStatus(t, svc, second.ID, StatusDone)
}

func TestShutdown_DrainsRunningAndQueuedTasks(t *testing.T) {
	svc := newTestService(t, Config{Workers: 1, QueueSize: 10})
	svc.Register("slow", func(context.Context, json.RawMessage) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	svc.Start()

	ids := []string{
		mustSubmit(t, svc, "slow", 0).ID,
		mustSubmit(t, svc, "slow", 0).ID,
		mustSubmit(t, svc, "slow", 0).ID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	for _, id := range ids {
		got, _ := svc.Get(id)
		if got.Status != StatusDone {
			t.Fatalf("task %s status = %q after graceful shutdown, want done", id, got.Status)
		}
	}
}

func TestShutdown_TimeoutCancelsRunningTasks(t *testing.T) {
	svc := newTestService(t, Config{Workers: 1})

	started := make(chan struct{})
	svc.Register("hang", func(ctx context.Context, _ json.RawMessage) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	svc.Start()

	task := mustSubmit(t, svc, "hang", 1)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("task did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := svc.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: want DeadlineExceeded, got %v", err)
	}

	got, _ := svc.Get(task.ID)
	if got.Status != StatusFailed {
		t.Fatalf("status = %q, want failed (handler context must be cancelled)", got.Status)
	}
}

func TestShutdown_RejectsNewTasks(t *testing.T) {
	svc := newTestService(t, Config{})
	svc.Register("ok", func(context.Context, json.RawMessage) error { return nil })
	svc.Start()

	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := svc.Submit("ok", nil, 0); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
}

func TestShutdown_AbortsPendingRetry(t *testing.T) {
	svc := newTestService(t, Config{BaseDelay: time.Hour, MaxDelay: time.Hour})
	svc.Register("bad", func(context.Context, json.RawMessage) error { return errors.New("fail") })
	svc.Start()

	task := mustSubmit(t, svc, "bad", 3)
	waitStatus(t, svc, task.ID, StatusRetrying)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := svc.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Shutdown waited for the retry timer instead of aborting it")
	}
	if got, _ := svc.Get(task.ID); got.Status != StatusRetrying {
		t.Fatalf("status = %q, want retrying", got.Status)
	}
}

func TestBackoff(t *testing.T) {
	svc := newTestService(t, Config{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second})

	tests := []struct {
		attempt int
		base    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, time.Second},
		{50, time.Second},
	}
	for _, tc := range tests {
		for i := 0; i < 50; i++ {
			got := svc.backoff(tc.attempt)
			if got < tc.base || got > tc.base+tc.base/5 {
				t.Fatalf("attempt %d: backoff %v not in [%v, %v]", tc.attempt, got, tc.base, tc.base+tc.base/5)
			}
		}
	}
}
