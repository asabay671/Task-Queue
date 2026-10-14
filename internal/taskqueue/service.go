package taskqueue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand"
	"sync"
	"time"
)

var ErrUnknownType = errors.New("unknown task type")

type Handler func(ctx context.Context, payload json.RawMessage) error

type Config struct {
	Workers     int
	QueueSize   int
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	TaskTimeout time.Duration
}

type Service struct {
	cfg      Config
	store    *Store
	queue    *Queue
	handlers map[string]Handler
	log      *slog.Logger

	wg         sync.WaitGroup
	stop       chan struct{} 
	stopOnce   sync.Once
	workCtx    context.Context
	cancelWork context.CancelFunc
}

func NewService(cfg Config, log *slog.Logger) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		cfg:        cfg,
		store:      NewStore(),
		queue:      NewQueue(cfg.QueueSize),
		handlers:   make(map[string]Handler),
		log:        log,
		stop:       make(chan struct{}),
		workCtx:    ctx,
		cancelWork: cancel,
	}
}

func (s *Service) Register(taskType string, h Handler) { s.handlers[taskType] = h }

func (s *Service) Start() {
	for i := 0; i < s.cfg.Workers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
}

func (s *Service) Submit(taskType string, payload json.RawMessage, maxAttempts int) (Task, error) {
	if _, ok := s.handlers[taskType]; !ok {
		return Task{}, ErrUnknownType
	}
	if maxAttempts <= 0 {
		maxAttempts = s.cfg.MaxAttempts
	}
	now := time.Now()
	t := &Task{
		ID:          newID(),
		Type:        taskType,
		Payload:     payload,
		Status:      StatusPending,
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	s.store.Create(t)
	if err := s.queue.Enqueue(t.ID); err != nil {
		s.store.Delete(t.ID)
		return Task{}, err
	}
	return *t, nil
}

func (s *Service) Get(id string) (Task, bool) { return s.store.Get(id) }

func (s *Service) worker(n int) {
	defer s.wg.Done()
	for id := range s.queue.C() {
		s.process(n, id)
	}
	s.log.Info("worker stopped", "worker", n)
}

func (s *Service) process(worker int, id string) {
	t, ok := s.store.Update(id, func(t *Task) {
		t.Status = StatusRunning
		t.Attempts++
	})
	if !ok {
		return
	}
	log := s.log.With("task", id, "type", t.Type, "attempt", t.Attempts, "worker", worker)

	ctx, cancel := context.WithTimeout(s.workCtx, s.cfg.TaskTimeout)
	err := runSafe(ctx, s.handlers[t.Type], t.Payload)
	cancel()

	if err == nil {
		s.store.Update(id, func(t *Task) { t.Status = StatusDone; t.LastError = "" })
		log.Info("task done")
		return
	}

	if t.Attempts >= t.MaxAttempts {
		s.store.Update(id, func(t *Task) { t.Status = StatusFailed; t.LastError = err.Error() })
		log.Error("task failed permanently", "err", err)
		return
	}

	delay := s.backoff(t.Attempts)
	s.store.Update(id, func(t *Task) { t.Status = StatusRetrying; t.LastError = err.Error() })
	log.Warn("task failed, will retry", "err", err, "delay", delay)
	s.scheduleRetry(id, delay)
}

func (s *Service) scheduleRetry(id string, delay time.Duration) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			if err := s.queue.Enqueue(id); err != nil {
				s.store.Update(id, func(t *Task) {
					t.Status = StatusFailed
					t.LastError = "requeue: " + err.Error()
				})
			}
		case <-s.stop:
		}
	}()
}

func (s *Service) backoff(attempt int) time.Duration {
	d := s.cfg.MaxDelay
	if attempt <= 20 {
		if x := s.cfg.BaseDelay << (attempt - 1); x > 0 && x < d {
			d = x
		}
	}
	return d + time.Duration(mrand.Int63n(int64(d)/5+1))
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.queue.Close()
	})

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.log.Warn("shutdown deadline exceeded, cancelling running tasks")
		s.cancelWork()
		<-done
		return ctx.Err()
	}
}

func runSafe(ctx context.Context, h Handler, payload json.RawMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, payload)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
