package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taskqueue/internal/api"
	"taskqueue/internal/taskqueue"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "HTTP listen address")
		workers = flag.Int("workers", 4, "number of concurrent workers")
		qsize   = flag.Int("queue", 100, "queue capacity")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	svc := taskqueue.NewService(taskqueue.Config{
		Workers:     *workers,
		QueueSize:   *qsize,
		MaxAttempts: 3,
		BaseDelay:   time.Second,
		MaxDelay:    30 * time.Second,
		TaskTimeout: 30 * time.Second,
	}, log)

	// Демо-обработчики. Замени на свою "тяжёлую" логику.
	svc.Register("sleep", func(ctx context.Context, p json.RawMessage) error {
		var in struct {
			Seconds int `json:"seconds"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return fmt.Errorf("bad payload: %w", err)
		}
		select {
		case <-time.After(time.Duration(in.Seconds) * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	svc.Register("flaky", func(ctx context.Context, _ json.RawMessage) error {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rand.Float64() < 0.6 {
			return errors.New("random failure")
		}
		return nil
	})

	svc.Start()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("listening", "addr", *addr, "workers", *workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1) перестаём принимать HTTP-запросы, 2) останавливаем воркеров.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	if err := svc.Shutdown(shutdownCtx); err != nil {
		log.Error("worker shutdown", "err", err)
	}
	log.Info("bye")
}
