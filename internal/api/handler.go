package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"taskqueue/internal/taskqueue"
)

type Server struct{ svc *taskqueue.Service }

func New(svc *taskqueue.Service) http.Handler {
	s := &Server{svc: svc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", s.create)
	mux.HandleFunc("GET /tasks/{id}", s.get)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

type createRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "field 'type' is required")
		return
	}

	t, err := s.svc.Submit(req.Type, req.Payload, req.MaxAttempts)
	switch {
	case errors.Is(err, taskqueue.ErrUnknownType):
		writeError(w, http.StatusBadRequest, "unknown task type")
	case errors.Is(err, taskqueue.ErrQueueFull):
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "queue is full, try later")
	case errors.Is(err, taskqueue.ErrQueueClosed):
		writeError(w, http.StatusServiceUnavailable, "service is shutting down")
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal error")
	default:
		w.Header().Set("Location", "/tasks/"+t.ID)
		writeJSON(w, http.StatusAccepted, t)
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	t, ok := s.svc.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
