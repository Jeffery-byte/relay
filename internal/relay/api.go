package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

type Repository interface {
	Submit(context.Context, []float64, string) (string, bool, error)
	Get(context.Context, string) (Job, error)
	Ping(context.Context) error
}

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func Handler(repo Repository) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := repo.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		defer r.Body.Close()
		var request struct {
			Numbers []*float64 `json:"numbers"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "expected a JSON object containing numbers")
			return
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			writeError(w, http.StatusBadRequest, "expected exactly one JSON object")
			return
		}
		numbers := make([]float64, len(request.Numbers))
		for i, n := range request.Numbers {
			if n == nil {
				writeError(w, http.StatusBadRequest, "numbers cannot contain null")
				return
			}
			numbers[i] = *n
		}
		if err := Validate(numbers); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if len(key) > 200 {
			writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 200 bytes")
			return
		}
		id, created, err := repo.Submit(r.Context(), numbers, key)
		if errors.Is(err, ErrConflict) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if err != nil {
			slog.Error("submit job", "error", err)
			writeError(w, http.StatusServiceUnavailable, "unable to persist job; retry with the same Idempotency-Key")
			return
		}
		w.Header().Set("Location", "/jobs/"+id)
		writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "created": created})
	})
	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		job, err := repo.Get(r.Context(), r.PathValue("id"))
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if err != nil {
			slog.Error("get job", "error", err)
			writeError(w, http.StatusServiceUnavailable, "unable to retrieve job")
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
	return mux
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("encode response", "error", err)
	}
}
