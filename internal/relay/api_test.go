package relay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeRepo struct {
	err     error
	called  bool
	numbers []float64
	key     string
}

func (f *fakeRepo) Submit(_ context.Context, n []float64, key string) (string, bool, error) {
	f.called = true
	f.numbers = n
	f.key = key
	return "job123", true, f.err
}
func (f *fakeRepo) Get(context.Context, string) (Job, error) {
	return Job{ID: "job123", Status: "completed", Result: &Report{Count: 1, Sum: 5, Min: 5, Max: 5, Mean: 5}, History: []Attempt{}}, f.err
}
func (f *fakeRepo) Ping(context.Context) error { return f.err }

func TestSubmitValidation(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"numbers":[]}`, `{"numbers":[null]}`, `{"numbers":["5"]}`, `{"numbers":[1e999]}`, `{"numbers":[1],"unknown":true}`, `{"numbers":[1]} {}`, `{"numbers":[1]} garbage`, strings.Repeat(" ", 1<<20) + `{"numbers":[1]}`} {
		f := &fakeRepo{}
		w := httptest.NewRecorder()
		Handler(f).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest || f.called {
			t.Fatalf("invalid request accepted: status=%d called=%v", w.Code, f.called)
		}
	}
}

func TestSubmitContract(t *testing.T) {
	f := &fakeRepo{}
	r := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"numbers":[5]}`))
	r.Header.Set("Idempotency-Key", "example")
	w := httptest.NewRecorder()
	Handler(f).ServeHTTP(w, r)
	if w.Code != http.StatusAccepted || w.Header().Get("Location") != "/jobs/job123" || f.key != "example" || len(f.numbers) != 1 {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
}

func TestErrorResponses(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		err                error
		want               int
	}{
		{"POST", "/jobs", `{"numbers":[5]}`, ErrConflict, 409},
		{"POST", "/jobs", `{"numbers":[5]}`, errors.New("private database detail"), 503},
		{"GET", "/jobs/missing", "", ErrNotFound, 404},
		{"GET", "/jobs/a", "", errors.New("private database detail"), 503},
		{"GET", "/ready", "", errors.New("database unavailable"), 503},
		{"GET", "/health", "", errors.New("database unavailable"), 200},
		{"DELETE", "/jobs/a", "", nil, 405},
	} {
		w := httptest.NewRecorder()
		Handler(&fakeRepo{err: tc.err}).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.want || strings.Contains(w.Body.String(), "private database detail") {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
}

func TestLongIdempotencyKey(t *testing.T) {
	f := &fakeRepo{}
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"numbers":[1]}`))
	r.Header.Set("Idempotency-Key", strings.Repeat("x", 201))
	w := httptest.NewRecorder()
	Handler(f).ServeHTTP(w, r)
	if w.Code != 400 || f.called {
		t.Fatal("long key accepted")
	}
}
