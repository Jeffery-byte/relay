//go:build integration

package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://relay:relay@localhost:5432/relay?sslmode=disable"
	}
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var b [8]byte
	if _, err = rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := "relay_test_" + hex.EncodeToString(b[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &Store{Pool: pool, MaxAttempts: 3, RetryDelay: 20 * time.Millisecond}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func submit(t *testing.T, s *Store, key string) string {
	t.Helper()
	id, created, err := s.Submit(context.Background(), []float64{10, 20, 30}, key)
	if err != nil || !created {
		t.Fatalf("submit: %s %v %v", id, created, err)
	}
	return id
}

func TestConcurrentIdempotentSubmissions(t *testing.T) {
	s := testStore(t)
	var wg sync.WaitGroup
	var created atomic.Int32
	ids := make(chan string, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, newJob, err := s.Submit(context.Background(), []float64{10, 20, 30}, "same-key")
			if err != nil {
				t.Error(err)
				return
			}
			if newJob {
				created.Add(1)
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	if created.Load() != 1 {
		t.Fatalf("created %d jobs", created.Load())
	}
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate submission returned a different ID")
		}
	}
	_, _, err := s.Submit(context.Background(), []float64{99}, "same-key")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("different payload: %v", err)
	}
	var jobs, outbox int
	if err := s.Pool.QueryRow(context.Background(), "SELECT (SELECT count(*) FROM jobs),(SELECT count(*) FROM outbox)").Scan(&jobs, &outbox); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || outbox != 1 {
		t.Fatalf("jobs=%d outbox=%d", jobs, outbox)
	}
}

func TestOutboxSurvivesPublishFailure(t *testing.T) {
	s := testStore(t)
	id := submit(t, s, "")
	ctx := context.Background()
	sent, err := s.DispatchOne(ctx, func(context.Context, string) error { return errors.New("broker unavailable") })
	if sent || err == nil {
		t.Fatal("publish failure was ignored")
	}
	sent, err = s.DispatchOne(ctx, func(_ context.Context, got string) error {
		if got != id {
			t.Fatal("wrong job")
		}
		return nil
	})
	if !sent || err != nil {
		t.Fatalf("recovery: %v %v", sent, err)
	}
	sent, err = s.DispatchOne(ctx, func(context.Context, string) error { t.Fatal("published twice without failure"); return nil })
	if sent || err != nil {
		t.Fatalf("empty outbox: %v %v", sent, err)
	}
}

func TestConcurrentRedeliveryStoresOneResult(t *testing.T) {
	s := testStore(t)
	id := submit(t, s, "")
	ctx := context.Background()
	var wg sync.WaitGroup
	var calls atomic.Int32
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Process(ctx, id, func(n []float64, _ int) (Report, error) { calls.Add(1); return GenerateReport(n) })
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	j, err := s.Get(ctx, id)
	if err != nil || calls.Load() != 1 || j.Attempts != 1 || len(j.History) != 1 || j.Status != "completed" || j.Result.Sum != 60 {
		t.Fatalf("calls=%d job=%+v err=%v", calls.Load(), j, err)
	}
}

func TestRetryScheduleAndRecovery(t *testing.T) {
	s := testStore(t)
	id := submit(t, s, "")
	ctx := context.Background()
	process := func(n []float64, a int) (Report, error) {
		if a < 3 {
			return Report{}, errors.New("transient failure")
		}
		return GenerateReport(n)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err := s.Process(ctx, id, process); err != nil {
			t.Fatal(err)
		}
		j, err := s.Get(ctx, id)
		if err != nil || j.Attempts != attempt {
			t.Fatalf("job=%+v err=%v", j, err)
		}
		if attempt < 3 {
			if err := s.Process(ctx, id, process); err != nil {
				t.Fatal(err)
			}
			j, _ = s.Get(ctx, id)
			if j.Attempts != attempt {
				t.Fatal("early redelivery bypassed backoff")
			}
			time.Sleep(25 * time.Millisecond * time.Duration(1<<(attempt-1)))
		}
	}
	j, err := s.Get(ctx, id)
	if err != nil || j.Status != "completed" || j.Attempts != 3 || len(j.History) != 3 || j.LastError != nil || j.History[0].Error == nil {
		t.Fatalf("job=%+v err=%v", j, err)
	}
}

func TestBoundedFailure(t *testing.T) {
	s := testStore(t)
	s.RetryDelay = 0
	id := submit(t, s, "")
	ctx := context.Background()
	var calls int
	for range 5 {
		if err := s.Process(ctx, id, func([]float64, int) (Report, error) { calls++; return Report{}, errors.New("permanent failure") }); err != nil {
			t.Fatal(err)
		}
	}
	j, err := s.Get(ctx, id)
	if err != nil || j.Status != "failed" || j.Attempts != 3 || len(j.History) != 3 || calls != 3 || j.LastError == nil {
		t.Fatalf("calls=%d job=%+v err=%v", calls, j, err)
	}
}

func TestInterruptedAttemptRollsBack(t *testing.T) {
	s := testStore(t)
	id := submit(t, s, "")
	ctx, cancel := context.WithCancel(context.Background())
	err := s.Process(ctx, id, func(n []float64, _ int) (Report, error) { cancel(); return GenerateReport(n) })
	if err == nil {
		t.Fatal("cancelled transaction committed")
	}
	j, err := s.Get(context.Background(), id)
	if err != nil || j.Attempts != 0 || len(j.History) != 0 || j.Status != "queued" {
		t.Fatalf("interrupted job=%+v err=%v", j, err)
	}
	if err := s.Process(context.Background(), id, func(n []float64, _ int) (Report, error) { return GenerateReport(n) }); err != nil {
		t.Fatal(err)
	}
	j, err = s.Get(context.Background(), id)
	if err != nil || j.Attempts != 1 || j.Status != "completed" {
		t.Fatalf("recovered job=%+v err=%v", j, err)
	}
}
