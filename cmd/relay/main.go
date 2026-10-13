package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Jeffery-byte/relay/internal/relay"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	if err := run(); err != nil {
		slog.Error("relay stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 || (os.Args[1] != "api" && os.Args[1] != "worker") {
		return errors.New("usage: relay api|worker")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	maxAttempts, err := intEnv("MAX_ATTEMPTS", 3, 1, 10)
	if err != nil {
		return err
	}
	demoFailures, err := intEnv("DEMO_FAIL_ATTEMPTS", 0, 0, 10)
	if err != nil {
		return err
	}
	config, err := pgxpool.ParseConfig(env("DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable"))
	if err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	config.MaxConns = 10
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := &relay.Store{Pool: pool, MaxAttempts: maxAttempts, RetryDelay: time.Second}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = store.Migrate(startup)
	cancel()
	if err != nil {
		return fmt.Errorf("initialize database: %w", err)
	}
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := env("KAFKA_TOPIC", "relay.jobs")
	if os.Args[1] == "worker" {
		return worker(ctx, store, brokers, topic, demoFailures)
	}
	return api(ctx, store, brokers, topic)
}

func api(ctx context.Context, store *relay.Store, brokers []string, topic string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, WriteTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, BatchTimeout: 10 * time.Millisecond}
	defer writer.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); dispatch(ctx, store, writer) }()
	server := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: relay.Handler(store), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("API started", "address", server.Addr)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-done:
	}
	cancel()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		slog.Error("HTTP shutdown", "error", err)
	}
	wg.Wait()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

func dispatch(ctx context.Context, store *relay.Store, writer *kafka.Writer) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		sent, err := store.DispatchOne(attempt, func(ctx context.Context, id string) error {
			data, _ := json.Marshal(map[string]string{"job_id": id})
			return writer.WriteMessages(ctx, kafka.Message{Key: []byte(id), Value: data})
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("dispatch deferred; job remains in outbox", "error", err)
		}
		if err != nil || !sent {
			if !pause(ctx, 500*time.Millisecond) {
				return
			}
		}
	}
}

func worker(ctx context.Context, store *relay.Store, brokers []string, topic string, demoFailures int) error {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: env("KAFKA_GROUP", "relay.workers"), StartOffset: kafka.FirstOffset, CommitInterval: 0, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second, WatchPartitionChanges: true})
	defer reader.Close()
	slog.Info("worker started", "topic", topic)
	if demoFailures > 0 {
		slog.Warn("demo failure injection enabled", "attempts", demoFailures)
	}
	for ctx.Err() == nil {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}
		var event struct {
			JobID string `json:"job_id"`
		}
		if err := json.Unmarshal(msg.Value, &event); err != nil || event.JobID == "" {
			slog.Error("skipping malformed message", "partition", msg.Partition, "offset", msg.Offset)
		} else {
			// Retry THIS message on storage failures. Fetching another message could
			// later commit past the failed one in the same partition.
			for {
				attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
				err = store.Process(attempt, event.JobID, func(numbers []float64, attempt int) (relay.Report, error) {
					if attempt <= demoFailures {
						return relay.Report{}, errors.New("injected demo failure")
					}
					return relay.GenerateReport(numbers)
				})
				cancel()
				if errors.Is(err, relay.ErrNotFound) {
					slog.Error("skipping unknown job", "job_id", event.JobID)
					break
				}
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return nil
				}
				slog.Error("storage failure; offset not committed", "job_id", event.JobID, "error", err)
				if !pause(ctx, time.Second) {
					return nil
				}
			}
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit offset: %w", err)
		}
		slog.Info("message handled", "job_id", event.JobID, "partition", msg.Partition, "offset", msg.Offset)
	}
	return nil
}

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback, min, max int) (int, error) {
	value, err := strconv.Atoi(env(key, strconv.Itoa(fallback)))
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s must be between %d and %d", key, min, max)
	}
	return value, nil
}
