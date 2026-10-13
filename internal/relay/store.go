package relay

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var ErrNotFound = errors.New("job not found")
var ErrConflict = errors.New("idempotency key already used with different input")

type Attempt struct {
	Number int       `json:"attempt"`
	Error  *string   `json:"error,omitempty"`
	At     time.Time `json:"finished_at"`
}

type Job struct {
	ID          string    `json:"id"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	Result      *Report   `json:"result,omitempty"`
	LastError   *string   `json:"last_error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	History     []Attempt `json:"history"`
}

type Store struct {
	Pool        *pgxpool.Pool
	MaxAttempts int
	RetryDelay  time.Duration
}

func (s *Store) Migrate(ctx context.Context) error {
	// Serialize initialization when API and workers start together.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(728451)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Submit(ctx context.Context, numbers []float64, key string) (string, bool, error) {
	if err := Validate(numbers); err != nil {
		return "", false, err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", false, err
	}
	id := hex.EncodeToString(random[:])
	data, err := json.Marshal(numbers)
	if err != nil {
		return "", false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)
	var dbKey *string
	if key != "" {
		dbKey = &key
	}
	tag, err := tx.Exec(ctx, `INSERT INTO jobs(id, idempotency_key, numbers, max_attempts)
		VALUES ($1,$2,$3,$4) ON CONFLICT (idempotency_key) DO NOTHING`, id, dbKey, data, s.MaxAttempts)
	if err != nil {
		return "", false, err
	}
	if tag.RowsAffected() == 0 {
		var same bool
		if err := tx.QueryRow(ctx, `SELECT id, numbers = $2::jsonb FROM jobs WHERE idempotency_key=$1`, key, data).Scan(&id, &same); err != nil {
			return "", false, err
		}
		if !same {
			return "", false, ErrConflict
		}
		return id, false, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox(job_id) VALUES ($1)`, id); err != nil {
		return "", false, err
	}
	return id, true, tx.Commit(ctx)
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	// A consistent snapshot prevents attempts and history disagreeing during a retry.
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	j := Job{History: []Attempt{}}
	var result []byte
	err = tx.QueryRow(ctx, `SELECT id,status,attempts,max_attempts,result,last_error,created_at,updated_at FROM jobs WHERE id=$1`, id).
		Scan(&j.ID, &j.Status, &j.Attempts, &j.MaxAttempts, &result, &j.LastError, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, err
	}
	if result != nil {
		j.Result = &Report{}
		if err := json.Unmarshal(result, j.Result); err != nil {
			return Job{}, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT attempt,error,finished_at FROM job_attempts WHERE job_id=$1 ORDER BY attempt`, id)
	if err != nil {
		return Job{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.Number, &a.Error, &a.At); err != nil {
			return Job{}, err
		}
		j.History = append(j.History, a)
	}
	if err := rows.Err(); err != nil {
		return Job{}, err
	}
	return j, tx.Commit(ctx)
}

// DispatchOne retains the outbox entry until Kafka acknowledges the publish.
// A crash between publish and commit can redeliver; workers tolerate that.
func (s *Store) DispatchOne(ctx context.Context, publish func(context.Context, string) error) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id string
	err = tx.QueryRow(ctx, `SELECT job_id FROM outbox WHERE published_at IS NULL AND available_at <= now()
		ORDER BY available_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := publish(ctx, id); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at=now() WHERE job_id=$1`, id); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type Processor func([]float64, int) (Report, error)

// Process keeps the job row locked during this small, side-effect-free workload.
// This serializes duplicate deliveries, including concurrent worker instances.
func (s *Store) Process(ctx context.Context, id string, process Processor) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var status string
	var attempts, maxAttempts int
	var numbers []byte
	var due bool
	err = tx.QueryRow(ctx, `SELECT status,attempts,max_attempts,numbers,next_attempt_at<=now() FROM jobs WHERE id=$1 FOR UPDATE`, id).
		Scan(&status, &attempts, &maxAttempts, &numbers, &due)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != "queued" || !due {
		return tx.Commit(ctx)
	}
	var input []float64
	if err := json.Unmarshal(numbers, &input); err != nil {
		return err
	}
	attempts++
	result, processErr := process(input, attempts)
	var errorText *string
	if processErr != nil {
		text := processErr.Error()
		errorText = &text
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_attempts(job_id,attempt,error) VALUES ($1,$2,$3)`, id, attempts, errorText); err != nil {
		return err
	}
	if processErr == nil {
		data, err := json.Marshal(result)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE jobs SET status='completed',attempts=$2,result=$3,last_error=NULL,updated_at=now() WHERE id=$1`, id, attempts, data)
		if err != nil {
			return err
		}
	} else if attempts >= maxAttempts {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status='failed',attempts=$2,last_error=$3,updated_at=now() WHERE id=$1`, id, attempts, errorText); err != nil {
			return err
		}
	} else {
		delay := s.RetryDelay * time.Duration(1<<(attempts-1))
		if _, err := tx.Exec(ctx, `UPDATE jobs SET attempts=$2,last_error=$3,updated_at=now(),next_attempt_at=now()+($4 * interval '1 millisecond') WHERE id=$1`, id, attempts, errorText, delay.Milliseconds()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO outbox(job_id,available_at) SELECT id,next_attempt_at FROM jobs WHERE id=$1
			ON CONFLICT(job_id) DO UPDATE SET available_at=EXCLUDED.available_at,published_at=NULL`, id); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("persist processing outcome: %w", err)
	}
	return nil
}
