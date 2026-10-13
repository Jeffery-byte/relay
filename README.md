# Relay

Relay is a small distributed job-processing system built with **Go, Kafka,
PostgreSQL, and Docker**. Submit a list of numbers, receive a job ID, then retrieve
a report containing its count, sum, minimum, maximum, and mean.

The simple workload makes the delivery behavior easier to study: broker outages,
worker restarts, retries, and duplicate messages.

## Run

Requirements: Docker with Docker Compose v2. Go 1.24+ is needed only for local
development; the Docker build supplies its own toolchain.

```sh
docker compose up --build -d
curl http://localhost:8080/ready
```

The first startup downloads images and builds the application. Compose waits for
PostgreSQL and Kafka, creates a three-partition `relay.jobs` topic, and starts the
API and worker. The API and workers initialize the schema under an advisory lock.

Submit a job:

```sh
curl -i http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: report-example-1' \
  -d '{"numbers":[10,20,30,40,50]}'
```

The response is `202 Accepted`, with a `Location` header and body:

```json
{"id":"<job-id>","created":true}
```

Retrieve the result using the returned ID:

```sh
curl http://localhost:8080/jobs/<job-id>
```

Once completed, the response includes:

```json
{
  "id": "<job-id>",
  "status": "completed",
  "attempts": 1,
  "max_attempts": 3,
  "result": {"count":5,"sum":150,"min":10,"max":50,"mean":30},
  "created_at": "...",
  "updated_at": "...",
  "history": [{"attempt":1,"finished_at":"..."}]
}
```

Resubmitting the same numbers with the same `Idempotency-Key` returns the same ID
with `created:false`. Reusing that key for different numbers returns `409`.
Without a key, each submission intentionally creates a new job.

## Architecture

```mermaid
flowchart TD
    Client["Client"] --> API["Go API"]
    API -->|"Atomic job + outbox insert"| DB[("PostgreSQL")]
    DB -->|"Pending outbox records"| Dispatch["Dispatcher inside API"]
    Dispatch -->|"Acknowledged publish"| Kafka["Kafka: 3 partitions"]
    Kafka --> Worker["Go workers"]
    Worker -->|"Result or durable retry"| DB
    Worker -->|"Commit offset after DB commit"| Kafka
```

- **Persistent state:** PostgreSQL stores jobs, results, and attempt history.
- **Transactional outbox:** accepting a job and scheduling its dispatch are one
  database transaction. A broker outage leaves the job pending for later dispatch.
- **At-least-once delivery:** the dispatcher can publish twice if it crashes after
  Kafka acknowledges a message but before the outbox transaction commits.
- **Duplicate protection:** workers lock the job row while computing this small,
  side-effect-free report. Completed and failed jobs do not execute again.
- **Bounded retries:** a failed report is rescheduled in the same transaction as
  its attempt record. The default is three attempts, with 1s then 2s backoff.
- **Offset ordering:** workers use explicit message fetching and synchronous
  commits. Results or retry records are committed before the Kafka offset. On a
  database failure, the worker retries that same message without fetching past it.
- **Crash recovery:** a worker interruption before the transaction commits rolls
  it back. Kafka redelivery then recomputes the report. An interruption after the
  commit is safe because the stored terminal status prevents another execution.
- **Worker distribution:** workers share `relay.workers`; up to three workers can
  actively consume at once with the default three partitions.

Job states are `queued`, `completed`, and `failed`. Execution holds a database
transaction, so there is no separately committed `running` state in this MVP.
Early duplicate deliveries during backoff are acknowledged without another
attempt; the pending outbox record still schedules the future retry.

## API

| Method | Endpoint | Behavior |
| --- | --- | --- |
| POST | `/jobs` | Validate and durably accept report input; `202`, `400`, `409`, or `503` |
| GET | `/jobs/{id}` | Return state, report, errors, and attempt history; `200`, `404`, or `503` |
| GET | `/health` | Process liveness |
| GET | `/ready` | Database availability; Kafka outages do not prevent durable acceptance |

Input must contain 1–10,000 finite numbers, with no nulls or unknown fields. Bodies
are limited to 1 MiB and idempotency keys to 200 bytes. A sum outside the float64
range fails the job after the retry limit. Reports use float64 arithmetic, so this
is not an exact-decimal financial calculation service.

## Observe recovery

```sh
# Multiple workers; the API remains a single process.
docker compose up -d --scale worker=3
docker compose logs -f api worker

# Stop the worker, submit a job, then start it and query that job again.
docker compose stop worker
docker compose start worker

# Stop Kafka, submit a job, then bring Kafka back. The outbox retains the job.
docker compose stop kafka
docker compose start kafka
```

For a deterministic retry demonstration, recreate workers with failure injection:

```sh
# Fail the first two attempts, then succeed on the third.
DEMO_FAIL_ATTEMPTS=2 docker compose up -d --force-recreate worker
EXPECT_ATTEMPTS=3 python3 scripts/smoke.py

# Fail all three attempts and store a terminal failure.
DEMO_FAIL_ATTEMPTS=3 docker compose up -d --force-recreate worker
EXPECT_ATTEMPTS=3 EXPECT_STATUS=failed python3 scripts/smoke.py

# Restore normal processing.
DEMO_FAIL_ATTEMPTS=0 docker compose up -d --force-recreate worker
```

`DEMO_FAIL_ATTEMPTS` is disabled by default and affects every job processed by that
worker. Keep all workers on the same setting during a demonstration.

## Test and develop

```sh
go test -race ./...
go vet ./...

# Requires a running PostgreSQL instance. Each test creates an isolated schema
# and removes only that schema; it does not truncate application data.
go test -race -tags integration ./internal/relay -count=1

# Requires the full Compose stack and Python 3 (standard library only).
python3 scripts/smoke.py
python3 scripts/recovery.py
```

Integration tests cover concurrent idempotent submissions, publish failures,
concurrent redelivery, retry backoff, terminal failure, and interrupted
transactions. GitHub Actions runs these checks plus the Docker pipeline,
worker/broker outage checks, and injected retry scenarios.

Run services directly against the Compose infrastructure:

```sh
go run ./cmd/relay api
go run ./cmd/relay worker
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://relay:relay@localhost:5432/relay?sslmode=disable` | PostgreSQL connection |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_TOPIC` | `relay.jobs` | Job topic |
| `KAFKA_GROUP` | `relay.workers` | Consumer group |
| `HTTP_ADDR` | `:8080` | API bind address |
| `MAX_ATTEMPTS` | `3` | Attempts for newly accepted jobs, from 1 to 10 |
| `DEMO_FAIL_ATTEMPTS` | `0` | Worker-only failure injection, from 0 to 10 |
| `RELAY_TEST_DATABASE_URL` | Same local database URL | Integration test database |

Compose sets internal service addresses automatically. The PostgreSQL and Kafka
data volumes survive `docker compose down`; `docker compose down -v` deletes
local job and broker data. Ports are bound to localhost for this development setup.

## Scope and tradeoffs

This is a local learning MVP with one broker and one PostgreSQL instance. It does
not provide broker replication, database failover, authentication, or production
monitoring. There is no load-test throughput claim.

Processing while holding a row lock is appropriate for this small report workload.
Long-running work would need leases, heartbeats, and a different transaction
boundary. External effects such as payments or emails would need their own
idempotency guarantees; the current duplicate protection covers database report
results. Malformed Kafka records and unknown job IDs are logged and acknowledged
so they cannot indefinitely block a partition. A dead-letter topic is future work.
