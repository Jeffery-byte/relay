# Relay

A distributed job-processing system for learning asynchronous execution, durable job state, and failure recovery.

**Status:** repository initialized. The application is not implemented yet.

## Stack

| Component | Purpose |
| --- | --- |
| Go | Job submission API and background workers |
| Kafka | Durable queue between the API and workers |
| PostgreSQL | Job status, attempts, and results |
| Docker Compose | Run the services locally |

## First version

Submit a list of numbers and receive a job ID. A background worker generates a summary report containing count, sum, minimum, maximum, and mean. Retrieve the job status and stored result through the API.

Planned endpoints:

- `POST /jobs`: validate input and submit a report job.
- `GET /jobs/{id}`: retrieve status, attempts, and result.
- `GET /health`: basic service health.

## Three-day build plan

### Day 1: Working pipeline

- [ ] Start Kafka and PostgreSQL with Docker Compose.
- [ ] Create the job submission API and database schema.
- [ ] Publish jobs to Kafka and process them with one worker.
- [ ] Save report results and expose job status.

### Day 2: Failure handling

- [ ] Add bounded retries and terminal failure state.
- [ ] Protect stored results against duplicate message delivery.
- [ ] Commit Kafka offsets only after durable processing.
- [ ] Test worker crashes and restarts.
- [ ] Address the gap between saving a job and publishing its message.

### Day 3: Multiple workers and verification

- [ ] Run multiple workers in one Kafka consumer group.
- [ ] Verify normal execution, invalid input, retries, and recovery.
- [ ] Document setup, architecture, tradeoffs, and observed results.

## Systems questions

- What happens when a worker crashes before saving a result?
- What happens when it saves a result but crashes before committing its offset?
- How do retries avoid duplicate results?
- How do Kafka partitions distribute work among workers?
- What happens if the API saves a job but cannot publish it?

## Scope

The first version runs locally with one Kafka broker and one PostgreSQL instance. It focuses on worker failure recovery, not broker or database high availability. Redis is outside the initial scope. Performance and reliability claims will be added only after corresponding tests have been run and recorded.

## Running

Setup commands will be added with the working implementation.
