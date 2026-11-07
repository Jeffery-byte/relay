#!/usr/bin/env python3
"""Kill a worker inside its transaction, then verify Kafka redelivery and recovery.

Run against a dedicated Compose demo/test stack. This recreates the worker
service at scale one and restores it with failure injection disabled.
"""
import json
import os
import subprocess
import time
import urllib.request
import uuid

BASE = "http://localhost:8080"


def compose(*args, delay="0", capture=False):
    env = dict(os.environ, DEMO_FAIL_ATTEMPTS="0", DEMO_PROCESS_DELAY_MS=delay)
    return subprocess.run(
        ["docker", "compose", *args],
        env=env,
        check=True,
        capture_output=capture,
        text=True,
        timeout=90,
    )


def get(job_id):
    with urllib.request.urlopen(BASE + "/jobs/" + job_id, timeout=5) as response:
        return json.load(response)


def main():
    try:
        compose("up", "-d", "--force-recreate", "--scale", "worker=1", "worker", delay="10000")
        request = urllib.request.Request(
            BASE + "/jobs",
            data=json.dumps({"numbers": [2, 4, 6]}).encode(),
            headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            if response.status != 202:
                raise AssertionError("job was not accepted")
            job_id = json.load(response)["id"]

        # The marker is emitted inside Store.Process's callback, after its row
        # lock, not merely after fetching a Kafka message or starting a worker.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = compose("logs", "--no-color", "worker", capture=True)
            if any("demo processing delay" in line and f"job_id={job_id}" in line
                   for line in (logs.stdout + logs.stderr).splitlines()):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("worker never entered the processing transaction")

        compose("kill", "--signal", "SIGKILL", "worker")
        interrupted = get(job_id)
        if (interrupted["status"] != "queued" or interrupted["attempts"] != 0
                or interrupted["history"] or "result" in interrupted):
            raise AssertionError(f"killed transaction did not roll back: {interrupted}")

        compose("up", "-d", "--force-recreate", "--scale", "worker=1", "worker")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            recovered = get(job_id)
            if recovered["status"] == "completed":
                if (recovered["attempts"] != 1 or len(recovered["history"]) != 1
                        or recovered["result"] != {"count": 3, "sum": 12, "min": 2, "max": 6, "mean": 4}):
                    raise AssertionError(f"unexpected recovered result: {recovered}")
                print("PASS: SIGKILL during processing rolled back; redelivery stored one result", flush=True)
                return
            if recovered["status"] == "failed":
                raise AssertionError(f"recovered job failed: {recovered}")
            time.sleep(0.5)
        raise AssertionError(f"job did not recover: {get(job_id)}")
    finally:
        compose("up", "-d", "--force-recreate", "--scale", "worker=1", "worker")


if __name__ == "__main__":
    main()
