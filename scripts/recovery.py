#!/usr/bin/env python3
"""Docker-only outage checks. Restores stopped services even after failure."""
import json
import subprocess
import time
import urllib.request
import uuid

base = "http://localhost:8080"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True)


def submit():
    req = urllib.request.Request(
        base + "/jobs",
        data=json.dumps({"numbers": [2, 4, 6]}).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        if response.status != 202:
            raise AssertionError("job rejected during outage")
        return json.load(response)["id"]


def get(job_id):
    with urllib.request.urlopen(base + "/jobs/" + job_id, timeout=5) as response:
        return json.load(response)


def wait(job_id):
    for _ in range(120):
        job = get(job_id)
        if job["status"] == "completed":
            if job["attempts"] != 1 or job["result"]["sum"] != 12:
                raise AssertionError(job)
            return
        time.sleep(0.5)
    raise AssertionError(f"job did not recover: {get(job_id)}")


try:
    compose("stop", "worker")
    job_id = submit()
    time.sleep(1)
    if get(job_id)["status"] != "queued":
        raise AssertionError("job executed while worker stopped")
    compose("start", "worker")
    wait(job_id)
    print("PASS: queued job recovered after worker restart", flush=True)

    compose("stop", "kafka")
    job_id = submit()
    if get(job_id)["status"] != "queued":
        raise AssertionError("job should remain queued during broker outage")
    compose("start", "kafka")
    wait(job_id)
    print("PASS: accepted job survived broker outage through durable outbox", flush=True)
finally:
    compose("start", "kafka", "worker")
