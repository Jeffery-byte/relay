#!/usr/bin/env python3
"""Exercise the running API -> PostgreSQL -> Kafka -> worker pipeline."""
import json
import os
import time
import urllib.error
import urllib.request
import uuid

base = os.getenv("RELAY_URL", "http://localhost:8080")
attempts = int(os.getenv("EXPECT_ATTEMPTS", "1"))
expected_status = os.getenv("EXPECT_STATUS", "completed")


def request(method, path, body=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def check(condition, message):
    if not condition:
        raise AssertionError(message)


for _ in range(120):
    try:
        if request("GET", "/ready")[0] == 200:
            break
    except (OSError, urllib.error.URLError):
        pass
    time.sleep(0.5)
else:
    raise RuntimeError("API did not become ready")

key = "smoke-" + str(uuid.uuid4())
payload = {"numbers": [10, 20, 30, 40, 50]}
status, job = request("POST", "/jobs", payload, key)
check(status == 202 and job["created"], f"submit failed: {status} {job}")
status, duplicate = request("POST", "/jobs", payload, key)
check(status == 202 and duplicate["id"] == job["id"] and not duplicate["created"], "duplicate created a new job")
check(request("POST", "/jobs", {"numbers": [99]}, key)[0] == 409, "key conflict accepted")
check(request("POST", "/jobs", {"numbers": [None]})[0] == 400, "invalid input accepted")
check(request("GET", "/jobs/missing")[0] == 404, "missing job not handled")

for _ in range(120):
    status, result = request("GET", "/jobs/" + job["id"])
    check(status == 200, "job retrieval failed")
    if result["status"] in ("completed", "failed"):
        break
    time.sleep(0.5)
else:
    raise RuntimeError(f"job did not finish: {result}")

check(result["status"] == expected_status, f"unexpected status: {result}")
check(result["attempts"] == attempts and len(result["history"]) == attempts, f"unexpected attempts: {result}")
if expected_status == "completed":
    check(result["result"] == {"count": 5, "sum": 150, "min": 10, "max": 50, "mean": 30}, "incorrect report")
else:
    check("result" not in result and "last_error" in result, "failed job missing error")
print(f"PASS: submission, idempotency, validation, and {expected_status} after {attempts} attempt(s)")
