import time
import json
import logging
import random
from redis import Redis
import httpx
from prometheus_client import Counter, Histogram, start_http_server

from logging_config import configure_logging

JOB_MANAGER = "http://localhost:8000"
REDIS_URL = "redis://localhost:6379/0"
POLL_INTERVAL = 2.0
MAX_RETRIES = 5
BASE_BACKOFF = 1.0

jobs_picked_total = Counter("worker_jobs_picked_total", "Jobs picked by worker")
worker_failures_total = Counter("worker_failures_total", "Job failures at worker")
worker_job_runtime_seconds = Histogram("worker_job_runtime_seconds", "Job runtime")

def mark_running(r: Redis, job_id: str):
    r.hset(f"job:{job_id}", mapping={
        "state": "running",
        "started_at": time.time(),
    })

def mark_done(r: Redis, job_id: str, result: dict):
    r.hset(f"job:{job_id}", mapping={
        "state": "succeeded",
        "finished_at": time.time(),
        "result": json.dumps(result),
    })

def mark_failed(r: Redis, job_id: str, error: str):
    r.hset(f"job:{job_id}", mapping={
        "state": "failed",
        "finished_at": time.time(),
        "error": error,
    })

def requeue_with_backoff(r: Redis, job_id: str, attempts: int):
    delay = BASE_BACKOFF * (2 ** attempts)
    r.zadd("jobs:delayed", {job_id: time.time() + delay})

def execute(job: dict):
    params = json.loads(job.get("params", "{}")) if isinstance(job.get("params"), str) else job.get("params", {})
    duration = float(params.get("sleep", 0.5))
    fail_rate = float(params.get("fail_rate", 0.1))
    time.sleep(duration)
    if random.random() < fail_rate:
        raise RuntimeError("simulated failure")
    return {"ok": True, "duration": duration}

def main():
    configure_logging()
    log = logging.getLogger("worker")
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    start_http_server(9000)

    while True:
        now = time.time()
        ready = r.zrangebyscore("jobs:delayed", 0, now, start=0, num=1)
        if ready:
            job_id = ready[0]
            r.zrem("jobs:delayed", job_id)
            r.rpush("jobs:queue", job_id)

        job = r.brpop("jobs:queue", timeout=5)
        if not job:
            log.info("No job, waiting...")
            continue
        _, job_id = job
        jobs_picked_total.inc()
        log.info("Picked job %s", job_id)

        job_data = r.hgetall(f"job:{job_id}")
        if not job_data:
            log.warning("Job %s missing data", job_id)
            continue

        try:
            mark_running(r, job_id)
            start = time.time()
            result = execute(job_data)
            worker_job_runtime_seconds.observe(time.time() - start)
            mark_done(r, job_id, result)
            log.info("Completed job %s", job_id)
        except Exception as exc:  # broad on purpose
            worker_failures_total.inc()
            attempts = int(job_data.get("attempts", 0)) + 1
            r.hset(f"job:{job_id}", mapping={"attempts": attempts})
            log.exception("Job %s failed (attempt %s): %s", job_id, attempts, exc)
            if attempts < MAX_RETRIES:
                requeue_with_backoff(r, job_id, attempts)
            else:
                mark_failed(r, job_id, str(exc))

if __name__ == "__main__":
    main()
