from __future__ import annotations

import asyncio

from .models import JOBS, Job, Worker, now

# -----------------------
# Tunable parameters
# -----------------------
HEARTBEAT_TTL_S = 20  # worker considered alive if heartbeat within this window
FAIL_WINDOW_S = 60  # window for counting recent failures for quarantine
QUARANTINE_THRESHOLD = 3  # failures within FAIL_WINDOW_S that trigger quarantine
QUARANTINE_COOLDOWN_S = 30  # how long a worker stays quarantined
RUNNING_TTL_S = 60  # if a job has been running longer than this, consider it orphaned
RETRY_BASE_DELAY_S = 1.0  # exponential backoff base


def worker_alive(w: Worker) -> bool:
    return (now() - w.last_seen) <= HEARTBEAT_TTL_S


def worker_quarantined(w: Worker) -> bool:
    return now() < w.quarantined_until


def backoff_s(attempts: int) -> float:
    """Exponential backoff based on attempt number (1,2,3...)."""

    # attempt 1 failure => retry after 1s
    # attempt 2 failure => retry after 2s
    # attempt 3 failure => retry after 4s
    return RETRY_BASE_DELAY_S * (2 ** max(0, attempts - 1))


def assign_job_to_worker(j: Job, w: Worker) -> None:
    w.busy = True
    j.state = "running"
    j.assigned_at = now()
    j.last_worker = w.worker_id
    j.attempts += 1
    j.next_retry_at = None


def record_failure_and_maybe_quarantine(w: Worker) -> bool:
    ts = now()
    w.failures_window.append(ts)
    recent = [t for t in w.failures_window if (ts - t) <= FAIL_WINDOW_S]
    if len(recent) >= QUARANTINE_THRESHOLD:
        w.quarantined_until = ts + QUARANTINE_COOLDOWN_S
        return True
    return False


async def retry_later(job_id: str, job_q: asyncio.Queue[str], delay_s: float) -> None:
    """Re-queue a job after an async delay."""

    await asyncio.sleep(delay_s)
    j = JOBS.get(job_id)
    if not j:
        return
    if j.state in ("succeeded", "dead"):
        return
    j.state = "queued"
    j.assigned_at = None
    j.last_worker = None
    j.next_retry_at = None
    await job_q.put(job_id)


def should_retry(j: Job) -> bool:
    """Retry rule: retry if attempts so far <= max_retries."""

    # attempts increments when assigned. After attempt=1 fails:
    # retry if 1 <= max_retries (max_retries counts retries)
    return j.attempts <= j.max_retries


