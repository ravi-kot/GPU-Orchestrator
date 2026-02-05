from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from .event_log import get_event_logger
from .logging_config import configure_logging
from .models import JOBS, WORKERS, Job, Worker, now
from .scheduler import (
    RUNNING_TTL_S,
    assign_job_to_worker,
    backoff_s,
    record_failure_and_maybe_quarantine,
    retry_later,
    should_retry,
    worker_alive,
    worker_quarantined,
)

# -----------------------
# App + state
# -----------------------
app = FastAPI(title="AI Infra Orchestrator - Job Manager")
JOB_Q: asyncio.Queue[str] = asyncio.Queue()
log = logging.getLogger("job-manager")
event_log = get_event_logger()

# -----------------------
# Prometheus metrics
# -----------------------
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Total jobs submitted")
JOBS_COMPLETED = Counter("jobs_completed_total", "Total jobs completed", ["status"])  # succeeded|failed|dead
QUEUE_DEPTH = Gauge("queue_depth", "Jobs waiting in queue")
JOBS_INFLIGHT = Gauge("jobs_inflight", "Jobs currently running")
WORKERS_ACTIVE = Gauge("workers_active", "Workers seen recently")
WORKERS_QUARANTINED = Gauge("workers_quarantined", "Workers currently quarantined")
REMEDIATIONS = Counter("remediation_actions_total", "Remediation actions", ["action"])  # retry|quarantine|requeue_orphan
JOB_RUNTIME_S = Histogram("job_runtime_seconds", "Job runtime seconds")
QUEUE_DELAY_S = Histogram("job_queue_delay_seconds", "Time from submit to first assignment (seconds)")

# -----------------------
# API models
# -----------------------


class SubmitJobReq(BaseModel):
    job_type: str = Field(default="cv_blur", description="sleep | matmul | cv_blur")
    payload: Dict[str, Any] = Field(default_factory=dict)
    max_retries: int = Field(default=2, ge=0, le=10)


class WorkerRegisterReq(BaseModel):
    name: str = "worker"
    capabilities: Dict[str, Any] = Field(default_factory=dict)


class WorkReportReq(BaseModel):
    job_id: str
    worker_id: str
    status: str  # succeeded | failed
    runtime_s: float
    error: Optional[str] = None


# -----------------------
# Background tasks
# -----------------------


async def metrics_loop() -> None:
    """Continuously update gauges from in-memory state."""

    while True:
        try:
            QUEUE_DEPTH.set(JOB_Q.qsize())
            inflight = sum(1 for j in JOBS.values() if j.state == "running")
            JOBS_INFLIGHT.set(inflight)
            active = [w for w in WORKERS.values() if worker_alive(w)]
            WORKERS_ACTIVE.set(len(active))
            WORKERS_QUARANTINED.set(sum(1 for w in active if worker_quarantined(w)))
        except Exception:
            # Metrics must never crash the service
            pass
        await asyncio.sleep(2)


async def orphan_reaper_loop() -> None:
    """Requeue jobs stuck in running state (simulates a production watchdog)."""

    while True:
        try:
            t = now()
            for j in list(JOBS.values()):
                if j.state != "running" or j.assigned_at is None:
                    continue
                if (t - j.assigned_at) > RUNNING_TTL_S:
                    # free worker if known
                    if j.last_worker and j.last_worker in WORKERS:
                        WORKERS[j.last_worker].busy = False
                    # requeue
                    j.state = "queued"
                    j.assigned_at = None
                    j.last_worker = None
                    await JOB_Q.put(j.job_id)
                    REMEDIATIONS.labels(action="requeue_orphan").inc()
        except Exception:
            pass
        await asyncio.sleep(5)


@app.on_event("startup")
async def startup() -> None:
    configure_logging()
    asyncio.create_task(metrics_loop())
    asyncio.create_task(orphan_reaper_loop())


# -----------------------
# Routes
# -----------------------


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/jobs")
async def submit_job(req: SubmitJobReq):
    job_id = str(uuid.uuid4())
    JOBS[job_id] = Job(
        job_id=job_id,
        job_type=req.job_type,
        payload=req.payload,
        max_retries=req.max_retries,
    )
    await JOB_Q.put(job_id)
    JOBS_SUBMITTED.inc()
    log.info("job submitted", extra={"event": "job_submitted", "job_id": job_id, "state": "queued"})
    event_log.emit("job_submitted", {"job_id": job_id, "state": "queued"})
    return {"job_id": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    j = JOBS.get(job_id)
    if not j:
        return Response(status_code=404)
    return {
        "job_id": j.job_id,
        "job_type": j.job_type,
        "state": j.state,
        "attempts": j.attempts,
        "max_retries": j.max_retries,
        "created_at": j.created_at,
        "assigned_at": j.assigned_at,
        "finished_at": j.finished_at,
        "next_retry_at": j.next_retry_at,
        "last_worker": j.last_worker,
        "last_error": j.last_error,
    }


@app.post("/workers/register")
def register_worker(req: WorkerRegisterReq):
    worker_id = str(uuid.uuid4())
    WORKERS[worker_id] = Worker(worker_id=worker_id, name=req.name, capabilities=req.capabilities)
    return {"worker_id": worker_id}


@app.post("/workers/{worker_id}/heartbeat")
def worker_heartbeat(worker_id: str):
    w = WORKERS.get(worker_id)
    if not w:
        return Response(status_code=404)
    w.last_seen = now()
    return {"ok": True}


@app.get("/work/pull")
async def pull_work(worker_id: str):
    w = WORKERS.get(worker_id)
    if not w:
        return Response(status_code=404)

    # Refresh heartbeat on pull.
    w.last_seen = now()

    # Do not assign if quarantined or busy
    if worker_quarantined(w) or w.busy:
        return Response(status_code=204)

    # Try a few times to pop a valid queued job_id (skip stale IDs).
    job_id = None
    for _ in range(5):
        try:
            candidate = await asyncio.wait_for(JOB_Q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            break
        j = JOBS.get(candidate)
        if j and j.state == "queued":
            job_id = candidate
            break

    if job_id is None:
        return Response(status_code=204)

    j = JOBS[job_id]

    # Track queue delay only on first assignment
    if j.attempts == 0:
        QUEUE_DELAY_S.observe(max(0.0, now() - j.created_at))

    assign_job_to_worker(j, w)
    log.info(
        "job assigned",
        extra={"event": "job_assigned", "job_id": j.job_id, "worker_id": worker_id, "attempts": j.attempts, "state": j.state},
    )
    event_log.emit(
        "job_assigned",
        {"job_id": j.job_id, "worker_id": worker_id, "attempts": j.attempts, "state": j.state},
    )

    return {
        "job_id": j.job_id,
        "job_type": j.job_type,
        "payload": j.payload,
    }


@app.post("/work/report")
async def report_work(req: WorkReportReq):
    j = JOBS.get(req.job_id)
    w = WORKERS.get(req.worker_id)
    if not j or not w:
        return Response(status_code=404)

    w.busy = False
    j.last_worker = req.worker_id

    if req.runtime_s >= 0:
        JOB_RUNTIME_S.observe(req.runtime_s)

    if req.status == "succeeded":
        j.state = "succeeded"
        j.finished_at = now()
        j.last_error = None
        JOBS_COMPLETED.labels(status="succeeded").inc()
        log.info(
            "job succeeded",
            extra={"event": "job_completed", "job_id": j.job_id, "worker_id": req.worker_id, "status": "succeeded", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state},
        )
        event_log.emit(
            "job_completed",
            {"job_id": j.job_id, "worker_id": req.worker_id, "status": "succeeded", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state, "error": ""},
        )
        return {"ok": True}

    # failed
    j.state = "failed"
    j.last_error = req.error or "unknown error"

    # quarantine decision
    if record_failure_and_maybe_quarantine(w):
        REMEDIATIONS.labels(action="quarantine").inc()
        log.warning("worker quarantined", extra={"event": "worker_quarantined", "worker_id": req.worker_id})
        event_log.emit("worker_quarantined", {"worker_id": req.worker_id})

    if should_retry(j):
        delay = backoff_s(j.attempts)
        j.state = "retrying"
        j.next_retry_at = now() + delay
        REMEDIATIONS.labels(action="retry").inc()
        asyncio.create_task(retry_later(j.job_id, JOB_Q, delay))
        JOBS_COMPLETED.labels(status="failed").inc()
        log.warning(
            "job failed; retry scheduled",
            extra={"event": "job_failed", "job_id": j.job_id, "worker_id": req.worker_id, "status": "failed", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state, "error": j.last_error},
        )
        event_log.emit(
            "job_failed",
            {"job_id": j.job_id, "worker_id": req.worker_id, "status": "failed", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state, "error": j.last_error},
        )
        return {"ok": True, "retry_in_s": delay}

    # terminal failure
    j.state = "dead"
    j.finished_at = now()
    JOBS_COMPLETED.labels(status="dead").inc()
    log.error(
        "job dead",
        extra={"event": "job_dead", "job_id": j.job_id, "worker_id": req.worker_id, "status": "dead", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state, "error": j.last_error},
    )
    event_log.emit(
        "job_dead",
        {"job_id": j.job_id, "worker_id": req.worker_id, "status": "dead", "runtime_s": req.runtime_s, "attempts": j.attempts, "state": j.state, "error": j.last_error},
    )
    return {"ok": True, "dead": True}
