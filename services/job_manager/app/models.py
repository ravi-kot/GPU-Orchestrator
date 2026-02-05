from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Optional


def now() -> float:
    return time.time()


@dataclass
class Job:
    """Represents one unit of work in the system."""

    job_id: str
    job_type: str
    payload: Dict[str, Any]
    # number of retries after the first attempt
    max_retries: int

    created_at: float = field(default_factory=now)
    assigned_at: Optional[float] = None
    finished_at: Optional[float] = None
    next_retry_at: Optional[float] = None

    attempts: int = 0
    state: str = "queued"  # queued | running | retrying | succeeded | failed | dead

    last_worker: Optional[str] = None
    last_error: Optional[str] = None


@dataclass
class Worker:
    """Represents a worker process/container that can execute jobs."""

    worker_id: str
    name: str
    capabilities: Dict[str, Any]

    last_seen: float = field(default_factory=now)
    quarantined_until: float = 0.0
    busy: bool = False

    # Timestamps of recent failures for quarantine decisions
    failures_window: Deque[float] = field(default_factory=lambda: deque(maxlen=100))


# In-memory stores (MVP). For production you would use a DB/Redis.
JOBS: Dict[str, Job] = {}
WORKERS: Dict[str, Worker] = {}


