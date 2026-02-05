import os
import logging
import random
import time
from typing import Any, Dict

import cv2 as cv  # IMPORTANT: keep this import style (matches guide)
import numpy as np
import requests

from logging_config import configure_logging

MANAGER_URL = os.environ.get("MANAGER_URL", "http://job-manager:8000")
WORKER_NAME = os.environ.get("WORKER_NAME", "worker")
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.05"))
HEARTBEAT_S = float(os.environ.get("HEARTBEAT_S", "5"))


def register() -> str:
    r = requests.post(
        f"{MANAGER_URL}/workers/register",
        json={"name": WORKER_NAME, "capabilities": {"gpu": False}},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["worker_id"]


def heartbeat(worker_id: str) -> None:
    requests.post(f"{MANAGER_URL}/workers/{worker_id}/heartbeat", timeout=10).raise_for_status()


def do_job(job_type: str, payload: Dict[str, Any]) -> None:
    # Failure injection for incident replay
    if random.random() < FAIL_RATE:
        raise RuntimeError("injected failure")

    if job_type == "sleep":
        time.sleep(float(payload.get("t", 0.2)))
        return

    if job_type == "matmul":
        n = int(payload.get("n", 512))
        a = np.random.randn(n, n).astype(np.float32)
        b = np.random.randn(n, n).astype(np.float32)
        _ = a @ b
        return

    # default: cv_blur (AI-flavored synthetic workload)
    h = int(payload.get("h", 720))
    w = int(payload.get("w", 1280))
    k = int(payload.get("k", 11))
    # OpenCV requires odd kernel size
    if k % 2 == 0:
        k += 1
    img = (np.random.rand(h, w, 3) * 255).astype(np.uint8)
    _ = cv.GaussianBlur(img, (k, k), 0)


def main() -> None:
    configure_logging()
    log = logging.getLogger("worker")

    # Manager may not be ready when the worker starts; retry register with backoff.
    attempt = 0
    worker_id = None
    while worker_id is None:
        try:
            worker_id = register()
        except Exception as e:
            attempt += 1
            delay = min(10.0, 0.5 * (2 ** min(attempt, 5)))
            log.warning(
                "register failed; retrying",
                extra={"event": "worker_register_failed", "error": str(e)},
            )
            time.sleep(delay)

    log.info("registered", extra={"event": "worker_registered", "worker_id": worker_id})
    last_hb = 0.0

    while True:
        now = time.time()
        if now - last_hb >= HEARTBEAT_S:
            try:
                heartbeat(worker_id)
                log.info("heartbeat", extra={"event": "worker_heartbeat", "worker_id": worker_id})
            except Exception:
                # best-effort heartbeat
                pass
            last_hb = now

        # Pull work
        try:
            r = requests.get(f"{MANAGER_URL}/work/pull", params={"worker_id": worker_id}, timeout=15)
            if r.status_code == 204:
                time.sleep(0.1)
                continue
            r.raise_for_status()
            job = r.json()
            log.info(
                "picked job",
                extra={"event": "job_picked", "worker_id": worker_id, "job_id": job.get("job_id"), "job_type": job.get("job_type")},
            )
        except Exception:
            time.sleep(0.2)
            continue

        # Execute + report
        t0 = time.time()
        status = "succeeded"
        err = None
        try:
            do_job(job["job_type"], job.get("payload", {}))
        except Exception as e:
            status = "failed"
            err = str(e)

        runtime_s = max(0.0, time.time() - t0)
        log.info(
            "job finished",
            extra={
                "event": "job_finished",
                "worker_id": worker_id,
                "job_id": job.get("job_id"),
                "job_type": job.get("job_type"),
                "status": status,
                "runtime_s": runtime_s,
                "error": err,
            },
        )
        try:
            requests.post(
                f"{MANAGER_URL}/work/report",
                json={
                    "job_id": job["job_id"],
                    "worker_id": worker_id,
                    "status": status,
                    "runtime_s": runtime_s,
                    "error": err,
                },
                timeout=15,
            ).raise_for_status()
        except Exception:
            # If reporting fails, the job may become orphaned; the manager reaper will requeue it.
            pass


if __name__ == "__main__":
    main()
