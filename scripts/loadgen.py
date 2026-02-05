import argparse
import time
from typing import Dict, List

import requests

DEFAULT_URL = "http://localhost:8000"


def submit_jobs(base_url: str, n: int, job_type: str) -> List[str]:
    ids: List[str] = []
    for _ in range(n):
        r = requests.post(
            f"{base_url}/jobs",
            json={"job_type": job_type, "payload": {}, "max_retries": 2},
            timeout=10,
        )
        r.raise_for_status()
        ids.append(r.json()["job_id"])
    return ids


def poll(base_url: str, job_ids: List[str], poll_s: float = 0.2) -> Dict[str, int]:
    done = set()
    counts = {"succeeded": 0, "failed": 0, "dead": 0}

    while len(done) < len(job_ids):
        for job_id in job_ids:
            if job_id in done:
                continue
            r = requests.get(f"{base_url}/jobs/{job_id}", timeout=10)
            if r.status_code != 200:
                continue
            j = r.json()
            state = j["state"]
            if state in ("succeeded", "dead"):
                done.add(job_id)
                counts[state] += 1
        time.sleep(poll_s)

    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--n", type=int, default=300)
    ap.add_argument("--job-type", default="cv_blur")
    args = ap.parse_args()

    t0 = time.time()
    job_ids = submit_jobs(args.url, args.n, args.job_type)
    counts = poll(args.url, job_ids)
    dt = max(1e-9, time.time() - t0)

    print("=== LOADGEN RESULTS ===")
    print(f"jobs: {args.n}")
    print(f"wall_time_s: {dt:.2f}")
    print(f"throughput_jobs_per_s: {args.n / dt:.2f}")
    print(counts)


if __name__ == "__main__":
    main()


