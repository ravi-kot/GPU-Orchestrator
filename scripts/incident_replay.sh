#!/usr/bin/env bash
set -euo pipefail

echo "[1/4] Baseline: start stack and scale workers"
make up
make scale N=3

echo "[2/4] Baseline load (low fail rate)"
python scripts/loadgen.py --n 200 --job-type cv_blur

echo "[3/4] Inject failures: set FAIL_RATE=0.25 for worker in infra/docker-compose.yml, then recreate workers"
echo "Edit infra/docker-compose.yml (worker environment FAIL_RATE=0.25) and run:"
echo " make up"
echo "Then run high load:"
python scripts/loadgen.py --n 400 --job-type cv_blur

echo "[4/4] Recovery: restore FAIL_RATE=0.05 and run make up; observe alerts resolving in Prometheus/Grafana"


