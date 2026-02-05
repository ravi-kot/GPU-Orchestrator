# Overview

This system is a mini AI infrastructure control plane.

- Control plane: Job Manager (FastAPI)
- Data plane: Workers (containers)
- Observability: Prometheus (scrape) + Grafana (dashboards)

## Control plane responsibilities

- Accept job submissions
- Maintain job state machine
- Assign work to workers (pull model)
- Apply remediation (retry + quarantine)
- Export metrics

## Data plane responsibilities

- Register + heartbeat
- Pull work
- Execute workloads
- Report success/failure

## Key design decisions

- Pull-based work assignment
- In-memory stores for MVP (swap to Redis/DB later)
- Retry backoff
- Quarantine workers after failure bursts


