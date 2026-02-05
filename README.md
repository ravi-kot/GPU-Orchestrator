# AI Infra Orchestrator (Mini Control Plane)

A mini AI infrastructure control plane that schedules compute jobs, exports SLO-grade telemetry, and auto-remediates failures (retry + quarantine). Runs locally with Docker Compose, Prometheus, and Grafana.

## Why this project
This repo demonstrates real AI infrastructure skills:
- orchestration (queue, scheduling, concurrency, retries/backoff)
- observability (metrics, dashboards, alerts)
- reliability automation (failure detection + auto-remediation)
- incident replay + runbook (SRE-style)

## Architecture
Control plane:
- Job Manager (FastAPI): job API, scheduler, worker health, remediation
Data plane:
- Workers: pull jobs, run workloads, report results

Observability:
- Prometheus scrapes /metrics
- Grafana dashboards (p95 runtime, queue delay, success rate, backlog)

## Features (MVP)
- REST API: submit jobs, query status
- Worker registration + heartbeat
- Scheduling + concurrency control
- Retries w/ exponential backoff
- Worker quarantine after failure bursts
- Prometheus metrics + alert rules
- Grafana dashboard
- Load generator + incident replay script

## Quickstart
```bash
make up
make scale N=3
make load
```

Open:
- Job Manager: http://localhost:8000/healthz
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Demo (incident replay)
```bash
make incident
```

## What this proves
- orchestration: queue, scheduling, concurrency control
- observability: Prometheus metrics + Grafana dashboards + alert rules
- reliability automation: retries/backoff + quarantine
