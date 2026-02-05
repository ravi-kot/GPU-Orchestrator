# AI Infra Orchestrator (Mini Control Plane)

A mini AI infrastructure control-plane that schedules compute jobs, exports SLO-grade telemetry, and auto-remediates failures (retries + quarantine), with Prometheus + Grafana dashboards.

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
