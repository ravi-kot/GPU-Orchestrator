# Alerts and actions

## HighJobFailureRate

**Meaning:** job failures > 2% for 2 minutes.

**Immediate checks:**

1. Are workers up? `docker compose ps`
2. Worker logs: `make logs` and filter for `"injected failure"` or exceptions.
3. Check if workers are quarantined (Grafana panel or `workers_quarantined` metric).

**Remediation:**

- Reduce `FAIL_RATE` to baseline and restart workers.
- Scale workers up: `make scale N=5`

## QueueBacklogHigh

**Meaning:** queue depth above threshold.

**Checks:**

- Are there enough workers? Scale up.
- Is job runtime increased? Check p95 runtime panel.

**Remediation:**

- `make scale N=5`
- Consider reducing job payload size (lower matmul dimension) for demo.

## WorkersQuarantined

**Meaning:** one or more workers quarantined.

**Checks:**

- Inspect worker logs for frequent failures.
- Verify `FAIL_RATE` is not set too high.

**Remediation:**

- Restart workers with baseline `FAIL_RATE`.


