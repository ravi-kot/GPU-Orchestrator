from __future__ import annotations

import csv
import json
import os
import threading
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional


class EventLogger:
    """
    Writes job/worker events to an append-only file.

    Supported formats:
    - EVENT_LOG_FORMAT=csv  -> CSV with header
    - EVENT_LOG_FORMAT=json -> JSON Lines (one JSON object per line)
    """

    def __init__(self) -> None:
        self.enabled = os.environ.get("EVENT_LOG_ENABLED", "1") not in ("0", "false", "False")
        self.format = os.environ.get("EVENT_LOG_FORMAT", "json").lower()  # json | csv
        self.path = os.environ.get("EVENT_LOG_PATH", "/logs/job_events.jsonl")
        self._lock = threading.Lock()
        self._csv_header_written = False

    def emit(self, event: str, payload: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        rec: Dict[str, Any] = {"ts": time.time(), "event": event, **payload}

        # Normalize dataclasses
        for k, v in list(rec.items()):
            if is_dataclass(v):
                rec[k] = asdict(v)

        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with self._lock:
                if self.format == "csv":
                    self._emit_csv(rec)
                else:
                    self._emit_jsonl(rec)
        except Exception:
            # Never crash the service because of logging
            return

    def _emit_jsonl(self, rec: Dict[str, Any]) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _emit_csv(self, rec: Dict[str, Any]) -> None:
        # Keep a stable, simple schema
        fields = [
            "ts",
            "event",
            "job_id",
            "worker_id",
            "status",
            "runtime_s",
            "attempts",
            "state",
            "error",
        ]

        row = {k: rec.get(k, "") for k in fields}
        # best-effort coercions
        if row["runtime_s"] != "" and row["runtime_s"] is not None:
            try:
                row["runtime_s"] = float(row["runtime_s"])
            except Exception:
                pass

        file_exists = os.path.exists(self.path)
        with open(self.path, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            if (not file_exists) or (not self._csv_header_written and os.path.getsize(self.path) == 0):
                w.writeheader()
                self._csv_header_written = True
            w.writerow(row)


_EVENT_LOGGER: Optional[EventLogger] = None


def get_event_logger() -> EventLogger:
    global _EVENT_LOGGER
    if _EVENT_LOGGER is None:
        _EVENT_LOGGER = EventLogger()
    return _EVENT_LOGGER


