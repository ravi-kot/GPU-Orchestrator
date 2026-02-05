import logging
import json
import os
import sys
import time
from typing import Any, Dict

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
LOG_LEVEL = logging.INFO


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": time.time(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Common extras (if provided via logger.*(..., extra={...}))
        for k in ("event", "job_id", "worker_id", "status", "runtime_s", "attempts", "state", "error"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging():
    fmt = os.environ.get("LOG_FORMAT", "plain").lower()  # plain | json
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    root.handlers.clear()
    root.addHandler(handler)