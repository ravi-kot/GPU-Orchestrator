import logging
import json
import os
import sys
import time
from typing import Any, Dict

LOG_FORMAT = "%(asctime)s %(levelname)s [worker] %(message)s"

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": time.time(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in ("event", "job_id", "worker_id", "status", "runtime_s", "job_type", "error"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)
        return json.dumps(payload, ensure_ascii=False)

def configure_logging():
    handler = logging.StreamHandler(sys.stdout)
    fmt = os.environ.get("LOG_FORMAT", "plain").lower()  # plain | json
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(handler)
