from pathlib import Path
import sys

# Add services/job_manager to PYTHONPATH so tests can import app/*
ROOT = Path(__file__).resolve().parents[1]  # .../services/job_manager
sys.path.insert(0, str(ROOT))

from app.scheduler import backoff_s


def test_backoff_is_exponential():
    assert backoff_s(1) == 1.0
    assert backoff_s(2) == 2.0
    assert backoff_s(3) == 4.0


