"""Spool: Idempotenz, Ringpuffer-Begrenzung, Config-Cache, Retry-Backoff."""
import os
import tempfile

import pytest

from ecpg.spool import RING_MAX, Spool


@pytest.fixture
def spool():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    s = Spool(path)
    yield s
    s.close()
    os.unlink(path)


def test_add_job_idempotent(spool):
    spool.add_job({"job_id": "1", "artifact_url": "u", "printer_id": 5})
    spool.add_job({"job_id": "1", "artifact_url": "u", "printer_id": 5})
    assert len(spool.recent_jobs()) == 1


def test_update_job_fields(spool):
    spool.add_job({"job_id": "2", "artifact_url": "u", "printer_id": 5})
    spool.update_job("2", status="done", attempts=3)
    j = spool.get_job("2")
    assert j["status"] == "done" and j["attempts"] == 3


def test_due_jobs_respects_next_retry(spool):
    import time
    spool.add_job({"job_id": "3", "artifact_url": "u", "printer_id": 5})
    spool.update_job("3", status="pending", next_retry_at=time.time() + 999)
    assert all(j["job_id"] != "3" for j in spool.due_jobs())
    spool.update_job("3", next_retry_at=time.time() - 1)
    assert any(j["job_id"] == "3" for j in spool.due_jobs())


def test_config_cache_roundtrip(spool):
    cfg = {"printers": [{"id": 1, "uri": "ipp://x/ipp/print", "name": "P"}], "wut": {"host": "h"}}
    spool.save_config(cfg)
    assert spool.load_config() == cfg


def test_ring_buffer_limited(spool):
    for i in range(RING_MAX + 20):
        spool.add_raw_alarm(f"a{i}".encode(), "cp850", f"h{i}")
    # Es dürfen nie mehr als RING_MAX gespeichert sein
    assert len(spool.recent_alarms(limit=RING_MAX + 50)) <= RING_MAX


def test_pending_and_forwarded(spool):
    aid = spool.add_raw_alarm(b"alarm", "cp850", "hash1")
    assert any(a["id"] == aid for a in spool.pending_alarms())
    spool.mark_alarm_forwarded(aid)
    assert all(a["id"] != aid for a in spool.pending_alarms())
