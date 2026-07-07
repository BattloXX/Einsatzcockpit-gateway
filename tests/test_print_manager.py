"""Print-Manager mit Fake-CUPS-Backend + Mock-PDF-Download: Queue-Sync, Job-Flow,
Retry bei fehlendem Drucker."""
import os
import tempfile

import pytest

from ecpg.print_manager import FakeBackend, PrintManager
from ecpg.spool import Spool


@pytest.fixture
def env():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    data_dir = tempfile.mkdtemp()
    spool = Spool(path)
    statuses: list[tuple] = []

    async def cb(job_id, status, error):
        statuses.append((job_id, status, error))

    backend = FakeBackend()
    pm = PrintManager(spool, data_dir, cb, backend=backend)
    yield pm, spool, backend, statuses
    spool.close()
    os.unlink(path)


def test_sync_queues_creates_and_removes(env):
    pm, spool, backend, statuses = env
    pm.sync_queues({"printers": [{"id": 1, "uri": "ipp://a/ipp/print", "name": "A"}]})
    assert "ecpg_1" in backend.queues
    pm.sync_queues({"printers": []})
    assert "ecpg_1" not in backend.queues


async def test_job_flow_prints(monkeypatch, env):
    pm, spool, backend, statuses = env
    pm.sync_queues({"printers": [{"id": 1, "uri": "ipp://a/ipp/print", "name": "A"}]})

    async def fake_download(job):
        path = os.path.join(pm.data_dir, "spool", f"{job['job_id']}.pdf")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(b"%PDF-1.4 test")
        return path

    monkeypatch.setattr(pm, "_download", fake_download)
    pm.enqueue({"job_id": "10", "printer_id": 1, "artifact_url": "http://x/pdf", "document_type": "einsatzinfo"})
    await pm.process_due()

    assert backend.printed, "Fake-Backend hätte drucken müssen"
    assert spool.get_job("10")["status"] == "done"
    assert ("10", "done", None) in statuses


async def test_missing_printer_retries_then_fails(env):
    pm, spool, backend, statuses = env
    # Kein sync_queues → Drucker unbekannt
    pm.enqueue({"job_id": "20", "printer_id": 99, "artifact_url": "http://x/pdf"})
    for _ in range(6):
        await pm.process_due()
        # next_retry_at zurücksetzen, damit der Job erneut fällig wird
        spool.update_job("20", next_retry_at=0)
    assert spool.get_job("20")["status"] == "failed"
    assert any(s[1] == "failed" for s in statuses)


def test_notfall_print(env):
    pm, spool, backend, statuses = env
    pm.sync_queues({"printers": [{"id": 2, "uri": "ipp://b/ipp/print", "name": "B"}]})
    ok = pm.notfall_print(b"%PDF-1.4 notfall", 2)
    assert ok is True
    assert backend.printed
