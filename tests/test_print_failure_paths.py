import asyncio

import pytest

from ecpg.main import Agent
from ecpg.print_manager import FakeBackend, PrintManager
from ecpg.spool import Spool


@pytest.fixture
def manager(tmp_path):
    updates = []
    async def status(job_id, state, error):
        updates.append((job_id, state, error))
    pm = PrintManager(Spool(str(tmp_path / "spool.db")), str(tmp_path), status, backend=FakeBackend())
    pm.sync_queues({"printers": [{"id": 1, "uri": "ipp://audit/ipp/print"}]})
    yield pm, updates
    pm.spool.close()


@pytest.mark.parametrize("intent", [False, True])
@pytest.mark.parametrize("download_fails", [False, True])
async def test_cancel_during_download_cannot_revive_job(manager, tmp_path, intent, download_fails):
    pm, updates = manager
    downloading, resume = asyncio.Event(), asyncio.Event()
    intent_calls = []
    async def download(job):
        downloading.set()
        await resume.wait()
        if download_fails:
            raise RuntimeError("SECRET_ARTIFACT_URL")
        path = tmp_path / "audit.pdf"
        path.write_bytes(b"%PDF-1.4 audit")
        return str(path)
    async def grant(job):
        intent_calls.append(1)
        return True
    pm._download, pm.intent_cb = download, grant
    pm.enqueue({"job_id": "cancel", "printer_id": 1, "artifact_url": "http://audit/pdf", "requires_intent": intent})
    process = asyncio.create_task(pm.process_due())
    await downloading.wait()
    await pm.cancel("cancel")
    resume.set()
    await process
    assert pm.spool.get_job("cancel")["status"] == "canceled"
    assert not pm.backend.printed and not intent_calls
    assert [state for _, state, _ in updates] == ["canceled"]


async def test_unknown_cups_status_survives_restart_and_stops_false_progress(manager, tmp_path):
    pm, updates = manager
    pm.enqueue({"job_id": "lost", "printer_id": 1, "artifact_url": "http://audit/pdf"})
    pm.spool.update_job("lost", status="printing", cups_job=42, submitted_at=1)
    def missing(job_id):
        raise RuntimeError("CUPS job is gone")
    pm.backend.job_state = missing
    await pm.process_due()
    assert not updates
    assert pm.spool.get_job("lost")["status_failures"] == 1
    pm.spool.close()
    pm.spool = Spool(str(tmp_path / "spool.db"))
    agent = Agent.__new__(Agent)
    agent.spool = pm.spool
    class Cloud:
        def __init__(self):
            self.sent = []
        async def send_job_status(self, job_id, status, error, **kwargs):
            self.sent.append(status)
            return True
    agent.cloud = Cloud()
    await agent._resend_unreported_statuses()
    assert not agent.cloud.sent, "Outbox darf kein unbestaetigtes printing wiederholen"
    await pm.process_due()
    await pm.process_due()
    assert pm.spool.get_job("lost")["status"] == "uncertain"
    assert [state for _, state, _ in updates] == ["uncertain"]
    await agent._resend_unreported_statuses()
    assert agent.cloud.sent == ["uncertain"]
    assert not pm.backend.printed
    assert not pm.spool.update_job("lost", status="done")
    assert not pm.spool.update_job("lost", status="downloading")


async def test_transient_status_failure_recovers_without_second_submission(manager):
    pm, updates = manager
    pm.enqueue({"job_id": "recover", "printer_id": 1, "artifact_url": "http://audit/pdf"})
    pm.spool.update_job("recover", status="printing", cups_job=42, submitted_at=1)
    def temporary(job_id):
        raise RuntimeError("temporary CUPS failure")
    pm.backend.job_state = temporary
    await pm.process_due()
    pm.backend.job_state = lambda _: "done"
    await pm.process_due()
    assert pm.spool.get_job("recover")["status"] == "done"
    assert pm.spool.get_job("recover")["status_failures"] == 0
    assert [state for _, state, _ in updates] == ["done"]
    assert not pm.backend.printed
