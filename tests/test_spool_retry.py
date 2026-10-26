"""Spool: Idempotenz, Ringpuffer-Begrenzung, Config-Cache, Retry-Backoff."""
import os
import tempfile

import pytest

from ecpg.main import Agent
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


def test_unreported_status_jobs_includes_terminal_jobs(spool):
    spool.add_job({"job_id": "terminal", "artifact_url": "u", "printer_id": 5})
    spool.update_job("terminal", status="done")

    assert [j["job_id"] for j in spool.unreported_status_jobs()] == ["terminal"]

    spool.mark_status_reported("terminal", "done")
    assert spool.unreported_status_jobs() == []


@pytest.mark.asyncio
async def test_failed_status_send_is_replayed_later(spool):
    class FakeCloudConnector:
        def __init__(self):
            self.connected = False
            self.sent = []

        async def send_job_status(self, job_id, status, error=None, **kwargs):
            if not self.connected:
                return False
            self.sent.append((job_id, status, error))
            return True

    agent = Agent.__new__(Agent)
    agent.spool = spool
    agent.cloud = FakeCloudConnector()
    spool.add_job({"job_id": "lost", "artifact_url": "u", "printer_id": 5})
    spool.update_job("lost", status="done")

    await agent._report_job_status("lost", "done", None)
    assert [j["job_id"] for j in spool.unreported_status_jobs()] == ["lost"]

    agent.cloud.connected = True
    await agent._resend_unreported_statuses()
    assert agent.cloud.sent == [("lost", "done", None)]
    assert spool.unreported_status_jobs() == []


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


def test_restart_during_submit_is_uncertain_and_cleanup_keeps_tombstone(tmp_path):
    path = str(tmp_path / "spool.db")
    s = Spool(path)
    s.add_job({"job_id": "once", "printer_id": 1, "artifact_url": "old"})
    assert s.begin_submit("once")
    s.close()
    s = Spool(path)
    assert s.get_job("once")["status"] == "uncertain"
    s.cleanup_done(older_than_s=-1)
    s.add_job({"job_id": "once", "printer_id": 1, "artifact_url": "new"})
    assert not s.begin_submit("once")
    assert s.get_job("once")["status"] == "uncertain"
    s.close()


def test_delayed_ack_does_not_ack_newer_status(spool):
    spool.add_job({"job_id": "ack", "printer_id": 1})
    spool.update_job("ack", status="done")
    spool.mark_status_reported("ack", "printing")
    assert len(spool.unreported_status_jobs()) == 1
    spool.mark_status_reported("ack", "done")
    assert not spool.unreported_status_jobs()


@pytest.mark.asyncio
async def test_unclear_cups_exception_never_reprints(spool, tmp_path):
    from ecpg.print_manager import FakeBackend, PrintManager
    class Ambiguous(FakeBackend):
        def print_file(self, *args):
            super().print_file(*args)
            raise OSError("result lost after acceptance")
    backend = Ambiguous()
    async def status(*args):
        pass
    mgr = PrintManager(spool, str(tmp_path), status, backend)
    mgr.sync_queues({"printers": [{"id": 1, "uri": "ipp://test"}]})
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")
    spool.add_job({"job_id": "unclear", "printer_id": 1})
    spool.update_job("unclear", pdf_path=str(pdf))
    await mgr.process_due()
    await mgr.process_due()
    mgr.enqueue({"job_id": "unclear", "printer_id": 1})
    await mgr.process_due()
    assert len(backend.printed) == 1
    assert spool.get_job("unclear")["status"] == "uncertain"


@pytest.mark.asyncio
async def test_concurrent_managers_submit_only_once(spool, tmp_path):
    import asyncio

    from ecpg.print_manager import FakeBackend, PrintManager
    backend = FakeBackend()
    async def status(*args):
        pass
    managers = [PrintManager(spool, str(tmp_path), status, backend) for _ in range(2)]
    for mgr in managers:
        mgr.sync_queues({"printers": [{"id": 1, "uri": "ipp://test"}]})
    pdf = tmp_path / "one.pdf"
    pdf.write_bytes(b"%PDF")
    spool.add_job({"job_id": "one", "printer_id": 1})
    spool.update_job("one", pdf_path=str(pdf))
    await asyncio.gather(*(mgr.process_due() for mgr in managers))
    assert len(backend.printed) == 1
    assert spool.get_job("one")["status"] == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("granted", [True, False])
async def test_new_cloud_intent_required_before_local_submit(spool, tmp_path, granted):
    from ecpg.print_manager import FakeBackend, PrintManager
    backend = FakeBackend()
    async def status(*args):
        pass
    async def intent(job):
        assert not backend.printed
        assert job["dispatch_token"] == "claim" and job["generation"] == 2
        return granted
    mgr = PrintManager(spool, str(tmp_path), status, backend, intent_cb=intent)
    mgr.sync_queues({"printers": [{"id": 1, "uri": "ipp://test"}]})
    mgr.enqueue({"job_id": "77", "printer_id": 1, "requires_intent": True, "dispatch_token": "claim", "generation": 2})
    pdf = tmp_path / "intent.pdf"
    pdf.write_bytes(b"%PDF")
    spool.update_job("77", pdf_path=str(pdf))
    await mgr.process_due()
    assert len(backend.printed) == int(granted)
    assert spool.get_job("77")["status"] == ("done" if granted else "failed")


def test_duplicate_cannot_remove_required_cloud_intent(spool):
    spool.add_job({"job_id": "fenced", "requires_intent": True, "dispatch_token": "token", "generation": 1})
    spool.add_job({"job_id": "fenced"})
    assert spool.get_job("fenced")["requires_intent"] == 1
    assert spool.get_job("fenced")["dispatch_token"] == "token"


@pytest.mark.asyncio
async def test_intent_rpc_correlates_reply_without_blocking_reader():
    import asyncio
    import json

    from ecpg.cloud_connector import CloudConnector
    connector = CloudConnector("test", {})
    sent = []
    async def send(message):
        sent.append(message)
        return True
    connector._send = send
    task = asyncio.create_task(connector.request_print_intent({"job_id": "77", "dispatch_token": "token", "generation": 2}))
    await asyncio.sleep(0)
    await connector._on_message(json.dumps({"type": "intent_result", "rid": "wrong", "payload": {"ok": True}}))
    assert not task.done()
    await connector._on_message(json.dumps({"type": "intent_result", "rid": sent[0]["rid"], "payload": {"ok": True}}))
    assert await task
    assert connector._intent_waiters == {}


@pytest.mark.asyncio
async def test_status_needs_cloud_ack_until_legacy_protocol_negotiated():
    import json

    from ecpg.cloud_connector import CloudConnector
    connector = CloudConnector("test", {})
    async def send(message):
        return True
    connector._send = send
    assert not await connector.send_job_status("1", "done")
    await connector._on_message(json.dumps({"type": "config_sync", "capabilities": ["print-status-ack-v1"], "payload": {}}))
    assert not await connector.send_job_status("1", "done")
    await connector._on_message(json.dumps({"type": "config_sync", "payload": {}}))
    assert await connector.send_job_status("1", "done")
