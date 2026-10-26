import asyncio
import json
import time

import pytest

from ecpg.main import Agent
from ecpg.cloud_connector import CloudConnector
from ecpg.print_manager import FakeBackend, MAX_ATTEMPTS, PrintManager
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


@pytest.fixture
def intent_manager(manager, tmp_path):
    pm, _ = manager
    agent = Agent.__new__(Agent)
    agent.spool = pm.spool
    sent = []
    class Cloud:
        async def request_print_intent(self, job):
            return None
        async def send_job_status(self, job_id, status, error, **kwargs):
            sent.append((job_id, status, error, kwargs))
            return False  # Outbox bleibt bis zum ACK offen.
    agent.cloud = Cloud()
    pm.status_cb, pm.intent_cb = agent._report_job_status, agent._print_intent
    pm.enqueue({"job_id": "77", "printer_id": 1, "requires_intent": True,
                "dispatch_token": "claim", "generation": 2})
    pdf = tmp_path / "intent.pdf"
    pdf.write_bytes(b"%PDF-1.4 test")
    pm.spool.update_job("77", pdf_path=str(pdf), status="downloading")
    return pm, agent, sent


@pytest.mark.parametrize("failure", [None, TimeoutError, ConnectionError, RuntimeError, "no_cloud", "no_callback"])
async def test_missing_intent_stays_retryable_without_submission(intent_manager, failure):
    pm, agent, sent = intent_manager
    async def intent(job):
        if failure is not None:
            raise failure("SECRET_EXCEPTION_DATA")
        return None
    agent.cloud.request_print_intent = intent
    if failure == "no_cloud":
        agent.cloud = None
    elif failure == "no_callback":
        pm.intent_cb = None
    before = time.time()
    await pm.process_due()
    job = pm.spool.get_job("77")
    assert job["status"] == "pending" and job["attempts"] == 1
    assert job["submitted_at"] is None and job["cups_job"] is None
    assert before + 2 <= job["next_retry_at"] <= time.time() + 2
    assert "SECRET" not in job["error"]
    assert not pm.spool.due_jobs()
    await pm.process_due()
    assert not sent and not pm.backend.printed


async def test_missing_intent_exhausts_retries_and_replays_not_submitted(intent_manager):
    pm, agent, sent = intent_manager
    for attempt in range(MAX_ATTEMPTS):
        before = time.time()
        await pm.process_due()
        job = pm.spool.get_job("77")
        assert job["attempts"] == attempt + 1
        assert job["submitted_at"] is None and job["cups_job"] is None
        if attempt < MAX_ATTEMPTS - 1:
            delay = (2, 5, 10, 20)[attempt]
            assert job["status"] == "pending" and not sent
            assert before + delay <= job["next_retry_at"] <= time.time() + delay
            pm.spool.update_job("77", next_retry_at=0)
    assert job["status"] == "failed"
    assert len(sent) == 1 and sent[0][1] == "failed"
    assert sent[0][3] == {"not_submitted": True}
    pm.spool.close()
    pm.spool = Spool(str(pm.data_dir + "/spool.db"))
    agent.spool = pm.spool
    await agent._resend_unreported_statuses()
    assert sent == [sent[0], sent[0]]
    await pm.process_due()
    assert not pm.backend.printed


async def test_explicit_intent_denial_fails_immediately_without_submission(intent_manager):
    pm, agent, sent = intent_manager
    async def deny(job):
        return False
    agent.cloud.request_print_intent = deny
    await pm.process_due()
    job = pm.spool.get_job("77")
    assert job["status"] == "failed" and job["attempts"] == 1
    assert job["submitted_at"] is None and job["cups_job"] is None
    assert sent == [("77", "failed", "Cloud-Druckfreigabe verweigert; nicht gedruckt", {"not_submitted": True})]
    await pm.process_due()
    assert not pm.backend.printed


async def test_intent_granted_on_second_attempt_prints_once(intent_manager):
    pm, agent, sent = intent_manager
    await pm.process_due()
    assert not pm.backend.printed and not sent
    async def grant(job):
        return True
    agent.cloud.request_print_intent = grant
    pm.spool.update_job("77", next_retry_at=0)
    await pm.process_due()
    await pm.process_due()
    assert len(pm.backend.printed) == 1
    assert pm.spool.get_job("77")["status"] == "done"
    assert pm.spool.get_job("77")["submitted_at"] is not None
    assert [entry[1] for entry in sent] == ["printing", "done"]
    assert all(entry[3] == {"not_submitted": False} for entry in sent)


@pytest.mark.parametrize("reply", [True, False, None, "false"])
async def test_intent_rpc_distinguishes_grant_denial_and_missing_result(reply):
    connector = CloudConnector("test", {})
    async def send(message):
        await connector._on_message(json.dumps({"type": "intent_result", "rid": message["rid"],
                                                "payload": {"ok": reply}}))
        return True
    connector._send = send
    result = await connector.request_print_intent({"job_id": "77", "dispatch_token": "claim", "generation": 2})
    assert result is (reply if isinstance(reply, bool) else None)
    assert not connector._intent_waiters


@pytest.mark.parametrize("failure", ["timeout", "send_false", "exception"])
async def test_intent_rpc_transport_failure_is_not_a_denial(monkeypatch, failure):
    connector = CloudConnector("test", {})
    async def send(message):
        if failure == "exception":
            raise RuntimeError("send failed")
        return failure != "send_false"
    async def timeout(waiter, timeout):
        assert timeout == 10
        raise TimeoutError
    connector._send = send
    if failure == "timeout":
        monkeypatch.setattr(asyncio, "wait_for", timeout)
    assert await connector.request_print_intent({"job_id": "77", "dispatch_token": "claim", "generation": 2}) is None
    assert not connector._intent_waiters


@pytest.mark.parametrize("disconnect_error", [False, True])
async def test_disconnect_resolves_all_waiting_intents_immediately(monkeypatch, disconnect_error):
    connector = CloudConnector("test", {})
    disconnected, requested = asyncio.Event(), asyncio.Event()
    class Socket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        def __aiter__(self):
            return self
        async def __anext__(self):
            await disconnected.wait()
            if disconnect_error:
                raise ConnectionError("connection lost")
            raise StopAsyncIteration
        async def send(self, raw):
            if json.loads(raw)["type"] == "print_intent":
                requested.set()
    monkeypatch.setattr("ecpg.cloud_connector.websockets.connect", lambda *args, **kwargs: Socket())
    connection = asyncio.create_task(connector._connect_and_serve())
    await asyncio.sleep(0)
    tasks = [asyncio.create_task(connector.request_print_intent(
        {"job_id": str(job_id), "dispatch_token": "claim", "generation": 2})) for job_id in (77, 78)]
    await asyncio.wait_for(requested.wait(), timeout=1)
    assert len(connector._intent_waiters) == 2
    before = time.monotonic()
    disconnected.set()
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    assert results == [None, None] and time.monotonic() - before < 1
    if disconnect_error:
        with pytest.raises(ConnectionError):
            await connection
    else:
        await connection
    assert not connector._intent_waiters and connector._ws is None
    assert not connector.connected
