"""Druck-Manager: CUPS-Queues aus der Cloud-Config synchronisieren, Druckjobs
laden (signierte PDF-URL) → spoolen → an CUPS übergeben → Status pollen/melden.

Retry mit Backoff (Default 5 Versuche / ~10 min). CUPS-Zugriff über eine
austauschbare Backend-Klasse (pycups im Container, Fake in Tests/CI).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Awaitable, Callable

import httpx

logger = logging.getLogger("ecpg.print")

MAX_ATTEMPTS = 5
BACKOFF_BASE_S = 20  # 20, 40, 80, 160, ...


class CupsBackend:
    """Wrapper um pycups. Wird lazy importiert, damit Tests ohne CUPS laufen."""

    def __init__(self) -> None:
        import cups  # type: ignore  # noqa: PLC0415
        self._cups = cups
        self._conn = cups.Connection()

    def ensure_queue(self, name: str, uri: str) -> None:
        printers = self._conn.getPrinters()
        if name not in printers or printers[name].get("device-uri") != uri:
            self._conn.addPrinter(name, device=uri, ppdname="everywhere")
            self._conn.enablePrinter(name)
            self._conn.acceptJobs(name)

    def remove_queue(self, name: str) -> None:
        try:
            self._conn.deletePrinter(name)
        except Exception:  # pragma: no cover
            pass

    def print_file(self, queue: str, path: str, title: str, options: dict) -> int:
        cups_opts = {}
        copies = int((options or {}).get("copies", 1) or 1)
        cups_opts["copies"] = str(max(1, copies))
        duplex = (options or {}).get("duplex", "off")
        if duplex in ("long-edge", "long"):
            cups_opts["sides"] = "two-sided-long-edge"
        elif duplex in ("short-edge", "short"):
            cups_opts["sides"] = "two-sided-short-edge"
        else:
            cups_opts["sides"] = "one-sided"
        # Papiergroesse (nur wenn explizit gewaehlt; Standard = Druckervorgabe/A4).
        media = (options or {}).get("media")
        if media in ("A3", "A4"):
            cups_opts["media"] = media
        return self._conn.printFile(queue, path, title, cups_opts)

    def job_state(self, job_id: int) -> str:
        """Gibt 'printing' / 'done' / 'failed' zurück."""
        attrs = self._conn.getJobAttributes(job_id)
        state = attrs.get("job-state")
        # IPP job-state: 3=pending,4=held,5=processing,6=stopped,7=canceled,8=aborted,9=completed
        if state == 9:
            return "done"
        if state in (7, 8):
            return "failed"
        return "printing"


class FakeBackend:
    """Backend ohne echten Druck (CI/Dev). „Druckt" sofort erfolgreich."""

    def __init__(self) -> None:
        self.queues: dict[str, str] = {}
        self.printed: list[tuple[str, str]] = []
        self._job = 0

    def ensure_queue(self, name: str, uri: str) -> None:
        self.queues[name] = uri

    def remove_queue(self, name: str) -> None:
        self.queues.pop(name, None)

    def print_file(self, queue: str, path: str, title: str, options: dict) -> int:
        self._job += 1
        self.printed.append((queue, path))
        return self._job

    def job_state(self, job_id: int) -> str:
        return "done"


def make_backend() -> CupsBackend | FakeBackend:
    try:
        return CupsBackend()
    except Exception as exc:  # pragma: no cover - abhängig von CUPS
        logger.warning("CUPS nicht verfügbar (%s) – Fake-Backend aktiv", exc)
        return FakeBackend()


def _queue_name(printer_id: int) -> str:
    return f"ecpg_{printer_id}"


class PrintManager:
    def __init__(self, spool, data_dir: str, status_cb: Callable[[str, str, str | None], Awaitable[None]],
                 backend=None):
        self.spool = spool
        self.data_dir = data_dir
        self.status_cb = status_cb  # async (job_id, status, error)
        self.backend = backend or make_backend()
        self._printers: dict[int, dict] = {}  # printer_id → {name,uri,queue}
        os.makedirs(os.path.join(data_dir, "spool"), exist_ok=True)

    # ── Config / Queues ──────────────────────────────────────────────────────
    def sync_queues(self, config: dict) -> None:
        printers = {p["id"]: p for p in (config.get("printers") or [])}
        # Neue/aktualisierte Queues
        for pid, p in printers.items():
            queue = _queue_name(pid)
            try:
                self.backend.ensure_queue(queue, p["uri"])
            except Exception as exc:  # pragma: no cover
                logger.warning("Queue %s (%s) konnte nicht eingerichtet werden: %s", queue, p["uri"], exc)
            self._printers[pid] = {"name": p.get("name"), "uri": p["uri"], "queue": queue}
        # Entfernte Queues abbauen
        for pid in list(self._printers):
            if pid not in printers:
                self.backend.remove_queue(_queue_name(pid))
                self._printers.pop(pid, None)

    def queue_for(self, printer_id: int | None) -> str | None:
        info = self._printers.get(printer_id) if printer_id is not None else None
        return info["queue"] if info else None

    # ── Jobs ─────────────────────────────────────────────────────────────────
    def enqueue(self, payload: dict) -> None:
        self.spool.add_job({
            "job_id": payload.get("job_id"),
            "printer_id": payload.get("printer_id"),
            "printer_uri": None,
            "document_type": payload.get("document_type"),
            "artifact_url": payload.get("artifact_url"),
            "options": payload.get("options") or {},
        })

    async def process_due(self) -> None:
        for job in self.spool.due_jobs():
            await self._process_one(job)

    async def _process_one(self, job: dict) -> None:
        job_id = job["job_id"]
        queue = self.queue_for(job.get("printer_id"))
        if queue is None:
            await self._fail_or_retry(job, "Drucker nicht (mehr) konfiguriert")
            return

        # PDF laden (falls noch nicht gespoolt)
        pdf_path = job.get("pdf_path")
        if not pdf_path or not os.path.exists(pdf_path):
            try:
                pdf_path = await self._download(job)
            except Exception as exc:
                await self._fail_or_retry(job, f"Download fehlgeschlagen: {exc}")
                return
            self.spool.update_job(job_id, pdf_path=pdf_path, status="downloading")

        # An CUPS übergeben
        try:
            options = _json(job.get("options_json"))
            cups_job = await asyncio.to_thread(
                self.backend.print_file, queue, pdf_path, f"ECPG {job_id}", options
            )
        except Exception as exc:
            await self._fail_or_retry(job, f"Druckübergabe fehlgeschlagen: {exc}")
            return

        self.spool.update_job(job_id, status="printing")
        await self.status_cb(job_id, "printing", None)

        # Status pollen (kurz; CUPS verarbeitet asynchron)
        state = await self._poll_state(cups_job)
        if state == "done":
            self.spool.update_job(job_id, status="done", error=None)
            await self.status_cb(job_id, "done", None)
        elif state == "failed":
            await self._fail_or_retry(job, "CUPS meldet Abbruch")
        # 'printing' → bleibt, nächster Durchlauf pollt erneut

    async def _download(self, job: dict) -> str:
        url = job["artifact_url"]
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
        path = os.path.join(self.data_dir, "spool", f"{job['job_id']}.pdf")
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    async def _poll_state(self, cups_job: int, tries: int = 3) -> str:
        for _ in range(tries):
            try:
                state = await asyncio.to_thread(self.backend.job_state, cups_job)
            except Exception:  # pragma: no cover
                return "printing"
            if state in ("done", "failed"):
                return state
            await asyncio.sleep(1.0)
        return "printing"

    async def _fail_or_retry(self, job: dict, error: str) -> None:
        job_id = job["job_id"]
        attempts = (job.get("attempts") or 0) + 1
        if attempts >= MAX_ATTEMPTS:
            self.spool.update_job(job_id, status="failed", attempts=attempts, error=error)
            await self.status_cb(job_id, "failed", error)
            logger.warning("Job %s endgültig fehlgeschlagen: %s", job_id, error)
        else:
            delay = BACKOFF_BASE_S * (2 ** (attempts - 1))
            self.spool.update_job(job_id, status="pending", attempts=attempts,
                                  next_retry_at=time.time() + delay, error=error)
            logger.info("Job %s Fehler (%s), Retry #%d in %ds", job_id, error, attempts, delay)

    # ── Offline-Notdruck ─────────────────────────────────────────────────────
    def notfall_print(self, pdf_bytes: bytes, printer_id: int) -> bool:
        queue = self.queue_for(printer_id)
        if queue is None:
            logger.warning("Notfalldruck: kein gültiger Drucker (id=%s)", printer_id)
            return False
        path = os.path.join(self.data_dir, "spool", f"notfall_{int(time.time())}.pdf")
        with open(path, "wb") as fh:
            fh.write(pdf_bytes)
        try:
            self.backend.print_file(queue, path, "ALARM Notdruck", {"copies": 1})
            return True
        except Exception as exc:  # pragma: no cover
            logger.error("Notfalldruck fehlgeschlagen: %s", exc)
            return False


def _json(raw: str | None) -> dict:
    import json
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {}
