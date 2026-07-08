"""ECPG Agent – asyncio-Supervisor, der alle Bausteine orchestriert.

Ablauf:
1. Spool öffnen, Pairing sicherstellen (Token > ENV-Code > Statusseiten-Eingabe).
2. Cloud-Verbindung (WSS) aufbauen; bei Connect kommt config_sync.
3. Print-Manager (CUPS), Serial-Ingest (W&T), Discovery je nach Config.
4. Hintergrund-Loops: Spool abarbeiten, Alarme nachmelden, Cleanup.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import signal
from datetime import datetime

import httpx

from ecpg import pairing
from ecpg.alarm_parser import get_parser
from ecpg.cloud_connector import CloudConnector
from ecpg.offline_print import render_alarm_pdf
from ecpg.print_manager import PrintManager
from ecpg.serial_ingest import SerialIngest
from ecpg.serial_passthrough import SerialPassthrough
from ecpg.settings import settings
from ecpg.spool import Spool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("ecpg")


class Agent:
    def __init__(self) -> None:
        self.spool = Spool(settings.db_path)
        self.config: dict = self.spool.load_config()
        self.cloud: CloudConnector | None = None
        self.print_mgr: PrintManager | None = None
        # Serial-Fan-Out: roher W&T-Strom wird 1:1 an verbundene Clients verteilt.
        self.passthrough = SerialPassthrough()
        self.serial = SerialIngest(
            self._on_datagram, self._on_serial_status, on_raw=self.passthrough.broadcast,
        )
        self._stop = asyncio.Event()

    # ── Lifecycle ────────────────────────────────────────────────────────────
    async def run(self) -> None:
        token = await self._await_token()
        if token is None:
            logger.error("Kein Device-Token – Agent kann nicht starten")
            return

        self.print_mgr = PrintManager(self.spool, settings.data_dir, self._report_job_status)
        if self.config:
            self._apply_config(self.config)

        self.cloud = CloudConnector(token, {
            "config_sync": self._on_config_sync,
            "print_job": self._on_print_job,
            "cancel_job": self._on_cancel_job,
            "discover_printers": self._on_discover,
            "probe_printer": self._on_probe,
            "test_page": self._on_test_page,
            "update_available": self._on_update_available,
        })

        tasks = [
            asyncio.create_task(self.cloud.run()),
            asyncio.create_task(self._spool_loop()),
            asyncio.create_task(self._alarm_forward_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._printer_health_loop()),
        ]
        self.serial.start()
        await self.passthrough.apply()
        await self._stop.wait()
        await self.serial.stop()
        await self.passthrough.stop()
        if self.cloud:
            await self.cloud.stop()
        for t in tasks:
            t.cancel()

    async def _await_token(self) -> str | None:
        for _ in range(60):  # bis zu 5 min auf Pairing warten
            token = await pairing.ensure_paired(self.spool)
            if token:
                return token
            if self._stop.is_set():
                return None
            await asyncio.sleep(5)
        return None

    def stop(self) -> None:
        self._stop.set()

    # ── Config ───────────────────────────────────────────────────────────────
    async def _on_config_sync(self, payload: dict) -> None:
        logger.info("config_sync empfangen (%d Drucker)", len(payload.get("printers") or []))
        self.config = payload
        self.spool.save_config(payload)
        self._apply_config(payload)
        await self.passthrough.apply()
        # Frischer Health-Check direkt nach neuer Config (z. B. Drucker aktiviert).
        await self._report_printer_health()

    def _apply_config(self, config: dict) -> None:
        wut = config.get("wut") or {}
        if self.print_mgr:
            self.print_mgr.sync_queues(config)
        self.serial.update_config(wut)
        self.passthrough.update_config(wut)

    # ── Druck ────────────────────────────────────────────────────────────────
    async def _on_print_job(self, payload: dict) -> None:
        if not self.print_mgr:
            return
        self.print_mgr.enqueue(payload)
        await self.print_mgr.process_due()

    async def _on_cancel_job(self, payload: dict) -> None:
        job_id = payload.get("job_id")
        if job_id is not None:
            self.spool.update_job(str(job_id), status="canceled")
            await self._report_job_status(str(job_id), "canceled", None)

    async def _report_job_status(self, job_id, status: str, error: str | None) -> None:
        if self.cloud:
            await self.cloud.send_job_status(job_id, status, error)

    async def _spool_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self.print_mgr:
                    await self.print_mgr.process_due()
            except Exception as exc:
                logger.exception("Spool-Loop-Fehler: %s", exc)
            await asyncio.sleep(10)

    async def _cleanup_loop(self) -> None:
        while not self._stop.is_set():
            self.spool.cleanup_done()
            await asyncio.sleep(3600)

    # ── Drucker-Verfügbarkeit ────────────────────────────────────────────────
    async def _printer_health_loop(self) -> None:
        """Prueft regelmaessig die Erreichbarkeit der aktiven Drucker und meldet sie
        (+ Fan-Out-Client-Zahl) an die Cloud, damit Online/Offline korrekt angezeigt wird."""
        while not self._stop.is_set():
            interval = int((self.config.get("wut") or {}).get("health_interval_s") or settings.printer_health_interval_s)
            await asyncio.sleep(max(15, interval))
            await self._report_printer_health()

    async def _report_printer_health(self) -> None:
        if not self.cloud:
            return
        from ecpg import printer_health
        printers = self.config.get("printers") or []
        try:
            statuses = await printer_health.probe_all(printers, timeout=settings.printer_health_timeout_s)
            if statuses:
                await self.cloud.send_printer_status(statuses)
        except Exception as exc:
            logger.warning("Drucker-Health-Check fehlgeschlagen: %s", exc)
        # Fan-Out-Telemetrie (best effort)
        try:
            await self.cloud.send_passthrough_status(
                enabled=self.passthrough._enabled,
                listening=self.passthrough._server is not None,
                clients=self.passthrough.client_count(),
            )
        except Exception:
            pass

    # ── Discovery ────────────────────────────────────────────────────────────
    async def _on_discover(self, payload: dict) -> None:
        from ecpg.printer_discovery import discover
        printers = await discover()
        if self.cloud and printers:
            await self.cloud.send_printer_report(printers)

    async def _on_probe(self, payload: dict) -> None:
        from ecpg.printer_discovery import probe_ip
        result = await probe_ip(payload.get("ip", ""))
        if self.cloud and result:
            await self.cloud.send_printer_report([result])

    async def _on_test_page(self, payload: dict) -> None:
        if not self.print_mgr:
            return
        pdf = render_alarm_pdf("=== ECPG TESTSEITE ===\nDruck erfolgreich.", datetime.utcnow())
        self.print_mgr.notfall_print(pdf, payload.get("printer_id"))

    async def _on_update_available(self, payload: dict) -> None:
        logger.info("Update verfügbar: %s", payload.get("version"))

    # ── Serieller Alarm ──────────────────────────────────────────────────────
    async def _on_serial_status(self, connected: bool) -> None:
        if self.cloud:
            await self.cloud.send_serial_status(connected)

    async def _on_datagram(self, raw: bytes, charset: str) -> None:
        text = raw.decode(charset, errors="replace")
        raw_hash = hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()
        alarm_id = self.spool.add_raw_alarm(raw, charset, raw_hash)

        parser = get_parser(self.config.get("parser"))
        parsed = None
        try:
            parsed = parser.parse(text)
        except Exception as exc:
            logger.warning("Parser-Fehler: %s", exc)
        parse_status = "parsed" if parsed else "parse_failed"

        forwarded = await self._forward_alarm(text, charset, parsed, parse_status)
        if forwarded:
            self.spool.mark_alarm_forwarded(alarm_id)
            if self.cloud:
                await self.cloud.send_alarm_notice(raw_hash)
        else:
            # Cloud nicht erreichbar → Offline-Notdruck
            self._offline_notdruck(text)

    async def _forward_alarm(self, text: str, charset: str, parsed, parse_status: str) -> bool:
        token = pairing.get_device_token(self.spool)
        if not token:
            return False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    settings.alarms_url,
                    headers={"Authorization": f"Bearer {token}"},
                    json={"raw_text": text, "charset": charset,
                          "parsed": parsed, "parse_status": parse_status},
                )
            return resp.status_code == 200
        except httpx.HTTPError as exc:
            logger.warning("Alarm-Weiterleitung fehlgeschlagen: %s", exc)
            return False

    def _offline_notdruck(self, text: str) -> None:
        printer_id = (self.config.get("wut") or {}).get("notfalldruck_printer_id")
        if not printer_id or not self.print_mgr:
            logger.warning("Offline-Alarm ohne konfigurierten Notfalldrucker")
            return
        pdf = render_alarm_pdf(text, datetime.utcnow())
        self.print_mgr.notfall_print(pdf, printer_id)

    async def _alarm_forward_loop(self) -> None:
        """Puffernde Alarme bei Reconnect nachmelden."""
        while not self._stop.is_set():
            await asyncio.sleep(30)
            for row in self.spool.pending_alarms():
                text = bytes(row["raw_bytes"]).decode(row["charset"] or "cp850", errors="replace")
                parser = get_parser(self.config.get("parser"))
                parsed = None
                try:
                    parsed = parser.parse(text)
                except Exception:
                    pass
                ok = await self._forward_alarm(text, row["charset"], parsed,
                                               "parsed" if parsed else "parse_failed")
                if ok:
                    self.spool.mark_alarm_forwarded(row["id"])


async def _amain() -> None:
    agent = Agent()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, agent.stop)
    except NotImplementedError:  # Windows
        pass

    from ecpg.status_server import start_status_server
    status = await start_status_server(agent)
    try:
        await agent.run()
    finally:
        await status.cleanup()
        agent.spool.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
