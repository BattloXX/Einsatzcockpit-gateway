"""Cloud-Verbindung: persistente WSS zu /ws/gateway.

- Auth: Bearer Device-Token
- Reconnect mit exponentiellem Backoff (1s → 60s), Heartbeat alle 30s
- Bei (Re-)Connect: hello → Cloud pusht config_sync
- Eingehende Nachrichten werden per id dedupliziert und an Handler delegiert
- Ausgehend: job_status, serial_status, printer_report, alarm_notice, heartbeat
"""
from __future__ import annotations

import asyncio
import json
import logging
import platform
from collections import deque
from typing import Awaitable, Callable

import websockets

from ecpg import __version__
from ecpg.settings import settings

logger = logging.getLogger("ecpg.cloud")

Handler = Callable[[dict], Awaitable[None]]


class CloudConnector:
    def __init__(self, device_token: str, handlers: dict[str, Handler]):
        self.device_token = device_token
        self.handlers = handlers  # type → async handler(payload)
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._seen_ids: deque[str] = deque(maxlen=512)
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    async def run(self) -> None:
        backoff = settings.reconnect_min_s
        while not self._stop.is_set():
            try:
                await self._connect_and_serve()
                backoff = settings.reconnect_min_s
            except Exception as exc:
                logger.warning("Cloud-Verbindung getrennt: %s", exc)
            self._connected.clear()
            if self._stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, settings.reconnect_max_s)

    async def stop(self) -> None:
        self._stop.set()
        if self._ws:
            await self._ws.close()

    async def _connect_and_serve(self) -> None:
        headers = {"Authorization": f"Bearer {self.device_token}"}
        async with websockets.connect(
            settings.ws_url, additional_headers=headers,
            ping_interval=None, max_size=8 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            self._connected.set()
            logger.info("Mit Cloud verbunden: %s", settings.ws_url)
            await self._send({"type": "hello", "payload": {
                "version": __version__,
                "host_info": {"platform": platform.platform(), "python": platform.python_version()},
            }})
            hb = asyncio.create_task(self._heartbeat_loop())
            try:
                async for raw in ws:
                    await self._on_message(raw)
            finally:
                hb.cancel()
                self._ws = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(settings.heartbeat_s)
            try:
                await self._send({"type": "heartbeat"})
            except Exception:
                return

    async def _on_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        mtype = msg.get("type")
        mid = msg.get("id")

        if mtype == "pong":
            return
        if mtype == "ping":
            await self._send({"type": "pong"})
            return

        # Dedup at-least-once (nur Nachrichten mit id)
        if mid is not None:
            if mid in self._seen_ids:
                await self._send({"type": "ack", "id": mid})
                return
            self._seen_ids.append(mid)

        handler = self.handlers.get(mtype)
        if handler:
            try:
                await handler(msg.get("payload") or msg)
            except Exception as exc:
                logger.exception("Handler %s fehlgeschlagen: %s", mtype, exc)
        else:
            logger.debug("Unbehandelter Nachrichtentyp: %s", mtype)

        if mid is not None:
            await self._send({"type": "ack", "id": mid})

    async def _send(self, obj: dict) -> None:
        if self._ws is None:
            return
        await self._ws.send(json.dumps(obj, ensure_ascii=False))

    # ── Ausgehende Meldungen ─────────────────────────────────────────────────
    async def send_job_status(self, job_id, status: str, error: str | None = None) -> None:
        await self._send({"type": "job_status", "payload": {
            "job_id": job_id, "status": status, "error": error,
        }})

    async def send_serial_status(self, connected: bool) -> None:
        await self._send({"type": "serial_status", "payload": {"connected": connected}})

    async def send_printer_report(self, printers: list[dict]) -> None:
        await self._send({"type": "printer_report", "payload": {"printers": printers}})

    async def send_printer_status(self, statuses: list[dict]) -> None:
        """Erreichbarkeits-/Statusmeldung je Drucker: [{printer_id, status:{reachable,...}}]."""
        await self._send({"type": "printer_status", "payload": {"printers": statuses}})

    async def send_passthrough_status(self, *, enabled: bool, listening: bool, clients: int) -> None:
        await self._send({"type": "passthrough_status", "payload": {
            "enabled": enabled, "listening": listening, "clients": clients,
        }})

    async def send_alarm_notice(self, raw_hash: str) -> None:
        await self._send({"type": "alarm_notice", "payload": {"raw_hash": raw_hash}})

    async def send_log(self, level: str, message: str) -> None:
        await self._send({"type": "log_event", "payload": {"level": level, "message": message}})
