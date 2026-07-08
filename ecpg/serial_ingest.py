"""Serieller Alarmempfang über den W&T Com-Server (TCP-Socket-Server).

Das Gateway verbindet sich als TCP-Client, puffert den Zeichenstrom und schließt
ein Datagramm über eine konfigurierbare Strategie ab (Idle-Timeout – Default –
oder Form Feed 0x0C). Jedes Datagramm wird roh in den Ringpuffer gelegt und über
einen Callback verarbeitet. Verbindungsabriss → Reconnect + Statusmeldung.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

logger = logging.getLogger("ecpg.serial")

FORM_FEED = 0x0C


class SerialIngest:
    def __init__(
        self,
        on_datagram: Callable[[bytes, str], Awaitable[None]],
        on_status: Callable[[bool], Awaitable[None]],
        on_raw: Callable[[bytes], None] | None = None,
    ):
        self.on_datagram = on_datagram   # async (raw_bytes, charset)
        self.on_status = on_status       # async (connected)
        # Optionaler Roh-Abgriff (Serial-Fan-Out): jeder vom W&T gelesene Chunk wird
        # VOR der Datagramm-Zerlegung 1:1 weitergereicht. Synchron + fehlertolerant,
        # damit der Alarm-Empfang nie durch einen Downstream-Fehler blockiert wird.
        self.on_raw = on_raw
        self._task: asyncio.Task | None = None
        self._config: dict = {}
        self._connected = False
        self._stop = asyncio.Event()

    def update_config(self, wut_config: dict) -> None:
        self._config = wut_config or {}

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def connected(self) -> bool:
        return self._connected

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            host = self._config.get("host")
            port = int(self._config.get("port", 8000))
            if not host:
                await asyncio.sleep(5)
                continue
            try:
                reader, writer = await asyncio.open_connection(host, port)
            except OSError as exc:
                await self._set_connected(False)
                logger.warning("W&T-Verbindung %s:%s fehlgeschlagen: %s", host, port, exc)
                await asyncio.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 30)
                continue

            backoff = 1.0
            await self._set_connected(True)
            logger.info("W&T-Verbindung %s:%s aufgebaut", host, port)
            try:
                await self._read_loop(reader)
            except (OSError, asyncio.IncompleteReadError) as exc:
                logger.warning("W&T-Leseschleife beendet: %s", exc)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
                await self._set_connected(False)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        strategy = self._config.get("datagram_strategy", "idle")
        idle_ms = int(self._config.get("idle_ms", 2000))
        charset = self._config.get("charset", "cp850")
        buffer = bytearray()

        while not self._stop.is_set():
            if strategy == "formfeed":
                try:
                    chunk = await reader.readuntil(bytes([FORM_FEED]))
                except asyncio.IncompleteReadError as exc:
                    if exc.partial:
                        self._tap_raw(bytes(exc.partial))
                        await self._emit(bytes(exc.partial), charset)
                    return
                self._tap_raw(bytes(chunk))
                data = chunk.rstrip(bytes([FORM_FEED]))
                if data.strip():
                    await self._emit(bytes(data), charset)
            else:
                # Idle-Timeout: lesen bis idle_ms keine Daten mehr kommen.
                try:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=idle_ms / 1000)
                except asyncio.TimeoutError:
                    if buffer:
                        await self._emit(bytes(buffer), charset)
                        buffer = bytearray()
                    continue
                if not chunk:  # EOF
                    if buffer:
                        await self._emit(bytes(buffer), charset)
                    return
                self._tap_raw(bytes(chunk))
                buffer.extend(chunk)

    def _tap_raw(self, chunk: bytes) -> None:
        """Roh-Chunk an den Fan-Out weitergeben (Fehler nie den Empfang stören lassen)."""
        if self.on_raw is None or not chunk:
            return
        try:
            self.on_raw(chunk)
        except Exception:
            logger.exception("Roh-Abgriff (Fan-Out) fehlgeschlagen – ignoriert")

    async def _emit(self, raw: bytes, charset: str) -> None:
        logger.info("Alarm-Datagramm empfangen (%d Bytes, %s)", len(raw), charset)
        await self.on_datagram(raw, charset)

    async def _set_connected(self, connected: bool) -> None:
        if connected != self._connected:
            self._connected = connected
            await self.on_status(connected)
