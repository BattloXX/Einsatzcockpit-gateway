"""Serieller Alarmempfang über den W&T Com-Server (TCP-Socket-Server).

Das Gateway verbindet sich als TCP-Client, puffert den Zeichenstrom und schließt
ein Datagramm über eine konfigurierbare Strategie ab (Idle-Timeout – Default –
oder Form Feed 0x0C). Jedes Datagramm wird roh in den Ringpuffer gelegt und über
einen Callback verarbeitet. Verbindungsabriss → Reconnect + Statusmeldung.
"""
from __future__ import annotations

import asyncio
import logging
import random
import socket
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger("ecpg.serial")

FORM_FEED = 0x0C


def reconnect_delay(attempt: int, jitter: float) -> float:
    """Exponentieller Backoff mit injizierbarem Jitter (0..1), begrenzt auf 2..60 s."""
    base = min(2.0 * 2 ** min(max(attempt, 0), 5), 60.0)
    return max(2.0, min(60.0, base * (0.8 + 0.4 * jitter)))


class ReceptionLock:
    """Pro Datenverzeichnis nur ein Empfaenger; ohne fcntl (Windows) no-op."""
    def __init__(self, data_dir: str):
        self.path = Path(data_dir) / "serial-ingest.lock"
        self._file = None

    def acquire(self) -> bool:
        if self._file is not None:
            return True
        try:
            import fcntl
        except ImportError:
            logger.warning("Keine fcntl-Dateisperre auf dieser Plattform verfuegbar")
            return True
        handle = self.path.open("a")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return False
        except OSError:
            handle.close()
            raise
        self._file = handle
        return True

    def release(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class SerialIngest:
    def __init__(
        self,
        on_datagram: Callable[[bytes, str, bool], Awaitable[None]],
        on_status: Callable[[bool], Awaitable[None]],
        on_raw: Callable[[bytes], None] | None = None,
        data_dir: str | None = None,
    ):
        self.on_datagram = on_datagram   # async (raw_bytes, charset, incomplete)
        self.on_status = on_status       # async (connected)
        # Optionaler Roh-Abgriff (Serial-Fan-Out): jeder vom W&T gelesene Chunk wird
        # VOR der Datagramm-Zerlegung 1:1 weitergereicht. Synchron + fehlertolerant,
        # damit der Alarm-Empfang nie durch einen Downstream-Fehler blockiert wird.
        self.on_raw = on_raw
        self._task: asyncio.Task | None = None
        self._config: dict = {}
        self.last_received_at: datetime | None = None
        self.source_addr: str | None = None
        self._reception_lock = ReceptionLock(data_dir) if data_dir else None
        self._connected = False
        self._stop = asyncio.Event()

    def update_config(self, wut_config: dict) -> None:
        self._config = wut_config or {}

    def start(self) -> None:
        if self._task is None or self._task.done():
            if self._reception_lock and not self._reception_lock.acquire():
                logger.error("Alarmempfang startet nicht: Datenverzeichnis bereits durch andere Instanz gesperrt")
                return
            self._stop.clear()
            self._task = asyncio.create_task(self._run_locked())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._reception_lock:
            self._reception_lock.release()

    @property
    def connected(self) -> bool:
        return self._connected

    async def _run_locked(self) -> None:
        try:
            await self._run()
        finally:
            if self._reception_lock:
                self._reception_lock.release()

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            host = self._config.get("host")
            port = int(self._config.get("port", 8000))
            if not host:
                await asyncio.sleep(5)
                continue
            writer = None
            started = asyncio.get_running_loop().time()
            try:
                reader, writer = await asyncio.open_connection(host, port)
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.source_addr = f"{host}:{port}"
                await self._set_connected(True)
                logger.info("W&T-Verbindung %s:%s aufgebaut", host, port)
                await self._read_loop(reader)
            except OSError as exc:
                logger.warning("W&T-Verbindung %s:%s beendet: %s", host, port, exc)
            finally:
                if writer is not None:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except OSError:
                        logger.debug("W&T-Socket bereits geschlossen")
                await self._set_connected(False)
            if not self._stop.is_set():
                if asyncio.get_running_loop().time() - started >= 60:
                    attempt = 0
                await asyncio.sleep(reconnect_delay(attempt, random.random()))
                attempt += 1

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        strategy = self._config.get("datagram_strategy", "idle")
        timeout_ms = int(self._config.get(
            "incomplete_after_ms" if strategy == "formfeed" else "idle_ms",
            10000 if strategy == "formfeed" else 2000,
        ))
        charset = self._config.get("charset", "cp850")
        buffer = bytearray()

        try:
            while not self._stop.is_set():
                try:
                    chunk = await asyncio.wait_for(reader.read(4096), timeout=timeout_ms / 1000)
                except TimeoutError:
                    if buffer:
                        data = bytes(buffer)
                        buffer.clear()
                        if data.strip():
                            await self._emit(data, charset, incomplete=strategy == "formfeed")
                    continue
                if not chunk:
                    return
                self.last_received_at = datetime.now(UTC)
                self._tap_raw(bytes(chunk))
                buffer.extend(chunk)
                # Form Feed trennt in beiden Strategien; ein Rest bleibt bis zum
                # naechsten Chunk, Idle-Timeout oder Verbindungsende erhalten.
                while FORM_FEED in buffer:
                    end = buffer.index(FORM_FEED)
                    data = bytes(buffer[:end])
                    del buffer[:end + 1]
                    if data.strip():
                        await self._emit(data, charset)
        finally:
            if bytes(buffer).strip():
                await self._emit(bytes(buffer), charset, incomplete=True)

    def _tap_raw(self, chunk: bytes) -> None:
        """Roh-Chunk an den Fan-Out weitergeben (Fehler nie den Empfang stören lassen)."""
        if self.on_raw is None or not chunk:
            return
        try:
            self.on_raw(chunk)
        except Exception:
            logger.exception("Roh-Abgriff (Fan-Out) fehlgeschlagen – ignoriert")

    async def _emit(self, raw: bytes, charset: str, incomplete: bool = False) -> None:
        logger.info("Alarm-Datagramm empfangen (%d Bytes, %s)", len(raw), charset)
        await self.on_datagram(raw, charset, incomplete)

    async def _set_connected(self, connected: bool) -> None:
        if connected != self._connected:
            self._connected = connected
            await self.on_status(connected)
