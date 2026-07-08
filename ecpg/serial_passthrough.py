"""Serial-Fan-Out: TCP-Server, der den rohen W&T-Strom 1:1 an mehrere Clients verteilt.

Das Gateway hält die (exklusive) Verbindung zum W&T Com-Server und stellt den identischen
Bytestrom hier wieder bereit, sodass sich mehrere W&T-erwartende Clients gleichzeitig
verbinden koennen und dieselben Alarme erhalten.

- Nur Empfang: Downstream-Clients lesen; eingehende Bytes werden verworfen (kein Rueckkanal
  zum W&T -> keine Schreibkonflikte am physischen Port).
- Backpressure: je Client eine beschraenkte Queue. Ueberlaeuft sie (langsamer/toter Client),
  wird der Client getrennt, ohne den Upstream oder die anderen Clients zu blockieren.
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging

logger = logging.getLogger("ecpg.passthrough")

_QUEUE_MAX = 256  # gepufferte Chunks je Client, bevor er als "zu langsam" getrennt wird


class SerialPassthrough:
    def __init__(self) -> None:
        self._enabled = False
        self._bind = "0.0.0.0"
        self._port = 0
        self._max_clients = 8
        self._allowlist: list[str] = []
        self._server: asyncio.AbstractServer | None = None
        self._running_addr: tuple[str, int] | None = None
        self._clients: set[asyncio.Queue] = set()

    # ── Konfiguration (aus config_sync, Abschnitt wut) ───────────────────────────
    def update_config(self, cfg: dict) -> None:
        cfg = cfg or {}
        self._enabled = bool(cfg.get("passthrough_enabled"))
        self._port = int(cfg.get("passthrough_port") or 0)
        self._bind = (cfg.get("passthrough_bind") or "0.0.0.0").strip() or "0.0.0.0"
        self._max_clients = max(1, int(cfg.get("passthrough_max_clients") or 8))
        allow = cfg.get("passthrough_allowlist") or []
        if isinstance(allow, str):
            allow = [a.strip() for a in allow.replace(";", ",").split(",") if a.strip()]
        self._allowlist = list(allow)

    def _addr(self) -> tuple[str, int]:
        return (self._bind, self._port)

    async def apply(self) -> None:
        """(Re)Startet oder stoppt den Server passend zur aktuellen Konfiguration."""
        want = self._enabled and self._port > 0
        running = self._server is not None
        if running and (not want or self._running_addr != self._addr()):
            await self.stop()
            running = False
        if want and not running:
            await self.start()

    async def start(self) -> None:
        try:
            self._server = await asyncio.start_server(self._handle, self._bind, self._port)
        except OSError as exc:
            logger.warning("Serial-Fan-Out %s:%s konnte nicht starten: %s", self._bind, self._port, exc)
            self._server = None
            return
        self._running_addr = self._addr()
        logger.info("Serial-Fan-Out lauscht auf %s:%s", self._bind, self._port)

    async def stop(self) -> None:
        for q in list(self._clients):
            self._end_client(q)
        self._clients.clear()
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
            self._running_addr = None
            logger.info("Serial-Fan-Out gestoppt")

    # ── Verteilung ───────────────────────────────────────────────────────────────
    def broadcast(self, data: bytes) -> None:
        """Rohbytes vom W&T an alle verbundenen Clients (nicht blockierend)."""
        if not data:
            return
        for q in list(self._clients):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                logger.warning("Serial-Fan-Out: Client zu langsam – wird getrennt")
                self._clients.discard(q)
                self._end_client(q)

    def client_count(self) -> int:
        return len(self._clients)

    def _end_client(self, q: asyncio.Queue) -> None:
        try:
            q.put_nowait(None)  # Sentinel beendet die Schreibschleife
        except asyncio.QueueFull:
            # Queue voll -> leeren und Sentinel setzen
            try:
                while True:
                    q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(None)
            except Exception:
                pass

    def _allowed(self, peer_ip: str) -> bool:
        if not self._allowlist:
            return True
        try:
            ip = ipaddress.ip_address(peer_ip)
        except ValueError:
            return False
        for entry in self._allowlist:
            try:
                if ip in ipaddress.ip_network(entry, strict=False):
                    return True
            except ValueError:
                if str(peer_ip) == str(entry):
                    return True
        return False

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if peer else ""
        if not self._allowed(peer_ip):
            logger.warning("Serial-Fan-Out: %s nicht in Allowlist – abgewiesen", peer_ip)
            writer.close()
            return
        if len(self._clients) >= self._max_clients:
            logger.warning("Serial-Fan-Out: max_clients (%d) erreicht – %s abgewiesen", self._max_clients, peer_ip)
            writer.close()
            return

        q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._clients.add(q)
        logger.info("Serial-Fan-Out: Client verbunden %s (jetzt %d)", peer_ip, len(self._clients))
        drain_task = asyncio.create_task(self._drain_input(reader, q))
        try:
            while True:
                data = await q.get()
                if data is None:  # Sentinel
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            self._clients.discard(q)
            drain_task.cancel()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("Serial-Fan-Out: Client getrennt %s (jetzt %d)", peer_ip, len(self._clients))

    async def _drain_input(self, reader: asyncio.StreamReader, q: asyncio.Queue) -> None:
        """Receive-only: eingehende Bytes verwerfen, EOF erkennen -> Client beenden."""
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    self._end_client(q)
                    return
        except (ConnectionError, OSError):
            self._end_client(q)
