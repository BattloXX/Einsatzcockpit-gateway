"""Kleiner TCP-Server, der einen W&T Com-Server simuliert: verbindet sich ein
Client, spielt er eine Liste von Alarm-Mitschnitten ab (mit optionaler Pause).
Nutzbar für Integrationstests und die monatliche Funktionsprobe."""
from __future__ import annotations

import asyncio


class FakeComServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None
        self._payloads: list[bytes] = []

    def queue(self, data: bytes) -> None:
        self._payloads.append(data)

    async def _handle(self, reader, writer):
        for p in self._payloads:
            writer.write(p)
            await writer.drain()
            await asyncio.sleep(0.05)
        # Verbindung offen halten (kein sofortiges EOF)
        await asyncio.sleep(0.3)
        writer.close()

    async def __aenter__(self):
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
