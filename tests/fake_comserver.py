"""Kleiner TCP-Server, der einen W&T Com-Server simuliert: verbindet sich ein
Client, spielt er eine Liste von Alarm-Mitschnitten ab (mit optionaler Pause).
Nutzbar für Integrationstests und die monatliche Funktionsprobe."""
from __future__ import annotations

import asyncio


class FakeComServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, close_after: int | None = None, hold_open: float = 0.3):
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None
        self._payloads: list[tuple[bytes, float]] = []
        self.close_after = close_after
        self.hold_open = hold_open
        self.connections = 0
        self._clients: set[asyncio.Task] = set()

    def queue(self, data: bytes, pause: float = 0.05) -> None:
        self._payloads.append((data, pause))

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._clients.add(task)
        self.connections += 1
        try:
            for index, (data, pause) in enumerate(self._payloads, 1):
                writer.write(data)
                await writer.drain()
                if self.close_after is not None and index >= self.close_after:
                    return
                await asyncio.sleep(pause)
            await asyncio.sleep(self.hold_open)
        finally:
            writer.close()
            await writer.wait_closed()
            self._clients.discard(task)

    async def __aenter__(self):
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for task in list(self._clients):
            task.cancel()
        await asyncio.gather(*self._clients, return_exceptions=True)
