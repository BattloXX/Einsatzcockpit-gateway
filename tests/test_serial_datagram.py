"""Serieller Ingest gegen den simulierten Com-Server: Idle-Timeout- und
Form-Feed-Datagramm-Erkennung."""
import asyncio

import pytest

from ecpg.serial_ingest import SerialIngest
from fake_comserver import FakeComServer


async def _collect(config: dict, payloads: list[bytes], expect: int, timeout=3.0):
    received: list[bytes] = []
    status: list[bool] = []

    async def on_datagram(raw, charset):
        received.append(raw)

    async def on_status(connected):
        status.append(connected)

    async with FakeComServer() as server:
        for p in payloads:
            server.queue(p)
        cfg = {"host": server.host, "port": server.port, **config}
        ingest = SerialIngest(on_datagram, on_status)
        ingest.update_config(cfg)
        ingest.start()
        deadline = asyncio.get_event_loop().time() + timeout
        while len(received) < expect and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        await ingest.stop()
    return received, status


async def test_idle_timeout_splits_datagram():
    received, status = await _collect(
        {"datagram_strategy": "idle", "idle_ms": 300, "charset": "cp850"},
        [b"ALARM 1 Zeile eins\nZeile zwei"],
        expect=1,
    )
    assert len(received) == 1
    assert b"Zeile zwei" in received[0]
    assert True in status  # verbunden gemeldet


async def test_formfeed_splits_two_datagrams():
    received, _ = await _collect(
        {"datagram_strategy": "formfeed", "charset": "cp850"},
        [b"erster alarm\x0czweiter alarm\x0c"],
        expect=2,
    )
    assert len(received) == 2
    assert received[0] == b"erster alarm"
    assert received[1] == b"zweiter alarm"
