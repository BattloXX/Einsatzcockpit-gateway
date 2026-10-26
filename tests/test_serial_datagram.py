"""Serieller Ingest gegen den simulierten Com-Server: Idle-Timeout- und
Form-Feed-Datagramm-Erkennung."""
import asyncio

import pytest
from fake_comserver import FakeComServer

from ecpg.serial_ingest import SerialIngest


async def _collect(config: dict, payloads: list[bytes], expect: int, timeout=3.0):
    received: list[bytes] = []
    status: list[bool] = []

    async def on_datagram(raw, charset, incomplete):
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


@pytest.mark.parametrize("strategy", ["idle", "formfeed"])
async def test_chunk_stream_keeps_remainder_and_raw_tap(strategy):
    received = []
    tapped = []
    done = asyncio.Event()

    async def datagram(raw, charset, incomplete):
        received.append((raw, charset, incomplete))
        if len(received) == 3:
            done.set()

    async def status(connected):
        pass

    async with FakeComServer(close_after=3) as server:
        server.queue(b"fir", pause=0.02)
        server.queue(b"st\x0csecond\x0cthi", pause=0.02)
        server.queue(b"rd\x0c")
        ingest = SerialIngest(datagram, status, tapped.append)
        ingest.update_config({"host": server.host, "port": server.port,
                              "datagram_strategy": strategy, "idle_ms": 200})
        ingest.start()
        try:
            await asyncio.wait_for(done.wait(), 2)
            assert received == [(b"first", "cp850", False), (b"second", "cp850", False),
                                (b"third", "cp850", False)]
            assert b"".join(tapped) == b"first\x0csecond\x0cthird\x0c"
            assert ingest.last_received_at.utcoffset().total_seconds() == 0
        finally:
            await ingest.stop()


@pytest.mark.parametrize("strategy", ["idle", "formfeed"])
async def test_three_tcp_chunks_form_one_datagram(strategy):
    received, _ = await _collect(
        {"datagram_strategy": strategy, "idle_ms": 200},
        [b"alarm ", b"in three ", b"chunks\x0c"], expect=1,
    )
    assert received == [b"alarm in three chunks"]


@pytest.mark.parametrize("strategy", ["idle", "formfeed"])
async def test_eof_emits_incomplete_and_reconnects(strategy):
    import socket

    received = []
    statuses = []
    done = asyncio.Event()
    sockets = []
    original_open = asyncio.open_connection

    async def datagram(raw, charset, incomplete):
        received.append((raw, incomplete))
        if len(received) == 2:
            done.set()

    async def status(connected):
        statuses.append(connected)

    # Beobachtet den echten Client-Socket, ohne den TCP-Transport zu ersetzen.
    async def open_connection(*args):
        reader, writer = await original_open(*args)
        sockets.append(writer.get_extra_info("socket"))
        return reader, writer

    from unittest.mock import patch
    async with FakeComServer(close_after=1) as server:
        server.queue(b"abgeschnittener alarm")
        ingest = SerialIngest(datagram, status)
        ingest.update_config({"host": server.host, "port": server.port,
                              "datagram_strategy": strategy})
        with patch("ecpg.serial_ingest.asyncio.open_connection", open_connection):
            ingest.start()
            try:
                # Keepalive noch am offenen Socket pruefen, bevor der Server EOF liefert.
                while not sockets:
                    await asyncio.sleep(0)
                assert sockets[0].getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
                await asyncio.wait_for(done.wait(), 5)
                assert received == [(b"abgeschnittener alarm", True)] * 2
                assert statuses[:3] == [True, False, True]
                assert server.connections == 2
            finally:
                await ingest.stop()


async def test_formfeed_idle_timeout_marks_incomplete_then_continues():
    received = []
    done = asyncio.Event()

    async def datagram(raw, charset, incomplete):
        received.append((raw, incomplete))
        if len(received) == 2:
            done.set()

    async def status(connected):
        pass

    async with FakeComServer(hold_open=1) as server:
        server.queue(b"partial", pause=0.15)
        server.queue(b"next\x0c")
        ingest = SerialIngest(datagram, status)
        ingest.update_config({"host": server.host, "port": server.port,
                              "datagram_strategy": "formfeed", "incomplete_after_ms": 50})
        ingest.start()
        try:
            await asyncio.wait_for(done.wait(), 2)
            assert received == [(b"partial", True), (b"next", False)]
        finally:
            await ingest.stop()


def test_reconnect_backoff_bounds_and_jitter():
    from ecpg.serial_ingest import reconnect_delay
    assert [reconnect_delay(i, 0.5) for i in range(8)] == [2, 4, 8, 16, 32, 60, 60, 60]
    assert reconnect_delay(2, 0) < reconnect_delay(2, 1)
    for attempt in range(100):
        for jitter in (0, 0.5, 1):
            assert 2 <= reconnect_delay(attempt, jitter) <= 60


async def test_instance_lock_prevents_second_receiver(tmp_path, caplog):
    pytest.importorskip("fcntl")

    async def datagram(raw, charset, incomplete):
        pass

    async def status(connected):
        pass

    first = SerialIngest(datagram, status, data_dir=str(tmp_path))
    second = SerialIngest(datagram, status, data_dir=str(tmp_path))
    first.start()
    await asyncio.sleep(0)
    second.start()
    assert second._task is None
    assert "andere Instanz gesperrt" in caplog.text
    await first.stop()
    second.start()
    assert second._task is not None
    # Auch Stop direkt nach Start muss die Sperre wieder freigeben.
    await second.stop()
    first.start()
    assert not first._task.done()
    await first.stop()
