"""Serial-Fan-Out: mehrere Clients erhalten den rohen Strom 1:1; Allowlist/Enabled greifen."""
import asyncio
import socket

from ecpg.serial_passthrough import SerialPassthrough


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _wait_clients(pt, n, timeout=1.0):
    for _ in range(int(timeout / 0.02)):
        if pt.client_count() == n:
            return
        await asyncio.sleep(0.02)


async def test_fanout_broadcasts_identical_bytes_to_all():
    port = _free_port()
    pt = SerialPassthrough()
    pt.update_config({"passthrough_enabled": True, "passthrough_port": port, "passthrough_bind": "127.0.0.1"})
    await pt.apply()
    assert pt._server is not None
    r1, w1 = await asyncio.open_connection("127.0.0.1", port)
    r2, w2 = await asyncio.open_connection("127.0.0.1", port)
    await _wait_clients(pt, 2)
    assert pt.client_count() == 2

    pt.broadcast(b"ALARM 123\x0c")
    d1 = await asyncio.wait_for(r1.readexactly(10), 1)
    d2 = await asyncio.wait_for(r2.readexactly(10), 1)
    assert d1 == b"ALARM 123\x0c" == d2

    w1.close()
    w2.close()
    await pt.stop()


async def test_disabled_starts_no_server():
    pt = SerialPassthrough()
    pt.update_config({"passthrough_enabled": False, "passthrough_port": _free_port()})
    await pt.apply()
    assert pt._server is None
    await pt.stop()


async def test_allowlist_rejects_foreign_ip():
    port = _free_port()
    pt = SerialPassthrough()
    pt.update_config({
        "passthrough_enabled": True, "passthrough_port": port, "passthrough_bind": "127.0.0.1",
        "passthrough_allowlist": ["10.0.0.0/8"],
    })
    await pt.apply()
    _r, w = await asyncio.open_connection("127.0.0.1", port)
    await asyncio.sleep(0.1)
    # 127.0.0.1 ist nicht in 10.0.0.0/8 -> kein Client registriert
    assert pt.client_count() == 0
    w.close()
    await pt.stop()


async def test_apply_stops_when_disabled():
    port = _free_port()
    pt = SerialPassthrough()
    pt.update_config({"passthrough_enabled": True, "passthrough_port": port, "passthrough_bind": "127.0.0.1"})
    await pt.apply()
    assert pt._server is not None
    pt.update_config({"passthrough_enabled": False, "passthrough_port": port})
    await pt.apply()
    assert pt._server is None
