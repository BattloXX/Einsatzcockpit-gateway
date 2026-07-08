"""Drucker-Health-Check: URI-Parsing + Reachability (TCP-Connect)."""
import asyncio

from ecpg.printer_health import host_port, probe, probe_all


def test_host_port_parsing():
    assert host_port("ipp://192.168.1.5:631/ipp/print") == ("192.168.1.5", 631)
    assert host_port("ipps://drucker.local/ipp/print") == ("drucker.local", 631)
    assert host_port("socket://10.0.0.9:9100") == ("10.0.0.9", 9100)
    assert host_port("") is None
    assert host_port("keine-uri") is None


async def _free_closed_port() -> int:
    srv = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    srv.close()
    await srv.wait_closed()
    return port  # jetzt garantiert geschlossen


async def test_probe_reachable():
    srv = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    try:
        res = await probe(f"ipp://127.0.0.1:{port}/ipp/print", timeout=1)
        assert res["reachable"] is True
        assert res["checked_at"]
    finally:
        srv.close()
        await srv.wait_closed()


async def test_probe_unreachable():
    port = await _free_closed_port()
    res = await probe(f"ipp://127.0.0.1:{port}/ipp/print", timeout=1)
    assert res["reachable"] is False


async def test_probe_all_maps_printer_ids():
    srv = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    on = srv.sockets[0].getsockname()[1]
    off = await _free_closed_port()
    try:
        res = await probe_all([
            {"id": 5, "uri": f"ipp://127.0.0.1:{on}/ipp/print"},
            {"id": 6, "uri": f"ipp://127.0.0.1:{off}/ipp/print"},
            {"id": None, "uri": "ipp://x/y"},  # ohne id -> ignoriert
        ], timeout=1)
    finally:
        srv.close()
        await srv.wait_closed()
    by = {r["printer_id"]: r["status"]["reachable"] for r in res}
    assert by == {5: True, 6: False}
