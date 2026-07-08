"""Drucker-Health-Check: schnelle Erreichbarkeitspruefung per TCP-Connect.

Aus der Drucker-URI (ipp://host:port/..., ipps://host:port/...) werden Host und Port
extrahiert; ein kurzer TCP-Connect entscheidet ueber reachable true/false. Das ist guenstig,
schnell und ausreichend, um im Web-UI Online/Offline anzuzeigen und den Remotedruck stabil zu
halten (offline erkannte Drucker -> Job wird gespoolt/nachgeholt statt verloren).
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from urllib.parse import urlsplit

_DEFAULT_PORTS = {"ipp": 631, "ipps": 631, "http": 80, "https": 443, "socket": 9100}


def host_port(uri: str) -> tuple[str, int] | None:
    """Host und Port aus einer Drucker-URI; None wenn nicht ableitbar."""
    try:
        sp = urlsplit(uri or "")
        host = sp.hostname
        if not host:
            return None
        port = sp.port or _DEFAULT_PORTS.get(sp.scheme, 631)
        return host, int(port)
    except (ValueError, TypeError):
        return None


async def probe(uri: str, timeout: float = 3.0) -> dict:
    """Prueft die Erreichbarkeit eines Druckers. Gibt status-Dict fuer die Cloud zurueck."""
    now = datetime.now(UTC).isoformat()
    hp = host_port(uri)
    if hp is None:
        return {"reachable": False, "state": "unknown", "checked_at": now}
    host, port = hp
    writer = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        return {"reachable": True, "state": "idle", "checked_at": now}
    except (OSError, asyncio.TimeoutError):
        return {"reachable": False, "state": "offline", "checked_at": now}
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


async def probe_all(printers: list[dict], timeout: float = 3.0) -> list[dict]:
    """Prueft mehrere Drucker parallel. printers: [{id, uri}, ...].
    Rueckgabe: [{printer_id, status}, ...] fuer die Drucker mit id+uri."""
    targets = [p for p in printers if p.get("id") and p.get("uri")]
    results = await asyncio.gather(
        *(probe(p["uri"], timeout=timeout) for p in targets),
        return_exceptions=True,
    )
    out: list[dict] = []
    for p, res in zip(targets, results):
        if isinstance(res, Exception):
            res = {"reachable": False, "state": "error", "checked_at": datetime.now(UTC).isoformat()}
        out.append({"printer_id": p["id"], "status": res})
    return out
