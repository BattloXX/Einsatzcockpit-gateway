"""Druckererkennung: mDNS (zeroconf) + optionale SNMP-Abfrage. Fehlen die
Bibliotheken (CI/Dev), liefert die Discovery eine leere Liste statt zu crashen.

Ergebnis je Fund: {name, modell, uri, identity:{ip,mac,serial,uuid}, capabilities}
Aktivierung erfolgt in der Cloud – hier werden nur Vorschläge gemeldet.
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("ecpg.discovery")

MDNS_TYPES = ["_ipp._tcp.local.", "_ipps._tcp.local.", "_pdl-datastream._tcp.local."]

# Bonjour-TXT 'PaperMax', ab dem A3 unterstuetzt wird (Werte laut Bonjour-Printing-Spec:
# <legal-A4, legal-A4, tabloid-A3, isoC-A2, >isoC-A2).
_PAPER_MAX_A3 = {"tabloid-A3", "isoC-A2", ">isoC-A2"}


def _media_from_paper_max(paper_max: str | None) -> list[str]:
    """Unterstuetzte Papiergroessen aus dem PaperMax-Flag. A4 kann quasi jeder Drucker;
    A3 nur, wenn PaperMax mindestens tabloid-A3 meldet."""
    media = ["A4"]
    if paper_max in _PAPER_MAX_A3:
        media.append("A3")
    return media


async def discover(timeout: float = 4.0) -> list[dict]:
    """mDNS-Scan nach IPP-Druckern. Blockierende zeroconf-Arbeit im Thread."""
    try:
        return await asyncio.to_thread(_mdns_scan, timeout)
    except Exception as exc:  # pragma: no cover
        logger.warning("Discovery fehlgeschlagen: %s", exc)
        return []


def _mdns_scan(timeout: float) -> list[dict]:
    try:
        from zeroconf import ServiceBrowser, Zeroconf  # type: ignore
    except Exception:
        logger.info("zeroconf nicht installiert – mDNS-Discovery übersprungen")
        return []

    import socket
    import time

    found: dict[str, dict] = {}

    class _Listener:
        def add_service(self, zc, type_, name):
            info = zc.get_service_info(type_, name, timeout=int(timeout * 1000))
            if not info or not info.addresses:
                return
            ip = socket.inet_ntoa(info.addresses[0])
            props = {k.decode(): (v.decode() if isinstance(v, bytes) else v)
                     for k, v in (info.properties or {}).items() if k}
            scheme = "ipps" if "_ipps" in type_ else "ipp"
            uri = f"{scheme}://{ip}:{info.port}/ipp/print"
            found[uri] = {
                "name": props.get("ty") or name.split(".")[0],
                "modell": props.get("ty"),
                "uri": uri,
                "identity": {"ip": ip, "uuid": props.get("UUID")},
                "capabilities": {
                    "color": props.get("Color") == "T",
                    "duplex": props.get("Duplex") == "T",
                    # Bonjour-TXT 'PaperMax' meldet die groesste Papiergroesse. A3 wird
                    # ab 'tabloid-A3' unterstuetzt (darunter nur bis A4/legal).
                    "media": _media_from_paper_max(props.get("PaperMax")),
                },
            }

        def update_service(self, *a):
            pass

        def remove_service(self, *a):
            pass

    zc = Zeroconf()
    try:
        for t in MDNS_TYPES:
            ServiceBrowser(zc, t, _Listener())
        time.sleep(timeout)
    finally:
        zc.close()
    return list(found.values())


async def probe_ip(ip: str) -> dict | None:
    """Prüft einen Drucker per IP (IPP get-printer-attributes vereinfacht).

    Ohne HTTP/IPP-Client geben wir eine Basis-URI zurück, damit die Cloud den
    Drucker per IP anlegen kann; Fähigkeiten kommen aus späteren Reports."""
    ip = (ip or "").strip()
    if not ip:
        return None
    uri = f"ipp://{ip}/ipp/print"
    return {
        "name": f"Drucker {ip}",
        "uri": uri,
        "identity": {"ip": ip},
        "capabilities": {},
    }
