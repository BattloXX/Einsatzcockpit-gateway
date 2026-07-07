"""Pairing: Einmal-Code → langlebiges Device-Token.

Reihenfolge beim Start:
1. Bereits gespeichertes Device-Token im Spool → verwenden.
2. ECPG_PAIRING_CODE gesetzt → einlösen, Token speichern.
3. Sonst: auf Eingabe über die Statusseite warten (kv 'pairing_code').
"""
from __future__ import annotations

import logging

import httpx

from ecpg.settings import settings
from ecpg.spool import Spool

logger = logging.getLogger("ecpg.pairing")


def get_device_token(spool: Spool) -> str | None:
    return spool.get("device_token")


async def try_pair(spool: Spool, code: str) -> str | None:
    """Löst einen Pairing-Code ein. Gibt das Device-Token zurück oder None."""
    code = (code or "").strip().upper()
    if not code:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(settings.pair_url, json={"code": code})
    except httpx.HTTPError as exc:
        logger.warning("Pairing-Request fehlgeschlagen: %s", exc)
        return None
    if resp.status_code != 200:
        logger.warning("Pairing abgelehnt (%s): %s", resp.status_code, resp.text[:200])
        return None
    data = resp.json()
    token = data.get("device_token")
    if not token:
        return None
    spool.set("device_token", token)
    spool.set("gateway_id", str(data.get("gateway_id", "")))
    spool.set("gateway_name", data.get("name", ""))
    logger.info("Gekoppelt als Gateway %s (%s)", data.get("gateway_id"), data.get("name"))
    return token


async def ensure_paired(spool: Spool) -> str | None:
    """Stellt sicher, dass ein Device-Token vorliegt (Token > ENV-Code > None)."""
    token = get_device_token(spool)
    if token:
        return token
    if settings.pairing_code:
        return await try_pair(spool, settings.pairing_code)
    # Über Statusseite eingegebener Code?
    code = spool.get("pairing_code")
    if code:
        spool.set("pairing_code", "")
        return await try_pair(spool, code)
    return None
