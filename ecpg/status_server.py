"""Lokale Statusseite (read-only) + /healthz + Pairing-Code-Eingabe.

Erreichbar unter http://<gateway-ip>:8631/. Zeigt Verbindungsstatus, letzte
Alarme (Rohtext), Spool-Inhalt, Version. Keine Konfiguration lokal – außer der
Pairing-Code-Eingabe als Alternative zur ENV-Variable.
"""
from __future__ import annotations

import html
from datetime import datetime, timezone

from aiohttp import web

from ecpg import __version__
from ecpg.settings import settings


def _fmt(ts: float | None) -> str:
    if not ts:
        return "–"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y %H:%M:%S")


async def start_status_server(agent) -> web.AppRunner:
    app = web.Application()

    async def healthz(_req):
        return web.json_response({"status": "ok", "version": __version__})

    async def index(_req):
        cloud_ok = bool(agent.cloud and agent.cloud.connected)
        serial_ok = agent.serial.connected
        token = agent.spool.get("device_token")
        printers = (agent.config or {}).get("printers") or []
        alarms = agent.spool.recent_alarms(10)
        jobs = agent.spool.recent_jobs(15)

        rows_p = "".join(
            f"<tr><td>{html.escape(str(p.get('name')))}</td><td>{html.escape(str(p.get('uri')))}</td></tr>"
            for p in printers
        ) or "<tr><td colspan=2>keine</td></tr>"
        rows_a = "".join(
            f"<tr><td>{_fmt(a['received_at'])}</td><td>{'ja' if a['forwarded'] else 'nein'}</td>"
            f"<td><pre>{html.escape(bytes(a['raw_bytes']).decode(a['charset'] or 'cp850', 'replace')[:400])}</pre></td></tr>"
            for a in alarms
        ) or "<tr><td colspan=3>keine</td></tr>"
        rows_j = "".join(
            f"<tr><td>{html.escape(str(j['job_id']))}</td><td>{html.escape(str(j['document_type']))}</td>"
            f"<td>{html.escape(str(j['status']))}</td><td>{html.escape(str(j.get('error') or ''))}</td></tr>"
            for j in jobs
        ) or "<tr><td colspan=4>keine</td></tr>"

        pair_form = "" if token else """
          <h2>Kopplung</h2>
          <form method="post" action="/pair">
            <input name="code" placeholder="Pairing-Code" required>
            <button type="submit">Koppeln</button>
          </form>"""

        body = f"""<!doctype html><html lang=de><head><meta charset=utf-8>
        <meta name=viewport content="width=device-width,initial-scale=1">
        <title>ECPG Gateway</title>
        <style>body{{font-family:sans-serif;margin:1.5rem;max-width:900px}}
        h1{{color:#b71921}} table{{border-collapse:collapse;width:100%;margin:.5rem 0}}
        td,th{{border:1px solid #ccc;padding:4px 6px;text-align:left;font-size:.9rem;vertical-align:top}}
        pre{{margin:0;white-space:pre-wrap;font-size:.8rem}}
        .ok{{color:#137333;font-weight:bold}}.bad{{color:#b71921;font-weight:bold}}</style></head><body>
        <h1>ECPG Gateway v{__version__}</h1>
        <p>Cloud: <span class="{'ok' if cloud_ok else 'bad'}">{'verbunden' if cloud_ok else 'getrennt'}</span>
           &middot; Alarmleitung: <span class="{'ok' if serial_ok else 'bad'}">{'verbunden' if serial_ok else 'getrennt'}</span>
           &middot; Gekoppelt: {'ja' if token else 'nein'}</p>
        {pair_form}
        <h2>Drucker</h2><table><tr><th>Name</th><th>URI</th></tr>{rows_p}</table>
        <h2>Letzte Alarme</h2><table><tr><th>Empfangen</th><th>Weitergeleitet</th><th>Rohtext</th></tr>{rows_a}</table>
        <h2>Druckaufträge</h2><table><tr><th>Job</th><th>Dokument</th><th>Status</th><th>Fehler</th></tr>{rows_j}</table>
        </body></html>"""
        return web.Response(text=body, content_type="text/html")

    async def pair(req):
        data = await req.post()
        code = (data.get("code") or "").strip()
        if code:
            agent.spool.set("pairing_code", code)
        raise web.HTTPFound("/")

    app.router.add_get("/", index)
    app.router.add_get("/healthz", healthz)
    app.router.add_post("/pair", pair)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.status_port)
    await site.start()
    return runner
