"""Lokale Statusseite + /healthz + Pairing-Code-Eingabe + Job-Abbruch.

Erreichbar unter http://<gateway-ip>:8631/. Zeigt Verbindungsstatus, Drucker,
Druckaufträge (mit Abbrechen), letzte Alarme, Version.

Design: HydroFlow-Layout (Status-Kacheln, Panels, Chips, Mono-Labels) im
eigenständigen dunklen Tactical-Stil des Einsatzcockpit. BEWUSST self-contained
(kein CDN/keine Google-Fonts, Icons als Emoji) – das Gateway läuft lokal im
Feuerwehrhaus, ggf. ohne Internet.
"""
from __future__ import annotations

import html
from datetime import datetime, timezone

from aiohttp import web

from ecpg import __version__
from ecpg.settings import settings

_TERMINAL = {"done", "failed", "canceled"}

# ── Self-contained CSS (Palette = Einsatzcockpit/HydroFlow-Tokens) ──────────────
_CSS = """
:root{
  --bg:#081425; --s0:#040e1f; --s1:#152031; --s2:#111c2d; --s3:#1f2a3c; --s-hi:#2a3548;
  --line:#424754; --line2:#2a3548; --text:#d8e3fb; --dim:#c2c6d6; --faint:#8c909f;
  --primary:#adc6ff; --primary-c:#4d8eff; --green:#7ee7a0; --red:#ffb4ab; --amber:#ffb95f;
  --mono:'JetBrains Mono',ui-monospace,'SFMono-Regular',Menlo,Consolas,monospace;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
  font-family:'IBM Plex Sans',system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
  -webkit-tap-highlight-color:transparent;min-height:100vh}
::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}
.wrap{max-width:1120px;margin:0 auto;padding:20px 16px 48px}
.top{display:flex;align-items:center;gap:12px;margin-bottom:20px}
.top__ic{width:42px;height:42px;border-radius:10px;background:var(--primary-c);
  display:flex;align-items:center;justify-content:center;font-size:1.3rem;flex-shrink:0}
.top__t{font-size:1.25rem;font-weight:700;color:var(--primary);line-height:1.1}
.top__s{font-family:var(--mono);font-size:.62rem;letter-spacing:.06em;color:var(--faint);text-transform:uppercase}
.ver{margin-left:auto;font-family:var(--mono);font-size:.64rem;color:var(--dim);
  background:var(--s-hi);border:1px solid var(--line);border-radius:999px;padding:5px 11px}
.ver b{color:var(--amber)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}
.tile{background:var(--s1);border:1px solid var(--line);border-radius:12px;padding:14px;
  display:flex;align-items:center;gap:12px}
.tile__ic{width:40px;height:40px;border-radius:9px;flex-shrink:0;display:flex;
  align-items:center;justify-content:center;font-size:1.2rem;background:var(--s-hi)}
.tile__v{font-size:1.15rem;font-weight:700;line-height:1}
.tile__l{font-family:var(--mono);font-size:.6rem;letter-spacing:.07em;color:var(--faint);
  text-transform:uppercase;margin-top:4px}
.ok{color:var(--green)}.bad{color:var(--red)}.warn{color:var(--amber)}
.grid{display:grid;grid-template-columns:1fr;gap:16px}
@media(min-width:860px){.grid{grid-template-columns:1.4fr 1fr}}
.panel{background:var(--s1);border:1px solid var(--line);border-radius:12px;overflow:hidden}
.panel__hd{display:flex;align-items:center;gap:8px;padding:12px 14px;border-bottom:1px solid var(--line);
  background:rgba(42,53,72,.25)}
.panel__hd h2{margin:0;font-size:.98rem;font-weight:600}
.panel__c{padding:6px 0}
.tbl{width:100%;border-collapse:collapse}
.tbl th{text-align:left;font-family:var(--mono);font-size:.6rem;letter-spacing:.06em;
  text-transform:uppercase;color:var(--faint);padding:8px 14px;background:var(--s2)}
.tbl td{padding:9px 14px;font-size:.84rem;border-top:1px solid var(--line2);vertical-align:top}
.mono{font-family:var(--mono);font-size:.76rem;color:var(--primary)}
.chip{display:inline-flex;align-items:center;gap:5px;padding:2px 9px;border-radius:999px;
  font-size:.68rem;font-weight:700;border:1px solid transparent}
.chip::before{content:"";width:6px;height:6px;border-radius:50%;background:currentColor}
.chip--ok{color:var(--green);background:rgba(126,231,160,.12);border-color:rgba(126,231,160,.4)}
.chip--err{color:var(--red);background:rgba(255,180,171,.12);border-color:rgba(255,180,171,.4)}
.chip--warn{color:var(--amber);background:rgba(255,185,95,.12);border-color:rgba(255,185,95,.4)}
.btn{font:inherit;font-size:.74rem;font-weight:600;cursor:pointer;border-radius:7px;
  padding:5px 11px;border:1px solid var(--line);background:var(--s-hi);color:var(--text)}
.btn--danger{color:var(--red);border-color:rgba(255,180,171,.4);background:rgba(255,180,171,.1)}
.btn--primary{background:var(--primary-c);color:#00285d;border-color:transparent;font-weight:700}
.empty{padding:22px 14px;color:var(--faint);font-style:italic;text-align:center;font-size:.85rem}
.pair{background:var(--s1);border:1px solid var(--line);border-radius:12px;padding:16px;margin-bottom:16px}
.pair h2{margin:0 0 4px;font-size:1rem}.pair p{margin:0 0 10px;color:var(--dim);font-size:.84rem}
.pair form{display:flex;gap:8px;flex-wrap:wrap}
.pair input{flex:1;min-width:160px;background:var(--s0);border:1px solid var(--line);border-radius:8px;
  padding:9px 11px;color:var(--text);font-family:var(--mono);font-size:.9rem}
pre{margin:0;white-space:pre-wrap;word-break:break-word;font-family:var(--mono);font-size:.72rem;color:var(--dim)}
"""


def _fmt(ts: float | None) -> str:
    if not ts:
        return "–"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m. %H:%M:%S")


def _job_chip(status: str) -> str:
    cls = "chip--ok" if status == "done" else ("chip--err" if status in ("failed", "canceled") else "chip--warn")
    return f'<span class="chip {cls}">{html.escape(status)}</span>'


def _tile(icon: str, value: str, label: str, cls: str = "") -> str:
    return (f'<div class="tile"><div class="tile__ic">{icon}</div><div>'
            f'<div class="tile__v {cls}">{html.escape(value)}</div>'
            f'<div class="tile__l">{html.escape(label)}</div></div></div>')


def render_index(agent) -> str:
    """Baut die HTML-Statusseite. Reine Funktion (testbar) über das agent-Objekt."""
    cloud_ok = bool(getattr(agent, "cloud", None) and agent.cloud.connected)
    serial_ok = bool(getattr(agent, "serial", None) and agent.serial.connected)
    token = agent.spool.get("device_token")
    printers = (getattr(agent, "config", None) or {}).get("printers") or []
    alarms = agent.spool.recent_alarms(8)
    jobs = agent.spool.recent_jobs(15)

    tiles = "".join([
        _tile("☁️", "verbunden" if cloud_ok else "getrennt", "Cloud-Service", "ok" if cloud_ok else "bad"),
        _tile("📟", "aktiv" if serial_ok else "getrennt", "Alarmleitung", "ok" if serial_ok else "warn"),
        _tile("🔗", "ja" if token else "nein", "Gekoppelt", "ok" if token else "bad"),
        _tile("🖨️", str(len(printers)), "Drucker"),
    ])

    rows_p = "".join(
        f"<tr><td><b>{html.escape(str(p.get('name') or '–'))}</b></td>"
        f"<td class='mono'>{html.escape(str(p.get('uri') or ''))}</td>"
        f"<td style='text-align:right'><span class='chip chip--ok'>aktiv</span></td></tr>"
        for p in printers
    ) or "<tr><td colspan=3><div class='empty'>Keine Drucker konfiguriert</div></td></tr>"

    rows_j = ""
    for j in jobs:
        status = str(j["status"])
        jid = html.escape(str(j["job_id"]))
        cancel = ""
        if status not in _TERMINAL:
            cancel = (f'<form method="post" action="/jobs/{jid}/cancel" style="margin:0">'
                      f'<button class="btn btn--danger" type="submit">✖ Abbrechen</button></form>')
        err = html.escape(str(j.get("error") or ""))
        err_html = f"<div style='color:var(--faint);font-size:.72rem'>{err}</div>" if err else ""
        rows_j += (
            f"<tr><td class='mono'>#{jid}</td>"
            f"<td>{html.escape(str(j.get('document_type') or '–'))}{err_html}</td>"
            f"<td>{_job_chip(status)}</td>"
            f"<td style='text-align:right'>{cancel}</td></tr>"
        )
    rows_j = rows_j or "<tr><td colspan=4><div class='empty'>Keine Druckaufträge</div></td></tr>"

    if alarms:
        rows_a = "".join(
            f"<tr><td class='mono' style='white-space:nowrap'>{_fmt(a['received_at'])}</td>"
            f"<td>{_job_chip('done') if a['forwarded'] else _job_chip('printing')}</td>"
            f"<td><pre>{html.escape(bytes(a['raw_bytes']).decode(a['charset'] or 'cp850', 'replace')[:300])}</pre></td></tr>"
            for a in alarms
        )
        alarms_panel = ("<table class='tbl'><thead><tr><th>Empfangen</th><th>Cloud</th><th>Rohtext</th></tr></thead>"
                        f"<tbody>{rows_a}</tbody></table>")
    else:
        alarms_panel = "<div class='empty'>Keine Alarme empfangen · System nominal</div>"

    pair_form = "" if token else (
        '<div class="pair"><h2>🔗 Kopplung</h2>'
        '<p>Gateway mit dem Einsatzcockpit verbinden – Einmal-Code aus der Verwaltung eingeben.</p>'
        '<form method="post" action="/pair">'
        '<input name="code" placeholder="Pairing-Code" required>'
        '<button class="btn btn--primary" type="submit">Koppeln</button></form></div>'
    )

    return (
        "<!doctype html><html lang=de><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<title>ECPG Print Gateway</title><style>" + _CSS + "</style></head><body>"
        "<div class='wrap'>"
        "<div class='top'><div class='top__ic'>🖨️</div>"
        "<div><div class='top__t'>ECPG Print Gateway</div>"
        "<div class='top__s'>Lokales Drucker- &amp; Alarm-Gateway</div></div>"
        f"<span class='ver'>ECPG <b>v{__version__}</b></span></div>"
        f"<div class='tiles'>{tiles}</div>"
        f"{pair_form}"
        "<div class='grid'>"
        "<div style='display:flex;flex-direction:column;gap:16px'>"
        "<div class='panel'><div class='panel__hd'>🖨️ <h2>Drucker</h2></div>"
        "<div class='panel__c'><table class='tbl'><thead><tr><th>Name</th><th>URI</th><th></th></tr></thead>"
        f"<tbody>{rows_p}</tbody></table></div></div>"
        "<div class='panel'><div class='panel__hd'>📄 <h2>Druckaufträge</h2></div>"
        "<div class='panel__c'><table class='tbl'><thead><tr><th>Job</th><th>Dokument</th><th>Status</th><th></th></tr></thead>"
        f"<tbody>{rows_j}</tbody></table></div></div>"
        "</div>"
        "<div class='panel'><div class='panel__hd'>🔔 <h2>Letzte Alarme</h2></div>"
        f"<div class='panel__c'>{alarms_panel}</div></div>"
        "</div></div></body></html>"
    )


async def start_status_server(agent) -> web.AppRunner:
    app = web.Application()

    async def healthz(_req):
        return web.json_response({"status": "ok", "version": __version__})

    async def index(_req):
        return web.Response(text=render_index(agent), content_type="text/html")

    async def pair(req):
        data = await req.post()
        code = (data.get("code") or "").strip()
        if code:
            agent.spool.set("pairing_code", code)
        raise web.HTTPFound("/")

    async def cancel_job(req):
        job_id = req.match_info.get("job_id")
        if job_id and getattr(agent, "print_mgr", None):
            try:
                await agent.print_mgr.cancel(job_id)
            except Exception:  # pragma: no cover - Abbruch best-effort
                pass
        raise web.HTTPFound("/")

    app.router.add_get("/", index)
    app.router.add_get("/healthz", healthz)
    app.router.add_post("/pair", pair)
    app.router.add_post("/jobs/{job_id}/cancel", cancel_job)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", settings.status_port)
    await site.start()
    return runner
