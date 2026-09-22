# Einsatzcockpit Print & Alarm Gateway (ECPG)

Lokaler Docker-Container im Feuerwehrhaus, der die lokale Infrastruktur mit der
Einsatzcockpit-Cloud verbindet:

- **Alarmempfang:** TCP-Client zum W&T Com-Server (serielle Leitstellen-Leitung),
  Datagramm-Erkennung, Parser (RFL Vorarlberg), Ingest an die Cloud → Einsatz-Anlage.
- **Netzwerkdruck:** CUPS/IPP-Everywhere. Druckjobs kommen aus der Cloud (signierte
  PDF-URL), werden lokal gespoolt und gedruckt. Discovery via mDNS/SNMP.
- **Ausfallsicher:** Offline-Notdruck des Alarm-Rohtexts, lokaler Spool über Neustarts.

Ausschließlich **ausgehende** Verbindungen (WSS/HTTPS zur Cloud, TCP zum W&T, IPP zu
Druckern) – keine Portfreigaben nötig.

## Schnellstart

```bash
docker compose up -d
# Erststart: ECPG_PAIRING_CODE im compose setzen (Code aus dem Web-UI unter
# „Gateway / Druck" → Gateway → „Pairing-Code erzeugen"), Container starten,
# danach Code wieder entfernen. Alternativ Code auf der Statusseite eingeben.
```

Statusseite: `http://<gateway-ip>:8631/` (read-only, `/healthz` für Healthcheck).

## Konfiguration (ENV)

| Variable | Default | Bedeutung |
|---|---|---|
| `ECPG_CLOUD_URL` | `http://localhost:8092` | Basis-URL der Cloud |
| `ECPG_PAIRING_CODE` | – | Einmal-Code fürs erste Pairing |
| `ECPG_DATA_DIR` | `/data` | SQLite/Spool/Token |
| `ECPG_STATUS_PORT` | `8631` | Statusseite |
| `TZ` | `Europe/Vienna` | Zeitzone |

W&T-Verbindung, Drucker und Druckregeln werden **zentral im Web-UI** verwaltet und
per `config_sync` an das Gateway verteilt.

## Architektur

```
ecpg/
  main.py            # asyncio-Supervisor (Agent)
  settings.py        # ENV-Konfiguration
  cloud_connector.py # WSS-Client (Reconnect, Heartbeat, ack/dedup)
  pairing.py         # Code → Device-Token
  print_manager.py   # CUPS-Queues, Job: download→spool→CUPS→Status
  printer_discovery.py # mDNS/SNMP (optional)
  serial_ingest.py   # W&T-TCP-Client, Datagramm-Erkennung
  alarm_parser.py    # Pluggable Parser (RFL Vorarlberg)
  offline_print.py   # reportlab-Notdruck
  spool.py           # SQLite: Jobs, Alarm-Ring, Config-Cache
  status_server.py   # aiohttp Statusseite + /healthz + Pairing
```

## Protokoll (Cloud ↔ Gateway, WSS `/ws/gateway`)

Cloud → Gateway: `config_sync`, `print_job`, `cancel_job`, `discover_printers`,
`probe_printer`, `test_page`, `update_available`.
Gateway → Cloud: `hello` (inkl. `version`), `heartbeat`, `job_status`,
`serial_status`, `printer_report`, `printer_status` (periodischer Erreichbarkeits-
Check), `passthrough_status` (Serial-Fan-Out), `alarm_notice`, `log_event`. Alarme
werden zusätzlich verbindlich per REST `POST /api/v1/gateway/alarms` (idempotent via
Rohtext-Hash) ingestet.

## Versionierung

Die Version wird **automatisch** von der CI vergeben: `MAJOR.MINOR` aus `pyproject.toml`
plus die fortlaufende Workflow-Lauf-Nummer als Patch (z. B. `0.1.42`). Bei einem
`v*`-Git-Tag zählt stattdessen dessen Version. Der Wert wird zur Build-Zeit als
`ECPG_VERSION` ins Image injiziert, zur Laufzeit auf der Statusseite und via `hello`
an die Cloud gemeldet (Anzeige als Versions-Pill in der Gateway-Verwaltung) und als
Image-Tag vergeben – neben `latest` auch `MAJOR.MINOR.N` und `sha-<kurz>`.

Für einen **reproduzierbaren Betrieb** eine feste Version im `docker-compose.yml`
pinnen statt `:latest`, z. B. `image: ghcr.io/battloxx/einsatzcockpit-gateway:0.1.42`.
Für eine bewusste Major/Minor-Anhebung die Basis-Version in `pyproject.toml` ändern.

## Entwicklung / Tests

```bash
pip install -e .[dev]
pytest -q
```

Die Tests laufen ohne CUPS (Fake-Backend) und ohne echten Com-Server
(`tests/fake_comserver.py` spielt Mitschnitte ab).

## Lizenz

**GNU Lesser General Public License v2.1 (LGPL-2.1)** — Johannes Battlogg.
