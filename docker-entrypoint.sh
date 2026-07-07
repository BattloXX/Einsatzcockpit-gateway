#!/usr/bin/env bash
set -euo pipefail

# CUPS-Daemon im Hintergrund starten (lokaler Druck-Backend)
if command -v cupsd >/dev/null 2>&1; then
    cupsd || echo "cupsd konnte nicht gestartet werden (Fake-Backend greift)"
fi

# Avahi für mDNS-Discovery (best effort)
if command -v avahi-daemon >/dev/null 2>&1; then
    avahi-daemon --daemonize --no-drop-root >/dev/null 2>&1 || true
fi

exec python -m ecpg.main
