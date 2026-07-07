"""Konfiguration aus Umgebungsvariablen."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


@dataclass
class Settings:
    cloud_url: str = _env("ECPG_CLOUD_URL", "http://localhost:8092")
    pairing_code: str = _env("ECPG_PAIRING_CODE", "")
    data_dir: str = _env("ECPG_DATA_DIR", "/data")
    status_port: int = int(_env("ECPG_STATUS_PORT", "8631"))
    tz: str = _env("TZ", "Europe/Vienna")

    # Reconnect-Backoff
    reconnect_min_s: float = 1.0
    reconnect_max_s: float = 60.0
    heartbeat_s: float = 30.0

    @property
    def ws_url(self) -> str:
        base = self.cloud_url.rstrip("/")
        if base.startswith("https://"):
            return "wss://" + base[len("https://"):] + "/ws/gateway"
        if base.startswith("http://"):
            return "ws://" + base[len("http://"):] + "/ws/gateway"
        return base + "/ws/gateway"

    @property
    def pair_url(self) -> str:
        return self.cloud_url.rstrip("/") + "/api/v1/gateway/pair"

    @property
    def alarms_url(self) -> str:
        return self.cloud_url.rstrip("/") + "/api/v1/gateway/alarms"

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "gateway.db")


settings = Settings()
