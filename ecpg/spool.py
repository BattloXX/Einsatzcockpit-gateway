"""Lokaler SQLite-Speicher: Job-Spool, Alarm-Ringpuffer, Config-Cache, Key/Value.

Bewusst synchron (sqlite3) mit Thread-Lock – die Operationen sind klein und
schnell; das vermeidet eine zusätzliche Async-DB-Abhängigkeit. Überlebt Neustarts.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

RING_MAX = 500

_SCHEMA = """
CREATE TABLE IF NOT EXISTS spool_jobs (
    job_id        TEXT PRIMARY KEY,
    printer_uri   TEXT,
    printer_id    INTEGER,
    document_type TEXT,
    artifact_url  TEXT,
    options_json  TEXT,
    pdf_path      TEXT,
    cups_job      INTEGER,
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    error         TEXT,
    created_at    REAL,
    updated_at    REAL
);
CREATE TABLE IF NOT EXISTS raw_alarms_ring (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at REAL,
    raw_bytes   BLOB,
    charset     TEXT,
    raw_hash    TEXT,
    forwarded   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT
);
"""


class Spool:
    def __init__(self, db_path: str):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            # Migration für bestehende DBs: cups_job-Spalte nachrüsten (persistiert die
            # CUPS-Job-ID, damit ein 'printing'-Job NICHT bei jedem Durchlauf erneut an
            # CUPS übergeben wird → verhindert Endlosdruck).
            try:
                self._conn.execute("ALTER TABLE spool_jobs ADD COLUMN cups_job INTEGER")
            except sqlite3.OperationalError:
                pass  # Spalte existiert bereits
            self._conn.commit()

    # ── Key/Value ────────────────────────────────────────────────────────────
    def get(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            return row["v"] if row else None

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (key, value),
            )
            self._conn.commit()

    # ── Config-Cache ─────────────────────────────────────────────────────────
    def save_config(self, config: dict) -> None:
        self.set("config_cache", json.dumps(config))

    def load_config(self) -> dict:
        raw = self.get("config_cache")
        return json.loads(raw) if raw else {}

    # ── Job-Spool ────────────────────────────────────────────────────────────
    def add_job(self, job: dict) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO spool_jobs
                   (job_id, printer_uri, printer_id, document_type, artifact_url,
                    options_json, status, attempts, next_retry_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?, 'pending', 0, ?, ?, ?)""",
                (
                    str(job["job_id"]), job.get("printer_uri"), job.get("printer_id"),
                    job.get("document_type"), job.get("artifact_url"),
                    json.dumps(job.get("options") or {}), now, now, now,
                ),
            )
            self._conn.commit()

    def due_jobs(self, now: float | None = None) -> list[dict]:
        now = now or time.time()
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM spool_jobs
                   WHERE status IN ('pending','downloading','printing')
                     AND (next_retry_at IS NULL OR next_retry_at <= ?)
                   ORDER BY created_at""",
                (now,),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._lock:
            self._conn.execute(
                f"UPDATE spool_jobs SET {cols} WHERE job_id=?",
                (*fields.values(), str(job_id)),
            )
            self._conn.commit()

    def get_job(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM spool_jobs WHERE job_id=?", (str(job_id),)
            ).fetchone()
            return dict(row) if row else None

    def recent_jobs(self, limit: int = 30) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM spool_jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def cleanup_done(self, older_than_s: float = 86400) -> None:
        """Löscht Spool-PDF-Pfade + Zeilen erledigter Jobs nach older_than_s."""
        cutoff = time.time() - older_than_s
        with self._lock:
            self._conn.execute(
                "DELETE FROM spool_jobs WHERE status IN ('done','failed','canceled') AND updated_at < ?",
                (cutoff,),
            )
            self._conn.commit()

    # ── Alarm-Ringpuffer ─────────────────────────────────────────────────────
    def add_raw_alarm(self, raw_bytes: bytes, charset: str, raw_hash: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO raw_alarms_ring (received_at, raw_bytes, charset, raw_hash, forwarded) "
                "VALUES (?,?,?,?,0)",
                (time.time(), raw_bytes, charset, raw_hash),
            )
            # Ring begrenzen
            self._conn.execute(
                "DELETE FROM raw_alarms_ring WHERE id NOT IN "
                "(SELECT id FROM raw_alarms_ring ORDER BY id DESC LIMIT ?)",
                (RING_MAX,),
            )
            self._conn.commit()
            return cur.lastrowid

    def mark_alarm_forwarded(self, alarm_id: int) -> None:
        with self._lock:
            self._conn.execute("UPDATE raw_alarms_ring SET forwarded=1 WHERE id=?", (alarm_id,))
            self._conn.commit()

    def pending_alarms(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM raw_alarms_ring WHERE forwarded=0 ORDER BY id"
            ).fetchall()
            return [dict(r) for r in rows]

    def recent_alarms(self, limit: int = 20) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, received_at, charset, raw_hash, forwarded, raw_bytes "
                "FROM raw_alarms_ring ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
