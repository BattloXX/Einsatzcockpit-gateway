"""Automatische Versionierung: ENV ECPG_VERSION hat Vorrang, sonst Paket-Metadaten."""
from ecpg import _resolve_version


def test_env_version_wins(monkeypatch):
    monkeypatch.setenv("ECPG_VERSION", "0.1.42")
    assert _resolve_version() == "0.1.42"


def test_env_version_stripped(monkeypatch):
    monkeypatch.setenv("ECPG_VERSION", "  1.2.3\n")
    assert _resolve_version() == "1.2.3"


def test_fallback_without_env(monkeypatch):
    monkeypatch.delenv("ECPG_VERSION", raising=False)
    v = _resolve_version()
    # Paket-Metadaten (statische pyproject-Version) oder Dev-Fallback.
    assert isinstance(v, str) and v
