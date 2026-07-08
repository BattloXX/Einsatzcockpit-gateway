"""Einsatzcockpit Print & Alarm Gateway (ECPG)."""
import os


def _resolve_version() -> str:
    """Laufzeit-Version. Priorität:
    1. ENV ``ECPG_VERSION`` – wird im Image zur Build-Zeit injiziert (CI-Version).
    2. Installierte Paket-Metadaten (statische pyproject-Version).
    3. Entwicklungs-Fallback.
    """
    env = os.getenv("ECPG_VERSION")
    if env:
        return env.strip()
    try:
        from importlib.metadata import version
        return version("einsatzcockpit-gateway")
    except Exception:
        return "0.0.0+dev"


__version__ = _resolve_version()
