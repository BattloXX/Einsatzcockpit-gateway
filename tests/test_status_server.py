"""Statusseite (render_index): Kacheln, Drucker, Jobs mit Abbrechen, Pairing."""
from ecpg import __version__
from ecpg.status_server import render_index


class _Spool:
    def __init__(self, token=None):
        self._token = token

    def get(self, k):
        return self._token if k == "device_token" else None

    def recent_alarms(self, n):
        return []

    def recent_jobs(self, n):
        return [
            {"job_id": "1", "document_type": "einsatzinfo", "status": "printing", "error": None},
            {"job_id": "2", "document_type": "objektblatt", "status": "done", "error": None},
        ]


class _Conn:
    def __init__(self, connected):
        self.connected = connected


class _Agent:
    def __init__(self, token=None):
        self.cloud = _Conn(True)
        self.serial = _Conn(False)
        self.spool = _Spool(token)
        self.config = {"printers": [{"id": 1, "name": "MFG-Büro", "uri": "ipps://10.10.150.24/ipp/print"}]}


def test_render_basic_paired():
    h = render_index(_Agent(token="tok"))
    assert "ECPG Print Gateway" in h
    assert f"v{__version__}" in h
    assert "verbunden" in h            # Cloud-Kachel
    assert "MFG-Büro" in h             # Drucker
    assert "Kopplung" not in h         # gekoppelt → kein Pairing-Form


def test_cancel_button_only_for_nonterminal_jobs():
    h = render_index(_Agent(token="tok"))
    # printing-Job #1 → Abbrechen-Form; done-Job #2 → nicht
    assert '/jobs/1/cancel' in h
    assert '/jobs/2/cancel' not in h
    assert "✖ Abbrechen" in h


def test_pairing_form_when_unpaired():
    h = render_index(_Agent(token=None))
    assert "Kopplung" in h
    assert 'action="/pair"' in h
