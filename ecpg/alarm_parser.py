"""Pluggable Alarm-Parser.

Interface: parse(raw_text) -> dict | None. Der konkrete Parser (RFL Vorarlberg)
liest sein Regex-Set aus der Cloud-Config (parser_config.regex_set), damit
Anpassungen ohne Container-Update möglich sind. Fällt kein Feld, wird der Rohtext
trotzdem an die Cloud gemeldet (parse_failed) – nie einen Alarm verschlucken.

Ergebnis-Felder (alle optional außer report_text):
  alarm_type_code, reason, street, house_no, city, started_at, is_exercise
"""
from __future__ import annotations

import re

# Default-Regex-Set (überschreibbar via parser_config.regex_set aus der Cloud).
# Labels sind an den Zeilenanfang gebunden (^, MULTILINE), damit z. B. „Ort" nicht
# innerhalb von „Stichwort" greift.
DEFAULT_REGEX = {
    "einsatznummer": r"^\s*(?:Einsatz(?:nummer)?|E-Nr\.?)\b[\s:]*([A-Z0-9\-/]+)",
    "stichwort": r"^\s*(?:Stichwort|Alarmstichwort|Einsatzart)\b[\s:]*([^\n\r]+)",
    "adresse": r"^\s*(?:Adresse|Einsatzort|Ort)\b[\s:]*([^\n\r]+)",
    "bemerkung": r"^\s*(?:Bemerkung|Zusatz|Hinweis)\b[\s:]*([^\n\r]+)",
}

# Adresse „Straße 12, 6922 Wolfurt" grob zerlegen
_ADDR = re.compile(r"^(?P<street>.+?)\s+(?P<no>\d+\w?)\s*,?\s*(?:(?P<plz>\d{4,5})\s+)?(?P<city>[A-Za-zÄÖÜäöüß .\-]+)?$")


class AlarmParser:
    def parse(self, raw_text: str) -> dict | None:  # pragma: no cover - Interface
        raise NotImplementedError


class RflVbgParser(AlarmParser):
    def __init__(self, config: dict | None = None):
        cfg = config or {}
        regex = dict(DEFAULT_REGEX)
        regex.update(cfg.get("regex_set") or {})
        self._re = {k: re.compile(v, re.IGNORECASE | re.MULTILINE) for k, v in regex.items()}

    def parse(self, raw_text: str) -> dict | None:
        if not raw_text or not raw_text.strip():
            return None
        out: dict = {"report_text": raw_text}
        found = False

        m = self._re["stichwort"].search(raw_text) if "stichwort" in self._re else None
        if m:
            stichwort = m.group(1).strip()
            out["reason"] = stichwort
            out["alarm_type_code"] = _stichwort_to_code(stichwort)
            found = True

        m = self._re["adresse"].search(raw_text) if "adresse" in self._re else None
        if m:
            street, no, city = _split_address(m.group(1).strip())
            out["street"], out["house_no"], out["city"] = street, no, city
            found = True

        out["is_exercise"] = bool(re.search(r"\b(übung|uebung|probe)\b", raw_text, re.IGNORECASE))
        return out if found else None


def _stichwort_to_code(stichwort: str) -> str:
    s = stichwort.upper()
    if s.startswith("B") or "BRAND" in s:
        return "BRAND"
    if s.startswith("T") or "TECHN" in s:
        return "TECHNISCH"
    if "BMA" in s or "BRANDMELDE" in s:
        return "BMA"
    return "SONSTIGE"


def _split_address(text: str) -> tuple[str | None, str | None, str | None]:
    m = _ADDR.match(text)
    if not m:
        return text or None, None, None
    return (m.group("street") or None, m.group("no") or None, (m.group("city") or "").strip() or None)


def get_parser(parser_config: dict | None) -> AlarmParser:
    """Fabrik – aktuell nur RFL Vorarlberg; erweiterbar über parser_config.parser."""
    cfg = parser_config or {}
    name = cfg.get("parser", "rfl_vbg")
    if name == "rfl_vbg":
        return RflVbgParser(cfg)
    return RflVbgParser(cfg)
