"""Parser-Tests (RFL Vorarlberg) gegen anonymisierte Beispiel-Mitschnitte."""
from ecpg.alarm_parser import RflVbgParser, get_parser

SAMPLE = """\
Einsatznummer: 2026-04711
Stichwort: B3 Zimmerbrand
Adresse: Kirchstrasse 12, 6922 Wolfurt
Bemerkung: Rauch aus Fenster, Person vermisst
"""


def test_parse_basic_fields():
    p = RflVbgParser()
    r = p.parse(SAMPLE)
    assert r is not None
    assert r["alarm_type_code"] == "BRAND"
    assert "Zimmerbrand" in r["reason"]
    assert r["street"].startswith("Kirchstrasse")
    assert r["house_no"] == "12"
    assert "Wolfurt" in r["city"]
    assert r["is_exercise"] is False


def test_parse_uebung_flag():
    text = "Stichwort: T1 Übung\nAdresse: Hauptstrasse 1, 6922 Wolfurt"
    r = RflVbgParser().parse(text)
    assert r["is_exercise"] is True
    assert r["alarm_type_code"] == "TECHNISCH"


def test_parse_empty_returns_none():
    assert RflVbgParser().parse("   ") is None


def test_parse_unstructured_returns_none():
    # Kein erkennbares Feld → None (Cloud legt „unklassifiziert" an)
    assert RflVbgParser().parse("xxxxx yyyyy zzzz") is None


def test_regex_override_from_config():
    cfg = {"regex_set": {"stichwort": r"KW[\s:]*([^\n]+)"}}
    p = get_parser({"parser": "rfl_vbg", **cfg})
    r = p.parse("KW: B2 Kleinbrand\nAdresse: Weg 3, 6900 Bregenz")
    assert r["reason"].startswith("B2")
