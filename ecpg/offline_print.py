"""Offline-Notdruck: rendert den Alarm-Rohtext lokal als einfaches PDF (reportlab),
wenn die Cloud nicht erreichbar ist. Wird an den in der Config hinterlegten
Notfalldrucker geschickt."""
from __future__ import annotations

import io
from datetime import datetime


def render_alarm_pdf(raw_text: str, received_at: datetime | None = None) -> bytes:
    """Erzeugt ein A4-PDF mit dem Alarm-Rohtext (Monospace, umbruchsicher)."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    x, y = 18 * mm, height - 22 * mm

    c.setFont("Helvetica-Bold", 16)
    c.drawString(x, y, "ALARM - Notdruck (offline)")
    y -= 8 * mm
    c.setFont("Helvetica", 9)
    ts = (received_at or datetime.utcnow()).strftime("%d.%m.%Y %H:%M:%S")
    c.drawString(x, y, f"Empfangen: {ts} (UTC) - Cloud nicht erreichbar")
    y -= 8 * mm

    c.setFont("Courier", 10)
    for raw_line in (raw_text or "").splitlines() or [""]:
        for chunk in _wrap(raw_line, 88):
            if y < 20 * mm:
                c.showPage()
                c.setFont("Courier", 10)
                y = height - 22 * mm
            c.drawString(x, y, chunk)
            y -= 5 * mm

    c.showPage()
    c.save()
    return buf.getvalue()


def _wrap(text: str, width: int) -> list[str]:
    if not text:
        return [""]
    return [text[i:i + width] for i in range(0, len(text), width)]
