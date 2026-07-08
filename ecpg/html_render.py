"""HTML→PDF-Rendering per Headless-Chromium (Playwright).

Für Leaflet-Karten-Druckaufträge: die Cloud liefert keine fertige PDF, sondern eine
signierte HTML-Seite (mit Leaflet + OSM-Tiles). Diese kann WeasyPrint (kein JavaScript)
nicht rendern, daher lädt das Gateway sie hier per Chromium, wartet bis die Karte fertig
ist (window.__ecpgReady) und erzeugt das PDF via page.pdf().

Die Seitengröße (A4/A3, Hoch-/Querformat) übernimmt Chromium aus dem @page-CSS der
Druckseite (prefer_css_page_size=True) – dieselbe Vorlage wie beim lokalen Druck.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("ecpg.html_render")


async def render_url_to_pdf(
    url: str,
    out_path: str,
    ready_timeout_ms: int = 9000,
    nav_timeout_ms: int = 30000,
) -> str:
    """Lädt `url` in Headless-Chromium, wartet auf window.__ecpgReady (Fallback: Timeout)
    und schreibt das PDF nach `out_path`. Gibt out_path zurück."""
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        # --no-sandbox: läuft im Container typischerweise als root ohne User-Namespaces.
        browser = await pw.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            page = await browser.new_page()
            await page.goto(url, wait_until="networkidle", timeout=nav_timeout_ms)
            try:
                await page.wait_for_function(
                    "window.__ecpgReady === true", timeout=ready_timeout_ms
                )
            except Exception:
                # Karte/Tiles nicht rechtzeitig fertig → trotzdem drucken (best effort).
                logger.warning("render_url_to_pdf: __ecpgReady-Timeout, drucke aktuellen Stand")
            await page.pdf(
                path=out_path,
                prefer_css_page_size=True,
                print_background=True,
            )
        finally:
            await browser.close()
    return out_path
