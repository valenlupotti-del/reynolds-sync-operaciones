"""
Argenprop scraper for property valuation (tasacion).

Usage:
    python tasacion.py "Palermo" "departamento" "venta"
    python tasacion.py "San Isidro" "casa" "venta" 85

Returns market stats (avg/median/min/max USD per m2) from Argenprop listings.
"""
from __future__ import annotations
import sys
import re
import json
import logging
import statistics
import unicodedata
import requests

log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

TIPO_SLUG = {
    "departamento": "departamento",
    "depto":        "departamento",
    "casa":         "casa",
    "ph":           "ph",
    "local":        "local-comercial",
    "oficina":      "oficina",
    "terreno":      "terreno",
    "lote":         "terreno",
}

OPERACION_SLUG = {
    "venta":    "en-venta",
    "alquiler": "en-alquiler",
    "alq":      "en-alquiler",
    "rent":     "en-alquiler",
}


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return text


def build_url(tipo: str, operacion: str, barrio: str, page: int = 1) -> str:
    t = TIPO_SLUG.get(tipo.lower(), slugify(tipo))
    op = OPERACION_SLUG.get(operacion.lower(), "en-venta")
    b = slugify(barrio)
    base = f"https://www.argenprop.com/{t}-{op}-en-{b}"
    if page > 1:
        base += f"--pagina-{page}"
    return base


def fetch_page(url: str) -> str | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code == 403:
            log.error("Blocked by Cloudflare: %s", url)
            return None
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        log.error("Fetch error for %s: %s", url, e)
        return None


def extract_listings_from_html(html: str) -> list[dict]:
    """
    Argenprop-specific extraction.

    Each listing card wraps its price in <div class="card__monetary-values">.
    Splitting on that boundary ensures price + surface belong to the same card.
    Surface appears as HTML entity: 90 m&#xB2; (= m squared).
    """
    listings = []

    parts = html.split("card__monetary-values")

    for part in parts[1:]:  # first part is CSS/header
        # Price: <span class="card__currency">USD</span> 350.000
        price_m = re.search(
            r'<span class="card__currency">USD</span>\s*([\d\.\,]+)',
            part,
        )
        if not price_m:
            continue

        raw_price = price_m.group(1).replace(".", "").replace(",", "")
        try:
            price = float(raw_price)
        except ValueError:
            continue

        if not (5_000 < price < 50_000_000):
            continue

        # Surface: HTML-encoded m2 entity (&#xB2;) or plain m2/m²
        surf_m = re.search(
            r'(\d+(?:[,\.]\d+)?)\s*(?:m&#xB2;|m[2\xb2])',
            part,
            re.IGNORECASE,
        )
        if not surf_m:
            continue

        raw_surf = surf_m.group(1).replace(",", ".")
        try:
            surface = float(raw_surf)
        except ValueError:
            continue

        if not (12 < surface < 3000):
            continue

        listings.append({"price": price, "surface": surface, "source": "argenprop"})

    return listings


def scrape_barrio(tipo: str, operacion: str, barrio: str, max_pages: int = 3) -> list[dict]:
    all_listings = []
    for page in range(1, max_pages + 1):
        url = build_url(tipo, operacion, barrio, page)
        log.info("Fetching page %d: %s", page, url)
        html = fetch_page(url)
        if not html:
            break
        page_listings = extract_listings_from_html(html)
        if not page_listings:
            if "sin resultados" in html.lower() or "no encontramos" in html.lower():
                break
            if page > 1:
                break
        all_listings.extend(page_listings)
        log.info("Page %d: %d listings (total: %d)", page, len(page_listings), len(all_listings))
    return all_listings


def compute_stats(listings: list[dict]) -> dict | None:
    ratios = []
    for listing in listings:
        price = listing.get("price")
        surface = listing.get("surface")
        if price and surface and price > 0 and surface > 0:
            ratio = price / surface
            if 300 < ratio < 20_000:
                ratios.append(ratio)

    if not ratios:
        return None

    return {
        "count": len(ratios),
        "promedio_usd_m2": round(statistics.mean(ratios)),
        "mediana_usd_m2": round(statistics.median(ratios)),
        "minimo_usd_m2": round(min(ratios)),
        "maximo_usd_m2": round(max(ratios)),
        "desvio_usd_m2": round(statistics.stdev(ratios)) if len(ratios) > 1 else 0,
    }


def tasacion(
    barrio: str,
    tipo: str = "departamento",
    operacion: str = "venta",
    superficie: float | None = None,
) -> dict:
    """
    Main entry point.

    Args:
        barrio: neighborhood name (e.g. "Palermo", "San Isidro")
        tipo: property type (departamento, casa, ph, etc.)
        operacion: venta or alquiler
        superficie: optional m2 to calculate estimated value

    Returns dict with keys: barrio, tipo, operacion, stats, estimado (if superficie given)
    """
    listings = scrape_barrio(tipo, operacion, barrio, max_pages=3)
    stats = compute_stats(listings)

    result: dict = {
        "barrio": barrio,
        "tipo": tipo,
        "operacion": operacion,
        "listings_analizados": len(listings),
    }

    if stats:
        result["stats"] = stats
        if superficie and superficie > 0:
            avg = stats["promedio_usd_m2"]
            med = stats["mediana_usd_m2"]
            result["estimado"] = {
                "superficie_m2": superficie,
                "valor_por_promedio": round(avg * superficie),
                "valor_por_mediana": round(med * superficie),
                "rango_min": round(stats["minimo_usd_m2"] * superficie),
                "rango_max": round(stats["maximo_usd_m2"] * superficie),
            }
    else:
        result["error"] = "No se pudieron obtener datos suficientes de Argenprop"

    return result


def format_report(r: dict) -> str:
    lines = [
        f"TASACION -- {r['tipo'].upper()} EN {r['operacion'].upper()}",
        f"Barrio: {r['barrio']}",
        f"Propiedades analizadas: {r['listings_analizados']}",
        "",
    ]
    stats = r.get("stats")
    if stats:
        lines += [
            "=== PRECIO POR m2 (USD) ===",
            f"  Promedio:  USD {stats['promedio_usd_m2']:,}/m2",
            f"  Mediana:   USD {stats['mediana_usd_m2']:,}/m2",
            f"  Minimo:    USD {stats['minimo_usd_m2']:,}/m2",
            f"  Maximo:    USD {stats['maximo_usd_m2']:,}/m2",
        ]
        est = r.get("estimado")
        if est:
            lines += [
                "",
                f"=== ESTIMACION PARA {est['superficie_m2']} m2 ===",
                f"  Por promedio: USD {est['valor_por_promedio']:,}",
                f"  Por mediana:  USD {est['valor_por_mediana']:,}",
                f"  Rango:        USD {est['rango_min']:,} -- USD {est['rango_max']:,}",
            ]
    else:
        lines.append(r.get("error", "Sin datos"))
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    barrio    = sys.argv[1] if len(sys.argv) > 1 else "Palermo"
    tipo      = sys.argv[2] if len(sys.argv) > 2 else "departamento"
    operacion = sys.argv[3] if len(sys.argv) > 3 else "venta"
    superficie = float(sys.argv[4]) if len(sys.argv) > 4 else None

    result = tasacion(barrio, tipo, operacion, superficie)
    print(format_report(result))
    print("\nJSON:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
