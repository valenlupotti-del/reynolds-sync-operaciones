from __future__ import annotations
import os
import logging
import requests

log = logging.getLogger(__name__)

TOKKO_KEY = os.environ["TOKKO_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TOKKO_BASE = "https://tokkobroker.com/api/v1/property/"
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def fetch_all_properties() -> list[dict]:
    props = []
    offset = 0
    limit = 50
    while True:
        resp = requests.get(
            TOKKO_BASE,
            params={"key": TOKKO_KEY, "limit": limit, "offset": offset, "format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        objects = data.get("objects", [])
        props.extend(objects)
        log.info("Tokko: fetched %d properties (offset %d)", len(objects), offset)
        if len(objects) < limit:
            break
        offset += limit
    return props


def parse_property(p: dict) -> dict:
    # Prices by operation type
    precio_venta = None
    precio_alquiler = None
    precio_alq_temp = None
    moneda = "USD"
    for op in p.get("operations", []):
        prices = op.get("prices", [])
        if prices:
            price = prices[0].get("price")
            currency = prices[0].get("currency", "USD")
            moneda = currency
            op_type = op.get("operation_type", "")
            if op_type == "Sale":
                precio_venta = price
            elif op_type == "Rent":
                precio_alquiler = price
            elif "Temporary" in op_type:
                precio_alq_temp = price

    # Location
    location = p.get("location") or {}
    full_loc = location.get("full_location", "")
    parts = [x.strip() for x in full_loc.split("|")]
    pais = parts[0] if len(parts) > 0 else None
    region = parts[1] if len(parts) > 1 else None
    barrio = parts[-1] if len(parts) > 2 else None

    producer = p.get("producer") or {}
    branch = p.get("branch") or {}
    prop_type = p.get("type") or {}

    activa = True  # API only returns active properties

    return {
        "ref": p.get("reference_code"),
        "pais": pais,
        "region": region,
        "barrio": barrio,
        "direccion": p.get("address"),
        "tipo_propiedad": prop_type.get("name"),
        "productor": producer.get("name"),
        "sucursal": branch.get("name"),
        "fecha_creacion": (p.get("created_at") or "")[:10] or None,
        "total_construido": float(p.get("total_surface") or 0) or None,
        "terreno": float(p.get("surface") or 0) or None,
        "cubierta": float(p.get("roofed_surface") or 0) or None,
        "semi_cubierta": float(p.get("semiroofed_surface") or 0) or None,
        "descubierta": float(p.get("unroofed_surface") or 0) or None,
        "frente": float(p.get("front_measure") or 0) or None,
        "fondo": float(p.get("depth_measure") or 0) or None,
        "ambientes": int(p.get("room_amount") or 0) or None,
        "dormitorios": int(p.get("suite_amount") or 0) or None,
        "banos": int(p.get("bathroom_amount") or 0) or None,
        "toilettes": int(p.get("toilet_amount") or 0) or None,
        "cocheras": int(p.get("parking_lot_amount") or 0) or None,
        "plantas": int(p.get("floors_amount") or 0) or None,
        "antiguedad": int(p.get("age") or 0) or None,
        "zonificacion": p.get("zonification"),
        "ocupacion": p.get("occupation"),
        "expensas": float(p.get("expenses") or 0) or None,
        "precio_venta": precio_venta,
        "precio_alquiler": precio_alquiler,
        "precio_alq_temp": precio_alq_temp,
        "moneda": moneda,
        "propietario_nombre": None,
        "propietario_email": None,
        "propietario_celular": None,
        "emprendimiento": (p.get("development") or {}).get("name") if p.get("development") else None,
        "activa": activa,
        "public_url": p.get("public_url"),
    }


def upsert_properties(rows: list[dict]):
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/properties",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=rows,
        timeout=30,
    )
    if not resp.ok:
        log.error("Supabase upsert error %s: %s", resp.status_code, resp.text)
    resp.raise_for_status()


def run():
    log.info("Starting properties sync...")
    try:
        properties = fetch_all_properties()
        log.info("Total fetched from Tokko: %d", len(properties))
        rows = [parse_property(p) for p in properties]
        # Filter out rows without ref
        rows = [r for r in rows if r.get("ref")]
        # Upsert in batches of 100
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            upsert_properties(batch)
            log.info("Upserted batch %d-%d", i, i + len(batch))
        log.info("Properties sync complete: %d properties", len(rows))

        # Mark as inactive any property no longer in Tokko
        active_refs = [r["ref"] for r in rows]
        deactivate_resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/deactivate_missing_properties",
            headers={**SUPABASE_HEADERS, "Content-Type": "application/json"},
            json={"active_refs": active_refs},
            timeout=15,
        )
        if deactivate_resp.ok:
            log.info("Deactivated missing properties")
        else:
            # Fallback: direct update
            refs_str = '("' + '","'.join(active_refs) + '")'
            requests.patch(
                f"{SUPABASE_URL}/rest/v1/properties?ref=not.in.{refs_str}",
                headers={**SUPABASE_HEADERS, "Content-Type": "application/json"},
                json={"activa": False},
                timeout=15,
            )
            log.info("Marked missing properties as inactive")
    except Exception:
        log.exception("Properties sync failed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
