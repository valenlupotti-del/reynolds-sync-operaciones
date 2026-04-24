from __future__ import annotations
import os
import time
import logging
from datetime import datetime, timedelta
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOKKO_KEY   = os.environ["TOKKO_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BREVO_KEY   = os.environ["BREVO_KEY"]
BREVO_LIST_ID = int(os.environ.get("BREVO_LIST_ID", "2"))

LAST_SYNC_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "last_sync.txt")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

BREVO_HEADERS = {
    "api-key": BREVO_KEY,
    "Content-Type": "application/json",
}

BREVO_BATCH_SIZE = 150   # Brevo max per import request
SUPABASE_BATCH_SIZE = 200
BREVO_RATE_DELAY = 1.0   # seconds between Brevo batch requests


def get_last_sync_date() -> str:
    if os.path.exists(LAST_SYNC_FILE):
        with open(LAST_SYNC_FILE) as f:
            return f.read().strip()
    # Default: sync everything from 10 years ago on first run
    return (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")


def save_last_sync_date():
    with open(LAST_SYNC_FILE, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


def fetch_tokko_contacts(updated_since: str, offset: int = 0) -> dict:
    resp = requests.get(
        "https://www.tokkobroker.com/api/v1/contact/",
        params={"key": TOKKO_KEY, "format": "json", "limit": 50, "offset": offset, "updated_since": updated_since},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def map_contact(c: dict) -> dict:
    origen_tag = next(
        (t for t in (c.get("tags") or []) if t.get("group_name") == "Origen Del Contacto"),
        None,
    )
    return {
        "id":                  c["id"],
        "nombre":              c.get("name", ""),
        "email":               c.get("email") or None,
        "celular":             c.get("cellphone", "").strip() if c.get("cellphone") else None,
        "telefono":            c.get("phone", "").strip() if c.get("phone") else None,
        "empresa":             c.get("work_name") or None,
        "agente":              c["agent"]["name"] if c.get("agent") else None,
        "estado_oportunidad":  c.get("lead_status") or None,
        "cliente_de":          "Propietario" if c.get("is_owner") else ("Empresa" if c.get("is_company") else "Cliente"),
        "origen_contacto":     origen_tag["name"] if origen_tag else None,
        "fecha_creacion":      c["created_at"].split("T")[0] if c.get("created_at") else None,
        "ultima_actualizacion": datetime.now().isoformat(),
    }


def upsert_supabase_batch(contacts: list[dict]):
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/contacts?on_conflict=id",
        headers=SUPABASE_HEADERS,
        json=contacts,
        timeout=30,
    )
    if not resp.ok:
        log.error("Supabase upsert error %s: %s", resp.status_code, resp.text[:200])
    resp.raise_for_status()


def upsert_brevo_batch(contacts: list[dict]):
    """
    Import up to 150 contacts at once using Brevo's bulk import endpoint.
    Only contacts with email are included.
    """
    with_email = [c for c in contacts if c.get("email")]
    if not with_email:
        return

    brevo_contacts = [
        {
            "email": c["email"],
            "attributes": {
                "NOMBRE": c.get("nombre") or "",
                "SMS":    c.get("celular") or "",
            },
        }
        for c in with_email
    ]

    payload = {
        "listIds":       [BREVO_LIST_ID],
        "jsonBody":      brevo_contacts,
        "updateEnabled": True,
    }

    for attempt in range(3):
        resp = requests.post(
            "https://api.brevo.com/v3/contacts/import",
            headers=BREVO_HEADERS,
            json=payload,
            timeout=60,
        )
        if resp.status_code == 429:
            wait = 10 * (attempt + 1)
            log.warning("Brevo rate limit, waiting %ds...", wait)
            time.sleep(wait)
            continue
        if not resp.ok:
            log.error("Brevo import error %s: %s", resp.status_code, resp.text[:200])
        break

    time.sleep(BREVO_RATE_DELAY)


def run():
    last_sync = get_last_sync_date()
    log.info("Syncing contacts updated since: %s", last_sync)

    offset = 0
    total = 0
    supabase_batch: list[dict] = []
    brevo_batch: list[dict] = []

    while True:
        try:
            data = fetch_tokko_contacts(last_sync, offset)
        except Exception as e:
            log.error("Tokko fetch error at offset %d: %s", offset, e)
            break

        raw_contacts = data.get("objects", [])
        if not raw_contacts:
            break

        for c in raw_contacts:
            mapped = map_contact(c)
            supabase_batch.append(mapped)
            brevo_batch.append(mapped)
            total += 1

            # Flush Supabase batch
            if len(supabase_batch) >= SUPABASE_BATCH_SIZE:
                try:
                    upsert_supabase_batch(supabase_batch)
                    log.info("Supabase: upserted %d contacts (total %d)", len(supabase_batch), total)
                except Exception as e:
                    log.error("Supabase batch error: %s", e)
                supabase_batch = []

            # Flush Brevo batch
            if len(brevo_batch) >= BREVO_BATCH_SIZE:
                try:
                    upsert_brevo_batch(brevo_batch)
                    log.info("Brevo: imported %d contacts (total %d)", len(brevo_batch), total)
                except Exception as e:
                    log.error("Brevo batch error: %s", e)
                brevo_batch = []

        if len(raw_contacts) < 50:
            break

        offset += 50
        log.info("Fetched %d contacts so far...", total)

    # Flush remaining
    if supabase_batch:
        try:
            upsert_supabase_batch(supabase_batch)
            log.info("Supabase: upserted final %d contacts", len(supabase_batch))
        except Exception as e:
            log.error("Supabase final batch error: %s", e)

    if brevo_batch:
        try:
            upsert_brevo_batch(brevo_batch)
            log.info("Brevo: imported final %d contacts", len(brevo_batch))
        except Exception as e:
            log.error("Brevo final batch error: %s", e)

    save_last_sync_date()
    log.info("Sync complete. Total contacts processed: %d", total)


if __name__ == "__main__":
    run()
