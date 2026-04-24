import requests
import json
import os
from datetime import datetime, timedelta

TOKKO_KEY = os.environ["TOKKO_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BREVO_KEY = os.environ["BREVO_KEY"]
BREVO_LIST_NAME = "Reynolds Propiedades"
LAST_SYNC_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "last_sync.txt")


def get_last_sync_date():
    if os.path.exists(LAST_SYNC_FILE):
        with open(LAST_SYNC_FILE, "r") as f:
            return f.read().strip()
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def save_last_sync_date():
    os.makedirs(os.path.dirname(LAST_SYNC_FILE), exist_ok=True)
    with open(LAST_SYNC_FILE, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


def get_brevo_list_id():
    headers = {"api-key": BREVO_KEY}
    resp = requests.get("https://api.brevo.com/v3/contacts/lists?limit=50", headers=headers)
    for lst in resp.json().get("lists", []):
        if lst["name"] == BREVO_LIST_NAME:
            return lst["id"]
    return None


def fetch_tokko_contacts(created_since, offset=0):
    params = {
        "key": TOKKO_KEY,
        "format": "json",
        "limit": 50,
        "offset": offset,
        "updated_since": created_since,
    }
    resp = requests.get("https://www.tokkobroker.com/api/v1/contact/", params=params)
    return resp.json()


def upsert_supabase(contact):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    requests.post(
        f"{SUPABASE_URL}/rest/v1/contacts?on_conflict=id",
        headers=headers,
        json=contact,
    )


def add_to_brevo(contact, list_id):
    if not contact.get("email"):
        return
    headers = {"api-key": BREVO_KEY, "Content-Type": "application/json"}
    data = {
        "email": contact["email"],
        "attributes": {
            "NOMBRE": contact.get("nombre", ""),
            "SMS": contact.get("celular", ""),
        },
        "listIds": [list_id],
        "updateEnabled": True,
    }
    requests.post("https://api.brevo.com/v3/contacts", headers=headers, json=data)


def map_contact(c):
    origen_tag = next(
        (t for t in (c.get("tags") or []) if t.get("group_name") == "Origen Del Contacto"),
        None,
    )
    return {
        "id": c["id"],
        "nombre": c.get("name", ""),
        "email": c.get("email") or None,
        "celular": c.get("cellphone", "").strip() if c.get("cellphone") else None,
        "telefono": c.get("phone", "").strip() if c.get("phone") else None,
        "empresa": c.get("work_name") or None,
        "agente": c["agent"]["name"] if c.get("agent") else None,
        "estado_oportunidad": c.get("lead_status") or None,
        "cliente_de": "Propietario" if c.get("is_owner") else ("Empresa" if c.get("is_company") else "Cliente"),
        "origen_contacto": origen_tag["name"] if origen_tag else None,
        "fecha_creacion": c["created_at"].split("T")[0] if c.get("created_at") else None,
        "created_at": datetime.now().isoformat(),
    }


def main():
    last_sync = get_last_sync_date()
    print(f"Sincronizando contactos creados desde: {last_sync}")

    list_id = get_brevo_list_id() or 2
    print(f"Brevo list ID: {list_id}")

    offset = 0
    total = 0

    while True:
        data = fetch_tokko_contacts(last_sync, offset)
        contacts = data.get("objects", [])

        if not contacts:
            break

        for c in contacts:
            contact = map_contact(c)
            upsert_supabase(contact)
            if list_id:
                add_to_brevo(contact, list_id)
            total += 1

        print(f"  Procesados: {total}")

        if len(contacts) < 50:
            break

        offset += 50

    print(f"Listo. Total contactos procesados: {total}")
    save_last_sync_date()


if __name__ == "__main__":
    main()
