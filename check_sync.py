"""
Check which Tokko contacts (created since Jan 2026) are missing from Supabase.
"""
from __future__ import annotations
import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKKO_KEY    = os.environ["TOKKO_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

SINCE = "2026-01-01"


def fetch_tokko_ids(since: str) -> set[int]:
    ids = set()
    offset = 0
    while True:
        resp = requests.get(
            "https://www.tokkobroker.com/api/v1/contact/",
            params={"key": TOKKO_KEY, "format": "json", "limit": 100,
                    "offset": offset, "created_since": since},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        contacts = data.get("objects", [])
        if not contacts:
            break
        for c in contacts:
            ids.add(c["id"])
        print(f"  Tokko fetched {len(ids)} so far (offset {offset})...")
        if len(contacts) < 100:
            break
        offset += 100
    return ids


def fetch_supabase_ids() -> set[int]:
    ids = set()
    offset = 0
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/contacts",
            headers={**SUPABASE_HEADERS, "Range": f"{offset}-{offset+999}"},
            params={"select": "id"},
            timeout=30,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for r in rows:
            ids.add(r["id"])
        print(f"  Supabase fetched {len(ids)} so far...")
        if len(rows) < 1000:
            break
        offset += 1000
    return ids


print(f"Fetching Tokko contacts updated since {SINCE}...")
tokko_ids = fetch_tokko_ids(SINCE)
print(f"Tokko total: {len(tokko_ids)}")

print("Fetching Supabase contact IDs...")
supabase_ids = fetch_supabase_ids()
print(f"Supabase total: {len(supabase_ids)}")

missing = tokko_ids - supabase_ids
print(f"\nMissing from Supabase: {len(missing)}")
if missing:
    print("First 20 missing IDs:", sorted(missing)[:20])
else:
    print("All Tokko contacts are in Supabase!")
