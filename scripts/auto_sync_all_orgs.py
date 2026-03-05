#!/usr/bin/env python

import os
import sys
import asyncio
import sqlite3
import httpx

# Base URL of your running FastAPI app on the client EC2
# Can be overridden with DEERE_CONNECTOR_API_URL env var if needed.
API_BASE_URL = os.environ.get("DEERE_CONNECTOR_API_URL", "http://127.0.0.1:8000")


# Farmer id (JDOC user id/email) that should be synced.
# You MUST set DEERE_FARMER_ID in the environment to one of:
# klee@agricapture.com, leaf@agricapture.com, test2@agricapture.com,
# test@agricapture.com, wmacey2@agricapture.com, wmacey@agricapture.com
def get_default_farmer_id() -> str:
    farmer_id = os.environ.get("DEERE_FARMER_ID")
    if not farmer_id:
        raise RuntimeError(
            "DEERE_FARMER_ID is not set. Export it to one of the valid JDOC users "
            "before running this script."
        )
    return farmer_id


def get_db_path() -> str:
    """Locate the SQLite DB used by the app."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "agricapture.db")


def get_all_farmers() -> list[str]:
    """Read all farmer_ids (user_ids) that have tokens."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT user_id FROM user_tokens")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]

def get_org_ids_for_farmer(farmer_id: str) -> list[str]:
    """
    Read all org_ids for this farmer from organizations.
    If organizations table is keyed per farmer_id, filter on it.
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # organizations(org_id PRIMARY KEY, farmer_id TEXT NOT NULL, ...)
    cursor.execute(
        "SELECT DISTINCT org_id FROM organizations WHERE farmer_id = ?",
        (farmer_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]



async def sync_org(farmer_id: str, org_id: str) -> None:
    """Call the existing /admin/sync/farmer endpoint for a single org."""
    url = f"{API_BASE_URL}/admin/sync/farmer"
    params = {
        "farmer_id": farmer_id,
        "org_id": org_id,
    }
    async with httpx.AsyncClient(timeout=900.0) as client:
        print(f"[SYNC] Calling {url} with farmer_id={farmer_id}, org_id={org_id}")
        resp = await client.post(url, params=params)
        resp.raise_for_status()
        print(f"[SYNC] DONE farmer_id={farmer_id}, org_id={org_id}: {resp.json()}")


async def main():
    farmers = get_all_farmers()
    if not farmers:
        print("[SYNC] No farmers found in user_tokens; nothing to sync.")
        return

    print(f"[SYNC] Starting auto sync for farmers={farmers}")

    for farmer_id in farmers:
        org_ids = get_org_ids_for_farmer(farmer_id)
        if not org_ids:
            print(f"[SYNC] No organizations found for farmer_id={farmer_id}; skipping.")
            continue

        print(f"[SYNC] Farmer {farmer_id}: org_ids={org_ids}")
        for oid in org_ids:
            await sync_org(farmer_id, oid)

    print("[SYNC] Auto sync complete for all farmers.")


if __name__ == "__main__":
    asyncio.run(main())
