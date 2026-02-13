#!/usr/bin/env python

import os
import sys
import asyncio
import sqlite3
import httpx

# Base URL of your running FastAPI app
API_BASE_URL = os.environ.get("DEERE_CONNECTOR_API_URL", "http://34.200.216.123:8000")

# Farmer id used with JDOC (update if your app uses multiple farmers)
DEFAULT_FARMER_ID = os.environ.get("DEERE_FARMER_ID", "anonymous")


def get_db_path() -> str:
    """
    Locate the SQLite DB used by the app.
    Adjust if your path is different.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "agricapture.db")


def get_all_org_ids() -> list[str]:
    """
    Read all org_ids from the organizations table.
    """
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT org_id FROM organizations")
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]


async def sync_org(farmer_id: str, org_id: str) -> None:
    """
    Call the existing /admin/sync/farmer endpoint for a single org.
    """
    url = f"{API_BASE_URL}/admin/sync/farmer"
    params = {
        "farmer_id": farmer_id,
        "org_id": org_id,
        # Optionally add start_date/end_date if you want a window
    }
    async with httpx.AsyncClient(timeout=600.0) as client:
        print(f"[SYNC] Calling {url} with farmer_id={farmer_id}, org_id={org_id}")
        resp = await client.post(url, params=params)
        resp.raise_for_status()
        print(f"[SYNC] DONE org_id={org_id}: {resp.json()}")


async def main():
    farmer_id = DEFAULT_FARMER_ID
    org_ids = get_all_org_ids()

    if not org_ids:
        print("[SYNC] No organizations found in DB; nothing to sync.")
        return

    print(f"[SYNC] Starting auto sync for farmer_id={farmer_id}, org_ids={org_ids}")

    for oid in org_ids:
        await sync_org(farmer_id, oid)

    print("[SYNC] Auto sync complete.")


if __name__ == "__main__":
    asyncio.run(main())
