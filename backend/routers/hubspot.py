"""
HubSpot CRM router — /api/hubspot/*

Field mapping (contacts table → HubSpot properties):
  email         → email
  first_name    → firstname
  last_name     → lastname
  company       → company
  title         → jobtitle
  domain        → website
  company_country → country
  first_phone   → phone
  status        → hs_lead_status (Interested/Scheduled→OPEN_DEAL, Lead→NEW, else→IN_PROGRESS)

Deal is always created and linked to the contact on first sync.
"""
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel

from database import get_db
from shared import get_current_user

router = APIRouter()

HS_API_BASE = "https://api.hubapi.com/crm/v3"
HS_API_V4_BASE = "https://api.hubapi.com/crm/v4"

HS_LEAD_STATUS_MAP = {
    "Interested": "OPEN_DEAL",
    "Scheduled": "OPEN_DEAL",
    "Lead": "NEW",
}


def _get_hubspot_key(conn) -> Optional[str]:
    row = conn.execute(
        "SELECT value FROM settings WHERE key='hubspot_private_app_token'"
    ).fetchone()
    return row[0] if row and row[0] else None


def _build_hs_properties(contact: dict) -> dict:
    """Map Deduply contact fields to HubSpot contact properties."""
    props: dict = {}
    if contact.get("email"):           props["email"] = contact["email"]
    if contact.get("first_name"):      props["firstname"] = contact["first_name"]
    if contact.get("last_name"):       props["lastname"] = contact["last_name"]
    if contact.get("company"):         props["company"] = contact["company"]
    if contact.get("title"):           props["jobtitle"] = contact["title"]
    if contact.get("domain"):          props["website"] = contact["domain"]
    if contact.get("company_country"): props["country"] = contact["company_country"]
    if contact.get("first_phone"):     props["phone"] = contact["first_phone"]
    props["hs_lead_status"] = HS_LEAD_STATUS_MAP.get(
        contact.get("status", ""), "IN_PROGRESS"
    )
    return props


async def push_contact_to_hubspot(contact_id: int, api_key: str) -> None:
    """
    Create/update contact in HubSpot + create an associated deal.
    Safe to call from FastAPI BackgroundTasks — opens its own DB connection.
    """
    conn = get_db()
    try:
        row = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
        if not row:
            return
        c = dict(row)
        if not c.get("email"):
            return

        props = _build_hs_properties(c)
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=20.0) as client:
            hs_contact_id = c.get("hubspot_contact_id")

            if hs_contact_id:
                # Update the already-known HubSpot contact
                await client.patch(
                    f"{HS_API_BASE}/objects/contacts/{hs_contact_id}",
                    headers=headers,
                    json={"properties": props},
                )
            else:
                # Try to create
                resp = await client.post(
                    f"{HS_API_BASE}/objects/contacts",
                    headers=headers,
                    json={"properties": props},
                )
                if resp.status_code in (200, 201):
                    hs_contact_id = str(resp.json().get("id", ""))
                elif resp.status_code == 409:
                    # Contact exists — search by email to get its ID
                    search_resp = await client.post(
                        f"{HS_API_BASE}/objects/contacts/search",
                        headers=headers,
                        json={
                            "filterGroups": [{"filters": [
                                {"propertyName": "email", "operator": "EQ", "value": c["email"]}
                            ]}],
                            "limit": 1,
                        },
                    )
                    if search_resp.status_code == 200:
                        results = search_resp.json().get("results", [])
                        if results:
                            hs_contact_id = str(results[0]["id"])
                            # Update with latest data
                            await client.patch(
                                f"{HS_API_BASE}/objects/contacts/{hs_contact_id}",
                                headers=headers,
                                json={"properties": props},
                            )
                else:
                    return  # Unexpected error — bail out

            if not hs_contact_id:
                return

            # Create deal
            fname = (c.get("first_name") or "").strip()
            lname = (c.get("last_name") or "").strip()
            company = (c.get("company") or "").strip()
            name_part = f"{fname} {lname}".strip()
            deal_name = f"{name_part} - {company}" if company else (name_part or f"Contact {contact_id}")

            deal_resp = await client.post(
                f"{HS_API_BASE}/objects/deals",
                headers=headers,
                json={"properties": {
                    "dealname": deal_name,
                    "dealstage": "appointmentscheduled",
                    "pipeline": "default",
                }},
            )

            hs_deal_id = c.get("hubspot_deal_id")  # keep existing deal unless we create a new one
            if deal_resp.status_code in (200, 201):
                hs_deal_id = str(deal_resp.json().get("id", ""))
                # Associate deal → contact (association type 3 = deal-to-contact)
                if hs_deal_id:
                    await client.put(
                        f"{HS_API_V4_BASE}/objects/deals/{hs_deal_id}/associations/contacts/{hs_contact_id}/3",
                        headers=headers,
                    )

        now = datetime.now().isoformat()
        conn.execute(
            "UPDATE contacts SET hubspot_contact_id=?, hubspot_deal_id=?, "
            "hubspot_queued=0, hubspot_synced_at=?, updated_at=? WHERE id=?",
            (hs_contact_id, hs_deal_id, now, now, contact_id),
        )
        conn.commit()

    except Exception as e:
        print(f"[HubSpot] Error syncing contact {contact_id}: {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/hubspot/status")
async def hubspot_status(user: dict = Depends(get_current_user)):
    """Verify HubSpot private app token by fetching 1 contact."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    api_key = _get_hubspot_key(conn)
    conn.close()
    if not api_key:
        return {"configured": False, "reason": "not_configured"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{HS_API_BASE}/objects/contacts?limit=1",
                headers={"Authorization": f"Bearer {api_key}"},
            )
        if resp.status_code == 200:
            return {"configured": True, "status": "connected"}
        if resp.status_code == 401:
            return {"configured": True, "status": "invalid_token",
                    "detail": "Token rejected by HubSpot — check scopes"}
        return {"configured": True, "status": "error", "detail": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"configured": True, "status": "error", "detail": str(e)}


@router.post("/api/hubspot/sync/contact/{contact_id}")
async def sync_contact(
    contact_id: int,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Push a single contact to HubSpot (create/update) and create an associated deal."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    api_key = _get_hubspot_key(conn)
    conn.close()
    if not api_key:
        raise HTTPException(400, "HubSpot token not configured. Add hubspot_private_app_token in Settings.")
    background_tasks.add_task(push_contact_to_hubspot, contact_id, api_key)
    return {"status": "queued", "contact_id": contact_id}


class HubSpotBulkRequest(BaseModel):
    contact_ids: Optional[List[int]] = None
    status: Optional[str] = None      # filter by contact status
    queued_only: bool = False          # only contacts with hubspot_queued=1
    limit: int = 100


@router.post("/api/hubspot/sync/bulk")
async def sync_bulk(
    req: HubSpotBulkRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Push contacts to HubSpot — filtered by status, hubspot_queued flag, or explicit IDs."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    api_key = _get_hubspot_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "HubSpot token not configured")

    if req.contact_ids:
        ph = ",".join(["?"] * len(req.contact_ids))
        rows = conn.execute(
            f"SELECT id FROM contacts WHERE id IN ({ph}) AND is_duplicate=0",
            req.contact_ids,
        ).fetchall()
    else:
        where, params = ["is_duplicate=0", "email IS NOT NULL"], []
        if req.queued_only:
            where.append("hubspot_queued=1")
        if req.status:
            where.append("status=?")
            params.append(req.status)
        rows = conn.execute(
            f"SELECT id FROM contacts WHERE {' AND '.join(where)} LIMIT ?",
            params + [req.limit],
        ).fetchall()

    conn.close()
    ids = [r[0] for r in rows]
    for cid in ids:
        background_tasks.add_task(push_contact_to_hubspot, cid, api_key)
    return {"status": "queued", "count": len(ids)}


@router.get("/api/hubspot/sync/log")
def sync_log(
    limit: int = 50,
    user: dict = Depends(get_current_user),
):
    """Get contacts that have been synced to HubSpot (hubspot_synced_at IS NOT NULL)."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    rows = conn.execute(
        """SELECT id, first_name, last_name, email, company, status,
                  hubspot_contact_id, hubspot_deal_id, hubspot_synced_at, updated_at
           FROM contacts
           WHERE hubspot_synced_at IS NOT NULL
           ORDER BY hubspot_synced_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return {"data": [dict(r) for r in rows]}
