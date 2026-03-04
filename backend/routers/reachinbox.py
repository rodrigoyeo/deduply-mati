"""
ReachInbox router — /api/reachinbox/*
"""
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Depends

from database import get_db
from models import ReachInboxPushRequest, PushCampaignContactsRequest
from shared import get_current_user

router = APIRouter()

REACHINBOX_API_BASE = "https://api.reachinbox.ai/api/v1"
REACHINBOX_WORKSPACE_KEYS = {
    "US": "reachinbox_api_key_us",
    "MX": "reachinbox_api_key_mx",
}


def _get_reachinbox_key(workspace: str, conn) -> Optional[str]:
    """Fetch ReachInbox API key for a workspace from settings table."""
    settings_key = REACHINBOX_WORKSPACE_KEYS.get(workspace.upper())
    if not settings_key:
        return None
    row = conn.execute("SELECT value FROM settings WHERE key=?", (settings_key,)).fetchone()
    return row[0] if row and row[0] else None


@router.post("/api/reachinbox/push")
async def push_to_reachinbox(req: ReachInboxPushRequest, user: dict = Depends(get_current_user)):
    """Push contacts into a ReachInbox campaign sequence (US or MX workspace)."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    workspace = req.workspace.upper()
    if workspace not in ("US", "MX"):
        raise HTTPException(400, "workspace must be US or MX")

    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, f"ReachInbox API key for {workspace} not configured. "
                                 f"Add reachinbox_api_key_{workspace.lower()} in Settings.")

    allowed_statuses = req.email_status_filter or ["Valid"]
    placeholders = ",".join(["?"] * len(req.contact_ids))
    contacts = conn.execute(
        f"SELECT id, first_name, last_name, email, company, website, person_linkedin_url, "
        f"email_status, pipeline_stage FROM contacts WHERE id IN ({placeholders})",
        req.contact_ids
    ).fetchall()

    stats = {"pushed": 0, "skipped_invalid_email": 0, "skipped_already_pushed": 0, "failed": 0}
    now = datetime.now().isoformat()
    batch = []

    for c in contacts:
        cid, fname, lname, email, company, website, linkedin, email_status, pipeline = \
            c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7] or "Unknown", c[8] or "new"

        if email_status not in allowed_statuses:
            conn.execute(
                "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status, error_message) "
                "VALUES (?, ?, ?, ?, 'skipped', ?)",
                (cid, req.deduply_campaign_id, req.reachinbox_campaign_id, workspace,
                 f"email_status={email_status} not in {allowed_statuses}")
            )
            stats["skipped_invalid_email"] += 1
            continue

        already = conn.execute(
            "SELECT id FROM reachinbox_push_log WHERE contact_id=? AND reachinbox_campaign_id=? AND status='pushed'",
            (cid, req.reachinbox_campaign_id)
        ).fetchone()
        if already:
            stats["skipped_already_pushed"] += 1
            continue

        batch.append({"_db_id": cid, "email": email, "firstName": fname or "",
                      "lastName": lname or "", "companyName": company or "",
                      "website": website or "", "linkedinUrl": linkedin or ""})

    BATCH_SIZE = 50
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i:i + BATCH_SIZE]
        leads_payload = [{k: v for k, v in lead.items() if k != "_db_id"} for lead in chunk]
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{REACHINBOX_API_BASE}/leads",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={"campaignId": req.reachinbox_campaign_id, "leads": leads_payload}
                )
            resp_ok = resp.status_code in (200, 201)
            error_msg = None if resp_ok else (
                resp.json().get("message", f"HTTP {resp.status_code}") if resp.content else f"HTTP {resp.status_code}"
            )

            for lead in chunk:
                db_id = lead["_db_id"]
                if resp_ok:
                    conn.execute(
                        "UPDATE contacts SET reachinbox_workspace=?, reachinbox_campaign_id=?, "
                        "reachinbox_pushed_at=?, pipeline_stage='pushed', updated_at=? WHERE id=?",
                        (workspace, req.reachinbox_campaign_id, now, now, db_id)
                    )
                    conn.execute(
                        "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status) "
                        "VALUES (?, ?, ?, ?, 'pushed')",
                        (db_id, req.deduply_campaign_id, req.reachinbox_campaign_id, workspace)
                    )
                    stats["pushed"] += 1
                else:
                    conn.execute(
                        "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status, error_message) "
                        "VALUES (?, ?, ?, ?, 'failed', ?)",
                        (db_id, req.deduply_campaign_id, req.reachinbox_campaign_id, workspace, error_msg)
                    )
                    stats["failed"] += 1
        except Exception as e:
            for lead in chunk:
                conn.execute(
                    "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status, error_message) "
                    "VALUES (?, ?, ?, ?, 'failed', ?)",
                    (lead["_db_id"], req.deduply_campaign_id, req.reachinbox_campaign_id, workspace, str(e))
                )
            stats["failed"] += len(chunk)

    conn.commit()
    conn.close()
    return {
        "status": "ok",
        "workspace": workspace,
        "reachinbox_campaign_id": req.reachinbox_campaign_id,
        "stats": stats
    }


@router.get("/api/reachinbox/push-log")
def get_push_log(
    contact_id: Optional[int] = None,
    campaign_id: Optional[int] = None,
    limit: int = 100,
    user: dict = Depends(get_current_user)
):
    """Get ReachInbox push history."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    where, params = ["1=1"], []
    if contact_id:
        where.append("contact_id=?")
        params.append(contact_id)
    if campaign_id:
        where.append("campaign_id=?")
        params.append(campaign_id)
    rows = conn.execute(
        f"SELECT * FROM reachinbox_push_log WHERE {' AND '.join(where)} ORDER BY pushed_at DESC LIMIT ?",
        params + [limit]
    ).fetchall()
    conn.close()
    return {"data": [dict(r) for r in rows]}


@router.get("/api/reachinbox/campaigns")
async def list_reachinbox_campaigns(
    workspace: str = "US",
    user: dict = Depends(get_current_user)
):
    """Fetch campaign list from ReachInbox API. Returns fallback flag if API unavailable."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    workspace = workspace.upper()
    if workspace not in ("US", "MX"):
        raise HTTPException(400, "workspace must be US or MX")
    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    conn.close()
    if not api_key:
        return {"campaigns": [], "fallback": "manual_id", "reason": "not_configured"}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{REACHINBOX_API_BASE}/campaigns/all",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            )
        if resp.status_code >= 500:
            return {"campaigns": [], "fallback": "manual_id", "reason": f"api_error_{resp.status_code}"}
        if resp.status_code in (200, 201):
            body = resp.json()
            # ReachInbox /campaigns/all returns {data: {rows: [...]}}
            raw_data = body.get("data", body) if isinstance(body, dict) else body
            raw = raw_data.get("rows", raw_data) if isinstance(raw_data, dict) else raw_data
            if isinstance(raw, list):
                campaigns = [
                    {
                        "id": c.get("id"),
                        "name": c.get("name") or c.get("campaign_name") or f"Campaign {c.get('id')}",
                        "status": c.get("status"),
                        "leads": c.get("leadAddedCount", 0),
                        "sent": c.get("totalEmailSent", 0),
                        "opened": c.get("totalEmailOpened", 0),
                        "replied": c.get("totalEmailReplied", 0),
                        "bounced": c.get("totalEmailBounced", 0),
                    }
                    for c in raw if c.get("id")
                ]
                return {"campaigns": campaigns, "workspace": workspace, "count": len(campaigns)}
        return {"campaigns": [], "fallback": "manual_id", "reason": f"unexpected_{resp.status_code}"}
    except Exception:
        return {"campaigns": [], "fallback": "manual_id", "reason": "connection_error"}




@router.post("/api/reachinbox/sync-campaigns")
async def sync_reachinbox_campaigns(
    workspace: str = "US",
    user: dict = Depends(get_current_user)
):
    """Sync campaign stats from ReachInbox into our database."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    workspace = workspace.upper()
    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    if not api_key:
        conn.close()
        return {"error": "ReachInbox API key not configured", "workspace": workspace}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{REACHINBOX_API_BASE}/campaigns/all",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            )
        if resp.status_code != 200:
            conn.close()
            return {"error": f"ReachInbox API returned {resp.status_code}"}

        body = resp.json()
        raw_data = body.get("data", body) if isinstance(body, dict) else body
        rows = raw_data.get("rows", raw_data) if isinstance(raw_data, dict) else raw_data

        synced = 0
        for ri_camp in rows:
            name = ri_camp.get("name", "")
            # Try to match by name to our campaigns
            if USE_POSTGRES:
                existing = conn.execute("SELECT id FROM campaigns WHERE name = %s", (name,)).fetchone()
            else:
                existing = conn.execute("SELECT id FROM campaigns WHERE name = ?", (name,)).fetchone()

            stats = {
                "total_leads": ri_camp.get("leadAddedCount", 0),
                "emails_sent": ri_camp.get("totalEmailSent", 0),
                "emails_opened": ri_camp.get("totalEmailOpened", 0) or ri_camp.get("totalUniqueEmailOpened", 0),
                "emails_replied": ri_camp.get("totalEmailReplied", 0),
                "emails_bounced": ri_camp.get("totalEmailBounced", 0),
                "emails_clicked": ri_camp.get("totalLinkClicked", 0),
            }
            sent = stats["emails_sent"]
            stats["open_rate"] = round(100 * stats["emails_opened"] / sent, 1) if sent else 0
            stats["reply_rate"] = round(100 * stats["emails_replied"] / sent, 1) if sent else 0
            stats["click_rate"] = round(100 * stats["emails_clicked"] / sent, 1) if sent else 0

            ri_status = ri_camp.get("status", "")
            if ri_status == "Active":
                stats["status"] = "Active"
            elif ri_status == "Completed":
                stats["status"] = "Completed"
            elif ri_status == "Draft":
                stats["status"] = "Draft"

            if existing:
                # Update existing campaign
                set_clause = ", ".join([f"{k}={'%s' if USE_POSTGRES else '?'}" for k in stats.keys()])
                if USE_POSTGRES:
                    conn.execute(f"UPDATE campaigns SET {set_clause} WHERE id=%s",
                        list(stats.values()) + [existing[0]])
                else:
                    conn.execute(f"UPDATE campaigns SET {set_clause} WHERE id=?",
                        list(stats.values()) + [existing[0]])
                synced += 1

        conn.commit()
        conn.close()
        return {"synced": synced, "total_ri_campaigns": len(rows), "workspace": workspace}
    except Exception as e:
        conn.close()
        return {"error": str(e)}

@router.post("/api/reachinbox/campaigns/{ri_campaign_id}/push-contacts")
async def push_campaign_contacts(
    ri_campaign_id: int,
    req: PushCampaignContactsRequest,
    user: dict = Depends(get_current_user)
):
    """Push all valid contacts from a Deduply campaign into a ReachInbox campaign sequence."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    camp = conn.execute("SELECT id, market FROM campaigns WHERE id=?", (req.deduply_campaign_id,)).fetchone()
    if not camp:
        conn.close()
        raise HTTPException(404, f"Deduply campaign {req.deduply_campaign_id} not found")

    workspace = (camp[1] or "US").upper()
    api_key = _get_reachinbox_key(workspace, conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, f"ReachInbox API key for {workspace} not configured")

    allowed_statuses = req.email_status_filter or ["Valid"]
    ph = ",".join(["?"] * len(allowed_statuses))
    contacts = conn.execute(
        f"""SELECT c.id, c.first_name, c.last_name, c.email, c.company, c.website, c.person_linkedin_url
            FROM contacts c
            JOIN contact_campaigns cc ON c.id = cc.contact_id
            WHERE cc.campaign_id=? AND c.is_duplicate=0 AND c.email_status IN ({ph})""",
        [req.deduply_campaign_id] + allowed_statuses
    ).fetchall()

    stats = {"pushed": 0, "skipped_already_pushed": 0, "failed": 0}
    now = datetime.now().isoformat()
    batch = []

    for c in contacts:
        cid, fname, lname, email, company, website, linkedin = c[0], c[1], c[2], c[3], c[4], c[5], c[6]
        already = conn.execute(
            "SELECT id FROM reachinbox_push_log WHERE contact_id=? AND reachinbox_campaign_id=? AND status='pushed'",
            (cid, ri_campaign_id)
        ).fetchone()
        if already:
            stats["skipped_already_pushed"] += 1
            continue
        batch.append({"_db_id": cid, "email": email or "", "firstName": fname or "",
                      "lastName": lname or "", "companyName": company or "",
                      "website": website or "", "linkedinUrl": linkedin or ""})

    BATCH_SIZE = 50
    for i in range(0, len(batch), BATCH_SIZE):
        chunk = batch[i:i + BATCH_SIZE]
        leads_payload = [{k: v for k, v in lead.items() if k != "_db_id"} for lead in chunk]
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{REACHINBOX_API_BASE}/leads",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={"campaignId": ri_campaign_id, "leads": leads_payload}
                )
            resp_ok = resp.status_code in (200, 201)
            error_msg = None if resp_ok else (
                resp.json().get("message", f"HTTP {resp.status_code}") if resp.content else f"HTTP {resp.status_code}"
            )
            for lead in chunk:
                db_id = lead["_db_id"]
                if resp_ok:
                    conn.execute(
                        "UPDATE contacts SET reachinbox_workspace=?, reachinbox_campaign_id=?, "
                        "reachinbox_pushed_at=?, pipeline_stage='pushed', updated_at=? WHERE id=?",
                        (workspace, ri_campaign_id, now, now, db_id)
                    )
                    conn.execute(
                        "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status) "
                        "VALUES (?, ?, ?, ?, 'pushed')",
                        (db_id, req.deduply_campaign_id, ri_campaign_id, workspace)
                    )
                    stats["pushed"] += 1
                else:
                    conn.execute(
                        "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status, error_message) "
                        "VALUES (?, ?, ?, ?, 'failed', ?)",
                        (db_id, req.deduply_campaign_id, ri_campaign_id, workspace, error_msg)
                    )
                    stats["failed"] += 1
        except Exception as e:
            for lead in chunk:
                conn.execute(
                    "INSERT INTO reachinbox_push_log (contact_id, campaign_id, reachinbox_campaign_id, workspace, status, error_message) "
                    "VALUES (?, ?, ?, ?, 'failed', ?)",
                    (lead["_db_id"], req.deduply_campaign_id, ri_campaign_id, workspace, str(e))
                )
            stats["failed"] += len(chunk)

    conn.commit()
    conn.close()
    return {"status": "ok", "workspace": workspace, "reachinbox_campaign_id": ri_campaign_id, "stats": stats}


@router.post("/api/reachinbox/sync-status")
async def sync_reachinbox_status(
    workspace: str = "US",
    ri_campaign_id: Optional[int] = None,
    user: dict = Depends(get_current_user)
):
    """Pull latest campaign stats from ReachInbox API."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    workspace = workspace.upper()
    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    conn.close()
    if not api_key:
        raise HTTPException(400, f"ReachInbox API key for {workspace} not configured")
    try:
        url = f"{REACHINBOX_API_BASE}/campaign/{ri_campaign_id}/stats" if ri_campaign_id else f"{REACHINBOX_API_BASE}/campaign"
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            )
        if resp.status_code >= 400:
            raise HTTPException(502, f"ReachInbox API returned {resp.status_code}")
        return {"status": "ok", "workspace": workspace, "data": resp.json()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"ReachInbox API error: {str(e)}")


@router.get("/api/reachinbox/workspace-status")
def get_workspace_status(user: dict = Depends(get_current_user)):
    """Check which ReachInbox workspaces are configured."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    result = {}
    for workspace, key_name in REACHINBOX_WORKSPACE_KEYS.items():
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key_name,)).fetchone()
        result[workspace] = {"configured": bool(row and row[0]), "settings_key": key_name}
    conn.close()
    return result
