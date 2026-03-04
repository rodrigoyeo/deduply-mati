"""
ReachInbox router — /api/reachinbox/*
"""
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Depends

from database import get_db, USE_POSTGRES
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



@router.post("/api/reachinbox/sync-analytics")
async def sync_reachinbox_analytics(
    workspace: str = "US",
    user: dict = Depends(get_current_user)
):
    """Sync step-level and variant-level analytics from ReachInbox for all campaigns."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    workspace = workspace.upper()
    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    if not api_key:
        conn.close()
        return {"error": "ReachInbox API key not configured", "workspace": workspace}

    try:
        # Get all campaigns from ReachInbox
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{REACHINBOX_API_BASE}/campaigns/all",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            )
        if resp.status_code != 200:
            conn.close()
            return {"error": f"Failed to fetch campaigns: {resp.status_code}"}

        ri_camps = resp.json().get("data", {}).get("rows", [])
        total_steps = 0
        total_variants = 0

        for camp in ri_camps:
            ri_id = camp["id"]
            camp_name = camp.get("name", "")

            # Get analytics for this campaign
            async with httpx.AsyncClient(timeout=15.0) as client:
                a_resp = await client.post(
                    f"{REACHINBOX_API_BASE}/analytics?startDate=2024-01-01&endDate=2026-12-31",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "campaignId": ri_id,
                        "campaignAnalyticsRequired": True,
                        "includeSubsequenceIds": [],
                        "excludeSubsequenceIds": [],
                        "filter": "none"
                    }
                )

            if a_resp.status_code != 200:
                continue

            adata = a_resp.json().get("data", {})
            steps = adata.get("campaignStepAnalyticsResult", [])

            # Match to our campaign
            if USE_POSTGRES:
                our_camp = conn.execute("SELECT id FROM campaigns WHERE name = %s", (camp_name,)).fetchone()
            else:
                our_camp = conn.execute("SELECT id FROM campaigns WHERE name = ?", (camp_name,)).fetchone()
            
            our_camp_id = our_camp[0] if our_camp else None

            # Clear old sequence data for this campaign
            if our_camp_id:
                if USE_POSTGRES:
                    conn.execute("DELETE FROM campaign_sequences WHERE ri_campaign_id = %s", (ri_id,))
                else:
                    conn.execute("DELETE FROM campaign_sequences WHERE ri_campaign_id = ?", (ri_id,))

            for step_idx, step in enumerate(steps):
                step_num = step.get("stepNumber", step_idx + 1)
                step_sent = step.get("sent", 0)
                step_type = "initial" if step_idx == 0 else "follow-up"

                variants = step.get("variants", step.get("variantAnalytics", []))
                if not variants:
                    variants = [{"variant": 0, "sent": step_sent}]

                for v in variants:
                    v_idx = v.get("variant", 0)
                    v_sent = v.get("sent", 0)
                    v_opened = v.get("opened", 0)
                    v_replied = v.get("replied", 0)
                    v_bounced = v.get("bounced", 0)

                    if our_camp_id:
                        if USE_POSTGRES:
                            conn.execute("""INSERT INTO campaign_sequences 
                                (campaign_id, campaign_name, ri_campaign_id, workspace, step_number, step_type,
                                 variant_index, sent, opened, replied, bounced, synced_at)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                                (our_camp_id, camp_name, ri_id, workspace, step_num, step_type,
                                 v_idx, v_sent, v_opened, v_replied, v_bounced))
                        else:
                            conn.execute("""INSERT INTO campaign_sequences 
                                (campaign_id, campaign_name, ri_campaign_id, workspace, step_number, step_type,
                                 variant_index, sent, opened, replied, bounced, synced_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                                (our_camp_id, camp_name, ri_id, workspace, step_num, step_type,
                                 v_idx, v_sent, v_opened, v_replied, v_bounced))
                        total_variants += 1
                    total_steps += 1

            # Also update campaign-level stats
            if our_camp_id:
                camp_update = {
                    "emails_sent": adata.get("leadsContacted", camp.get("totalEmailSent", 0)),
                    "emails_opened": adata.get("opened", 0),
                    "emails_replied": adata.get("replied", 0),
                    "emails_bounced": adata.get("bounced", 0),
                }
                sent = camp_update["emails_sent"] or 1
                camp_update["open_rate"] = round(100 * camp_update["emails_opened"] / sent, 1)
                camp_update["reply_rate"] = round(100 * camp_update["emails_replied"] / sent, 1)

                if USE_POSTGRES:
                    conn.execute("""UPDATE campaigns SET 
                        emails_sent=%s, emails_opened=%s, emails_replied=%s, emails_bounced=%s,
                        open_rate=%s, reply_rate=%s WHERE id=%s""",
                        (*camp_update.values(), our_camp_id))
                else:
                    conn.execute("""UPDATE campaigns SET 
                        emails_sent=?, emails_opened=?, emails_replied=?, emails_bounced=?,
                        open_rate=?, reply_rate=? WHERE id=?""",
                        (*camp_update.values(), our_camp_id))

        conn.commit()
        conn.close()
        return {
            "workspace": workspace,
            "campaigns_synced": len(ri_camps),
            "total_steps": total_steps,
            "total_variants": total_variants,
            "message": f"Synced {len(ri_camps)} campaigns with {total_variants} sequence variants"
        }
    except Exception as e:
        conn.close()
        import traceback
        return {"error": str(e), "trace": traceback.format_exc()}


@router.get("/api/reachinbox/campaign-sequences")
async def get_campaign_sequences(
    workspace: str = "US",
    campaign_id: Optional[int] = None,
    user: dict = Depends(get_current_user)
):
    """Get synced sequence/variant data for campaigns."""
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    where = ["workspace=?"]
    params = [workspace.upper()]
    if campaign_id:
        where.append("campaign_id=?")
        params.append(campaign_id)

    rows = conn.execute(f"""
        SELECT * FROM campaign_sequences 
        WHERE {' AND '.join(where)}
        ORDER BY campaign_name, step_number, variant_index
    """, params).fetchall()
    conn.close()

    result = {}
    for r in rows:
        r = dict(r)
        key = r["campaign_name"]
        if key not in result:
            result[key] = {"campaign_id": r["campaign_id"], "ri_campaign_id": r["ri_campaign_id"], "steps": {}}
        step = r["step_number"]
        if step not in result[key]["steps"]:
            result[key]["steps"][step] = {"type": r["step_type"], "variants": []}
        result[key]["steps"][step]["variants"].append({
            "variant": r["variant_index"],
            "sent": r["sent"],
            "opened": r["opened"],
            "replied": r["replied"],
            "bounced": r["bounced"],
        })

    return {"workspace": workspace, "campaigns": result}

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
