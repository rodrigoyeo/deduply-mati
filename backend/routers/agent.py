"""
Agent-Native API — /agent/v1/*

Designed for AI agents (Hermes, Otto, OpenClaw, etc.) calling Deduply directly.
All responses are flat, workspace-aware, and include next_action hints.

Auth: Authorization: Bearer <api_token>  (users.api_token field)
"""
import json
from datetime import datetime, timedelta
from typing import Optional, List

import httpx
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from database import get_db, USE_POSTGRES
from shared import get_agent_user, leadgen_jobs
from workspace_routing import detect_workspace
from routers.leadgen import BulkRunRequest, ApproveContactsRequest, start_bulk_run, approve_contacts

router = APIRouter(prefix="/agent/v1", tags=["Agent API"])

BLITZAPI_BASE = "https://api.blitz-api.ai"
REACHINBOX_API_BASE = "https://api.reachinbox.ai/api/v1"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ContactStatusUpdate(BaseModel):
    status: str


class ContactIngestItem(BaseModel):
    email: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None
    title: Optional[str] = None
    company_country: Optional[str] = None
    domain: Optional[str] = None
    person_linkedin_url: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    industry: Optional[str] = None
    seniority: Optional[str] = None
    first_phone: Optional[str] = None
    company_city: Optional[str] = None
    company_state: Optional[str] = None
    notes: Optional[str] = None


class ContactIngestRequest(BaseModel):
    contacts: List[ContactIngestItem]
    outreach_list: Optional[str] = None
    campaign: Optional[str] = None


class AgentLeadgenSearchRequest(BaseModel):
    keywords: Optional[dict] = None
    industry: Optional[dict] = None
    hq: Optional[dict] = None
    employee_range: Optional[List[str]] = None
    founded_year: Optional[dict] = None
    max_results: int = 25
    workspace: Optional[str] = None


class AgentLeadgenImportRequest(BaseModel):
    company_ids: List[int]
    workspace: Optional[str] = None


class AgentReachInboxPushRequest(BaseModel):
    contact_ids: List[int]
    reachinbox_campaign_id: int
    workspace: str  # US or MX
    email_status_filter: Optional[List[str]] = None


class AgentHubspotPushRequest(BaseModel):
    contact_ids: Optional[List[int]] = None
    filters: Optional[dict] = None   # e.g. {"status": "Qualified"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now().isoformat()


def _get_setting(conn, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row and row[0] else None


def _row_to_dict(row) -> dict:
    """Convert a DB row (sqlite3.Row or psycopg2 DictRow) to plain dict."""
    if row is None:
        return {}
    try:
        return dict(row)
    except Exception:
        return {}


def _contact_to_agent_dict(row) -> dict:
    """Flatten a contact row into agent-friendly format."""
    d = _row_to_dict(row)
    return {
        "id": d.get("id"),
        "email": d.get("email"),
        "first_name": d.get("first_name"),
        "last_name": d.get("last_name"),
        "company": d.get("company"),
        "title": d.get("title"),
        "status": d.get("status"),
        "email_status": d.get("email_status"),
        "pipeline_stage": d.get("pipeline_stage"),
        "workspace": d.get("reachinbox_workspace") or "US",
        "company_country": d.get("company_country"),
        "domain": d.get("domain"),
        "person_linkedin_url": d.get("person_linkedin_url"),
        "reachinbox_workspace": d.get("reachinbox_workspace"),
        "reachinbox_pushed_at": d.get("reachinbox_pushed_at"),
        "enrichment_source": d.get("enrichment_source"),
        "times_contacted": d.get("times_contacted", 0),
        "created_at": d.get("created_at"),
        "updated_at": d.get("updated_at"),
    }


async def _blitzapi_get(path: str, api_key: str) -> dict:
    url = f"{BLITZAPI_BASE}{path}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"x-api-key": api_key})
    if resp.status_code == 401:
        raise HTTPException(401, "BlitzAPI key invalid")
    if resp.status_code == 402:
        raise HTTPException(402, "BlitzAPI insufficient credits")
    if resp.status_code not in (200, 201):
        raise HTTPException(502, f"BlitzAPI error {resp.status_code}")
    return resp.json()


def _get_reachinbox_key(workspace: str, conn) -> Optional[str]:
    key_name = f"reachinbox_api_key_{workspace.lower()}"
    return _get_setting(conn, key_name)


# ---------------------------------------------------------------------------
# GET /agent/v1/status
# ---------------------------------------------------------------------------

@router.get("/status")
def agent_status(user: dict = Depends(get_agent_user)):
    """
    Platform health snapshot for agents.
    Returns workspace key status, contact counts, API key availability, credit balance.
    """
    conn = get_db()

    # Contact counts
    total = conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0").fetchone()[0]
    us_count = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND (reachinbox_workspace='US' OR reachinbox_workspace IS NULL)"
    ).fetchone()[0]
    mx_count = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND reachinbox_workspace='MX'"
    ).fetchone()[0]

    # API key status
    blitz_key = _get_setting(conn, "blitzapi_api_key")
    ri_us_key = _get_setting(conn, "reachinbox_api_key_us")
    ri_mx_key = _get_setting(conn, "reachinbox_api_key_mx")
    hs_key = _get_setting(conn, "hubspot_private_app_token")

    # Pipeline stage counts
    stages = conn.execute("""
        SELECT pipeline_stage, COUNT(*) as n
        FROM contacts WHERE is_duplicate=0
        GROUP BY pipeline_stage
    """).fetchall()
    pipeline = {row[0] or "new": row[1] for row in stages}

    # Active leadgen jobs
    active_jobs = conn.execute(
        "SELECT COUNT(*) FROM lead_gen_jobs WHERE status='running'"
    ).fetchone()[0] if _table_exists(conn, "lead_gen_jobs") else 0

    conn.close()

    return {
        "timestamp": _now_iso(),
        "status": "healthy",
        "contacts": {
            "total": total,
            "us_workspace": us_count,
            "mx_workspace": mx_count,
        },
        "pipeline": pipeline,
        "api_keys": {
            "blitzapi": bool(blitz_key),
            "reachinbox_us": bool(ri_us_key),
            "reachinbox_mx": bool(ri_mx_key),
            "hubspot": bool(hs_key),
        },
        "active_leadgen_jobs": active_jobs,
        "next_action": "check_contacts" if total > 0 else "ingest_contacts",
    }


def _table_exists(conn, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# GET /agent/v1/contacts
# ---------------------------------------------------------------------------

@router.get("/contacts")
def agent_get_contacts(
    workspace: Optional[str] = None,
    status: Optional[str] = None,
    email_status: Optional[str] = None,
    pipeline_stage: Optional[str] = None,
    campaign_id: Optional[int] = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    user: dict = Depends(get_agent_user),
):
    """
    Query contacts. Flat response, no nested pagination.
    All contacts include their workspace field.
    """
    conn = get_db()
    where = ["c.is_duplicate=0"]
    params = []

    if workspace:
        ws = workspace.upper()
        if ws == "US":
            where.append("(c.reachinbox_workspace='US' OR c.reachinbox_workspace IS NULL)")
        elif ws == "MX":
            where.append("c.reachinbox_workspace='MX'")

    if status:
        where.append("c.status=?")
        params.append(status)

    if email_status:
        where.append("c.email_status=?")
        params.append(email_status)

    if pipeline_stage:
        where.append("c.pipeline_stage=?")
        params.append(pipeline_stage)

    if campaign_id:
        where.append("""c.id IN (
            SELECT contact_id FROM contact_campaigns WHERE campaign_id=?
        )""")
        params.append(campaign_id)

    where_sql = " AND ".join(where)

    total = conn.execute(
        f"SELECT COUNT(*) FROM contacts c WHERE {where_sql}", params
    ).fetchone()[0]

    rows = conn.execute(
        f"SELECT * FROM contacts c WHERE {where_sql} ORDER BY c.id DESC LIMIT ? OFFSET ?",
        params + [limit, offset]
    ).fetchall()

    contacts = [_contact_to_agent_dict(r) for r in rows]
    conn.close()

    ws_hint = workspace or "all"
    next_action = None
    if total > 0 and not status:
        next_action = "filter_by_pipeline_stage"
    elif status == "Lead":
        next_action = "push_to_reachinbox"

    return {
        "contacts": contacts,
        "total": total,
        "limit": limit,
        "offset": offset,
        "workspace": ws_hint,
        "timestamp": _now_iso(),
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# GET /agent/v1/contacts/{id}
# ---------------------------------------------------------------------------

@router.get("/contacts/{contact_id}")
def agent_get_contact(contact_id: int, user: dict = Depends(get_agent_user)):
    """Contact detail with full webhook event history."""
    conn = get_db()
    row = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Contact {contact_id} not found")

    contact = _contact_to_agent_dict(row)
    email = contact.get("email")

    # Fetch event history
    events = []
    if email:
        event_rows = conn.execute(
            "SELECT event_type, campaign_name, payload, created_at FROM webhook_events WHERE email=? ORDER BY created_at DESC LIMIT 50",
            (email,)
        ).fetchall()
        for e in event_rows:
            events.append({
                "event_type": e[0],
                "campaign": e[1],
                "created_at": e[3],
            })

    # ReachInbox push history
    push_log = []
    try:
        log_rows = conn.execute(
            "SELECT workspace, reachinbox_campaign_id, status, pushed_at FROM reachinbox_push_log WHERE contact_id=? ORDER BY pushed_at DESC",
            (contact_id,)
        ).fetchall()
        for l in log_rows:
            push_log.append({"workspace": l[0], "campaign_id": l[1], "status": l[2], "pushed_at": l[3]})
    except Exception:
        pass

    conn.close()

    contact["events"] = events
    contact["push_log"] = push_log
    contact["timestamp"] = _now_iso()

    return contact


# ---------------------------------------------------------------------------
# PATCH /agent/v1/contacts/{id}/status
# ---------------------------------------------------------------------------

@router.patch("/contacts/{contact_id}/status")
def agent_update_contact_status(
    contact_id: int,
    body: ContactStatusUpdate,
    user: dict = Depends(get_agent_user),
):
    """Update a contact's funnel status."""
    valid_statuses = [
        "Lead", "Contacted", "Replied", "Interested", "Meeting Booked",
        "No-Show", "Qualified", "Client", "Not Interested", "Bounced", "Unsubscribed"
    ]
    if body.status not in valid_statuses:
        raise HTTPException(400, f"Invalid status. Valid values: {valid_statuses}")

    conn = get_db()
    row = conn.execute("SELECT id, email FROM contacts WHERE id=?", (contact_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Contact {contact_id} not found")

    now = datetime.now().isoformat()
    conn.execute(
        "UPDATE contacts SET status=?, updated_at=? WHERE id=?",
        (body.status, now, contact_id)
    )
    conn.commit()
    conn.close()

    next_action = None
    if body.status in ("Interested", "Meeting Booked"):
        next_action = "push_to_hubspot"
    elif body.status == "Replied":
        next_action = "update_to_interested_or_not_interested"

    return {
        "contact_id": contact_id,
        "status": body.status,
        "updated_at": now,
        "timestamp": _now_iso(),
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/contacts/ingest
# ---------------------------------------------------------------------------

@router.post("/contacts/ingest")
def agent_ingest_contacts(body: ContactIngestRequest, user: dict = Depends(get_agent_user)):
    """
    Batch-insert contacts with dedup by email.
    Same logic as the Clay webhook ingest.
    """
    conn = get_db()
    now = datetime.now().isoformat()
    inserted = 0
    skipped = 0
    errors = []

    # Ensure outreach list exists
    list_id = None
    if body.outreach_list:
        conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (body.outreach_list,))
        row = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (body.outreach_list,)).fetchone()
        list_id = row[0] if row else None

    # Ensure campaign exists
    campaign_id = None
    if body.campaign:
        conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (body.campaign,))
        row = conn.execute("SELECT id FROM campaigns WHERE name=?", (body.campaign,)).fetchone()
        campaign_id = row[0] if row else None

    for item in body.contacts:
        try:
            data = {k: v for k, v in item.dict().items() if v is not None}
            email = data.get("email", "").strip().lower() if data.get("email") else None

            # Dedup by email
            if email:
                existing = conn.execute(
                    "SELECT id FROM contacts WHERE LOWER(email)=? AND is_duplicate=0", (email,)
                ).fetchone()
                if existing:
                    skipped += 1
                    # Still link to list/campaign if provided
                    if list_id:
                        conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?,?)", (existing[0], list_id))
                    if campaign_id:
                        conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?,?)", (existing[0], campaign_id))
                    continue

            # Set workspace
            data["reachinbox_workspace"] = detect_workspace(data)
            data["pipeline_stage"] = data.get("pipeline_stage", "new")
            data["created_at"] = now
            data["updated_at"] = now
            if email:
                data["email"] = email

            fields = list(data.keys())
            placeholders = ",".join(["?"] * len(fields))
            if USE_POSTGRES:
                placeholders = ",".join(["%s"] * len(fields))
                result = conn.execute(
                    f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders}) RETURNING id",
                    list(data.values())
                )
                cid = result.fetchone()[0]
            else:
                conn.execute(
                    f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders})",
                    list(data.values())
                )
                cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            if list_id:
                conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?,?)", (cid, list_id))
            if campaign_id:
                conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?,?)", (cid, campaign_id))

            inserted += 1
        except Exception as e:
            errors.append(str(e))

    conn.commit()
    conn.close()

    return {
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "errors": errors[:10],
        "outreach_list": body.outreach_list,
        "campaign": body.campaign,
        "timestamp": _now_iso(),
        "next_action": "verify_emails" if inserted > 0 else None,
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/leadgen/search
# ---------------------------------------------------------------------------

@router.post("/leadgen/search")
async def agent_leadgen_search(body: AgentLeadgenSearchRequest, user: dict = Depends(get_agent_user)):
    """
    Start a BlitzAPI company search job (non-blocking).
    Returns job_id immediately — poll /agent/v1/leadgen/jobs/{id} for results.
    """
    # Delegate to leadgen router logic — import and call the same background function
    from routers.leadgen import _get_blitzapi_key, _run_company_search, CompanySearchRequest
    import threading, uuid

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    conn.close()
    if not api_key:
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    conn2 = get_db()
    params_json = json.dumps({
        "keywords": body.keywords,
        "industry": body.industry,
        "hq": body.hq,
        "employee_range": body.employee_range,
        "founded_year": body.founded_year,
        "max_results": body.max_results,
    })
    conn2.execute(
        "INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at) VALUES (?,?,?,?,?,?,?)",
        (job_id, "company_search", "pending", params_json, body.workspace or "US", user.get("id"), now)
    )
    conn2.commit()
    conn2.close()

    search_req = CompanySearchRequest(
        keywords=body.keywords,
        industry=body.industry,
        hq=body.hq,
        employee_range=body.employee_range,
        founded_year=body.founded_year,
        max_results=body.max_results,
        workspace=body.workspace,
    )
    t = threading.Thread(target=_run_company_search, args=(job_id, search_req, api_key, user.get("id")), daemon=True)
    t.start()

    return {
        "job_id": job_id,
        "status": "running",
        "timestamp": _now_iso(),
        "next_action": f"poll GET /agent/v1/leadgen/jobs/{job_id}",
    }


# ---------------------------------------------------------------------------
# GET /agent/v1/leadgen/jobs/{id}
# ---------------------------------------------------------------------------

@router.get("/leadgen/jobs/{job_id}")
def agent_leadgen_job(job_id: str, user: dict = Depends(get_agent_user)):
    """Poll a lead gen job status + result count."""
    conn = get_db()
    row = conn.execute("SELECT * FROM lead_gen_jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Job {job_id} not found")

    job = _row_to_dict(row)

    # Live progress from in-memory store
    live = leadgen_jobs.get(job_id, {})

    conn.close()

    next_action = None
    if job.get("status") == "completed" and job.get("results_count", 0) > 0:
        next_action = f"POST /agent/v1/leadgen/import with company_ids from GET /api/leadgen/jobs/{job_id}"

    return {
        "job_id": job_id,
        "status": job.get("status"),
        "results_count": job.get("results_count", 0),
        "imported_count": job.get("imported_count", 0),
        "credits_used": float(job.get("credits_used") or 0),
        "workspace": job.get("workspace"),
        "created_at": job.get("created_at"),
        "completed_at": job.get("completed_at"),
        "error": job.get("error"),
        "live_progress": live,
        "timestamp": _now_iso(),
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/leadgen/import
# ---------------------------------------------------------------------------

@router.post("/leadgen/import")
async def agent_leadgen_import(body: AgentLeadgenImportRequest, user: dict = Depends(get_agent_user)):
    """
    Import companies → contacts via BlitzAPI employee-finder + email enrichment.
    Non-blocking — returns immediately.
    """
    from routers.leadgen import _get_blitzapi_key, _run_import_companies, ImportCompaniesRequest

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    conn.close()
    if not api_key:
        raise HTTPException(400, "BlitzAPI key not configured.")

    import threading
    import_req = ImportCompaniesRequest(company_ids=body.company_ids, workspace=body.workspace)
    t = threading.Thread(target=_run_import_companies, args=(import_req, api_key, user.get("id")), daemon=True)
    t.start()

    return {
        "status": "running",
        "company_ids": body.company_ids,
        "timestamp": _now_iso(),
        "next_action": "poll GET /agent/v1/contacts?pipeline_stage=new to see imported contacts",
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/reachinbox/push
# ---------------------------------------------------------------------------

@router.post("/reachinbox/push")
async def agent_reachinbox_push(
    body: AgentReachInboxPushRequest,
    user: dict = Depends(get_agent_user),
):
    """Push contacts to a ReachInbox campaign sequence."""
    workspace = body.workspace.upper()
    if workspace not in ("US", "MX"):
        raise HTTPException(400, "workspace must be US or MX")

    conn = get_db()
    api_key = _get_reachinbox_key(workspace, conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, f"ReachInbox API key for {workspace} not configured.")

    allowed_statuses = body.email_status_filter or ["Valid"]
    placeholders = ",".join(["?"] * len(body.contact_ids))
    status_placeholders = ",".join(["?"] * len(allowed_statuses))
    rows = conn.execute(
        f"SELECT id, email, first_name, last_name, company FROM contacts "
        f"WHERE id IN ({placeholders}) AND email IS NOT NULL AND email_status IN ({status_placeholders}) AND is_duplicate=0",
        body.contact_ids + allowed_statuses
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "pushed": 0,
            "skipped": len(body.contact_ids),
            "reason": "No contacts passed email_status filter",
            "timestamp": _now_iso(),
        }

    pushed = 0
    errors = []
    now = datetime.now().isoformat()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for row in rows:
            contact_id, email, first_name, last_name, company = row[0], row[1], row[2], row[3], row[4]
            payload = {
                "email": email,
                "firstName": first_name or "",
                "lastName": last_name or "",
                "companyName": company or "",
                "campaignId": body.reachinbox_campaign_id,
            }
            try:
                resp = await client.post(
                    f"{REACHINBOX_API_BASE}/onebox/leads",
                    json=payload,
                    headers={"x-api-key": api_key, "Content-Type": "application/json"}
                )
                if resp.status_code in (200, 201):
                    pushed += 1
                    # Log the push
                    conn2 = get_db()
                    conn2.execute(
                        "INSERT INTO reachinbox_push_log (contact_id, reachinbox_campaign_id, workspace, status, pushed_at) VALUES (?,?,?,?,?)",
                        (contact_id, body.reachinbox_campaign_id, workspace, "pushed", now)
                    )
                    conn2.execute(
                        "UPDATE contacts SET reachinbox_workspace=?, reachinbox_pushed_at=?, pipeline_stage='pushed', updated_at=? WHERE id=?",
                        (workspace, now, now, contact_id)
                    )
                    conn2.commit()
                    conn2.close()
                else:
                    errors.append(f"contact {contact_id}: HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"contact {contact_id}: {str(e)}")

    return {
        "pushed": pushed,
        "failed": len(errors),
        "errors": errors[:10],
        "workspace": workspace,
        "reachinbox_campaign_id": body.reachinbox_campaign_id,
        "timestamp": _now_iso(),
        "next_action": "monitor webhook events for replies" if pushed > 0 else None,
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/hubspot/push
# ---------------------------------------------------------------------------

@router.post("/hubspot/push")
def agent_hubspot_push(body: AgentHubspotPushRequest, user: dict = Depends(get_agent_user)):
    """
    Push contacts to HubSpot.
    Full implementation in Phase 5. Currently returns a stub if HubSpot is not yet configured.
    """
    conn = get_db()
    hs_token = _get_setting(conn, "hubspot_private_app_token")
    conn.close()

    if not hs_token:
        return {
            "status": "not_configured",
            "message": "HubSpot private app token not set. Add hubspot_private_app_token in Settings.",
            "timestamp": _now_iso(),
            "next_action": "configure_hubspot_token",
        }

    # HubSpot full implementation comes in Phase 5 (otto/hubspot-sync).
    # The agent can call this endpoint and get a clear signal on whether it's ready.
    return {
        "status": "pending_phase5",
        "message": "HubSpot push is scheduled for Phase 5 (otto/hubspot-sync). Token is configured and ready.",
        "timestamp": _now_iso(),
        "next_action": "wait_for_hubspot_phase",
    }


# ---------------------------------------------------------------------------
# GET /agent/v1/analytics/summary
# ---------------------------------------------------------------------------

@router.get("/analytics/summary")
def agent_analytics_summary(
    workspace: Optional[str] = None,
    user: dict = Depends(get_agent_user),
):
    """Campaign performance + funnel stats. Flat format, workspace-aware."""
    conn = get_db()

    # Funnel counts
    funnel_rows = conn.execute("""
        SELECT status, COUNT(*) as n
        FROM contacts WHERE is_duplicate=0
        GROUP BY status ORDER BY n DESC
    """).fetchall()
    funnel = {r[0]: r[1] for r in funnel_rows}

    # Workspace split
    ws_rows = conn.execute("""
        SELECT
            CASE WHEN reachinbox_workspace='MX' THEN 'MX' ELSE 'US' END as ws,
            COUNT(*) as n
        FROM contacts WHERE is_duplicate=0
        GROUP BY ws
    """).fetchall()
    workspace_split = {r[0]: r[1] for r in ws_rows}

    # Campaign performance
    camp_where = ""
    camp_params = []
    if workspace:
        camp_where = "WHERE market=?"
        camp_params = [workspace.upper()]

    camp_rows = conn.execute(f"""
        SELECT name, market, total_leads, emails_sent, emails_replied,
               reply_rate, open_rate, emails_bounced, opportunities, meetings_booked
        FROM campaigns {camp_where}
        ORDER BY reply_rate DESC LIMIT 20
    """, camp_params).fetchall()

    campaigns = []
    for r in camp_rows:
        campaigns.append({
            "name": r[0],
            "workspace": r[1] or "US",
            "total_leads": r[2] or 0,
            "emails_sent": r[3] or 0,
            "emails_replied": r[4] or 0,
            "reply_rate": r[5] or 0,
            "open_rate": r[6] or 0,
            "bounced": r[7] or 0,
            "opportunities": r[8] or 0,
            "meetings_booked": r[9] or 0,
        })

    # Top templates by reply rate (min 10 sends)
    tmpl_rows = conn.execute("""
        SELECT name, variant, times_sent, reply_rate, open_rate, is_winner
        FROM email_templates
        WHERE times_sent >= 10
        ORDER BY reply_rate DESC LIMIT 5
    """).fetchall()

    top_templates = []
    for r in tmpl_rows:
        top_templates.append({
            "name": r[0],
            "variant": r[1],
            "times_sent": r[2],
            "reply_rate": r[4] or 0,
            "open_rate": r[4] or 0,
            "is_winner": bool(r[5]),
        })

    # Pipeline stage breakdown
    stage_rows = conn.execute("""
        SELECT pipeline_stage, COUNT(*) FROM contacts
        WHERE is_duplicate=0 GROUP BY pipeline_stage
    """).fetchall()
    pipeline_stages = {r[0] or "new": r[1] for r in stage_rows}

    conn.close()

    # Derived next_action hint
    total_contacts = sum(funnel.values())
    total_pushed = pipeline_stages.get("pushed", 0) + pipeline_stages.get("active", 0)
    next_action = None
    if total_contacts == 0:
        next_action = "ingest_contacts_or_run_leadgen_search"
    elif pipeline_stages.get("new", 0) > 50:
        next_action = "verify_emails_then_push_to_reachinbox"
    elif total_pushed > 0 and funnel.get("Replied", 0) == 0:
        next_action = "check_reply_webhooks"

    return {
        "timestamp": _now_iso(),
        "workspace_filter": workspace or "all",
        "workspace_split": workspace_split,
        "funnel": funnel,
        "pipeline_stages": pipeline_stages,
        "campaigns": campaigns,
        "top_templates": top_templates,
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# GET /agent/v1/pipeline/stuck
# ---------------------------------------------------------------------------

@router.get("/pipeline/stuck")
def agent_pipeline_stuck(
    days: int = Query(3, ge=1, le=30),
    workspace: Optional[str] = None,
    user: dict = Depends(get_agent_user),
):
    """
    Contacts that haven't changed pipeline_stage in N days (default 3).
    Useful for agents to identify stalled contacts that need action.
    """
    conn = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    where = ["c.is_duplicate=0", "c.updated_at < ?"]
    params: list = [cutoff]

    if workspace:
        ws = workspace.upper()
        if ws == "MX":
            where.append("c.reachinbox_workspace='MX'")
        else:
            where.append("(c.reachinbox_workspace='US' OR c.reachinbox_workspace IS NULL)")

    rows = conn.execute(f"""
        SELECT id, email, first_name, last_name, company, status, pipeline_stage,
               reachinbox_workspace, updated_at
        FROM contacts c
        WHERE {' AND '.join(where)}
        ORDER BY c.updated_at ASC LIMIT 100
    """, params).fetchall()
    conn.close()

    stuck = []
    for r in rows:
        stuck.append({
            "id": r[0],
            "email": r[1],
            "name": f"{r[2] or ''} {r[3] or ''}".strip(),
            "company": r[4],
            "status": r[5],
            "pipeline_stage": r[6] or "new",
            "workspace": r[7] or "US",
            "last_updated": r[8],
        })

    next_action = None
    if stuck:
        stages = {c["pipeline_stage"] for c in stuck}
        if "new" in stages:
            next_action = "verify_emails_for_new_contacts"
        elif "pushed" in stages:
            next_action = "check_reachinbox_campaign_status"

    return {
        "stuck_contacts": stuck,
        "total": len(stuck),
        "days_threshold": days,
        "workspace": workspace or "all",
        "timestamp": _now_iso(),
        "next_action": next_action,
    }


# ---------------------------------------------------------------------------
# GET /agent/v1/blitz/credits
# ---------------------------------------------------------------------------

@router.get("/blitz/credits")
async def agent_blitz_credits(user: dict = Depends(get_agent_user)):
    """Current BlitzAPI credit balance."""
    conn = get_db()
    api_key = _get_setting(conn, "blitzapi_api_key")
    conn.close()

    if not api_key:
        return {
            "configured": False,
            "message": "BlitzAPI key not configured. Add blitzapi_api_key in Settings.",
            "timestamp": _now_iso(),
        }

    data = await _blitzapi_get("/v2/account/key-info", api_key)
    return {
        "configured": True,
        "valid": data.get("valid", False),
        "remaining_credits": data.get("remaining_credits", 0),
        "next_reset_at": data.get("next_reset_at"),
        "max_rps": data.get("max_requests_per_seconds"),
        "active_plans": data.get("active_plans", []),
        "timestamp": _now_iso(),
        "next_action": "run_leadgen_search" if data.get("remaining_credits", 0) > 10 else "recharge_credits",
    }


# ---------------------------------------------------------------------------
# POST /agent/v1/leadgen/bulk-run
# ---------------------------------------------------------------------------

@router.post("/leadgen/bulk-run")
async def agent_bulk_run(req: BulkRunRequest, user: dict = Depends(get_agent_user)):
    """Start a full autonomous pipeline: search companies → waterfall ICP → email → stage for approval.
    Hermes calls this. Results land in lead_gen_contacts (status=pending) until approved by human.
    Valid verticals: roofing, hvac, plumbing, landscaping
    """
    result = await start_bulk_run(req, user)
    result["next_action"] = f"poll GET /agent/v1/leadgen/jobs/{result['job_id']} every 60s until status=awaiting_approval"
    return result


@router.post("/leadgen/approve")
async def agent_approve_contacts(req: ApproveContactsRequest, user: dict = Depends(get_agent_user)):
    """Approve staged contacts — moves them to main contacts table.
    Use job_id to approve all pending for a job, or contact_ids for selective approval.
    """
    result = await approve_contacts(req, user)
    result["next_action"] = "POST /agent/v1/reachinbox/push to push approved contacts to ReachInbox"
    return result
