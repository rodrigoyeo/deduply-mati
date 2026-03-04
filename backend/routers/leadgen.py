"""
Lead Generation router — /api/leadgen/*
BlitzAPI enrichment engine: company search, employee finder, waterfall ICP, email finder.
"""
import json
import threading
import traceback
import uuid
from datetime import datetime
from typing import Optional, List

import httpx
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from database import get_db, USE_POSTGRES
from shared import get_current_user, leadgen_jobs
from workspace_routing import detect_workspace

router = APIRouter()

BLITZAPI_BASE = "https://api.blitz-api.ai"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class CompanySearchRequest(BaseModel):
    industries_include: Optional[List[str]] = None
    industries_exclude: Optional[List[str]] = None
    keywords_include: Optional[List[str]] = None   # description keywords to include
    keywords_exclude: Optional[List[str]] = None   # description keywords to exclude
    countries: Optional[List[str]] = None          # HQ country codes
    states: Optional[List[str]] = None             # HQ states
    employee_range: Optional[List[str]] = None     # ["51-200", "201-500"]
    company_types: Optional[List[str]] = None      # ["Privately Held", ...]
    exclude_domains: Optional[List[str]] = None
    max_results: int = 25
    workspace: Optional[str] = None


class ImportCompaniesRequest(BaseModel):
    company_ids: List[int]
    workspace: Optional[str] = None


class WaterfallDirectRequest(BaseModel):
    company_linkedin_url: str
    job_levels: Optional[List[str]] = None   # ["C-Level", "VP", "Director"]
    max_per_company: int = 3


class FindEmailRequest(BaseModel):
    contact_id: int


# ---------------------------------------------------------------------------
# BlitzAPI key fetch
# ---------------------------------------------------------------------------

def _get_blitzapi_key(conn) -> Optional[str]:
    """Fetch BlitzAPI key from settings table."""
    row = conn.execute("SELECT value FROM settings WHERE key=?", ("blitzapi_api_key",)).fetchone()
    return row[0] if row and row[0] else None


# ---------------------------------------------------------------------------
# BlitzAPI HTTP helpers
# ---------------------------------------------------------------------------

def _blitzapi_request_sync(method: str, path: str, api_key: str, body: dict = None) -> dict:
    """Synchronous BlitzAPI call for use inside background threads."""
    url = f"{BLITZAPI_BASE}{path}"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    with httpx.Client(timeout=30.0) as client:
        if method == "GET":
            resp = client.get(url, headers=headers)
        else:
            resp = client.post(url, headers=headers, json=body)
    if resp.status_code == 401:
        raise RuntimeError("BlitzAPI key invalid (401)")
    if resp.status_code == 402:
        raise RuntimeError("BlitzAPI insufficient credits (402)")
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"BlitzAPI error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


async def _blitzapi_request(method: str, path: str, api_key: str, body: dict = None) -> dict:
    """Async BlitzAPI call for use inside async endpoints."""
    url = f"{BLITZAPI_BASE}{path}"
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        if method == "GET":
            resp = await client.get(url, headers=headers)
        else:
            resp = await client.post(url, headers=headers, json=body)
    if resp.status_code == 401:
        raise HTTPException(401, "BlitzAPI key invalid")
    if resp.status_code == 402:
        raise HTTPException(402, "BlitzAPI insufficient credits")
    if resp.status_code not in (200, 201):
        raise HTTPException(502, f"BlitzAPI error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Workspace badge helper
# ---------------------------------------------------------------------------

def _workspace_for_company(company_row: dict, override: Optional[str] = None) -> str:
    """Determine workspace for a company result."""
    if override:
        return override.upper()
    # Auto-detect from HQ country code
    country = (company_row.get("hq_country") or "").upper()
    if country in ("MX", "MEXICO"):
        return "MX"
    return "US"


# ---------------------------------------------------------------------------
# Contact insertion helper (shared by import + waterfall)
# ---------------------------------------------------------------------------

def _insert_contact_from_person(conn, person: dict, company_row: dict, email: str, workspace: str) -> Optional[int]:
    """Insert a contact from BlitzAPI person + email data. Returns contact id or None if duplicate."""
    email_lower = (email or "").lower().strip()
    if not email_lower:
        return None

    # Dedup check
    existing = conn.execute(
        "SELECT id FROM contacts WHERE LOWER(email)=? AND is_duplicate=0",
        (email_lower,)
    ).fetchone()
    if existing:
        return None

    full_name = person.get("full_name", "")
    parts = full_name.split(" ", 1) if full_name else ["", ""]
    first_name = parts[0].strip()
    last_name = parts[1].strip() if len(parts) > 1 else ""

    contact_data = {
        "first_name": first_name,
        "last_name": last_name,
        "email": email_lower,
        "email_status": "Valid",
        "title": person.get("job_title", ""),
        "headline": person.get("headline", ""),
        "company": company_row.get("name", ""),
        "domain": company_row.get("domain", ""),
        "person_linkedin_url": person.get("linkedin_url", ""),
        "company_linkedin_url": company_row.get("linkedin_url", ""),
        "industry": company_row.get("industry", ""),
        "enrichment_source": "blitzapi",
        "pipeline_stage": "new",
        "reachinbox_workspace": workspace,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }

    # Clean out empty strings to avoid overwriting defaults
    contact_data = {k: v for k, v in contact_data.items() if v is not None and v != ""}
    contact_data["email"] = email_lower  # always keep email

    fields = list(contact_data.keys())
    if USE_POSTGRES:
        placeholders = ",".join(["%s"] * len(fields))
        result = conn.execute(
            f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders}) RETURNING id",
            list(contact_data.values())
        )
        cid = result.fetchone()[0]
    else:
        placeholders = ",".join(["?"] * len(fields))
        conn.execute(
            f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders})",
            list(contact_data.values())
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return cid


# ---------------------------------------------------------------------------
# Background thread: Company Search
# ---------------------------------------------------------------------------

def _run_company_search(job_id: str, api_key: str, search_body: dict, workspace_override: Optional[str]):
    """Background thread: calls BlitzAPI and stores company results."""
    print(f"[LEADGEN THREAD] Starting company search job {job_id}")
    conn = None
    try:
        conn = get_db()
        conn.execute(
            "UPDATE lead_gen_jobs SET status='running' WHERE id=?", (job_id,)
        )
        conn.commit()

        data = _blitzapi_request_sync("POST", "/v2/search/companies", api_key, search_body)

        results = data.get("results", [])
        now = datetime.now().isoformat()

        for company in results:
            hq = company.get("hq") or {}
            workspace = _workspace_for_company({
                "hq_country": hq.get("country_code", ""),
            }, workspace_override)

            raw_json = json.dumps(company)
            if USE_POSTGRES:
                conn.execute("""
                    INSERT INTO lead_gen_companies
                        (job_id, linkedin_url, linkedin_id, name, about, industry, type, size,
                         employees_on_linkedin, followers, founded_year, domain,
                         hq_country, hq_city, hq_continent, raw_data, workspace, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    job_id,
                    company.get("linkedin_url"),
                    company.get("linkedin_id"),
                    company.get("name"),
                    company.get("about"),
                    company.get("industry"),
                    company.get("type"),
                    company.get("size"),
                    company.get("employees_on_linkedin"),
                    company.get("followers"),
                    company.get("founded_year"),
                    company.get("domain"),
                    hq.get("country_code"),
                    hq.get("city"),
                    hq.get("continent"),
                    raw_json,
                    workspace,
                    now,
                ))
            else:
                conn.execute("""
                    INSERT INTO lead_gen_companies
                        (job_id, linkedin_url, linkedin_id, name, about, industry, type, size,
                         employees_on_linkedin, followers, founded_year, domain,
                         hq_country, hq_city, hq_continent, raw_data, workspace, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    job_id,
                    company.get("linkedin_url"),
                    company.get("linkedin_id"),
                    company.get("name"),
                    company.get("about"),
                    company.get("industry"),
                    company.get("type"),
                    company.get("size"),
                    company.get("employees_on_linkedin"),
                    company.get("followers"),
                    company.get("founded_year"),
                    company.get("domain"),
                    hq.get("country_code"),
                    hq.get("city"),
                    hq.get("continent"),
                    raw_json,
                    workspace,
                    now,
                ))

        results_count = len(results)
        conn.execute("""
            UPDATE lead_gen_jobs
            SET status='completed', results_count=?, completed_at=?
            WHERE id=?
        """, (results_count, datetime.now().isoformat(), job_id))
        conn.commit()

        leadgen_jobs[job_id] = {"status": "completed", "results_count": results_count}
        print(f"[LEADGEN THREAD] Job {job_id} completed: {results_count} companies")

    except Exception as e:
        err_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[LEADGEN THREAD] Job {job_id} failed: {err_msg}")
        try:
            if conn:
                conn.execute(
                    "UPDATE lead_gen_jobs SET status='failed', error=? WHERE id=?",
                    (str(e)[:500], job_id)
                )
                conn.commit()
        except Exception:
            pass
        leadgen_jobs[job_id] = {"status": "failed", "error": str(e)}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Background thread: Import Companies (employee-finder + email enrichment)
# ---------------------------------------------------------------------------

def _run_import_companies(job_id: str, api_key: str, company_ids: List[int], workspace_override: Optional[str]):
    """Background thread: fetch employees, find emails, insert contacts."""
    print(f"[LEADGEN THREAD] Starting import job {job_id} for {len(company_ids)} companies")
    conn = None
    imported = 0
    skipped = 0
    try:
        conn = get_db()
        conn.execute(
            "UPDATE lead_gen_jobs SET status='running' WHERE id=?", (job_id,)
        )
        conn.commit()

        for company_id in company_ids:
            company_row_raw = conn.execute(
                "SELECT id, name, domain, linkedin_url, hq_country, hq_city, industry, size, workspace FROM lead_gen_companies WHERE id=?",
                (company_id,)
            ).fetchone()
            if not company_row_raw:
                continue

            company_row = dict(company_row_raw)
            linkedin_url = company_row.get("linkedin_url", "")
            if not linkedin_url:
                continue

            workspace = workspace_override or company_row.get("workspace") or "US"

            # Step 1: Find employees via employee-finder
            try:
                emp_data = _blitzapi_request_sync("POST", "/v2/search/employee-finder", api_key, {
                    "company_linkedin_url": linkedin_url,
                    "job_level": ["C-Team", "VP", "Director"],
                    "job_function": [],
                    "sales_region": [],
                    "max_results": 5,
                    "page": 1,
                })
                employees = emp_data.get("results", [])
            except Exception as e:
                print(f"[LEADGEN THREAD] employee-finder failed for {linkedin_url}: {e}")
                employees = []

            # Step 2: For each employee, find work email
            for person in employees:
                person_linkedin = person.get("linkedin_url", "")
                if not person_linkedin:
                    skipped += 1
                    continue
                try:
                    email_data = _blitzapi_request_sync("POST", "/v2/enrichment/email", api_key, {
                        "person_linkedin_url": person_linkedin
                    })
                    if not email_data.get("found"):
                        skipped += 1
                        continue
                    email = email_data.get("email", "")
                    if not email:
                        skipped += 1
                        continue
                except Exception as e:
                    print(f"[LEADGEN THREAD] email enrichment failed for {person_linkedin}: {e}")
                    skipped += 1
                    continue

                cid = _insert_contact_from_person(conn, person, company_row, email, workspace)
                if cid:
                    imported += 1
                    conn.commit()
                else:
                    skipped += 1

            # Mark company as imported
            conn.execute(
                "UPDATE lead_gen_companies SET imported=1 WHERE id=?",
                (company_id,)
            )
            conn.commit()

        conn.execute("""
            UPDATE lead_gen_jobs
            SET status='completed', imported_count=?, completed_at=?
            WHERE id=?
        """, (imported, datetime.now().isoformat(), job_id))
        conn.commit()

        leadgen_jobs[job_id] = {"status": "completed", "imported": imported, "skipped": skipped}
        print(f"[LEADGEN THREAD] Import job {job_id} done: {imported} imported, {skipped} skipped")

    except Exception as e:
        err_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[LEADGEN THREAD] Import job {job_id} failed: {err_msg}")
        try:
            if conn:
                conn.execute(
                    "UPDATE lead_gen_jobs SET status='failed', error=? WHERE id=?",
                    (str(e)[:500], job_id)
                )
                conn.commit()
        except Exception:
            pass
        leadgen_jobs[job_id] = {"status": "failed", "error": str(e)}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/api/leadgen/companies/search")
async def search_companies(req: CompanySearchRequest, user: dict = Depends(get_current_user)):
    """Start a background company search job via BlitzAPI."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    # Build BlitzAPI search body — /v2/search/companies correct format
    company_filter: dict = {}
    if req.industries_include or req.industries_exclude:
        company_filter["industry"] = {
            "include": req.industries_include or [],
            "exclude": req.industries_exclude or [],
        }
    if req.keywords_include or req.keywords_exclude:
        company_filter["keywords"] = {
            "description": {
                "include": req.keywords_include or [],
                "exclude": req.keywords_exclude or [],
            }
        }
    hq_filter: dict = {}
    if req.countries:
        hq_filter["country_code"] = req.countries
    if req.states:
        hq_filter["state"] = req.states
    if hq_filter:
        company_filter["hq"] = hq_filter
    if req.employee_range:
        company_filter["employee_range"] = req.employee_range
    if req.company_types:
        company_filter["type"] = req.company_types
    if req.exclude_domains:
        company_filter["exclude_domains"] = req.exclude_domains

    search_body = {
        "company": company_filter,
        "max_results": req.max_results,
    }

    params_json = json.dumps(search_body)

    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (job_id, "company_search", "pending", params_json, req.workspace or "AUTO", user["id"], now))
    else:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (job_id, "company_search", "pending", params_json, req.workspace or "AUTO", user["id"], now))

    conn.commit()
    conn.close()

    leadgen_jobs[job_id] = {"status": "running"}

    thread = threading.Thread(
        target=_run_company_search,
        args=(job_id, api_key, search_body, req.workspace),
        daemon=True
    )
    thread.start()
    print(f"[LEADGEN] Started company search job {job_id}")

    return {"job_id": job_id, "status": "running"}


@router.get("/api/leadgen/jobs")
def list_jobs(workspace: Optional[str] = None, user: dict = Depends(get_current_user)):
    """List all lead generation jobs ordered by creation date."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    where = "1=1"
    params = []
    if workspace:
        where = "workspace=?"
        params.append(workspace.upper())

    if USE_POSTGRES:
        rows = conn.execute(f"""
            SELECT id, job_type, status, results_count, imported_count, credits_used,
                   workspace, created_at, completed_at, error, parameters, approval_status
            FROM lead_gen_jobs WHERE {where}
            ORDER BY created_at DESC LIMIT 100
        """, params).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT id, job_type, status, results_count, imported_count, credits_used,
                   workspace, created_at, completed_at, error, parameters, approval_status
            FROM lead_gen_jobs WHERE {where}
            ORDER BY created_at DESC LIMIT 100
        """, params).fetchall()
    conn.close()
    return {"data": [dict(r) for r in rows]}


@router.get("/api/leadgen/jobs/{job_id}")
def get_job(job_id: str, user: dict = Depends(get_current_user)):
    """Get job details plus first 10 companies."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    job = conn.execute("""
        SELECT id, job_type, status, results_count, imported_count, credits_used,
               workspace, created_at, completed_at, error
        FROM lead_gen_jobs WHERE id=?
    """, (job_id,)).fetchone()

    if not job:
        conn.close()
        raise HTTPException(404, "Job not found")

    companies = conn.execute("""
        SELECT id, name, about, industry, type, size, hq_country, hq_city, domain,
               linkedin_url, workspace, imported
        FROM lead_gen_companies
        WHERE job_id=?
        ORDER BY id
        LIMIT 100
    """, (job_id,)).fetchall()
    conn.close()

    return {
        "job": dict(job),
        "companies": [dict(c) for c in companies],
    }


@router.post("/api/leadgen/companies/import")
async def import_companies(req: ImportCompaniesRequest, user: dict = Depends(get_current_user)):
    """Import selected companies: find employees and emails, create contacts."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    if not req.company_ids:
        raise HTTPException(400, "No company_ids provided")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, workspace, created_by, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (job_id, "import_contacts", "pending", req.workspace or "AUTO", user["id"], now))
    else:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, workspace, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (job_id, "import_contacts", "pending", req.workspace or "AUTO", user["id"], now))

    conn.commit()
    conn.close()

    leadgen_jobs[job_id] = {"status": "running"}

    thread = threading.Thread(
        target=_run_import_companies,
        args=(job_id, api_key, req.company_ids, req.workspace),
        daemon=True
    )
    thread.start()
    print(f"[LEADGEN] Started import job {job_id} for {len(req.company_ids)} companies")

    return {"job_id": job_id, "status": "running"}


@router.post("/api/leadgen/waterfall-direct")
async def waterfall_direct(req: WaterfallDirectRequest, user: dict = Depends(get_current_user)):
    """Run waterfall ICP search directly from a LinkedIn URL (no DB company required)."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")

    job_levels = req.job_levels or ["C-Level", "VP", "Director"]

    # Map job levels to BlitzAPI cascade format
    level_title_map = {
        "C-Level": ["CEO", "Founder", "CFO", "COO", "CTO", "CMO", "CRO", "CISO", "CPO"],
        "VP": ["VP Sales", "VP Engineering", "VP Marketing", "VP Product", "Vice President"],
        "Director": ["Director of Sales", "Director of Engineering", "Director of Marketing", "Director"],
        "Manager": ["Manager", "Senior Manager", "Head of"],
    }

    cascade = []
    for level in job_levels:
        titles = level_title_map.get(level, [level])
        cascade.append({
            "include_title": titles,
            "exclude_title": [],
            "location": ["WORLD"],
            "include_headline_search": False,
        })

    waterfall_body = {
        "company_linkedin_url": req.company_linkedin_url,
        "cascade": cascade,
        "max_results": req.max_per_company,
    }

    data = await _blitzapi_request("POST", "/v2/search/waterfall-icp-keyword", api_key, waterfall_body)
    conn.close()

    results = data.get("results", [])
    imported_contacts = []

    conn2 = get_db()
    api_key2 = _get_blitzapi_key(conn2)

    for item in results:
        person = item.get("person", {})
        person_linkedin = person.get("linkedin_url", "")
        if not person_linkedin:
            continue

        try:
            email_data = await _blitzapi_request("POST", "/v2/enrichment/email", api_key2, {
                "person_linkedin_url": person_linkedin
            })
            email = email_data.get("email", "") if email_data.get("found") else ""
        except Exception:
            email = ""

        company_row = {
            "name": "",
            "domain": "",
            "linkedin_url": req.company_linkedin_url,
        }

        contact_info = {
            "full_name": person.get("full_name", ""),
            "job_title": person.get("job_title", ""),
            "headline": person.get("headline", ""),
            "linkedin_url": person_linkedin,
            "email": email,
            "icp_level": item.get("icp"),
        }

        if email:
            cid = _insert_contact_from_person(conn2, person, company_row, email, "US")
            contact_info["imported"] = cid is not None
            contact_info["contact_id"] = cid
        else:
            contact_info["imported"] = False

        imported_contacts.append(contact_info)

    conn2.commit()
    conn2.close()

    imported_count = sum(1 for c in imported_contacts if c.get("imported"))
    return {
        "results": imported_contacts,
        "total": len(imported_contacts),
        "imported": imported_count,
    }


@router.post("/api/leadgen/contacts/find-email")
async def find_contact_email(req: FindEmailRequest, user: dict = Depends(get_current_user)):
    """Find work email for an existing contact using BlitzAPI."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")

    contact = conn.execute(
        "SELECT id, person_linkedin_url, email FROM contacts WHERE id=?",
        (req.contact_id,)
    ).fetchone()
    if not contact:
        conn.close()
        raise HTTPException(404, "Contact not found")

    person_linkedin = contact[1]
    if not person_linkedin:
        conn.close()
        raise HTTPException(400, "Contact has no LinkedIn URL to look up email")

    data = await _blitzapi_request("POST", "/v2/enrichment/email", api_key, {
        "person_linkedin_url": person_linkedin
    })

    email = data.get("email", "") if data.get("found") else ""
    if email:
        now = datetime.now().isoformat()
        conn.execute(
            "UPDATE contacts SET email=?, email_status='Valid', enrichment_source='blitzapi', updated_at=? WHERE id=?",
            (email, now, req.contact_id)
        )
        conn.commit()

    conn.close()
    return {"found": data.get("found", False), "email": email}


@router.get("/api/leadgen/credits")
async def get_credits(user: dict = Depends(get_current_user)):
    """Get BlitzAPI account info and remaining credits."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured. Add blitzapi_api_key in Settings.")
    conn.close()

    data = await _blitzapi_request("GET", "/v2/account/key-info", api_key)
    return {
        "valid": data.get("valid", False),
        "remaining_credits": data.get("remaining_credits", 0),
        "next_reset_at": data.get("next_reset_at"),
        "allowed_apis": data.get("allowed_apis", []),
        "active_plans": data.get("active_plans", []),
    }


# ---------------------------------------------------------------------------
# Two-Stage Pipeline — Stage 2: Find Contacts (staging table)
# ---------------------------------------------------------------------------

class FindContactsRequest(BaseModel):
    company_ids: List[int]
    job_levels: Optional[List[str]] = ["C-Level", "VP", "Director", "Manager"]
    max_per_company: int = 5


class ApproveContactsRequest(BaseModel):
    contact_ids: Optional[List[int]] = None   # None = approve all pending for job
    job_id: Optional[str] = None


def _run_find_contacts(job_id: str, api_key: str, companies: list, job_levels: list, max_per_company: int):
    """Background thread: Waterfall ICP + email enrichment per company. Stores in lead_gen_contacts staging.
    
    Uses /v2/search/waterfall-icp-keyword (NOT employee-finder).
    Valid job_level values: C-Team, Director, Manager, Other, Staff, VP
    icp field in response = which cascade tier matched (1=owner, 2=GM, 3=ops)
    """
    conn = None
    try:
        conn = get_db()
        conn.execute("UPDATE lead_gen_jobs SET status='running' WHERE id=?", (job_id,))
        conn.commit()

        total_found = 0
        now = datetime.now().isoformat()

        # Standard cascade for home services SMBs (from Hermes research)
        waterfall_cascade = [
            {
                "include_title": ["owner", "CEO", "founder", "chief executive", "co-founder", "co-owner"],
                "exclude_title": ["assistant", "intern", "junior", "associate"],
                "location": ["WORLD"],
                "include_headline_search": True
            },
            {
                "include_title": ["president", "general manager", "managing director", "principal"],
                "exclude_title": ["assistant", "intern"],
                "location": ["WORLD"],
                "include_headline_search": False
            },
            {
                "include_title": ["operations manager", "COO", "director of operations", "VP operations"],
                "exclude_title": ["assistant", "intern"],
                "location": ["WORLD"],
                "include_headline_search": False
            }
        ]

        for company in companies:
            company_id = company["id"]
            linkedin_url = company.get("linkedin_url") or ""
            company_name = company.get("name") or ""
            company_domain = company.get("domain") or ""
            workspace = company.get("workspace") or "US"

            if not linkedin_url:
                continue

            try:
                # Step 1: Waterfall ICP — find best decision maker
                waterfall_body = {
                    "company_linkedin_url": linkedin_url,
                    "cascade": waterfall_cascade,
                    "max_results": max_per_company,
                }
                data = _blitzapi_request_sync("POST", "/v2/search/waterfall-icp-keyword", api_key, waterfall_body)
                results = data.get("results", [])

                for result in results:
                    icp_tier = result.get("icp")  # 1=owner, 2=GM, 3=ops
                    person = result.get("person") or {}
                    person_linkedin = person.get("linkedin_url") or ""

                    # Step 2: Email enrichment
                    email = person.get("email") or ""
                    if not email and person_linkedin:
                        try:
                            email_data = _blitzapi_request_sync(
                                "POST", "/v2/enrichment/email", api_key,
                                {"person_linkedin_url": person_linkedin}
                            )
                            if email_data.get("found"):
                                email = email_data.get("email") or ""
                        except Exception as e:
                            print(f"[LEADGEN] Email enrichment failed for {person_linkedin}: {e}")

                    company_industry = company_row.get("industry", "")
                    company_city = company_row.get("hq_city", "")
                    company_state = ""  # BlitzAPI uses hq_city, state parsed from it if needed
                    employee_bucket = company_row.get("size", "")

                    if USE_POSTGRES:
                        conn.execute("""
                            INSERT INTO lead_gen_contacts
                            (job_id, company_id, first_name, last_name, email, title,
                             linkedin_url, company_name, company_domain, workspace,
                             icp_tier, blitz_company_linkedin, blitz_person_linkedin,
                             industry, company_city, company_state, employee_bucket,
                             blitz_enriched_at, status, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)
                        """, (
                            job_id, company_id,
                            person.get("first_name"), person.get("last_name"),
                            email, person.get("title") or person.get("headline"),
                            person_linkedin, company_name, company_domain, workspace,
                            icp_tier, linkedin_url, person_linkedin,
                            company_industry, company_city, company_state, employee_bucket,
                            now, now
                        ))
                    else:
                        conn.execute("""
                            INSERT INTO lead_gen_contacts
                            (job_id, company_id, first_name, last_name, email, title,
                             linkedin_url, company_name, company_domain, workspace,
                             icp_tier, blitz_company_linkedin, blitz_person_linkedin,
                             industry, company_city, company_state, employee_bucket,
                             blitz_enriched_at, status, created_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
                        """, (
                            job_id, company_id,
                            person.get("first_name"), person.get("last_name"),
                            email, person.get("title") or person.get("headline"),
                            person_linkedin, company_name, company_domain, workspace,
                            icp_tier, linkedin_url, person_linkedin,
                            company_industry, company_city, company_state, employee_bucket,
                            now, now
                        ))
                    conn.commit()
                    total_found += 1

            except Exception as e:
                print(f"[LEADGEN] Error finding contacts for {company_name}: {e}")
                continue

        conn.execute(
            "UPDATE lead_gen_jobs SET status='awaiting_approval', results_count=? WHERE id=?",
            (total_found, job_id)
        )
        conn.commit()

    except Exception as e:
        print(f"[LEADGEN CONTACTS THREAD] Error: {e}\n{traceback.format_exc()}")
        if conn:
            try:
                conn.execute("UPDATE lead_gen_jobs SET status='failed', error=? WHERE id=?",
                             (str(e)[:500], job_id))
                conn.commit()
            except Exception:
                pass
    finally:
        if conn:
            conn.close()


@router.post("/api/leadgen/companies/find-contacts")
async def find_contacts_for_companies(req: FindContactsRequest, user: dict = Depends(get_current_user)):
    """Stage 2: Find ICP contacts for selected companies. Stores in staging table, NOT contacts table yet."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured.")

    # Fetch selected companies
    if USE_POSTGRES:
        placeholders = ",".join(["%s"] * len(req.company_ids))
    else:
        placeholders = ",".join(["?"] * len(req.company_ids))

    rows = conn.execute(
        f"SELECT id, linkedin_url, name, domain, workspace FROM lead_gen_companies WHERE id IN ({placeholders})",
        req.company_ids
    ).fetchall()

    if not rows:
        conn.close()
        raise HTTPException(404, "No companies found with those IDs")

    companies = [
        {"id": r[0], "linkedin_url": r[1], "name": r[2], "domain": r[3], "workspace": r[4]}
        for r in rows
    ]

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (%s,'contact_discovery','pending',%s,'AUTO',%s,%s)
        """, (job_id, json.dumps({"company_count": len(companies)}), user["id"], now))
    else:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (?,'contact_discovery','pending',?,'AUTO',?,?)
        """, (job_id, json.dumps({"company_count": len(companies)}), user["id"], now))

    conn.commit()
    conn.close()

    leadgen_jobs[job_id] = {"status": "running"}

    thread = threading.Thread(
        target=_run_find_contacts,
        args=(job_id, api_key, companies, req.job_levels, req.max_per_company),
        daemon=True
    )
    thread.start()

    return {"job_id": job_id, "status": "running", "company_count": len(companies)}


@router.get("/api/leadgen/contacts/preview")
async def get_contacts_preview(job_id: str, user: dict = Depends(get_current_user)):
    """Get staged contacts for review before approving import."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    rows = conn.execute("""
        SELECT id, job_id, company_id, first_name, last_name, email, title,
               linkedin_url, company_name, company_domain, workspace, status, contact_id
        FROM lead_gen_contacts WHERE job_id=? ORDER BY company_name, last_name
    """, (job_id,)).fetchall()
    conn.close()

    contacts = [
        {
            "id": r[0], "job_id": r[1], "company_id": r[2],
            "first_name": r[3], "last_name": r[4], "email": r[5],
            "title": r[6], "linkedin_url": r[7],
            "company_name": r[8], "company_domain": r[9],
            "workspace": r[10], "status": r[11], "contact_id": r[12]
        }
        for r in rows
    ]

    pending = sum(1 for c in contacts if c["status"] == "pending")
    approved = sum(1 for c in contacts if c["status"] == "approved")

    return {
        "contacts": contacts,
        "total": len(contacts),
        "pending": pending,
        "approved": approved,
    }


@router.post("/api/leadgen/contacts/approve")
async def approve_contacts(req: ApproveContactsRequest, user: dict = Depends(get_current_user)):
    """Approve staged contacts — moves them to the main contacts table."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()

    # Get contacts to approve
    if req.contact_ids:
        if USE_POSTGRES:
            placeholders = ",".join(["%s"] * len(req.contact_ids))
        else:
            placeholders = ",".join(["?"] * len(req.contact_ids))
        rows = conn.execute(
            f"SELECT * FROM lead_gen_contacts WHERE id IN ({placeholders}) AND status='pending'",
            req.contact_ids
        ).fetchall()
    elif req.job_id:
        rows = conn.execute(
            "SELECT * FROM lead_gen_contacts WHERE job_id=? AND status='pending'",
            (req.job_id,)
        ).fetchall()
    else:
        conn.close()
        raise HTTPException(400, "Provide contact_ids or job_id")

    if not rows:
        conn.close()
        return {"imported": 0, "skipped_duplicates": 0}

    imported = 0
    skipped = 0
    now = datetime.now().isoformat()

    for r in rows:
        lgc_id = r[0]
        email = r[5] or ""

        # Dedup check
        if email:
            existing = conn.execute("SELECT id FROM contacts WHERE email=?", (email,)).fetchone()
            if existing:
                conn.execute("UPDATE lead_gen_contacts SET status='rejected' WHERE id=?", (lgc_id,))
                conn.commit()
                skipped += 1
                continue

        # Insert into main contacts table
        workspace = r[10] or "US"
        # Column indices: 0=id,1=job_id,2=company_id,3=first_name,4=last_name,5=email,
        # 6=title,7=linkedin_url,8=company_name,9=company_domain,10=workspace,
        # 11=status,12=contact_id,13=icp_tier,14=blitz_company_linkedin,15=blitz_person_linkedin
        icp_tier = r[13] if len(r) > 13 else None
        blitz_co_li = r[14] if len(r) > 14 else None
        blitz_p_li = r[15] if len(r) > 15 else None

        if USE_POSTGRES:
            cur = conn.execute("""
                INSERT INTO contacts (first_name, last_name, email, title, person_linkedin_url,
                    company, domain, reachinbox_workspace, enrichment_source, pipeline_stage,
                    icp_tier, blitz_company_linkedin, blitz_person_linkedin, blitz_enriched_at, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'blitzapi','new',%s,%s,%s,NOW(),%s)
                RETURNING id
            """, (r[3], r[4], email, r[6], r[7], r[8], r[9], workspace,
                  icp_tier, blitz_co_li, blitz_p_li, now))
            contact_id = cur.fetchone()[0]
        else:
            cur = conn.execute("""
                INSERT INTO contacts (first_name, last_name, email, title, person_linkedin_url,
                    company, domain, reachinbox_workspace, enrichment_source, pipeline_stage,
                    icp_tier, blitz_company_linkedin, blitz_person_linkedin, blitz_enriched_at, created_at)
                VALUES (?,?,?,?,?,?,?,?,'blitzapi','new',?,?,?,?,?)
            """, (r[3], r[4], email, r[6], r[7], r[8], r[9], workspace,
                  icp_tier, blitz_co_li, blitz_p_li, now, now))
            contact_id = cur.lastrowid

        conn.execute(
            "UPDATE lead_gen_contacts SET status='approved', contact_id=? WHERE id=?",
            (contact_id, lgc_id)
        )
        conn.commit()
        imported += 1

    # Update job approval status
    if req.job_id:
        conn.execute(
            "UPDATE lead_gen_jobs SET approval_status='approved', approved_at=?, approved_by=? WHERE id=?",
            (now, user["id"], req.job_id)
        )
        conn.commit()

    conn.close()

    return {
        "imported": imported,
        "skipped_duplicates": skipped,
        "total_processed": imported + skipped,
    }


@router.post("/api/leadgen/contacts/reject")
async def reject_contacts(req: ApproveContactsRequest, user: dict = Depends(get_current_user)):
    """Reject staged contacts — removes them from the pipeline."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    if req.contact_ids:
        if USE_POSTGRES:
            placeholders = ",".join(["%s"] * len(req.contact_ids))
        else:
            placeholders = ",".join(["?"] * len(req.contact_ids))
        conn.execute(
            f"UPDATE lead_gen_contacts SET status='rejected' WHERE id IN ({placeholders})",
            req.contact_ids
        )
    elif req.job_id:
        conn.execute(
            "UPDATE lead_gen_contacts SET status='rejected' WHERE job_id=? AND status='pending'",
            (req.job_id,)
        )
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Bulk Run — Full Pipeline (Agent-callable overnight run)
# ---------------------------------------------------------------------------

class BulkRunRequest(BaseModel):
    vertical: str                              # "roofing" | "hvac" | "plumbing" | "landscaping" | custom
    country: str = "US"
    employee_range: Optional[List[str]] = ["11-50", "51-200"]
    max_companies: int = 500                   # safety cap
    max_per_company: int = 1                   # usually 1 — the best ICP per company
    workspace: Optional[str] = None            # override auto-detection
    keywords_include: Optional[List[str]] = None  # override preset includes
    keywords_exclude: Optional[List[str]] = None  # override preset excludes
    industry_include: Optional[List[str]] = None  # post-fetch filter: only keep companies with these industries
    industry_exclude: Optional[List[str]] = None  # post-fetch filter: drop companies with these industries

# Preset verticals from Hermes research
VERTICAL_PRESETS = {
    "roofing": {
        "keywords_include": ["roofing", "roofing contractor", "roofing company", "roofing services"],
        "keywords_exclude": ["saas", "agency", "marketing", "technology", "media", "wholesale",
                             "distributor", "software", "recruiting", "staffing", "manufacturer",
                             "magazine", "association", "expo", "university", "college", "franchise",
                             "advertising", "chemical", "supply", "materials"],
    },
    "hvac": {
        "keywords_include": ["hvac", "heating and cooling", "air conditioning contractor", "hvac services"],
        "keywords_exclude": ["parts", "wholesale", "distributor", "manufacturer", "association",
                             "magazine", "software", "saas", "technology", "staffing"],
    },
    "plumbing": {
        "keywords_include": ["plumbing", "plumbing contractor", "plumbing services", "plumber"],
        "keywords_exclude": ["parts", "wholesale", "distributor", "manufacturer", "association",
                             "software", "saas", "technology", "staffing", "magazine"],
    },
    "landscaping": {
        "keywords_include": ["landscaping", "lawn care", "landscape contractor", "landscaping services"],
        "keywords_exclude": ["wholesale", "distributor", "manufacturer", "association",
                             "software", "saas", "technology", "staffing", "magazine", "design school"],
    },
}


def _run_bulk_pipeline(job_id: str, api_key: str, req_data: dict):
    """Background thread: full pipeline — search companies → waterfall ICP → email enrichment → stage."""
    conn = None
    stats = {"companies_searched": 0, "icp_found": 0, "emails_found": 0, "dupes_skipped": 0, "staged": 0}

    try:
        conn = get_db()
        conn.execute("UPDATE lead_gen_jobs SET status='running' WHERE id=?", (job_id,))
        conn.commit()

        vertical = req_data["vertical"]
        country = req_data.get("country", "US")
        employee_range = req_data.get("employee_range", ["11-50", "51-200"])
        max_companies = req_data.get("max_companies", 500)
        max_per_company = req_data.get("max_per_company", 1)
        workspace_override = req_data.get("workspace")

        preset = VERTICAL_PRESETS.get(vertical.lower(), {})
        keywords_include = req_data.get("keywords_include") or preset.get("keywords_include", [vertical])
        keywords_exclude = req_data.get("keywords_exclude") or preset.get("keywords_exclude", [])

        now = datetime.now().isoformat()

        # Load industry filters
        industry_include = req_data.get("industry_include") or preset.get("industry_include")
        industry_exclude = req_data.get("industry_exclude") or preset.get("industry_exclude")
        skipped_irrelevant = 0

        # Phase 1: Paginate company search
        cursor = None
        all_companies = []
        while len(all_companies) < max_companies:
            body = {
                "company": {
                    "keywords": {"include": keywords_include, "exclude": keywords_exclude},
                    "hq": {"country_code": [country]},
                    "employee_range": employee_range,
                },
                "max_results": min(50, max_companies - len(all_companies)),
            }
            if cursor:
                body["cursor"] = cursor

            try:
                data = _blitzapi_request_sync("POST", "/v2/search/companies", api_key, body)
            except Exception as e:
                print(f"[BULK RUN] Company search failed: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for company in results:
                # Post-fetch industry relevance filter
                company_industry = (company.get("industry") or "").lower()
                company_name = (company.get("name") or "").lower()
                company_about = (company.get("about") or "").lower()
                
                if industry_include:
                    if not any(ind.lower() in company_industry for ind in industry_include):
                        skipped_irrelevant += 1
                        continue
                
                if industry_exclude:
                    if any(ind.lower() in company_industry for ind in industry_exclude):
                        skipped_irrelevant += 1
                        continue
                
                # Name-based noise filter: skip if company name contains AI/tech buzzwords
                noise_names = ["ai", "software", "tech", "digital", "cloud", "data", "cyber",
                               "analytics", "saas", "app", "platform", "labs", "ventures"]
                if any(f" {n} " in f" {company_name} " or company_name.endswith(f" {n}") 
                       or company_name.startswith(f"{n} ") for n in noise_names):
                    if not any(v in company_about for v in [vertical.lower()]):
                        skipped_irrelevant += 1
                        continue

                hq = company.get("hq") or {}
                workspace = workspace_override or _workspace_for_company(
                    {"hq_country": hq.get("country_code", "")}, None
                )
                # Store company
                if USE_POSTGRES:
                    cur2 = conn.execute("""
                        INSERT INTO lead_gen_companies
                        (job_id, linkedin_url, linkedin_id, name, about, industry, type, size,
                         employees_on_linkedin, followers, founded_year, domain,
                         hq_country, hq_city, hq_continent, raw_data, workspace, created_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id
                    """, (job_id, company.get("linkedin_url"), company.get("linkedin_id"),
                          company.get("name"), company.get("about"), company.get("industry"),
                          company.get("type"), company.get("size"), company.get("employees_on_linkedin"),
                          company.get("followers"), company.get("founded_year"), company.get("domain"),
                          hq.get("country_code"), hq.get("city"), hq.get("continent"),
                          json.dumps(company), workspace, now))
                    company_db_id = cur2.fetchone()[0]
                else:
                    cur2 = conn.execute("""
                        INSERT INTO lead_gen_companies
                        (job_id, linkedin_url, linkedin_id, name, about, industry, type, size,
                         employees_on_linkedin, followers, founded_year, domain,
                         hq_country, hq_city, hq_continent, raw_data, workspace, created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (job_id, company.get("linkedin_url"), company.get("linkedin_id"),
                          company.get("name"), company.get("about"), company.get("industry"),
                          company.get("type"), company.get("size"), company.get("employees_on_linkedin"),
                          company.get("followers"), company.get("founded_year"), company.get("domain"),
                          hq.get("country_code"), hq.get("city"), hq.get("continent"),
                          json.dumps(company), workspace, now))
                    company_db_id = cur2.lastrowid

                conn.commit()
                all_companies.append({
                    "id": company_db_id,
                    "linkedin_url": company.get("linkedin_url"),
                    "name": company.get("name"),
                    "domain": company.get("domain"),
                    "workspace": workspace,
                })
                stats["companies_searched"] += 1

            cursor = data.get("cursor")
            if not cursor:
                break

        print(f"[BULK RUN] {job_id}: Found {stats['companies_searched']} companies, starting ICP search...")

        # Phase 2: Waterfall ICP + email per company (sequential for now)
        waterfall_cascade = [
            {"include_title": ["owner", "CEO", "founder", "chief executive", "co-founder", "co-owner"],
             "exclude_title": ["assistant", "intern", "junior"], "location": ["WORLD"], "include_headline_search": True},
            {"include_title": ["president", "general manager", "managing director", "principal"],
             "exclude_title": ["assistant", "intern"], "location": ["WORLD"], "include_headline_search": False},
            {"include_title": ["operations manager", "COO", "director of operations"],
             "exclude_title": ["assistant", "intern"], "location": ["WORLD"], "include_headline_search": False},
        ]

        for company in all_companies:
            linkedin_url = company.get("linkedin_url") or ""
            if not linkedin_url:
                continue
            try:
                wf_data = _blitzapi_request_sync("POST", "/v2/search/waterfall-icp-keyword", api_key, {
                    "company_linkedin_url": linkedin_url,
                    "cascade": waterfall_cascade,
                    "max_results": max_per_company,
                })
                for result in wf_data.get("results", []):
                    icp_tier = result.get("icp")
                    person = result.get("person") or {}
                    person_li = person.get("linkedin_url") or ""
                    stats["icp_found"] += 1

                    # Email enrichment
                    email = person.get("email") or ""
                    if not email and person_li:
                        try:
                            ed = _blitzapi_request_sync("POST", "/v2/enrichment/email", api_key,
                                                        {"person_linkedin_url": person_li})
                            if ed.get("found"):
                                email = ed.get("email") or ""
                                stats["emails_found"] += 1
                        except Exception:
                            pass

                    # Dedup check
                    if email:
                        existing = conn.execute("SELECT id FROM contacts WHERE email=?", (email,)).fetchone()
                        if existing:
                            stats["dupes_skipped"] += 1
                            continue

                    # Stage in lead_gen_contacts (with company metadata)
                    c_industry = company.get("industry", "")
                    c_city = company.get("hq_city", "")
                    c_state = ""
                    c_employees = company.get("size", "")

                    if USE_POSTGRES:
                        conn.execute("""
                            INSERT INTO lead_gen_contacts
                            (job_id, company_id, first_name, last_name, email, title, linkedin_url,
                             company_name, company_domain, workspace, icp_tier,
                             blitz_company_linkedin, blitz_person_linkedin,
                             industry, company_city, company_state, employee_bucket,
                             blitz_enriched_at, status, created_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)
                        """, (job_id, company["id"], person.get("first_name"), person.get("last_name"),
                              email, person.get("title") or person.get("headline"), person_li,
                              company.get("name"), company.get("domain"), company.get("workspace"),
                              icp_tier, linkedin_url, person_li,
                              c_industry, c_city, c_state, c_employees,
                              now, now))
                    else:
                        conn.execute("""
                            INSERT INTO lead_gen_contacts
                            (job_id, company_id, first_name, last_name, email, title, linkedin_url,
                             company_name, company_domain, workspace, icp_tier,
                             blitz_company_linkedin, blitz_person_linkedin,
                             industry, company_city, company_state, employee_bucket,
                             blitz_enriched_at, status, created_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)
                        """, (job_id, company["id"], person.get("first_name"), person.get("last_name"),
                              email, person.get("title") or person.get("headline"), person_li,
                              company.get("name"), company.get("domain"), company.get("workspace"),
                              icp_tier, linkedin_url, person_li,
                              c_industry, c_city, c_state, c_employees,
                              now, now))
                    conn.commit()
                    stats["staged"] += 1

            except Exception as e:
                print(f"[BULK RUN] ICP failed for {company.get('name')}: {e}")
                continue

        # Mark job awaiting approval
        stats_json = json.dumps(stats)
        conn.execute("""
            UPDATE lead_gen_jobs SET status='awaiting_approval', results_count=?,
            parameters=?, completed_at=? WHERE id=?
        """, (stats["staged"], stats_json, datetime.now().isoformat(), job_id))
        conn.commit()
        print(f"[BULK RUN] {job_id} complete: {stats}")

    except Exception as e:
        print(f"[BULK RUN] Fatal error: {e}\n{traceback.format_exc()}")
        if conn:
            try:
                conn.execute("UPDATE lead_gen_jobs SET status='failed', error=? WHERE id=?",
                             (str(e)[:500], job_id))
                conn.commit()
            except Exception:
                pass
    finally:
        if conn:
            conn.close()


@router.post("/api/leadgen/bulk-run")
async def start_bulk_run(req: BulkRunRequest, user: dict = Depends(get_current_user)):
    """Start a full autonomous pipeline run: search → waterfall ICP → email → stage for approval.
    
    This is the primary endpoint Hermes calls. Results land in lead_gen_contacts with status='pending'.
    Human approves via POST /api/leadgen/contacts/approve.
    """
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    api_key = _get_blitzapi_key(conn)
    if not api_key:
        conn.close()
        raise HTTPException(400, "BlitzAPI key not configured.")

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    label = f"{req.vertical.title()} {req.country} — bulk run"

    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (%s,'bulk_run','pending',%s,%s,%s,%s)
        """, (job_id, json.dumps({"vertical": req.vertical, "country": req.country,
              "max_companies": req.max_companies, "label": label}),
              req.workspace or req.country, user["id"], now))
    else:
        conn.execute("""
            INSERT INTO lead_gen_jobs (id, job_type, status, parameters, workspace, created_by, created_at)
            VALUES (?,'bulk_run','pending',?,?,?,?)
        """, (job_id, json.dumps({"vertical": req.vertical, "country": req.country,
              "max_companies": req.max_companies, "label": label}),
              req.workspace or req.country, user["id"], now))
    conn.commit()
    conn.close()

    leadgen_jobs[job_id] = {"status": "running"}
    thread = threading.Thread(
        target=_run_bulk_pipeline,
        args=(job_id, api_key, req.dict()),
        daemon=True
    )
    thread.start()

    return {
        "job_id": job_id,
        "status": "running",
        "vertical": req.vertical,
        "country": req.country,
        "max_companies": req.max_companies,
        "message": f"Bulk run started for {req.vertical} in {req.country}. Poll /api/leadgen/jobs/{job_id} for status."
    }
