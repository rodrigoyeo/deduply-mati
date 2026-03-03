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
                "SELECT id, name, domain, linkedin_url, hq_country, workspace FROM lead_gen_companies WHERE id=?",
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
def list_jobs(user: dict = Depends(get_current_user)):
    """List all lead generation jobs ordered by creation date."""
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    rows = conn.execute("""
        SELECT id, job_type, status, results_count, imported_count, credits_used,
               workspace, created_at, completed_at, error
        FROM lead_gen_jobs
        ORDER BY created_at DESC
        LIMIT 100
    """).fetchall()
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
