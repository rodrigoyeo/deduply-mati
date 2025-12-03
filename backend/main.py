#!/usr/bin/env python3
"""
Deduply v5.1 - Cold Email Operations Platform
FastAPI Backend with full CSV support, proper relationships, duplicate detection
"""

from fastapi import FastAPI, HTTPException, Request, Query, UploadFile, File, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import sqlite3
import pandas as pd
import json
import io
import hashlib
import secrets
import os

DATABASE_PATH = os.getenv("DATABASE_PATH", "deduply.db")

app = FastAPI(title="Deduply API", version="5.1")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def get_db():
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL, name TEXT, role TEXT DEFAULT 'member',
        api_token TEXT UNIQUE, is_active BOOLEAN DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, email TEXT,
        title TEXT, headline TEXT, company TEXT, seniority TEXT, first_phone TEXT, corporate_phone TEXT,
        employees INTEGER, employee_bucket TEXT, industry TEXT, keywords TEXT,
        person_linkedin_url TEXT, website TEXT, domain TEXT, company_linkedin_url TEXT,
        company_city TEXT, company_state TEXT, company_country TEXT, region TEXT,
        country_strategy TEXT,
        outreach_lists TEXT, campaigns_assigned TEXT, status TEXT DEFAULT 'Lead',
        email_status TEXT DEFAULT 'Unknown', times_contacted INTEGER DEFAULT 0,
        last_contacted_at TIMESTAMP, opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        notes TEXT, source_file TEXT, is_duplicate BOOLEAN DEFAULT 0, duplicate_of INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, description TEXT,
        country TEXT, status TEXT DEFAULT 'Active', total_leads INTEGER DEFAULT 0,
        emails_sent INTEGER DEFAULT 0, emails_opened INTEGER DEFAULT 0, emails_clicked INTEGER DEFAULT 0,
        emails_replied INTEGER DEFAULT 0, emails_bounced INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0, click_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS email_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, variant TEXT DEFAULT 'A',
        step_type TEXT DEFAULT 'Main', subject TEXT, body TEXT,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_clicked INTEGER DEFAULT 0,
        times_replied INTEGER DEFAULT 0, open_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        is_winner BOOLEAN DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS template_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_replied INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings INTEGER DEFAULT 0,
        FOREIGN KEY (template_id) REFERENCES email_templates(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(template_id, campaign_id))""")

    # Migration: Add opportunities and meetings columns if they don't exist
    try:
        cur.execute("ALTER TABLE template_campaigns ADD COLUMN opportunities INTEGER DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE template_campaigns ADD COLUMN meetings INTEGER DEFAULT 0")
    except: pass
    
    cur.execute("""CREATE TABLE IF NOT EXISTS outreach_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        description TEXT, contact_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, event_type TEXT, email TEXT,
        campaign_name TEXT, template_id INTEGER, payload TEXT, processed BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        token = secrets.token_urlsafe(32)
        pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
        cur.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                   ("admin@deduply.io", pwd_hash, "Admin", "admin", token))
        print(f"Created admin: admin@deduply.io / admin123")
    
    for idx in ["CREATE INDEX IF NOT EXISTS idx_email ON contacts(email)",
                "CREATE INDEX IF NOT EXISTS idx_status ON contacts(status)",
                "CREATE INDEX IF NOT EXISTS idx_dup ON contacts(is_duplicate)",
                "CREATE INDEX IF NOT EXISTS idx_country_strategy ON contacts(country_strategy)"]:
        try: cur.execute(idx)
        except: pass

    # Migration: Add new columns to existing tables
    migrations = [
        "ALTER TABLE contacts ADD COLUMN country_strategy TEXT",
        "ALTER TABLE campaigns ADD COLUMN country TEXT"
    ]
    for m in migrations:
        try: cur.execute(m)
        except: pass  # Column already exists

    conn.commit(); conn.close()

init_db()

class UserLogin(BaseModel):
    email: str
    password: str

class UserCreate(BaseModel):
    email: str
    password: str
    name: Optional[str] = None
    role: str = "member"

class ContactCreate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    seniority: Optional[str] = None
    first_phone: Optional[str] = None
    company_country: Optional[str] = None
    outreach_lists: Optional[str] = None
    campaigns_assigned: Optional[str] = None
    status: str = "Lead"
    notes: Optional[str] = None

class ContactUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    seniority: Optional[str] = None
    first_phone: Optional[str] = None
    company_country: Optional[str] = None
    outreach_lists: Optional[str] = None
    campaigns_assigned: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None

class BulkUpdateRequest(BaseModel):
    contact_ids: Optional[List[int]] = None
    filters: Optional[dict] = None  # For "select all" with filters
    field: str  # The field to update
    value: Optional[str] = None
    action: Optional[str] = None  # add, remove, set, delete

class CampaignCreate(BaseModel):
    name: str
    description: Optional[str] = None
    country: Optional[str] = None
    status: str = "Active"

class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    country: Optional[str] = None
    status: Optional[str] = None
    emails_sent: Optional[int] = None
    emails_opened: Optional[int] = None
    emails_clicked: Optional[int] = None
    emails_replied: Optional[int] = None
    emails_bounced: Optional[int] = None
    opportunities: Optional[int] = None
    meetings_booked: Optional[int] = None

class TemplateCreate(BaseModel):
    name: str
    variant: str = "A"
    step_type: str = "Main"
    subject: Optional[str] = None
    body: Optional[str] = None
    campaign_ids: Optional[List[int]] = None

class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    variant: Optional[str] = None
    step_type: Optional[str] = None
    times_sent: Optional[int] = None
    times_opened: Optional[int] = None
    times_clicked: Optional[int] = None
    times_replied: Optional[int] = None
    is_winner: Optional[bool] = None
    campaign_ids: Optional[List[int]] = None

class MergeRequest(BaseModel):
    primary_id: int
    duplicate_ids: List[int]

def compute_employee_bucket(emp):
    if emp is None or (isinstance(emp, float) and pd.isna(emp)): return None
    try:
        n = int(float(emp))
        if n <= 10: return "1-10"
        elif n <= 50: return "11-50"
        elif n <= 200: return "51-200"
        elif n <= 500: return "201-500"
        elif n <= 1000: return "501-1000"
        else: return "1000+"
    except: return None

def update_counts():
    conn = get_db()
    for row in conn.execute("SELECT name FROM campaigns"):
        cnt = conn.execute("SELECT COUNT(*) FROM contacts WHERE campaigns_assigned LIKE ? AND is_duplicate=0", (f"%{row[0]}%",)).fetchone()[0]
        conn.execute("UPDATE campaigns SET total_leads=? WHERE name=?", (cnt, row[0]))
    for row in conn.execute("SELECT name FROM outreach_lists"):
        cnt = conn.execute("SELECT COUNT(*) FROM contacts WHERE outreach_lists LIKE ? AND is_duplicate=0", (f"%{row[0]}%",)).fetchone()[0]
        conn.execute("UPDATE outreach_lists SET contact_count=? WHERE name=?", (cnt, row[0]))
    conn.commit(); conn.close()

def recalc_rates(campaign_id, conn=None):
    """Recalculate campaign rates. Can use existing connection or create new one."""
    should_close = conn is None
    if should_close:
        conn = get_db()
    conn.execute("""UPDATE campaigns SET
        open_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_opened/emails_sent,1) ELSE 0 END,
        click_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_clicked/emails_sent,1) ELSE 0 END,
        reply_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_replied/emails_sent,1) ELSE 0 END WHERE id=?""", (campaign_id,))
    if should_close:
        conn.commit()
        conn.close()

def recalc_template_rates(template_id, conn=None):
    """Recalculate template rates. Can use existing connection or create new one."""
    should_close = conn is None
    if should_close:
        conn = get_db()
    row = conn.execute("SELECT COALESCE(SUM(times_sent),0), COALESCE(SUM(times_opened),0), COALESCE(SUM(times_replied),0) FROM template_campaigns WHERE template_id=?", (template_id,)).fetchone()
    t = conn.execute("SELECT times_sent, times_opened, times_replied FROM email_templates WHERE id=?", (template_id,)).fetchone()
    ts = (t[0] or 0) + row[0]; to = (t[1] or 0) + row[1]; tr = (t[2] or 0) + row[2]
    conn.execute("UPDATE email_templates SET open_rate=?, reply_rate=?, updated_at=? WHERE id=?",
                (round(100*to/ts,1) if ts>0 else 0, round(100*tr/ts,1) if ts>0 else 0, datetime.now().isoformat(), template_id))
    if should_close:
        conn.commit()
        conn.close()

def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization: return None
    token = authorization.replace("Bearer ", "").strip()
    conn = get_db(); user = conn.execute("SELECT * FROM users WHERE api_token=? AND is_active=1", (token,)).fetchone(); conn.close()
    return dict(user) if user else None

# Health check endpoint for Railway deployment
@app.get("/api/health")
def health_check():
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.post("/api/auth/login")
def login(creds: UserLogin):
    conn = get_db(); pwd_hash = hashlib.sha256(creds.password.encode()).hexdigest()
    user = conn.execute("SELECT * FROM users WHERE email=? AND password_hash=? AND is_active=1", (creds.email, pwd_hash)).fetchone(); conn.close()
    if not user: raise HTTPException(401, "Invalid credentials")
    return {"token": user['api_token'], "user": {"id": user['id'], "email": user['email'], "name": user['name'], "role": user['role']}}

@app.get("/api/auth/me")
def get_me(user: dict = Depends(get_current_user)):
    if not user: raise HTTPException(401, "Not authenticated")
    return {"id": user['id'], "email": user['email'], "name": user['name'], "role": user['role']}

@app.post("/api/auth/register")
def register(user: UserCreate):
    conn = get_db()
    try:
        token = secrets.token_urlsafe(32); pwd_hash = hashlib.sha256(user.password.encode()).hexdigest()
        conn.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)", (user.email, pwd_hash, user.name, user.role, token))
        conn.commit(); return {"message": "Created", "token": token}
    except sqlite3.IntegrityError: raise HTTPException(400, "Email exists")
    finally: conn.close()

@app.get("/api/users")
def get_users():
    conn = get_db(); users = conn.execute("SELECT id, email, name, role, is_active, created_at FROM users ORDER BY created_at DESC").fetchall(); conn.close()
    return {"data": [dict(u) for u in users]}

@app.delete("/api/users/{user_id}")
def delete_user(user_id: int):
    conn = get_db(); conn.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,)); conn.commit(); conn.close()
    return {"message": "Deactivated"}

@app.get("/api/contacts")
def get_contacts(page: int = 1, page_size: int = 50, search: Optional[str] = None, status: Optional[str] = None,
                campaigns: Optional[str] = None, outreach_lists: Optional[str] = None, country: Optional[str] = None,
                country_strategy: Optional[str] = None, seniority: Optional[str] = None, industry: Optional[str] = None,
                show_duplicates: bool = False, sort_by: str = "id", sort_order: str = "desc"):
    conn = get_db(); where = ["1=1"] if show_duplicates else ["is_duplicate=0"]; params = []
    if search: where.append("(first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ? OR title LIKE ?)"); params.extend([f"%{search}%"]*5)
    if status: where.append("status=?"); params.append(status)
    if campaigns: where.append("campaigns_assigned LIKE ?"); params.append(f"%{campaigns}%")
    if outreach_lists: where.append("outreach_lists LIKE ?"); params.append(f"%{outreach_lists}%")
    if country: where.append("company_country=?"); params.append(country)
    if country_strategy: where.append("country_strategy=?"); params.append(country_strategy)
    if seniority: where.append("seniority=?"); params.append(seniority)
    if industry: where.append("industry LIKE ?"); params.append(f"%{industry}%")
    where_sql = " AND ".join(where)
    total = conn.execute(f"SELECT COUNT(*) FROM contacts WHERE {where_sql}", params).fetchone()[0]
    valid_sorts = ['id', 'first_name', 'last_name', 'email', 'company', 'status', 'created_at', 'employees']
    sort_by = sort_by if sort_by in valid_sorts else 'id'
    rows = conn.execute(f"SELECT * FROM contacts WHERE {where_sql} ORDER BY {sort_by} {'DESC' if sort_order.lower()=='desc' else 'ASC'} LIMIT ? OFFSET ?",
                       params + [page_size, (page-1)*page_size]).fetchall(); conn.close()
    return {"data": [dict(r) for r in rows], "total": total, "page": page, "page_size": page_size, "total_pages": max(1, (total+page_size-1)//page_size)}

@app.get("/api/contacts/{contact_id}")
def get_contact(contact_id: int):
    conn = get_db(); row = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone(); conn.close()
    if not row: raise HTTPException(404, "Not found")
    return dict(row)

@app.post("/api/contacts")
def create_contact(contact: ContactCreate):
    conn = get_db(); data = contact.dict(exclude_none=True)
    if data.get('campaigns_assigned'):
        for c in data['campaigns_assigned'].split(','): conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c.strip(),))
    if data.get('outreach_lists'):
        for l in data['outreach_lists'].split(','): conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (l.strip(),))
    fields = list(data.keys())
    conn.execute(f"INSERT INTO contacts ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})", list(data.values()))
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]; conn.commit(); conn.close(); update_counts()
    return {"id": cid, "message": "Created"}

@app.put("/api/contacts/{contact_id}")
def update_contact(contact_id: int, contact: ContactUpdate):
    conn = get_db(); data = {k: v for k, v in contact.dict().items() if v is not None}
    if not data: raise HTTPException(400, "No fields")
    if data.get('campaigns_assigned'):
        for c in data['campaigns_assigned'].split(','): conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c.strip(),))
    if data.get('outreach_lists'):
        for l in data['outreach_lists'].split(','): conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (l.strip(),))
    data['updated_at'] = datetime.now().isoformat()
    conn.execute(f"UPDATE contacts SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?", list(data.values())+[contact_id])
    conn.commit(); conn.close(); update_counts()
    return {"message": "Updated"}

@app.delete("/api/contacts/{contact_id}")
def delete_contact(contact_id: int):
    conn = get_db(); conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,)); conn.commit(); conn.close(); update_counts()
    return {"message": "Deleted"}

@app.post("/api/contacts/bulk")
def bulk_update(req: BulkUpdateRequest):
    conn = get_db()

    # Get contact IDs either from direct list or by applying filters
    if req.contact_ids:
        contact_ids = req.contact_ids
    elif req.filters:
        # Build query from filters (same logic as get_contacts)
        query = "SELECT id FROM contacts WHERE is_duplicate=0"
        params = []
        f = req.filters
        if f.get('search'):
            query += " AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ? OR title LIKE ?)"
            s = f"%{f['search']}%"; params.extend([s, s, s, s, s])
        if f.get('status'): query += " AND status=?"; params.append(f['status'])
        if f.get('campaign'): query += " AND campaigns_assigned LIKE ?"; params.append(f"%{f['campaign']}%")
        if f.get('outreach_list'): query += " AND outreach_lists LIKE ?"; params.append(f"%{f['outreach_list']}%")
        if f.get('country_strategy'): query += " AND country_strategy=?"; params.append(f['country_strategy'])
        if f.get('country'): query += " AND company_country=?"; params.append(f['country'])
        if f.get('seniority'): query += " AND seniority=?"; params.append(f['seniority'])
        if f.get('industry'): query += " AND industry=?"; params.append(f['industry'])
        rows = conn.execute(query, params).fetchall()
        contact_ids = [row['id'] for row in rows]
    else:
        raise HTTPException(400, "No contacts specified")

    if not contact_ids:
        return {"updated": 0}

    updated = 0
    field = req.field
    action = req.action or 'set'
    value = req.value
    now = datetime.now().isoformat()

    # Handle delete action
    if action == 'delete':
        placeholders = ','.join(['?'] * len(contact_ids))
        conn.execute(f"DELETE FROM contacts WHERE id IN ({placeholders})", contact_ids)
        updated = len(contact_ids)
    # Handle list fields (campaigns_assigned, outreach_lists)
    elif field in ['campaigns_assigned', 'outreach_lists']:
        for cid in contact_ids:
            row = conn.execute("SELECT * FROM contacts WHERE id=?", (cid,)).fetchone()
            if not row: continue
            current = set(x.strip() for x in (row[field] or '').split(',') if x.strip())
            if action == 'add':
                current.add(value)
                if field == 'campaigns_assigned':
                    conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (value,))
                elif field == 'outreach_lists':
                    conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (value,))
            elif action == 'remove':
                current.discard(value)
            elif action == 'set':
                current = {value} if value else set()
            new_value = ', '.join(sorted(current)) if current else None
            conn.execute(f"UPDATE contacts SET {field}=?, updated_at=? WHERE id=?", (new_value, now, cid))
            updated += 1
    # Handle simple field updates (status, country_strategy, etc.)
    else:
        # Validate field name to prevent SQL injection
        allowed_fields = ['status', 'country_strategy', 'seniority', 'company_country', 'industry', 'title', 'company', 'first_name', 'last_name', 'notes']
        if field not in allowed_fields:
            raise HTTPException(400, f"Field {field} cannot be bulk updated")
        placeholders = ','.join(['?'] * len(contact_ids))
        conn.execute(f"UPDATE contacts SET {field}=?, updated_at=? WHERE id IN ({placeholders})", [value, now] + contact_ids)
        updated = len(contact_ids)

    conn.commit(); conn.close(); update_counts()
    return {"updated": updated}

@app.get("/api/duplicates")
def get_duplicates():
    conn = get_db()
    rows = conn.execute("SELECT LOWER(email) as email, COUNT(*) as cnt, GROUP_CONCAT(id) as ids FROM contacts WHERE email IS NOT NULL AND email != '' AND is_duplicate=0 GROUP BY LOWER(email) HAVING COUNT(*)>1 ORDER BY cnt DESC LIMIT 100").fetchall()
    groups = []
    for row in rows:
        ids = [int(i) for i in row['ids'].split(',')]
        contacts = conn.execute(f"SELECT * FROM contacts WHERE id IN ({','.join(['?']*len(ids))})", ids).fetchall()
        groups.append({"email": row['email'], "count": row['cnt'], "contacts": [dict(c) for c in contacts]})
    conn.close()
    return {"groups": groups, "total_groups": len(groups)}

@app.post("/api/duplicates/merge")
def merge_duplicates(req: MergeRequest):
    conn = get_db()
    primary = conn.execute("SELECT * FROM contacts WHERE id=?", (req.primary_id,)).fetchone()
    if not primary: raise HTTPException(404, "Not found")
    all_lists = set(l.strip() for l in (primary['outreach_lists'] or '').split(',') if l.strip())
    all_camps = set(c.strip() for c in (primary['campaigns_assigned'] or '').split(',') if c.strip())
    for dup_id in req.duplicate_ids:
        dup = conn.execute("SELECT * FROM contacts WHERE id=?", (dup_id,)).fetchone()
        if dup:
            all_lists.update(l.strip() for l in (dup['outreach_lists'] or '').split(',') if l.strip())
            all_camps.update(c.strip() for c in (dup['campaigns_assigned'] or '').split(',') if c.strip())
            conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (req.primary_id, dup_id))
    conn.execute("UPDATE contacts SET outreach_lists=?, campaigns_assigned=?, updated_at=? WHERE id=?",
                (', '.join(sorted(all_lists)) or None, ', '.join(sorted(all_camps)) or None, datetime.now().isoformat(), req.primary_id))
    conn.commit(); conn.close(); update_counts()
    return {"message": f"Merged {len(req.duplicate_ids)} contacts"}

@app.post("/api/duplicates/unmerge/{contact_id}")
def unmerge(contact_id: int):
    conn = get_db(); conn.execute("UPDATE contacts SET is_duplicate=0, duplicate_of=NULL WHERE id=?", (contact_id,)); conn.commit(); conn.close()
    return {"message": "Restored"}

@app.get("/api/duplicates/stats")
def get_duplicate_stats():
    """Get comprehensive duplicate statistics"""
    conn = get_db()
    # Total duplicate groups (emails with more than 1 contact)
    total_groups = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT LOWER(email) FROM contacts
            WHERE email IS NOT NULL AND email != '' AND is_duplicate=0
            GROUP BY LOWER(email) HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # Total contacts that are duplicates
    total_duplicates = conn.execute("""
        SELECT COALESCE(SUM(cnt - 1), 0) FROM (
            SELECT COUNT(*) as cnt FROM contacts
            WHERE email IS NOT NULL AND email != '' AND is_duplicate=0
            GROUP BY LOWER(email) HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    # Already merged duplicates
    merged_count = conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=1").fetchone()[0]

    conn.close()
    return {
        "total_groups": total_groups,
        "total_duplicates": total_duplicates,
        "merged_count": merged_count,
        "potential_savings": total_duplicates
    }

@app.post("/api/duplicates/auto-merge")
def auto_merge_all_duplicates():
    """
    Automatically merge ALL duplicate groups.
    Keeps the first contact as primary, merges all others.
    Combines outreach_lists and campaigns_assigned from all duplicates.
    """
    conn = get_db()

    # Get all duplicate groups
    rows = conn.execute("""
        SELECT LOWER(email) as email, GROUP_CONCAT(id) as ids
        FROM contacts
        WHERE email IS NOT NULL AND email != '' AND is_duplicate=0
        GROUP BY LOWER(email) HAVING COUNT(*) > 1
    """).fetchall()

    merged_groups = 0
    merged_contacts = 0

    for row in rows:
        ids = [int(i) for i in row['ids'].split(',')]
        if len(ids) < 2:
            continue

        # Get all contacts in this group
        contacts = conn.execute(
            f"SELECT * FROM contacts WHERE id IN ({','.join(['?']*len(ids))}) ORDER BY created_at ASC",
            ids
        ).fetchall()

        if not contacts:
            continue

        # First contact is primary (oldest)
        primary = contacts[0]
        primary_id = primary['id']
        duplicates = contacts[1:]

        # Collect all outreach lists and campaigns
        all_lists = set(l.strip() for l in (primary['outreach_lists'] or '').split(',') if l.strip())
        all_camps = set(c.strip() for c in (primary['campaigns_assigned'] or '').split(',') if c.strip())

        # Also collect best values for other fields
        best_data = {
            'first_name': primary['first_name'],
            'last_name': primary['last_name'],
            'title': primary['title'],
            'company': primary['company'],
            'first_phone': primary['first_phone'],
            'corporate_phone': primary['corporate_phone'],
            'person_linkedin_url': primary['person_linkedin_url'],
            'website': primary['website'],
            'times_contacted': primary['times_contacted'] or 0,
            'meetings_booked': primary['meetings_booked'] or 0,
            'opportunities': primary['opportunities'] or 0,
        }

        for dup in duplicates:
            # Merge lists and campaigns
            all_lists.update(l.strip() for l in (dup['outreach_lists'] or '').split(',') if l.strip())
            all_camps.update(c.strip() for c in (dup['campaigns_assigned'] or '').split(',') if c.strip())

            # Fill in missing data from duplicates
            for field in ['first_name', 'last_name', 'title', 'company', 'first_phone', 'corporate_phone', 'person_linkedin_url', 'website']:
                if not best_data[field] and dup[field]:
                    best_data[field] = dup[field]

            # Sum up metrics
            best_data['times_contacted'] += dup['times_contacted'] or 0
            best_data['meetings_booked'] += dup['meetings_booked'] or 0
            best_data['opportunities'] += dup['opportunities'] or 0

            # Mark as duplicate
            conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (primary_id, dup['id']))
            merged_contacts += 1

        # Update primary with merged data
        conn.execute("""
            UPDATE contacts SET
                first_name=?, last_name=?, title=?, company=?, first_phone=?, corporate_phone=?,
                person_linkedin_url=?, website=?, times_contacted=?, meetings_booked=?, opportunities=?,
                outreach_lists=?, campaigns_assigned=?, updated_at=?
            WHERE id=?
        """, (
            best_data['first_name'], best_data['last_name'], best_data['title'], best_data['company'],
            best_data['first_phone'], best_data['corporate_phone'], best_data['person_linkedin_url'],
            best_data['website'], best_data['times_contacted'], best_data['meetings_booked'],
            best_data['opportunities'], ', '.join(sorted(all_lists)) or None,
            ', '.join(sorted(all_camps)) or None, datetime.now().isoformat(), primary_id
        ))

        merged_groups += 1

    conn.commit()
    conn.close()
    update_counts()

    return {
        "message": f"Auto-merged {merged_groups} duplicate groups ({merged_contacts} contacts)",
        "groups_merged": merged_groups,
        "contacts_merged": merged_contacts
    }

@app.post("/api/duplicates/merge-group/{email}")
def merge_duplicate_group(email: str):
    """Merge a single duplicate group by email"""
    conn = get_db()

    # Get all contacts with this email
    contacts = conn.execute(
        "SELECT * FROM contacts WHERE LOWER(email)=? AND is_duplicate=0 ORDER BY created_at ASC",
        (email.lower(),)
    ).fetchall()

    if len(contacts) < 2:
        conn.close()
        return {"message": "No duplicates found for this email"}

    primary = contacts[0]
    primary_id = primary['id']
    duplicates = contacts[1:]

    all_lists = set(l.strip() for l in (primary['outreach_lists'] or '').split(',') if l.strip())
    all_camps = set(c.strip() for c in (primary['campaigns_assigned'] or '').split(',') if c.strip())

    for dup in duplicates:
        all_lists.update(l.strip() for l in (dup['outreach_lists'] or '').split(',') if l.strip())
        all_camps.update(c.strip() for c in (dup['campaigns_assigned'] or '').split(',') if c.strip())
        conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (primary_id, dup['id']))

    conn.execute("UPDATE contacts SET outreach_lists=?, campaigns_assigned=?, updated_at=? WHERE id=?",
                (', '.join(sorted(all_lists)) or None, ', '.join(sorted(all_camps)) or None,
                 datetime.now().isoformat(), primary_id))

    conn.commit()
    conn.close()
    update_counts()

    return {"message": f"Merged {len(duplicates)} duplicates", "merged_count": len(duplicates)}

@app.post("/api/import/preview")
async def preview_import(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = None
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try: df = pd.read_csv(io.BytesIO(content), encoding=enc, nrows=10); break
            except: continue
        if df is None: raise HTTPException(400, "Cannot read file")
        total_df = None
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try: total_df = pd.read_csv(io.BytesIO(content), encoding=enc); break
            except: continue
        total_rows = len(total_df) if total_df is not None else 0
        
        target_columns = ["first_name", "last_name", "email", "title", "headline", "company", "seniority",
            "first_phone", "corporate_phone", "employees", "industry", "keywords", "person_linkedin_url",
            "website", "domain", "company_linkedin_url", "company_city", "company_state", "company_country",
            "outreach_lists", "campaigns_assigned", "notes"]
        
        suggestions = {}
        for col in df.columns:
            cl = str(col).lower().replace(' ', '_').replace('#_', '').replace('-', '_')
            if 'first' in cl and 'name' in cl: suggestions[col] = 'first_name'
            elif 'last' in cl and 'name' in cl: suggestions[col] = 'last_name'
            elif cl in ['email', 'e_mail']: suggestions[col] = 'email'
            elif cl in ['title', 'job_title']: suggestions[col] = 'title'
            elif cl == 'headline': suggestions[col] = 'headline'
            elif cl in ['company', 'company_name']: suggestions[col] = 'company'
            elif 'seniority' in cl: suggestions[col] = 'seniority'
            elif 'first_phone' in cl: suggestions[col] = 'first_phone'
            elif 'corporate_phone' in cl: suggestions[col] = 'corporate_phone'
            elif cl in ['employees', '_employees']: suggestions[col] = 'employees'
            elif cl == 'industry': suggestions[col] = 'industry'
            elif cl == 'keywords': suggestions[col] = 'keywords'
            elif 'person_linkedin' in cl: suggestions[col] = 'person_linkedin_url'
            elif cl == 'website': suggestions[col] = 'website'
            elif cl in ['domain', 'dominio']: suggestions[col] = 'domain'
            elif 'company_linkedin' in cl: suggestions[col] = 'company_linkedin_url'
            elif 'company_city' in cl: suggestions[col] = 'company_city'
            elif 'company_state' in cl: suggestions[col] = 'company_state'
            elif 'company_country' in cl: suggestions[col] = 'company_country'
            elif 'outreach' in cl: suggestions[col] = 'outreach_lists'
            elif 'campaign' in cl or 'assigned' in cl: suggestions[col] = 'campaigns_assigned'
        
        return {"filename": file.filename, "total_rows": total_rows, "columns": list(df.columns),
                "preview": df.head(5).fillna('').to_dict(orient='records'), "target_columns": target_columns, "suggested_mapping": suggestions}
    except Exception as e:
        raise HTTPException(400, f"Error: {str(e)}")

@app.post("/api/import/execute")
async def execute_import(file: UploadFile = File(...), column_mapping: str = Query(...), outreach_list: str = Query(None),
                        campaigns: str = Query(None), country_strategy: str = Query(None), check_duplicates: bool = Query(True), merge_duplicates: bool = Query(True)):
    content = await file.read()
    df = None
    for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
        try: df = pd.read_csv(io.BytesIO(content), encoding=enc); break
        except: continue
    if df is None: raise HTTPException(400, "Cannot read CSV")
    
    mapping = json.loads(column_mapping); conn = get_db()
    if outreach_list: conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (outreach_list,))
    campaign_list = [c.strip() for c in (campaigns or '').split(',') if c.strip()]
    for c in campaign_list: conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c,))
    
    email_index = {}
    if check_duplicates:
        # Only load non-duplicate contacts to ensure we merge into the primary record
        for row in conn.execute("SELECT id, LOWER(email), outreach_lists, campaigns_assigned, country_strategy FROM contacts WHERE email IS NOT NULL AND is_duplicate=0"):
            email_index[row[1]] = {'id': row[0], 'lists': row[2] or '', 'campaigns': row[3] or '', 'country_strategy': row[4] or ''}
    
    stats = {'imported': 0, 'merged': 0, 'duplicates_found': 0, 'failed': 0}
    for _, row in df.iterrows():
        try:
            data = {}
            for src_col, tgt_col in mapping.items():
                if tgt_col and src_col in row:
                    val = row[src_col]
                    if pd.notna(val):
                        if tgt_col == 'employees':
                            try: data[tgt_col] = int(float(val))
                            except: pass
                        else: data[tgt_col] = str(val).strip()
            
            csv_lists = set(l.strip() for l in (data.get('outreach_lists') or '').split(',') if l.strip())
            if outreach_list: csv_lists.add(outreach_list)
            if csv_lists:
                data['outreach_lists'] = ', '.join(sorted(csv_lists))
                for l in csv_lists: conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (l,))
            
            csv_camps = set(c.strip() for c in (data.get('campaigns_assigned') or '').split(',') if c.strip())
            csv_camps.update(campaign_list)
            if csv_camps:
                data['campaigns_assigned'] = ', '.join(sorted(csv_camps))
                for c in csv_camps: conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c,))

            # Apply country_strategy from form if not already set from CSV
            if country_strategy and not data.get('country_strategy'):
                data['country_strategy'] = country_strategy

            data['employee_bucket'] = compute_employee_bucket(data.get('employees'))
            data['source_file'] = file.filename
            email = (data.get('email') or '').lower().strip()
            
            if check_duplicates and email and email in email_index:
                stats['duplicates_found'] += 1
                if merge_duplicates:
                    existing = email_index[email]
                    curr_lists = set(l.strip() for l in existing['lists'].split(',') if l.strip())
                    curr_camps = set(c.strip() for c in existing['campaigns'].split(',') if c.strip())
                    curr_lists.update(l.strip() for l in (data.get('outreach_lists') or '').split(',') if l.strip())
                    curr_camps.update(c.strip() for c in (data.get('campaigns_assigned') or '').split(',') if c.strip())
                    # Get new country_strategy (form input takes precedence, then CSV, then keep existing)
                    new_strategy = data.get('country_strategy') or existing.get('country_strategy')
                    conn.execute("UPDATE contacts SET outreach_lists=?, campaigns_assigned=?, country_strategy=?, updated_at=? WHERE id=?",
                                (', '.join(sorted(curr_lists)) or None, ', '.join(sorted(curr_camps)) or None, new_strategy, datetime.now().isoformat(), existing['id']))
                    stats['merged'] += 1
                continue
            
            if data:
                fields = list(data.keys())
                conn.execute(f"INSERT INTO contacts ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})", list(data.values()))
                stats['imported'] += 1
                if email: email_index[email] = {'id': conn.execute("SELECT last_insert_rowid()").fetchone()[0], 'lists': data.get('outreach_lists', ''), 'campaigns': data.get('campaigns_assigned', ''), 'country_strategy': data.get('country_strategy', '')}
        except: stats['failed'] += 1
    conn.commit(); conn.close(); update_counts()
    return stats

@app.get("/api/contacts/export")
def export_contacts(columns: Optional[str] = None, status: Optional[str] = None, campaigns: Optional[str] = None, outreach_lists: Optional[str] = None):
    conn = get_db(); where, params = ["is_duplicate=0"], []
    if status: where.append("status=?"); params.append(status)
    if campaigns: where.append("campaigns_assigned LIKE ?"); params.append(f"%{campaigns}%")
    if outreach_lists: where.append("outreach_lists LIKE ?"); params.append(f"%{outreach_lists}%")
    all_cols = ['id','first_name','last_name','email','title','headline','company','seniority','first_phone','corporate_phone','employees','employee_bucket','industry','keywords','person_linkedin_url','website','domain','company_linkedin_url','company_city','company_state','company_country','region','outreach_lists','campaigns_assigned','status','email_status','times_contacted','opportunities','meetings_booked','notes','created_at']
    selected = [c.strip() for c in (columns or '').split(',') if c.strip() in all_cols] or all_cols[:15]
    df = pd.read_sql_query(f"SELECT {','.join(selected)} FROM contacts WHERE {' AND '.join(where)}", conn, params=params); conn.close()
    buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"})

@app.get("/api/contacts/columns")
def get_columns():
    return {"columns": [{"id": "first_name", "label": "First Name"}, {"id": "last_name", "label": "Last Name"}, {"id": "email", "label": "Email"}, {"id": "title", "label": "Title"}, {"id": "headline", "label": "Headline"}, {"id": "company", "label": "Company"}, {"id": "seniority", "label": "Seniority"}, {"id": "first_phone", "label": "Phone"}, {"id": "corporate_phone", "label": "Corp. Phone"}, {"id": "employees", "label": "Employees"}, {"id": "employee_bucket", "label": "Company Size"}, {"id": "industry", "label": "Industry"}, {"id": "person_linkedin_url", "label": "LinkedIn"}, {"id": "website", "label": "Website"}, {"id": "company_city", "label": "City"}, {"id": "company_state", "label": "State"}, {"id": "company_country", "label": "Country"}, {"id": "region", "label": "Region"}, {"id": "country_strategy", "label": "Country Strategy"}, {"id": "outreach_lists", "label": "Outreach Lists"}, {"id": "campaigns_assigned", "label": "Campaigns"}, {"id": "status", "label": "Status"}, {"id": "email_status", "label": "Email Status"}, {"id": "times_contacted", "label": "Times Contacted"}, {"id": "notes", "label": "Notes"}, {"id": "created_at", "label": "Created"}]}

@app.get("/api/filters")
def get_filters():
    conn = get_db()
    opts = {'statuses': ['Lead', 'Contacted', 'Engaged', 'Opportunity', 'Client', 'Not Interested', 'Bounced', 'Unsubscribed'],
            'countries': [r[0] for r in conn.execute("SELECT DISTINCT company_country FROM contacts WHERE company_country IS NOT NULL AND company_country != '' ORDER BY company_country")],
            'country_strategies': ['Mexico', 'United States', 'Germany', 'Spain'],
            'seniorities': [r[0] for r in conn.execute("SELECT DISTINCT seniority FROM contacts WHERE seniority IS NOT NULL AND seniority != '' ORDER BY seniority")],
            'industries': [r[0] for r in conn.execute("SELECT DISTINCT industry FROM contacts WHERE industry IS NOT NULL AND industry != '' ORDER BY industry LIMIT 50")],
            'campaigns': [r[0] for r in conn.execute("SELECT name FROM campaigns ORDER BY name")],
            'outreach_lists': [r[0] for r in conn.execute("SELECT name FROM outreach_lists ORDER BY name")]}
    conn.close(); return opts

@app.get("/api/stats")
def get_stats():
    conn = get_db()
    stats = {'total_contacts': conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
             'unique_contacts': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0").fetchone()[0],
             'duplicates': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=1").fetchone()[0],
             'total_campaigns': conn.execute("SELECT COUNT(*) FROM campaigns").fetchone()[0],
             'total_lists': conn.execute("SELECT COUNT(*) FROM outreach_lists").fetchone()[0],
             'total_templates': conn.execute("SELECT COUNT(*) FROM email_templates").fetchone()[0]}
    r = conn.execute("SELECT COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_opened),0), COALESCE(SUM(emails_replied),0) FROM campaigns").fetchone()
    # Get opportunities and meetings from template_campaigns (rolled up from templates)
    tc = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns").fetchone()
    stats['emails_sent'] = r[0]; stats['emails_opened'] = r[1]; stats['emails_replied'] = r[2]
    stats['opportunities'] = tc[0]; stats['meetings_booked'] = tc[1]
    stats['avg_open_rate'] = round(100*r[1]/r[0],1) if r[0]>0 else 0; stats['avg_reply_rate'] = round(100*r[2]/r[0],1) if r[0]>0 else 0
    stats['by_status'] = {row[0] or 'Unknown': row[1] for row in conn.execute("SELECT status, COUNT(*) FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY COUNT(*) DESC")}
    stats['by_campaign'] = [(r[0], r[1]) for r in conn.execute("SELECT name, total_leads FROM campaigns ORDER BY total_leads DESC LIMIT 10")]
    stats['by_list'] = [(r[0], r[1]) for r in conn.execute("SELECT name, contact_count FROM outreach_lists ORDER BY contact_count DESC LIMIT 10")]
    conn.close(); return stats

@app.get("/api/stats/database")
def get_database_stats():
    """Get detailed database analytics for Dashboard - Database tab"""
    conn = get_db()
    insights = {}

    # Contact distribution by country
    insights['by_country'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT company_country, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY company_country ORDER BY cnt DESC LIMIT 15")]

    # Contact distribution by country strategy
    insights['by_country_strategy'] = [{"name": r[0] or "Not Assigned", "value": r[1]} for r in
        conn.execute("SELECT country_strategy, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY country_strategy ORDER BY cnt DESC")]

    # Distribution by seniority
    insights['by_seniority'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT seniority, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY seniority ORDER BY cnt DESC LIMIT 10")]

    # Distribution by industry
    insights['by_industry'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT industry, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY industry ORDER BY cnt DESC LIMIT 15")]

    # Distribution by company size (employees bucket)
    insights['by_company_size'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT employee_bucket, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY employee_bucket ORDER BY cnt DESC")]

    # Contact status breakdown
    insights['by_status'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT status, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY cnt DESC")]

    # Top companies by contacts
    insights['top_companies'] = [{"name": r[0], "value": r[1]} for r in
        conn.execute("SELECT company, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 AND company IS NOT NULL AND company != '' GROUP BY company ORDER BY cnt DESC LIMIT 10")]

    # Contacts added over time (last 30 days)
    insights['contacts_timeline'] = [{"date": r[0], "value": r[1]} for r in
        conn.execute("SELECT DATE(created_at), COUNT(*) FROM contacts WHERE is_duplicate=0 AND created_at >= DATE('now', '-30 days') GROUP BY DATE(created_at) ORDER BY DATE(created_at)")]

    # Data quality metrics
    insights['data_quality'] = {
        'with_email': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND email IS NOT NULL AND email != ''").fetchone()[0],
        'with_phone': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND (first_phone IS NOT NULL OR corporate_phone IS NOT NULL)").fetchone()[0],
        'with_linkedin': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND person_linkedin_url IS NOT NULL AND person_linkedin_url != ''").fetchone()[0],
        'with_company': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND company IS NOT NULL AND company != ''").fetchone()[0],
        'total': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0").fetchone()[0]
    }

    conn.close()
    return insights

@app.get("/api/stats/performance")
def get_performance_stats():
    """Get performance analytics for Dashboard - Performance tab"""
    conn = get_db()
    perf = {}

    # Campaign performance with calculated opportunities/meetings from template_campaigns
    campaigns = []
    for r in conn.execute("""
        SELECT id, name, country, status, total_leads, emails_sent, emails_opened, emails_replied,
               emails_bounced, open_rate, click_rate, reply_rate
        FROM campaigns ORDER BY emails_sent DESC
    """):
        c = dict(r)
        # Get opps/meetings from template_campaigns
        tc = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns WHERE campaign_id=?", (c['id'],)).fetchone()
        c['opportunities'] = tc[0]
        c['meetings_booked'] = tc[1]
        campaigns.append(c)
    perf['campaigns'] = campaigns

    # Overall metrics - get opps/meetings from template_campaigns
    row = conn.execute("""
        SELECT COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_opened),0), COALESCE(SUM(emails_clicked),0),
               COALESCE(SUM(emails_replied),0), COALESCE(SUM(emails_bounced),0)
        FROM campaigns
    """).fetchone()
    tc_totals = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns").fetchone()
    perf['totals'] = {
        'sent': row[0], 'opened': row[1], 'clicked': row[2], 'replied': row[3],
        'bounced': row[4], 'opportunities': tc_totals[0], 'meetings': tc_totals[1],
        'open_rate': round(100*row[1]/row[0], 1) if row[0] > 0 else 0,
        'click_rate': round(100*row[2]/row[0], 1) if row[0] > 0 else 0,
        'reply_rate': round(100*row[3]/row[0], 1) if row[0] > 0 else 0,
        'bounce_rate': round(100*row[4]/row[0], 1) if row[0] > 0 else 0
    }

    # Performance by country strategy
    perf['by_country'] = [{"country": r[0] or "Not Set", "campaigns": r[1], "sent": r[2], "replied": r[3]} for r in
        conn.execute("SELECT country, COUNT(*), COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_replied),0) FROM campaigns GROUP BY country ORDER BY SUM(emails_sent) DESC")]

    # Top performing templates - calculate from template_campaigns
    perf['top_templates'] = []
    for t in conn.execute("SELECT id, name, variant, step_type FROM email_templates"):
        tc = conn.execute("""
            SELECT COALESCE(SUM(times_sent),0), COALESCE(SUM(times_opened),0), COALESCE(SUM(times_replied),0),
                   COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0)
            FROM template_campaigns WHERE template_id=?
        """, (t['id'],)).fetchone()
        if tc[0] > 0:
            perf['top_templates'].append({
                'id': t['id'], 'name': t['name'], 'variant': t['variant'], 'step_type': t['step_type'],
                'times_sent': tc[0], 'times_opened': tc[1], 'times_replied': tc[2],
                'opportunities': tc[3], 'meetings': tc[4],
                'open_rate': round(100*tc[1]/tc[0], 1) if tc[0] > 0 else 0,
                'reply_rate': round(100*tc[2]/tc[0], 1) if tc[0] > 0 else 0
            })
    perf['top_templates'].sort(key=lambda x: x['reply_rate'], reverse=True)
    perf['top_templates'] = perf['top_templates'][:10]

    conn.close()
    return perf

@app.get("/api/campaigns")
def get_campaigns(search: Optional[str] = None, status: Optional[str] = None):
    conn = get_db(); where, params = ["1=1"], []
    if search: where.append("(name LIKE ? OR description LIKE ?)"); params.extend([f"%{search}%"]*2)
    if status: where.append("status=?"); params.append(status)
    rows = conn.execute(f"SELECT * FROM campaigns WHERE {' AND '.join(where)} ORDER BY created_at DESC", params).fetchall()
    result = []
    for r in rows:
        c = dict(r)
        # Calculate opportunities and meetings from template_campaigns
        tc_metrics = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns WHERE campaign_id=?", (c['id'],)).fetchone()
        c['opportunities'] = tc_metrics[0]
        c['meetings_booked'] = tc_metrics[1]
        result.append(c)
    conn.close()
    return {"data": result}

@app.get("/api/campaigns/{campaign_id}")
def get_campaign(campaign_id: int):
    conn = get_db(); row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not row: raise HTTPException(404, "Not found")
    campaign = dict(row)

    # Get all templates for this campaign grouped by step
    templates = conn.execute("""
        SELECT et.*, tc.times_sent as campaign_sent, tc.times_opened as campaign_opened,
               tc.times_replied as campaign_replied, tc.opportunities as campaign_opportunities,
               tc.meetings as campaign_meetings, tc.id as dashcard_id
        FROM template_campaigns tc
        JOIN email_templates et ON tc.template_id = et.id
        WHERE tc.campaign_id = ?
        ORDER BY et.step_type, et.variant
    """, (campaign_id,)).fetchall()

    # Group by step_type
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']
    grouped = {}

    for t in templates:
        template_dict = dict(t)
        step = template_dict['step_type']

        # Use campaign-specific metrics
        template_dict['sent'] = template_dict['campaign_sent'] or 0
        template_dict['opened'] = template_dict['campaign_opened'] or 0
        template_dict['replied'] = template_dict['campaign_replied'] or 0
        template_dict['opportunities'] = template_dict['campaign_opportunities'] or 0
        template_dict['meetings'] = template_dict['campaign_meetings'] or 0
        template_dict['open_rate'] = round(100 * template_dict['opened'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0
        template_dict['reply_rate'] = round(100 * template_dict['replied'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0

        if step not in grouped:
            grouped[step] = []
        grouped[step].append(template_dict)

    # Convert to ordered list
    template_breakdown = []
    for step in step_order:
        if step in grouped:
            step_metrics = {
                'sent': sum(t['sent'] for t in grouped[step]),
                'opened': sum(t['opened'] for t in grouped[step]),
                'replied': sum(t['replied'] for t in grouped[step]),
                'opportunities': sum(t['opportunities'] for t in grouped[step]),
                'meetings': sum(t['meetings'] for t in grouped[step])
            }
            step_metrics['open_rate'] = round(100 * step_metrics['opened'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            step_metrics['reply_rate'] = round(100 * step_metrics['replied'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0

            template_breakdown.append({
                'step_type': step,
                'variants': grouped[step],
                'step_metrics': step_metrics
            })

    # Add any other steps
    for step in grouped:
        if step not in step_order:
            step_metrics = {
                'sent': sum(t['sent'] for t in grouped[step]),
                'opened': sum(t['opened'] for t in grouped[step]),
                'replied': sum(t['replied'] for t in grouped[step]),
                'opportunities': sum(t['opportunities'] for t in grouped[step]),
                'meetings': sum(t['meetings'] for t in grouped[step])
            }
            step_metrics['open_rate'] = round(100 * step_metrics['opened'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            step_metrics['reply_rate'] = round(100 * step_metrics['replied'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0

            template_breakdown.append({
                'step_type': step,
                'variants': grouped[step],
                'step_metrics': step_metrics
            })

    campaign['template_breakdown'] = template_breakdown
    conn.close()
    return campaign

@app.post("/api/campaigns")
def create_campaign(campaign: CampaignCreate):
    conn = get_db()
    try: conn.execute("INSERT INTO campaigns (name, description, status) VALUES (?, ?, ?)", (campaign.name, campaign.description, campaign.status)); cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]; conn.commit(); return {"id": cid, "message": "Created"}
    except sqlite3.IntegrityError: raise HTTPException(400, "Name exists")
    finally: conn.close()

@app.put("/api/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, campaign: CampaignUpdate):
    conn = get_db(); data = {k: v for k, v in campaign.dict().items() if v is not None}
    if not data: raise HTTPException(400, "No fields")
    conn.execute(f"UPDATE campaigns SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?", list(data.values())+[campaign_id]); conn.commit(); conn.close(); recalc_rates(campaign_id)
    return {"message": "Updated"}

@app.delete("/api/campaigns/{campaign_id}")
def delete_campaign(campaign_id: int):
    conn = get_db(); conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,)); conn.commit(); conn.close(); return {"message": "Deleted"}

@app.get("/api/templates")
def get_templates(campaign_id: Optional[int] = None, search: Optional[str] = None):
    conn = get_db(); where, params = ["1=1"], []
    if search: where.append("(name LIKE ? OR subject LIKE ?)"); params.extend([f"%{search}%"]*2)
    rows = conn.execute(f"SELECT * FROM email_templates WHERE {' AND '.join(where)} ORDER BY created_at DESC", params).fetchall()
    result = []
    for r in rows:
        t = dict(r)
        camps = conn.execute("SELECT c.id, c.name, tc.times_sent, tc.times_opened, tc.times_replied, tc.opportunities, tc.meetings FROM template_campaigns tc JOIN campaigns c ON tc.campaign_id=c.id WHERE tc.template_id=?", (t['id'],)).fetchall()
        t['campaigns'] = [dict(c) for c in camps]
        t['campaign_ids'] = [c['id'] for c in camps]  # Add campaign_ids for frontend
        t['campaign_names'] = ', '.join(c['name'] for c in camps)
        ts = t['times_sent'] + sum(c['times_sent'] or 0 for c in camps); to = t['times_opened'] + sum(c['times_opened'] or 0 for c in camps); tr = t['times_replied'] + sum(c['times_replied'] or 0 for c in camps)
        opps = sum(c['opportunities'] or 0 for c in camps); meets = sum(c['meetings'] or 0 for c in camps)
        t['total_sent'] = ts; t['total_opened'] = to; t['total_replied'] = tr
        t['opportunities'] = opps; t['meetings'] = meets
        t['total_open_rate'] = round(100*to/ts,1) if ts>0 else 0; t['total_reply_rate'] = round(100*tr/ts,1) if ts>0 else 0
        result.append(t)
    conn.close(); return {"data": result}

@app.get("/api/templates/{template_id}")
def get_template(template_id: int):
    conn = get_db(); row = conn.execute("SELECT * FROM email_templates WHERE id=?", (template_id,)).fetchone()
    if not row: raise HTTPException(404, "Not found")
    t = dict(row)
    camps = conn.execute("SELECT c.id, c.name FROM template_campaigns tc JOIN campaigns c ON tc.campaign_id=c.id WHERE tc.template_id=?", (template_id,)).fetchall()
    t['campaign_ids'] = [c['id'] for c in camps]; t['campaign_names'] = [c['name'] for c in camps]; conn.close(); return t

@app.get("/api/templates/grouped/by-step")
def get_templates_grouped_by_step():
    """Get all templates grouped by step_type with variants and aggregated metrics"""
    conn = get_db()
    rows = conn.execute("SELECT * FROM email_templates ORDER BY step_type, variant").fetchall()

    # Group templates by step_type
    grouped = {}
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']

    for r in rows:
        t = dict(r)
        step = t['step_type']

        # Get campaign metrics for this template
        camps = conn.execute("""SELECT c.id, c.name, tc.times_sent, tc.times_opened, tc.times_replied, tc.opportunities, tc.meetings
                                FROM template_campaigns tc
                                JOIN campaigns c ON tc.campaign_id=c.id
                                WHERE tc.template_id=?""", (t['id'],)).fetchall()
        t['campaigns'] = [dict(c) for c in camps]
        t['campaign_ids'] = [c['id'] for c in camps]  # Add campaign_ids for frontend
        t['campaign_names'] = ', '.join(c['name'] for c in camps)

        # Calculate total metrics (template metrics + all campaign metrics)
        ts = t['times_sent'] + sum(c['times_sent'] or 0 for c in camps)
        to = t['times_opened'] + sum(c['times_opened'] or 0 for c in camps)
        tr = t['times_replied'] + sum(c['times_replied'] or 0 for c in camps)
        opps = sum(c['opportunities'] or 0 for c in camps)
        meets = sum(c['meetings'] or 0 for c in camps)

        t['total_sent'] = ts
        t['total_opened'] = to
        t['total_replied'] = tr
        t['opportunities'] = opps
        t['meetings'] = meets
        t['total_open_rate'] = round(100 * to / ts, 1) if ts > 0 else 0
        t['total_reply_rate'] = round(100 * tr / ts, 1) if ts > 0 else 0

        if step not in grouped:
            grouped[step] = []
        grouped[step].append(t)

    # Convert to ordered list
    result = []
    for step in step_order:
        if step in grouped:
            result.append({
                'step_type': step,
                'variants': grouped[step],
                'total_variants': len(grouped[step])
            })

    # Add any other steps not in the standard order
    for step in grouped:
        if step not in step_order:
            result.append({
                'step_type': step,
                'variants': grouped[step],
                'total_variants': len(grouped[step])
            })

    conn.close()
    return {"data": result}

@app.post("/api/templates")
def create_template(template: TemplateCreate):
    conn = get_db()
    conn.execute("INSERT INTO email_templates (name, variant, step_type, subject, body) VALUES (?, ?, ?, ?, ?)", (template.name, template.variant, template.step_type, template.subject, template.body))
    tid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    if template.campaign_ids:
        for cid in template.campaign_ids: conn.execute("INSERT OR IGNORE INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)", (tid, cid))
    conn.commit(); conn.close(); return {"id": tid, "message": "Created"}

@app.put("/api/templates/{template_id}")
def update_template(template_id: int, template: TemplateUpdate):
    conn = get_db(); data = {k: v for k, v in template.dict().items() if v is not None and k != 'campaign_ids'}
    if data:
        data['updated_at'] = datetime.now().isoformat()
        conn.execute(f"UPDATE email_templates SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?", list(data.values())+[template_id])
    if template.campaign_ids is not None:
        conn.execute("DELETE FROM template_campaigns WHERE template_id=?", (template_id,))
        for cid in template.campaign_ids: conn.execute("INSERT INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)", (template_id, cid))
    conn.commit(); conn.close(); recalc_template_rates(template_id); return {"message": "Updated"}

@app.delete("/api/templates/{template_id}")
def delete_template(template_id: int):
    conn = get_db(); conn.execute("DELETE FROM template_campaigns WHERE template_id=?", (template_id,)); conn.execute("DELETE FROM email_templates WHERE id=?", (template_id,)); conn.commit(); conn.close(); return {"message": "Deleted"}

class TemplateCampaignMetricsUpdate(BaseModel):
    times_sent: Optional[int] = None
    times_opened: Optional[int] = None
    times_replied: Optional[int] = None
    opportunities: Optional[int] = None
    meetings: Optional[int] = None

@app.put("/api/campaigns/{campaign_id}/templates/{template_id}/metrics")
def update_template_campaign_metrics(campaign_id: int, template_id: int, metrics: TemplateCampaignMetricsUpdate):
    """Update metrics for a specific template within a specific campaign"""
    conn = get_db()
    # Check if the relationship exists
    tc = conn.execute("SELECT * FROM template_campaigns WHERE template_id=? AND campaign_id=?", (template_id, campaign_id)).fetchone()
    if not tc:
        raise HTTPException(404, "Template not associated with this campaign")

    data = {k: v for k, v in metrics.dict().items() if v is not None}
    if not data:
        raise HTTPException(400, "No metrics to update")

    conn.execute(f"UPDATE template_campaigns SET {','.join([f'{k}=?' for k in data.keys()])} WHERE template_id=? AND campaign_id=?",
                list(data.values()) + [template_id, campaign_id])
    conn.commit()
    conn.close()

    # Recalculate campaign and template rates
    recalc_rates(campaign_id)
    recalc_template_rates(template_id)

    return {"message": "Updated"}

@app.get("/api/lists")
def get_lists():
    conn = get_db(); rows = conn.execute("SELECT * FROM outreach_lists ORDER BY name").fetchall(); conn.close(); return {"data": [dict(r) for r in rows]}

@app.post("/webhook/reachinbox")
async def reachinbox_webhook(request: Request):
    """
    ReachInbox Webhook Handler

    Accepts events: sent, opened, clicked, replied, bounced, unsubscribed

    Expected payload formats:
    1. Standard: { "event": "opened", "email": "user@example.com", "campaign_name": "My Campaign" }
    2. Alternative: { "type": "open", "to": "user@example.com", "campaign": "My Campaign" }
    3. With template: { "event": "sent", "email": "...", "campaign_name": "...", "template_id": 123 }

    Updates: campaigns metrics, contacts status, template_campaigns metrics (if template_id provided)
    """
    try:
        payload = await request.json()
    except:
        payload = {}

    conn = get_db()

    # Extract fields from various possible formats (ReachInbox compatibility)
    event_type = payload.get('event', payload.get('type', payload.get('event_type', 'unknown')))
    email = payload.get('email', payload.get('to', payload.get('recipient', payload.get('recipient_email'))))
    campaign_name = payload.get('campaign_name', payload.get('campaign', payload.get('campaignName', payload.get('sequence_name'))))
    template_id = payload.get('template_id', payload.get('templateId', payload.get('step_id')))
    message_id = payload.get('message_id', payload.get('messageId', payload.get('id')))
    timestamp = payload.get('timestamp', payload.get('occurred_at', datetime.now().isoformat()))

    # Normalize event type
    el = event_type.lower().strip()
    normalized_event = 'unknown'
    if 'sent' in el or 'deliver' in el: normalized_event = 'sent'
    elif 'open' in el: normalized_event = 'opened'
    elif 'click' in el: normalized_event = 'clicked'
    elif 'repl' in el or 'response' in el: normalized_event = 'replied'
    elif 'bounce' in el: normalized_event = 'bounced'
    elif 'unsub' in el: normalized_event = 'unsubscribed'
    elif 'fail' in el or 'error' in el: normalized_event = 'failed'

    # Log webhook event
    conn.execute("""INSERT INTO webhook_events
        (source, event_type, email, campaign_name, template_id, payload, processed)
        VALUES (?, ?, ?, ?, ?, ?, 1)""",
        ('reachinbox', normalized_event, email, campaign_name, template_id, json.dumps(payload)))

    campaign_id = None

    # Update campaign metrics
    if campaign_name:
        camp = conn.execute("SELECT id FROM campaigns WHERE LOWER(name) = LOWER(?) OR name LIKE ?",
                           (campaign_name, f"%{campaign_name}%")).fetchone()
        if camp:
            campaign_id = camp[0]
            if normalized_event == 'sent':
                conn.execute("UPDATE campaigns SET emails_sent=emails_sent+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'opened':
                conn.execute("UPDATE campaigns SET emails_opened=emails_opened+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'clicked':
                conn.execute("UPDATE campaigns SET emails_clicked=emails_clicked+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'replied':
                conn.execute("UPDATE campaigns SET emails_replied=emails_replied+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'bounced':
                conn.execute("UPDATE campaigns SET emails_bounced=emails_bounced+1 WHERE id=?", (campaign_id,))
            recalc_rates(campaign_id, conn)

    # Update template_campaigns metrics if template_id provided
    if template_id and campaign_id:
        tc = conn.execute("SELECT id FROM template_campaigns WHERE template_id=? AND campaign_id=?",
                         (template_id, campaign_id)).fetchone()
        if tc:
            if normalized_event == 'sent':
                conn.execute("UPDATE template_campaigns SET times_sent=times_sent+1 WHERE template_id=? AND campaign_id=?",
                           (template_id, campaign_id))
            elif normalized_event == 'opened':
                conn.execute("UPDATE template_campaigns SET times_opened=times_opened+1 WHERE template_id=? AND campaign_id=?",
                           (template_id, campaign_id))
            elif normalized_event == 'replied':
                conn.execute("UPDATE template_campaigns SET times_replied=times_replied+1 WHERE template_id=? AND campaign_id=?",
                           (template_id, campaign_id))
            recalc_template_rates(template_id, conn)

    # Update contact status
    if email:
        contact = conn.execute("SELECT id, status FROM contacts WHERE LOWER(email)=?", (email.lower(),)).fetchone()
        if contact:
            if normalized_event == 'sent':
                conn.execute("""UPDATE contacts SET
                    times_contacted=times_contacted+1,
                    last_contacted_at=?,
                    status=CASE WHEN status='Lead' THEN 'Contacted' ELSE status END
                    WHERE id=?""", (datetime.now().isoformat(), contact[0]))
            elif normalized_event == 'opened':
                # Optional: track opens (could add a last_opened_at field in future)
                pass
            elif normalized_event == 'replied':
                conn.execute("UPDATE contacts SET status='Engaged' WHERE id=? AND status IN ('Lead','Contacted')", (contact[0],))
            elif normalized_event == 'bounced':
                conn.execute("UPDATE contacts SET email_status='Invalid', status='Bounced' WHERE id=?", (contact[0],))
            elif normalized_event == 'unsubscribed':
                conn.execute("UPDATE contacts SET status='Unsubscribed' WHERE id=?", (contact[0],))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "message": "Processed",
        "event": normalized_event,
        "campaign_matched": campaign_id is not None,
        "contact_matched": email is not None
    }

@app.post("/webhook/bulkemailchecker")
async def bulkemailchecker_webhook(request: Request):
    """
    BulkEmailChecker Webhook Handler

    Accepts email validation results in multiple formats:
    1. Array: { "results": [{ "email": "...", "status": "valid" }, ...] }
    2. Single: { "email": "...", "status": "valid", "reason": "..." }
    3. Alternative: { "data": [{ "address": "...", "result": "deliverable" }] }

    Status mapping:
    - valid/deliverable/safe → 'Valid'
    - invalid/undeliverable/bad → 'Invalid'
    - risky/unknown/catch_all/role → 'Risky'

    Updates: contacts.email_status, optionally contacts.status for invalid emails
    """
    try:
        payload = await request.json()
    except:
        payload = {}

    conn = get_db()

    # Extract results array from various formats
    results = payload.get('results', payload.get('data', payload.get('emails', [payload])))
    if not isinstance(results, list):
        results = [results]

    stats = {'processed': 0, 'valid': 0, 'invalid': 0, 'risky': 0, 'not_found': 0}

    for r in results:
        # Extract email from various field names
        email = r.get('email', r.get('address', r.get('email_address', r.get('to'))))
        if not email:
            continue

        # Extract status from various field names
        raw_status = r.get('status', r.get('result', r.get('state', r.get('verdict', 'unknown'))))
        raw_status_lower = str(raw_status).lower()

        # Normalize status
        if raw_status_lower in ['valid', 'deliverable', 'safe', 'ok', 'good', 'verified']:
            email_status = 'Valid'
            stats['valid'] += 1
        elif raw_status_lower in ['invalid', 'undeliverable', 'bad', 'bounce', 'rejected', 'syntax_error', 'mailbox_not_found']:
            email_status = 'Invalid'
            stats['invalid'] += 1
        elif raw_status_lower in ['risky', 'unknown', 'catch_all', 'catch-all', 'role', 'disposable', 'accept_all', 'spamtrap']:
            email_status = 'Risky'
            stats['risky'] += 1
        else:
            email_status = raw_status.capitalize() if raw_status else 'Unknown'

        # Extract additional info if available
        reason = r.get('reason', r.get('message', r.get('description', '')))
        score = r.get('score', r.get('quality_score'))

        # Update contact
        existing = conn.execute("SELECT id, email_status FROM contacts WHERE LOWER(email)=?", (email.lower(),)).fetchone()
        if existing:
            # Update email_status
            conn.execute("UPDATE contacts SET email_status=?, updated_at=? WHERE id=?",
                        (email_status, datetime.now().isoformat(), existing[0]))

            # If email is invalid, optionally update contact status to Bounced
            if email_status == 'Invalid':
                conn.execute("UPDATE contacts SET status='Bounced' WHERE id=? AND status NOT IN ('Client', 'Opportunity')",
                           (existing[0],))

            stats['processed'] += 1
        else:
            stats['not_found'] += 1

    # Log webhook event
    conn.execute("""INSERT INTO webhook_events
        (source, event_type, email, payload, processed)
        VALUES (?, ?, ?, ?, 1)""",
        ('bulkemailchecker', 'validation', None, json.dumps(payload)))

    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "message": f"Processed {stats['processed']} emails",
        "stats": stats
    }

@app.post("/webhook/{source}")
async def generic_webhook(source: str, request: Request):
    try: payload = await request.json()
    except: payload = {}
    conn = get_db(); conn.execute("INSERT INTO webhook_events (source, event_type, payload, processed) VALUES (?, ?, ?, 1)", (source, 'generic', json.dumps(payload))); conn.commit(); conn.close()
    return {"status": "received"}

@app.get("/api/webhooks")
def get_webhooks(limit: int = 50):
    conn = get_db(); rows = conn.execute("SELECT * FROM webhook_events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall(); conn.close()
    return {"data": [dict(r) for r in rows]}

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.1"}

@app.get("/api/info")
def info():
    return {"name": "Deduply", "version": "5.1"}

if __name__ == "__main__":
    import uvicorn
    print("Starting Deduply API v5.1")
    print("Login: admin@deduply.io / admin123")
    uvicorn.run(app, host="0.0.0.0", port=8001)
