#!/usr/bin/env python3
"""
Deduply v5.2 - Cold Email Operations Platform
FastAPI Backend with proper relational database design
"""

from fastapi import FastAPI, HTTPException, Request, Query, UploadFile, File, Depends, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from database import get_db, USE_POSTGRES
import pandas as pd
import json
import io
import hashlib
import secrets
import os
import bcrypt
import httpx
import asyncio
import threading
import time
import tempfile
import uuid
import traceback
from data_cleaning import (
    clean_name, clean_company_name, extract_domain_name,
    preview_name_cleaning, preview_company_cleaning, analyze_data_quality
)

app = FastAPI(title="Deduply API", version="5.2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Global store for background verification tasks
background_tasks = {}

# Global store for background import tasks
import_tasks = {}

# Temp directory for import files
IMPORT_TEMP_DIR = os.path.join(tempfile.gettempdir(), 'deduply_imports')
os.makedirs(IMPORT_TEMP_DIR, exist_ok=True)

def init_db():
    conn = get_db()

    if USE_POSTGRES:
        # PostgreSQL: Tables already created via schema.sql in Supabase
        # Just verify connection and ensure admin user exists
        try:
            result = conn.execute("SELECT 1").fetchone()
            print("[DB] PostgreSQL connection successful")

            # Check if admin user exists
            result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
            if result[0] == 0:
                token = secrets.token_urlsafe(32)
                pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
                conn.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                           ("admin@deduply.io", pwd_hash, "Admin", "admin", token))
                conn.commit()
                print(f"Created admin: admin@deduply.io / admin123")
        except Exception as e:
            print(f"[DB] PostgreSQL error: {e}")
        conn.close()
        return

    # SQLite: Create tables locally
    print("[DB] Using SQLite database")
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL, name TEXT, role TEXT DEFAULT 'member',
        api_token TEXT UNIQUE, is_active BOOLEAN DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, email TEXT,
        title TEXT, headline TEXT, company TEXT, seniority TEXT, first_phone TEXT, corporate_phone TEXT,
        employees INTEGER, employee_bucket TEXT, industry TEXT, keywords TEXT,
        person_linkedin_url TEXT, website TEXT, domain TEXT, company_linkedin_url TEXT,
        city TEXT, state TEXT, country TEXT,
        company_city TEXT, company_state TEXT, company_country TEXT,
        company_street_address TEXT, company_postal_code TEXT,
        annual_revenue INTEGER, annual_revenue_text TEXT,
        company_description TEXT, company_seo_description TEXT, company_founded_year INTEGER,
        region TEXT, country_strategy TEXT, status TEXT DEFAULT 'Lead',
        email_status TEXT DEFAULT 'Not Verified', times_contacted INTEGER DEFAULT 0,
        last_contacted_at TIMESTAMP, opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        notes TEXT, source_file TEXT, is_duplicate BOOLEAN DEFAULT 0, duplicate_of INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, description TEXT,
        country TEXT, status TEXT DEFAULT 'Active', total_leads INTEGER DEFAULT 0,
        emails_sent INTEGER DEFAULT 0, emails_opened INTEGER DEFAULT 0, emails_clicked INTEGER DEFAULT 0,
        emails_replied INTEGER DEFAULT 0, emails_bounced INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0, click_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS email_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, variant TEXT DEFAULT 'A',
        step_type TEXT DEFAULT 'Main', subject TEXT, body TEXT,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_clicked INTEGER DEFAULT 0,
        times_replied INTEGER DEFAULT 0, open_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        is_winner BOOLEAN DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS template_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_replied INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings INTEGER DEFAULT 0,
        FOREIGN KEY (template_id) REFERENCES email_templates(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(template_id, campaign_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(contact_id, campaign_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, list_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (list_id) REFERENCES outreach_lists(id) ON DELETE CASCADE,
        UNIQUE(contact_id, list_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS technologies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_technologies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, technology_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (technology_id) REFERENCES technologies(id) ON DELETE CASCADE,
        UNIQUE(contact_id, technology_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS outreach_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        description TEXT, contact_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, event_type TEXT, email TEXT,
        campaign_name TEXT, template_id INTEGER, payload TEXT, processed BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # Settings table for API keys and configuration
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL, value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # Add email verification columns to contacts (migration)
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_verified_at TIMESTAMP")
    except: pass
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_verification_event TEXT")
    except: pass
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_is_disposable BOOLEAN DEFAULT 0")
    except: pass
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_is_free_service BOOLEAN DEFAULT 0")
    except: pass
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_is_role_account BOOLEAN DEFAULT 0")
    except: pass
    try:
        conn.execute("ALTER TABLE contacts ADD COLUMN email_suggested TEXT")
    except: pass

    # Add country column to email_templates (migration)
    try:
        conn.execute("ALTER TABLE email_templates ADD COLUMN country TEXT")
    except: pass

    result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    if result[0] == 0:
        token = secrets.token_urlsafe(32)
        pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
        conn.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                   ("admin@deduply.io", pwd_hash, "Admin", "admin", token))
        print(f"Created admin: admin@deduply.io / admin123")

    conn.commit()
    conn.close()

init_db()

# Password helper functions (supports both legacy SHA256 and new bcrypt)
def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against stored hash (supports both SHA256 legacy and bcrypt)"""
    # Check if it's a bcrypt hash (starts with $2b$)
    if stored_hash.startswith('$2b$') or stored_hash.startswith('$2a$'):
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    # Legacy SHA256 hash
    sha256_hash = hashlib.sha256(password.encode()).hexdigest()
    return sha256_hash == stored_hash

# Helper functions for junction tables
def get_contact_campaigns(conn, contact_id):
    """Get campaign names for a contact as comma-separated string"""
    rows = conn.execute("""
        SELECT c.name FROM contact_campaigns cc
        JOIN campaigns c ON cc.campaign_id = c.id
        WHERE cc.contact_id = ?
        ORDER BY c.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None

def get_contact_lists(conn, contact_id):
    """Get outreach list names for a contact as comma-separated string"""
    rows = conn.execute("""
        SELECT ol.name FROM contact_lists cl
        JOIN outreach_lists ol ON cl.list_id = ol.id
        WHERE cl.contact_id = ?
        ORDER BY ol.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None

def get_contact_technologies(conn, contact_id):
    """Get technology names for a contact as comma-separated string"""
    rows = conn.execute("""
        SELECT t.name FROM contact_technologies ct
        JOIN technologies t ON ct.technology_id = t.id
        WHERE ct.contact_id = ?
        ORDER BY t.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None

def set_contact_campaigns(conn, contact_id, campaign_names):
    """Set campaigns for a contact (replaces existing)"""
    conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (contact_id,))
    if campaign_names:
        names = [n.strip() for n in campaign_names.split(',') if n.strip()]
        for name in names:
            # Ensure campaign exists
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (name,))
            camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (name,)).fetchone()
            if camp:
                conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)",
                           (contact_id, camp[0]))

def set_contact_lists(conn, contact_id, list_names):
    """Set outreach lists for a contact (replaces existing)"""
    conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (contact_id,))
    if list_names:
        names = [n.strip() for n in list_names.split(',') if n.strip()]
        for name in names:
            # Ensure list exists
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (name,))
            lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (name,)).fetchone()
            if lst:
                conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)",
                           (contact_id, lst[0]))

def add_contact_campaign(conn, contact_id, campaign_name):
    """Add a single campaign to a contact"""
    conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (campaign_name,))
    camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (campaign_name,)).fetchone()
    if camp:
        conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)",
                   (contact_id, camp[0]))

def add_contact_list(conn, contact_id, list_name):
    """Add a single outreach list to a contact"""
    conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (list_name,))
    lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (list_name,)).fetchone()
    if lst:
        conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)",
                   (contact_id, lst[0]))

def add_contact_technology(conn, contact_id, tech_name):
    """Add a single technology to a contact"""
    conn.execute("INSERT OR IGNORE INTO technologies (name) VALUES (?)", (tech_name,))
    tech = conn.execute("SELECT id FROM technologies WHERE name=?", (tech_name,)).fetchone()
    if tech:
        conn.execute("INSERT OR IGNORE INTO contact_technologies (contact_id, technology_id) VALUES (?, ?)",
                   (contact_id, tech[0]))

def remove_contact_campaign(conn, contact_id, campaign_name):
    """Remove a single campaign from a contact"""
    camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (campaign_name,)).fetchone()
    if camp:
        conn.execute("DELETE FROM contact_campaigns WHERE contact_id=? AND campaign_id=?",
                   (contact_id, camp[0]))

def remove_contact_list(conn, contact_id, list_name):
    """Remove a single outreach list from a contact"""
    lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (list_name,)).fetchone()
    if lst:
        conn.execute("DELETE FROM contact_lists WHERE contact_id=? AND list_id=?",
                   (contact_id, lst[0]))

def enrich_contact_with_relations(conn, contact_dict):
    """Add campaigns_assigned and outreach_lists to contact dict"""
    contact_dict['campaigns_assigned'] = get_contact_campaigns(conn, contact_dict['id'])
    contact_dict['outreach_lists'] = get_contact_lists(conn, contact_dict['id'])
    return contact_dict

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
    filters: Optional[dict] = None
    field: str
    value: Optional[str] = None
    action: Optional[str] = None

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
    country: Optional[str] = None
    campaign_ids: Optional[List[int]] = None

class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    variant: Optional[str] = None
    step_type: Optional[str] = None
    country: Optional[str] = None
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
    """Update campaign total_leads and outreach_list contact_count from junction tables"""
    conn = get_db()
    # Update campaign counts using junction table
    conn.execute("""
        UPDATE campaigns SET total_leads = (
            SELECT COUNT(DISTINCT cc.contact_id)
            FROM contact_campaigns cc
            JOIN contacts c ON cc.contact_id = c.id
            WHERE cc.campaign_id = campaigns.id AND c.is_duplicate = 0
        )
    """)
    # Update outreach list counts using junction table
    conn.execute("""
        UPDATE outreach_lists SET contact_count = (
            SELECT COUNT(DISTINCT cl.contact_id)
            FROM contact_lists cl
            JOIN contacts c ON cl.contact_id = c.id
            WHERE cl.list_id = outreach_lists.id AND c.is_duplicate = 0
        )
    """)
    conn.commit(); conn.close()

def recalc_rates(campaign_id, conn=None):
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
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=? AND is_active=1", (creds.email,)).fetchone()
    conn.close()
    if not user or not verify_password(creds.password, user['password_hash']):
        raise HTTPException(401, "Invalid credentials")
    return {"token": user['api_token'], "user": {"id": user['id'], "email": user['email'], "name": user['name'], "role": user['role']}}

@app.get("/api/auth/me")
def get_me(user: dict = Depends(get_current_user)):
    if not user: raise HTTPException(401, "Not authenticated")
    return {"id": user['id'], "email": user['email'], "name": user['name'], "role": user['role']}

@app.post("/api/auth/register")
def register(user: UserCreate):
    conn = get_db()
    try:
        token = secrets.token_urlsafe(32)
        pwd_hash = hash_password(user.password)  # Use bcrypt
        conn.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)", (user.email, pwd_hash, user.name, user.role, token))
        conn.commit(); return {"message": "Created", "token": token}
    except sqlite3.IntegrityError: raise HTTPException(400, "Email exists")
    finally: conn.close()

class ChangePassword(BaseModel):
    current_password: str
    new_password: str

@app.post("/api/auth/change-password")
def change_password(data: ChangePassword, user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    db_user = conn.execute("SELECT * FROM users WHERE id=?", (user['id'],)).fetchone()

    if not db_user or not verify_password(data.current_password, db_user['password_hash']):
        conn.close()
        raise HTTPException(400, "Current password is incorrect")

    if len(data.new_password) < 6:
        conn.close()
        raise HTTPException(400, "New password must be at least 6 characters")

    new_hash = hash_password(data.new_password)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user['id']))
    conn.commit()
    conn.close()
    return {"message": "Password changed successfully"}

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
                email_status: Optional[str] = None, show_duplicates: bool = False, sort_by: str = "id", sort_order: str = "desc"):
    conn = get_db()
    where = ["1=1"] if show_duplicates else ["c.is_duplicate=0"]
    params = []
    joins = []

    if search:
        where.append("(c.first_name LIKE ? OR c.last_name LIKE ? OR c.email LIKE ? OR c.company LIKE ? OR c.title LIKE ?)")
        params.extend([f"%{search}%"]*5)
    if status: where.append("c.status=?"); params.append(status)
    if country: where.append("c.company_country=?"); params.append(country)
    if country_strategy: where.append("c.country_strategy=?"); params.append(country_strategy)
    if seniority: where.append("c.seniority=?"); params.append(seniority)
    if industry: where.append("c.industry LIKE ?"); params.append(f"%{industry}%")
    if email_status: where.append("c.email_status=?"); params.append(email_status)

    # Filter by campaign using junction table
    if campaigns:
        joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
        where.append("camp.name = ?")
        params.append(campaigns)

    # Filter by outreach list using junction table
    if outreach_lists:
        joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
        where.append("ol.name = ?")
        params.append(outreach_lists)

    where_sql = " AND ".join(where)
    joins_sql = " ".join(joins)

    # Count total (use DISTINCT because of joins)
    total = conn.execute(f"SELECT COUNT(DISTINCT c.id) FROM contacts c {joins_sql} WHERE {where_sql}", params).fetchone()[0]

    valid_sorts = ['id', 'first_name', 'last_name', 'email', 'company', 'status', 'created_at', 'employees']
    sort_by = sort_by if sort_by in valid_sorts else 'id'

    # Get contacts
    rows = conn.execute(f"""
        SELECT DISTINCT c.* FROM contacts c {joins_sql}
        WHERE {where_sql}
        ORDER BY c.{sort_by} {'DESC' if sort_order.lower()=='desc' else 'ASC'}
        LIMIT ? OFFSET ?
    """, params + [page_size, (page-1)*page_size]).fetchall()

    # Enrich with campaigns and lists
    result = []
    for r in rows:
        contact = dict(r)
        enrich_contact_with_relations(conn, contact)
        result.append(contact)

    conn.close()
    return {"data": result, "total": total, "page": page, "page_size": page_size, "total_pages": max(1, (total+page_size-1)//page_size)}

@app.get("/api/contacts/export")
def export_contacts(columns: Optional[str] = None, search: Optional[str] = None, status: Optional[str] = None,
                   campaigns: Optional[str] = None, outreach_lists: Optional[str] = None, country: Optional[str] = None,
                   country_strategy: Optional[str] = None, seniority: Optional[str] = None, industry: Optional[str] = None,
                   email_status: Optional[str] = None, valid_emails_only: bool = False):
    conn = get_db()
    where = ["c.is_duplicate=0"]
    params = []
    joins = []

    if search:
        where.append("(c.first_name LIKE ? OR c.last_name LIKE ? OR c.email LIKE ? OR c.company LIKE ? OR c.title LIKE ?)")
        params.extend([f"%{search}%"]*5)
    if status: where.append("c.status=?"); params.append(status)
    if country: where.append("c.company_country=?"); params.append(country)
    if country_strategy: where.append("c.country_strategy=?"); params.append(country_strategy)
    if seniority: where.append("c.seniority=?"); params.append(seniority)
    if industry: where.append("c.industry LIKE ?"); params.append(f"%{industry}%")
    # Email verification filters
    if email_status: where.append("c.email_status=?"); params.append(email_status)
    if valid_emails_only: where.append("c.email_status='Valid'")
    if campaigns:
        joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
        where.append("camp.name=?"); params.append(campaigns)
    if outreach_lists:
        joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
        where.append("ol.name=?"); params.append(outreach_lists)

    all_cols = ['id', 'first_name', 'last_name', 'email', 'title', 'headline', 'company', 'seniority',
        'first_phone', 'corporate_phone', 'employees', 'employee_bucket', 'industry', 'keywords',
        'person_linkedin_url', 'website', 'domain', 'company_linkedin_url',
        'city', 'state', 'country', 'company_city', 'company_state', 'company_country',
        'company_street_address', 'company_postal_code', 'annual_revenue', 'annual_revenue_text',
        'company_description', 'company_seo_description', 'company_founded_year',
        'region', 'country_strategy', 'status', 'email_status', 'times_contacted', 'last_contacted_at',
        'opportunities', 'meetings_booked', 'notes', 'created_at']
    selected = [c.strip() for c in (columns or '').split(',') if c.strip() in all_cols] or all_cols

    joins_sql = " ".join(joins)
    where_sql = " AND ".join(where)

    rows = conn.execute(f"SELECT DISTINCT c.id, {','.join(['c.'+c for c in selected if c != 'id'])} FROM contacts c {joins_sql} WHERE {where_sql}", params).fetchall()

    data = []
    for r in rows:
        row_dict = dict(r)
        cid = row_dict.get('id') or r[0]
        row_dict['campaigns_assigned'] = get_contact_campaigns(conn, cid)
        row_dict['outreach_lists'] = get_contact_lists(conn, cid)
        row_dict['company_technologies'] = get_contact_technologies(conn, cid)
        data.append(row_dict)

    conn.close()

    df = pd.DataFrame(data)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"})

@app.get("/api/contacts/columns")
def get_columns():
    return {"columns": [
        {"id": "first_name", "label": "First Name"}, {"id": "last_name", "label": "Last Name"}, {"id": "email", "label": "Email"},
        {"id": "title", "label": "Title"}, {"id": "headline", "label": "Headline"}, {"id": "company", "label": "Company"},
        {"id": "seniority", "label": "Seniority"}, {"id": "first_phone", "label": "Phone"}, {"id": "corporate_phone", "label": "Corp. Phone"},
        {"id": "employees", "label": "Employees"}, {"id": "employee_bucket", "label": "Company Size"}, {"id": "industry", "label": "Industry"},
        {"id": "keywords", "label": "Keywords"}, {"id": "person_linkedin_url", "label": "LinkedIn"}, {"id": "website", "label": "Website"},
        {"id": "domain", "label": "Domain"}, {"id": "company_linkedin_url", "label": "Company LinkedIn"},
        {"id": "city", "label": "City"}, {"id": "state", "label": "State"}, {"id": "country", "label": "Country"},
        {"id": "company_city", "label": "Company City"}, {"id": "company_state", "label": "Company State"}, {"id": "company_country", "label": "Company Country"},
        {"id": "company_street_address", "label": "Company Address"}, {"id": "company_postal_code", "label": "Postal Code"},
        {"id": "annual_revenue", "label": "Revenue"}, {"id": "annual_revenue_text", "label": "Revenue (Text)"},
        {"id": "company_description", "label": "Company Desc"}, {"id": "company_seo_description", "label": "SEO Desc"},
        {"id": "company_technologies", "label": "Technologies"}, {"id": "company_founded_year", "label": "Founded"},
        {"id": "region", "label": "Region"}, {"id": "country_strategy", "label": "Country Strategy"},
        {"id": "outreach_lists", "label": "Outreach Lists"}, {"id": "campaigns_assigned", "label": "Campaigns"},
        {"id": "status", "label": "Status"}, {"id": "email_status", "label": "Email Status"},
        {"id": "times_contacted", "label": "Times Contacted"}, {"id": "last_contacted_at", "label": "Last Contact"},
        {"id": "notes", "label": "Notes"}, {"id": "created_at", "label": "Created"}
    ]}

@app.get("/api/contacts/{contact_id}")
def get_contact(contact_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Not found")
    contact = dict(row)
    enrich_contact_with_relations(conn, contact)
    conn.close()
    return contact

@app.post("/api/contacts")
def create_contact(contact: ContactCreate):
    conn = get_db()
    data = contact.dict(exclude_none=True)

    # Extract campaigns and lists before inserting
    campaigns_str = data.pop('campaigns_assigned', None)
    lists_str = data.pop('outreach_lists', None)

    fields = list(data.keys())
    conn.execute(f"INSERT INTO contacts ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})", list(data.values()))
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Set campaigns and lists using junction tables
    if campaigns_str:
        set_contact_campaigns(conn, cid, campaigns_str)
    if lists_str:
        set_contact_lists(conn, cid, lists_str)

    conn.commit()
    conn.close()
    update_counts()
    return {"id": cid, "message": "Created"}

@app.put("/api/contacts/{contact_id}")
def update_contact(contact_id: int, contact: ContactUpdate):
    conn = get_db()
    data = {k: v for k, v in contact.dict().items() if v is not None}
    if not data: raise HTTPException(400, "No fields")

    # Extract campaigns and lists
    campaigns_str = data.pop('campaigns_assigned', None)
    lists_str = data.pop('outreach_lists', None)

    if data:
        data['updated_at'] = datetime.now().isoformat()
        conn.execute(f"UPDATE contacts SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?", list(data.values())+[contact_id])

    # Update campaigns and lists if provided
    if campaigns_str is not None:
        set_contact_campaigns(conn, contact_id, campaigns_str)
    if lists_str is not None:
        set_contact_lists(conn, contact_id, lists_str)

    conn.commit()
    conn.close()
    update_counts()
    return {"message": "Updated"}

@app.delete("/api/contacts/{contact_id}")
def delete_contact(contact_id: int):
    conn = get_db()
    # Junction tables will cascade delete
    conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
    conn.commit()
    conn.close()
    update_counts()
    return {"message": "Deleted"}

@app.post("/api/contacts/bulk")
def bulk_update(req: BulkUpdateRequest):
    conn = get_db()

    # Get contact IDs either from direct list or by applying filters
    if req.contact_ids:
        contact_ids = req.contact_ids
    elif req.filters:
        query = "SELECT DISTINCT c.id FROM contacts c"
        joins = []
        where = ["c.is_duplicate=0"]
        params = []
        f = req.filters

        if f.get('search'):
            where.append("(c.first_name LIKE ? OR c.last_name LIKE ? OR c.email LIKE ? OR c.company LIKE ? OR c.title LIKE ?)")
            s = f"%{f['search']}%"; params.extend([s]*5)
        if f.get('status'): where.append("c.status=?"); params.append(f['status'])
        if f.get('campaign'):
            joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
            where.append("camp.name=?"); params.append(f['campaign'])
        if f.get('outreach_list'):
            joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
            where.append("ol.name=?"); params.append(f['outreach_list'])
        if f.get('country_strategy'): where.append("c.country_strategy=?"); params.append(f['country_strategy'])
        if f.get('country'): where.append("c.company_country=?"); params.append(f['country'])
        if f.get('seniority'): where.append("c.seniority=?"); params.append(f['seniority'])
        if f.get('industry'): where.append("c.industry=?"); params.append(f['industry'])

        query = f"{query} {' '.join(joins)} WHERE {' AND '.join(where)}"
        rows = conn.execute(query, params).fetchall()
        contact_ids = [row[0] for row in rows]
    else:
        raise HTTPException(400, "No contacts specified")

    if not contact_ids:
        conn.close()
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
    # Handle campaigns_assigned
    elif field == 'campaigns_assigned':
        for cid in contact_ids:
            if action == 'add' and value:
                add_contact_campaign(conn, cid, value)
            elif action == 'remove' and value:
                remove_contact_campaign(conn, cid, value)
            elif action == 'set':
                set_contact_campaigns(conn, cid, value)
            conn.execute("UPDATE contacts SET updated_at=? WHERE id=?", (now, cid))
            updated += 1
    # Handle outreach_lists
    elif field == 'outreach_lists':
        for cid in contact_ids:
            if action == 'add' and value:
                add_contact_list(conn, cid, value)
            elif action == 'remove' and value:
                remove_contact_list(conn, cid, value)
            elif action == 'set':
                set_contact_lists(conn, cid, value)
            conn.execute("UPDATE contacts SET updated_at=? WHERE id=?", (now, cid))
            updated += 1
    # Handle simple field updates
    else:
        allowed_fields = ['status', 'email_status', 'country_strategy', 'seniority', 'company_country', 'industry', 'title', 'company', 'first_name', 'last_name', 'notes']
        if field not in allowed_fields:
            conn.close()
            raise HTTPException(400, f"Field {field} cannot be bulk updated")
        placeholders = ','.join(['?'] * len(contact_ids))
        conn.execute(f"UPDATE contacts SET {field}=?, updated_at=? WHERE id IN ({placeholders})", [value, now] + contact_ids)
        updated = len(contact_ids)

    conn.commit()
    conn.close()
    update_counts()
    return {"updated": updated}

@app.get("/api/duplicates")
def get_duplicates():
    conn = get_db()
    rows = conn.execute("SELECT LOWER(email) as email, COUNT(*) as cnt, GROUP_CONCAT(id) as ids FROM contacts WHERE email IS NOT NULL AND email != '' AND is_duplicate=0 GROUP BY LOWER(email) HAVING COUNT(*)>1 ORDER BY cnt DESC LIMIT 100").fetchall()
    groups = []
    for row in rows:
        ids = [int(i) for i in row['ids'].split(',')]
        contacts = conn.execute(f"SELECT * FROM contacts WHERE id IN ({','.join(['?']*len(ids))})", ids).fetchall()
        enriched = []
        for c in contacts:
            contact = dict(c)
            enrich_contact_with_relations(conn, contact)
            enriched.append(contact)
        groups.append({"email": row['email'], "count": row['cnt'], "contacts": enriched})
    conn.close()
    return {"groups": groups, "total_groups": len(groups)}

@app.post("/api/duplicates/merge")
def merge_duplicates(req: MergeRequest):
    conn = get_db()
    primary = conn.execute("SELECT * FROM contacts WHERE id=?", (req.primary_id,)).fetchone()
    if not primary: raise HTTPException(404, "Not found")

    # Get all campaigns and lists from primary
    primary_camps = set(r[0] for r in conn.execute(
        "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
        (req.primary_id,)).fetchall())
    primary_lists = set(r[0] for r in conn.execute(
        "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
        (req.primary_id,)).fetchall())

    for dup_id in req.duplicate_ids:
        dup = conn.execute("SELECT * FROM contacts WHERE id=?", (dup_id,)).fetchone()
        if dup:
            # Get campaigns and lists from duplicate
            dup_camps = set(r[0] for r in conn.execute(
                "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
                (dup_id,)).fetchall())
            dup_lists = set(r[0] for r in conn.execute(
                "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
                (dup_id,)).fetchall())

            primary_camps.update(dup_camps)
            primary_lists.update(dup_lists)

            # Mark as duplicate (junction table entries will be deleted due to CASCADE)
            conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (req.primary_id, dup_id))
            # Delete junction entries for duplicate
            conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (dup_id,))
            conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (dup_id,))

    # Set merged campaigns and lists on primary
    set_contact_campaigns(conn, req.primary_id, ', '.join(sorted(primary_camps)) if primary_camps else None)
    set_contact_lists(conn, req.primary_id, ', '.join(sorted(primary_lists)) if primary_lists else None)
    conn.execute("UPDATE contacts SET updated_at=? WHERE id=?", (datetime.now().isoformat(), req.primary_id))

    conn.commit()
    conn.close()
    update_counts()
    return {"message": f"Merged {len(req.duplicate_ids)} contacts"}

@app.post("/api/duplicates/unmerge/{contact_id}")
def unmerge(contact_id: int):
    conn = get_db()
    conn.execute("UPDATE contacts SET is_duplicate=0, duplicate_of=NULL WHERE id=?", (contact_id,))
    conn.commit()
    conn.close()
    return {"message": "Restored"}

@app.get("/api/duplicates/stats")
def get_duplicate_stats():
    conn = get_db()
    total_groups = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT LOWER(email) FROM contacts
            WHERE email IS NOT NULL AND email != '' AND is_duplicate=0
            GROUP BY LOWER(email) HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    total_duplicates = conn.execute("""
        SELECT COALESCE(SUM(cnt - 1), 0) FROM (
            SELECT COUNT(*) as cnt FROM contacts
            WHERE email IS NOT NULL AND email != '' AND is_duplicate=0
            GROUP BY LOWER(email) HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

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
    conn = get_db()

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

        contacts = conn.execute(
            f"SELECT * FROM contacts WHERE id IN ({','.join(['?']*len(ids))}) ORDER BY created_at ASC",
            ids
        ).fetchall()

        if not contacts:
            continue

        primary = contacts[0]
        primary_id = primary['id']
        duplicates = contacts[1:]

        # Collect all campaigns and lists
        all_camps = set(r[0] for r in conn.execute(
            "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
            (primary_id,)).fetchall())
        all_lists = set(r[0] for r in conn.execute(
            "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
            (primary_id,)).fetchall())

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
            # Collect campaigns and lists from duplicate
            dup_camps = set(r[0] for r in conn.execute(
                "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
                (dup['id'],)).fetchall())
            dup_lists = set(r[0] for r in conn.execute(
                "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
                (dup['id'],)).fetchall())

            all_camps.update(dup_camps)
            all_lists.update(dup_lists)

            for field in ['first_name', 'last_name', 'title', 'company', 'first_phone', 'corporate_phone', 'person_linkedin_url', 'website']:
                if not best_data[field] and dup[field]:
                    best_data[field] = dup[field]

            best_data['times_contacted'] += dup['times_contacted'] or 0
            best_data['meetings_booked'] += dup['meetings_booked'] or 0
            best_data['opportunities'] += dup['opportunities'] or 0

            conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (primary_id, dup['id']))
            conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (dup['id'],))
            conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (dup['id'],))
            merged_contacts += 1

        conn.execute("""
            UPDATE contacts SET
                first_name=?, last_name=?, title=?, company=?, first_phone=?, corporate_phone=?,
                person_linkedin_url=?, website=?, times_contacted=?, meetings_booked=?, opportunities=?,
                updated_at=?
            WHERE id=?
        """, (
            best_data['first_name'], best_data['last_name'], best_data['title'], best_data['company'],
            best_data['first_phone'], best_data['corporate_phone'], best_data['person_linkedin_url'],
            best_data['website'], best_data['times_contacted'], best_data['meetings_booked'],
            best_data['opportunities'], datetime.now().isoformat(), primary_id
        ))

        set_contact_campaigns(conn, primary_id, ', '.join(sorted(all_camps)) if all_camps else None)
        set_contact_lists(conn, primary_id, ', '.join(sorted(all_lists)) if all_lists else None)

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
    conn = get_db()

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

    all_camps = set(r[0] for r in conn.execute(
        "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
        (primary_id,)).fetchall())
    all_lists = set(r[0] for r in conn.execute(
        "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
        (primary_id,)).fetchall())

    for dup in duplicates:
        dup_camps = set(r[0] for r in conn.execute(
            "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
            (dup['id'],)).fetchall())
        dup_lists = set(r[0] for r in conn.execute(
            "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
            (dup['id'],)).fetchall())

        all_camps.update(dup_camps)
        all_lists.update(dup_lists)

        conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (primary_id, dup['id']))
        conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (dup['id'],))
        conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (dup['id'],))

    set_contact_campaigns(conn, primary_id, ', '.join(sorted(all_camps)) if all_camps else None)
    set_contact_lists(conn, primary_id, ', '.join(sorted(all_lists)) if all_lists else None)
    conn.execute("UPDATE contacts SET updated_at=? WHERE id=?", (datetime.now().isoformat(), primary_id))

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
            "website", "domain", "company_linkedin_url",
            # Person location
            "city", "state", "country",
            # Company location
            "company_city", "company_state", "company_country", "company_street_address", "company_postal_code",
            # Company details
            "annual_revenue", "annual_revenue_text", "company_description", "company_seo_description",
            "company_technologies", "company_founded_year",
            # System fields
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
            elif 'corporate_phone' in cl or 'company_phone' in cl: suggestions[col] = 'corporate_phone'
            elif cl in ['employees', '_employees', 'employees_count']: suggestions[col] = 'employees'
            elif cl == 'industry': suggestions[col] = 'industry'
            elif cl == 'keywords': suggestions[col] = 'keywords'
            elif cl == 'linkedin' or 'person_linkedin' in cl: suggestions[col] = 'person_linkedin_url'
            elif cl in ['website', 'company_website']: suggestions[col] = 'website'
            elif cl in ['domain', 'dominio', 'company_domain']: suggestions[col] = 'domain'
            elif 'company_linkedin' in cl: suggestions[col] = 'company_linkedin_url'
            # Company location (check BEFORE person location to avoid conflicts)
            elif 'company_city' in cl: suggestions[col] = 'company_city'
            elif 'company_state' in cl: suggestions[col] = 'company_state'
            elif 'company_country' in cl: suggestions[col] = 'company_country'
            elif 'company_street' in cl: suggestions[col] = 'company_street_address'
            elif 'company_postal' in cl: suggestions[col] = 'company_postal_code'
            # Person location (must come AFTER company location)
            elif cl == 'city' or cl == 'person_city': suggestions[col] = 'city'
            elif cl == 'state' or cl == 'person_state': suggestions[col] = 'state'
            elif cl == 'country' or cl == 'person_country': suggestions[col] = 'country'
            # Company details - "Clean" = numeric, regular = text format
            elif 'revenue_clean' in cl: suggestions[col] = 'annual_revenue'
            elif 'annual_revenue' in cl and 'clean' not in cl: suggestions[col] = 'annual_revenue_text'
            elif 'short_description' in cl: suggestions[col] = 'company_description'
            elif 'seo_description' in cl: suggestions[col] = 'company_seo_description'
            elif 'technologies' in cl: suggestions[col] = 'company_technologies'
            elif 'founded_year' in cl: suggestions[col] = 'company_founded_year'
            # System fields
            elif 'outreach' in cl: suggestions[col] = 'outreach_lists'
            elif 'campaign' in cl or 'assigned' in cl: suggestions[col] = 'campaigns_assigned'

        return {"filename": file.filename, "total_rows": total_rows, "columns": list(df.columns),
                "preview": df.head(5).fillna('').to_dict(orient='records'), "target_columns": target_columns, "suggested_mapping": suggestions}
    except Exception as e:
        raise HTTPException(400, f"Error: {str(e)}")

@app.post("/api/import/execute")
async def execute_import(file: UploadFile = File(...), column_mapping: str = Query(...), outreach_list: str = Query(None),
                        campaigns: str = Query(None), country_strategy: str = Query(None), check_duplicates: bool = Query(True),
                        merge_duplicates: bool = Query(True), verify_emails: bool = Query(False)):
    """Start a background import job. Returns job_id immediately."""
    # Read and validate file
    content = await file.read()
    df = None
    for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
        try: df = pd.read_csv(io.BytesIO(content), encoding=enc); break
        except: continue
    if df is None: raise HTTPException(400, "Cannot read CSV")

    total_rows = len(df)

    # Save file to temp location
    job_uuid = str(uuid.uuid4())
    temp_file_path = os.path.join(IMPORT_TEMP_DIR, f"{job_uuid}.csv")
    with open(temp_file_path, 'wb') as f:
        f.write(content)

    # Create job record
    conn = get_db()
    if USE_POSTGRES:
        result = conn.execute("""
            INSERT INTO import_jobs
            (status, total_rows, file_name, file_path, column_mapping,
             outreach_list, campaigns, country_strategy, check_duplicates,
             merge_duplicates, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            'pending', total_rows, file.filename, temp_file_path,
            column_mapping, outreach_list or '', campaigns or '',
            country_strategy or '', check_duplicates, merge_duplicates,
            datetime.now().isoformat()
        ))
        job_id = result.fetchone()[0]
    else:
        conn.execute("""
            INSERT INTO import_jobs
            (status, total_rows, file_name, file_path, column_mapping,
             outreach_list, campaigns, country_strategy, check_duplicates,
             merge_duplicates, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            'pending', total_rows, file.filename, temp_file_path,
            column_mapping, outreach_list or '', campaigns or '',
            country_strategy or '', 1 if check_duplicates else 0, 1 if merge_duplicates else 0,
            datetime.now().isoformat()
        ))
        job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    # Start background thread
    start_import_thread(job_id)
    import_tasks[job_id] = True

    print(f"[IMPORT] Started background job {job_id} for {total_rows} rows")
    return {
        "job_id": job_id,
        "total_rows": total_rows,
        "message": "Import started in background"
    }


def start_import_thread(job_id: int):
    """Start import job in a background thread."""
    thread = threading.Thread(target=run_import_job_sync, args=(job_id,), daemon=True)
    thread.start()
    print(f"[IMPORT] Started background thread for job {job_id}")


def run_import_job_sync(job_id: int):
    """Synchronous background task to import CSV (runs in thread)."""
    print(f"[IMPORT THREAD] Starting job {job_id}")
    conn = None

    try:
        conn = get_db()

        # Get job details
        job = conn.execute("""
            SELECT file_path, column_mapping, outreach_list, campaigns,
                   country_strategy, check_duplicates, merge_duplicates, file_name
            FROM import_jobs WHERE id=?
        """, (job_id,)).fetchone()

        if not job:
            print(f"[IMPORT THREAD] Job {job_id} not found")
            return

        file_path = job[0]
        column_mapping = job[1]
        outreach_list = job[2] if job[2] else None
        campaigns = job[3] if job[3] else None
        country_strategy = job[4] if job[4] else None
        check_duplicates = bool(job[5])
        merge_duplicates = bool(job[6])
        file_name = job[7]

        # Parse column mapping
        mapping = json.loads(column_mapping)

        # Read CSV file
        df = None
        with open(file_path, 'rb') as f:
            content = f.read()
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc)
                break
            except:
                continue

        if df is None:
            conn.execute("""
                UPDATE import_jobs SET status='failed', error_message='Cannot read CSV file'
                WHERE id=?
            """, (job_id,))
            conn.commit()
            return

        # Mark job as running
        conn.execute("""
            UPDATE import_jobs SET status='running', started_at=? WHERE id=?
        """, (datetime.now().isoformat(), job_id))
        conn.commit()

        # Ensure outreach list exists
        if outreach_list:
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (outreach_list,))

        # Ensure campaigns exist
        campaign_list = [c.strip() for c in (campaigns or '').split(',') if c.strip()]
        for c in campaign_list:
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c,))

        # Build email index for duplicate checking (only non-duplicates)
        email_index = {}
        if check_duplicates:
            for row in conn.execute("SELECT id, LOWER(email) FROM contacts WHERE email IS NOT NULL AND is_duplicate=0"):
                email_index[row[1]] = row[0]

        stats = {'imported': 0, 'merged': 0, 'duplicates_found': 0, 'failed': 0}
        processed = 0

        for idx, row in df.iterrows():
            # Check if job was cancelled
            job_status = conn.execute("SELECT status FROM import_jobs WHERE id=?", (job_id,)).fetchone()
            if job_status and job_status[0] == 'cancelled':
                print(f"[IMPORT THREAD] Job {job_id} was cancelled")
                break

            try:
                data = {}
                csv_campaigns = set()
                csv_lists = set()
                csv_technologies = set()

                for src_col, tgt_col in mapping.items():
                    if tgt_col and src_col in row:
                        val = row[src_col]
                        if pd.notna(val):
                            # Integer fields
                            if tgt_col in ['employees', 'annual_revenue', 'company_founded_year']:
                                try: data[tgt_col] = int(float(str(val).replace(',', '').replace('$', '')))
                                except: pass
                            # Junction table fields
                            elif tgt_col == 'campaigns_assigned':
                                csv_campaigns.update(c.strip() for c in str(val).split(',') if c.strip())
                            elif tgt_col == 'outreach_lists':
                                csv_lists.update(l.strip() for l in str(val).split(',') if l.strip())
                            elif tgt_col == 'company_technologies':
                                csv_technologies.update(t.strip() for t in str(val).split(',') if t.strip())
                            # Normalize email_status values
                            elif tgt_col == 'email_status':
                                raw = str(val).strip().lower()
                                if raw in ['passed', 'valid', 'ok', 'good', 'verified', 'deliverable', 'true', '1']:
                                    data['email_status'] = 'Valid'
                                elif raw in ['failed', 'invalid', 'bad', 'undeliverable', 'bounce', 'bounced', 'false', '0']:
                                    data['email_status'] = 'Invalid'
                                elif raw in ['unknown', 'catchall', 'catch-all', 'catch_all', 'risky', 'accept_all', 'accept-all']:
                                    data['email_status'] = 'Unknown'
                                elif raw in ['not verified', 'not_verified', 'unverified', 'pending', '']:
                                    data['email_status'] = 'Not Verified'
                                else:
                                    data['email_status'] = 'Not Verified'
                            # Normalize lead status values
                            elif tgt_col == 'status':
                                raw = str(val).strip().lower()
                                status_map = {
                                    'lead': 'Lead', 'new': 'Lead', 'prospect': 'Lead', 'new lead': 'Lead',
                                    'contacted': 'Contacted', 'emailed': 'Contacted', 'reached': 'Contacted', 'sent': 'Contacted',
                                    'replied': 'Replied', 'responded': 'Replied', 'response': 'Replied', 'reply': 'Replied',
                                    'engaged': 'Engaged', 'engaging': 'Engaged', 'interested': 'Engaged',
                                    'meeting': 'Meeting Booked', 'meeting booked': 'Meeting Booked', 'meetings': 'Meeting Booked',
                                    'booked': 'Meeting Booked', 'scheduled': 'Meeting Booked', 'call booked': 'Meeting Booked',
                                    'opportunity': 'Opportunity', 'deal': 'Opportunity', 'qualified': 'Opportunity', 'opp': 'Opportunity',
                                    'client': 'Client', 'customer': 'Client', 'won': 'Client', 'closed': 'Client', 'closed won': 'Client',
                                    'not interested': 'Not Interested', 'unqualified': 'Not Interested', 'bad fit': 'Not Interested',
                                    'no interest': 'Not Interested', 'rejected': 'Not Interested', 'lost': 'Not Interested',
                                    'bounced': 'Bounced', 'bounce': 'Bounced', 'hard bounce': 'Bounced', 'invalid email': 'Bounced',
                                    'unsubscribed': 'Unsubscribed', 'unsub': 'Unsubscribed', 'opted out': 'Unsubscribed', 'opt out': 'Unsubscribed'
                                }
                                data['status'] = status_map.get(raw, str(val).strip().title())
                            else:
                                data[tgt_col] = str(val).strip()

                # Add form-specified campaigns and lists
                csv_campaigns.update(campaign_list)
                if outreach_list:
                    csv_lists.add(outreach_list)

                # Apply country_strategy from form if not already set
                if country_strategy and not data.get('country_strategy'):
                    data['country_strategy'] = country_strategy

                data['employee_bucket'] = compute_employee_bucket(data.get('employees'))
                data['source_file'] = file_name
                email = (data.get('email') or '').lower().strip()

                # Update current row indicator
                conn.execute("UPDATE import_jobs SET current_row=? WHERE id=?", (email or f"Row {idx+1}", job_id))
                conn.commit()

                if check_duplicates and email and email in email_index:
                    stats['duplicates_found'] += 1
                    if merge_duplicates:
                        existing_id = email_index[email]
                        for camp in csv_campaigns:
                            add_contact_campaign(conn, existing_id, camp)
                        for lst in csv_lists:
                            add_contact_list(conn, existing_id, lst)
                        for tech in csv_technologies:
                            add_contact_technology(conn, existing_id, tech)
                        if data.get('country_strategy'):
                            conn.execute("UPDATE contacts SET country_strategy=?, updated_at=? WHERE id=?",
                                       (data['country_strategy'], datetime.now().isoformat(), existing_id))
                        stats['merged'] += 1
                else:
                    if data:
                        fields = list(data.keys())
                        if USE_POSTGRES:
                            placeholders = ','.join(['%s']*len(fields))
                            result = conn.execute(f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders}) RETURNING id", list(data.values()))
                            cid = result.fetchone()[0]
                        else:
                            placeholders = ','.join(['?']*len(fields))
                            conn.execute(f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders})", list(data.values()))
                            cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                        for camp in csv_campaigns:
                            add_contact_campaign(conn, cid, camp)
                        for lst in csv_lists:
                            add_contact_list(conn, cid, lst)
                        for tech in csv_technologies:
                            add_contact_technology(conn, cid, tech)

                        stats['imported'] += 1
                        if email:
                            email_index[email] = cid

            except Exception as e:
                stats['failed'] += 1

            processed += 1

            # Update progress every 10 rows (or every row for small imports)
            if processed % 10 == 0 or processed == len(df):
                conn.execute("""
                    UPDATE import_jobs SET
                        processed_count=?, imported_count=?, merged_count=?,
                        duplicates_found=?, failed_count=?
                    WHERE id=?
                """, (
                    processed, stats['imported'], stats['merged'],
                    stats['duplicates_found'], stats['failed'], job_id
                ))
                conn.commit()

        # Mark job as completed
        conn.execute("""
            UPDATE import_jobs SET
                status='completed', current_row=NULL, completed_at=?,
                processed_count=?, imported_count=?, merged_count=?,
                duplicates_found=?, failed_count=?
            WHERE id=?
        """, (
            datetime.now().isoformat(), processed,
            stats['imported'], stats['merged'],
            stats['duplicates_found'], stats['failed'], job_id
        ))
        conn.commit()

        # Cleanup temp file
        try:
            os.remove(file_path)
        except:
            pass

        update_counts()
        print(f"[IMPORT THREAD] Job {job_id} completed: {stats}")

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[IMPORT THREAD] Job {job_id} failed: {error_msg}")
        try:
            if conn:
                conn.execute("UPDATE import_jobs SET status='failed', error_message=? WHERE id=?",
                            (str(e)[:500], job_id))
                conn.commit()
        except:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass
        if job_id in import_tasks:
            del import_tasks[job_id]


@app.get("/api/import/job/{job_id}")
def get_import_job(job_id: int):
    """Get status of an import job."""
    conn = get_db()
    job = conn.execute("""
        SELECT id, status, total_rows, processed_count, imported_count,
               merged_count, duplicates_found, failed_count, current_row,
               error_message, file_name, created_at, started_at, completed_at
        FROM import_jobs WHERE id=?
    """, (job_id,)).fetchone()
    conn.close()

    if not job:
        raise HTTPException(404, "Job not found")

    return {
        "id": job[0],
        "status": job[1],
        "total_rows": job[2],
        "processed_count": job[3],
        "imported_count": job[4],
        "merged_count": job[5],
        "duplicates_found": job[6],
        "failed_count": job[7],
        "current_row": job[8],
        "error_message": job[9],
        "file_name": job[10],
        "created_at": job[11],
        "started_at": job[12],
        "completed_at": job[13]
    }


@app.post("/api/import/job/{job_id}/cancel")
def cancel_import_job(job_id: int):
    """Cancel a running import job."""
    conn = get_db()
    conn.execute("UPDATE import_jobs SET status='cancelled' WHERE id=? AND status='running'", (job_id,))
    conn.commit()
    conn.close()
    return {"status": "cancelled"}


@app.get("/api/import/jobs/active")
def get_active_import_jobs():
    """Get all active (pending/running) import jobs."""
    conn = get_db()
    jobs = conn.execute("""
        SELECT id, status, total_rows, processed_count, imported_count,
               merged_count, duplicates_found, failed_count, current_row,
               file_name, created_at
        FROM import_jobs WHERE status IN ('pending', 'running')
        ORDER BY created_at DESC
    """).fetchall()
    conn.close()

    return [{
        "id": j[0], "status": j[1], "total_rows": j[2],
        "processed_count": j[3], "imported_count": j[4],
        "merged_count": j[5], "duplicates_found": j[6],
        "failed_count": j[7], "current_row": j[8],
        "file_name": j[9], "created_at": j[10]
    } for j in jobs]

@app.get("/api/filters")
def get_filters():
    conn = get_db()
    opts = {'statuses': ['Lead', 'Contacted', 'Replied', 'Scheduled', 'Show', 'No-Show', 'Qualified', 'Client', 'Not Interested', 'Bounced', 'Unsubscribed'],
            'countries': [r[0] for r in conn.execute("SELECT DISTINCT company_country FROM contacts WHERE company_country IS NOT NULL AND company_country != '' ORDER BY company_country")],
            'country_strategies': ['Mexico', 'United States', 'Germany', 'Spain'],
            'seniorities': [r[0] for r in conn.execute("SELECT DISTINCT seniority FROM contacts WHERE seniority IS NOT NULL AND seniority != '' ORDER BY seniority")],
            'industries': [r[0] for r in conn.execute("SELECT DISTINCT industry FROM contacts WHERE industry IS NOT NULL AND industry != '' ORDER BY industry LIMIT 50")],
            'campaigns': [r[0] for r in conn.execute("SELECT name FROM campaigns ORDER BY name")],
            'outreach_lists': [r[0] for r in conn.execute("SELECT name FROM outreach_lists ORDER BY name")]}
    conn.close()
    return opts

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
    tc = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns").fetchone()
    stats['emails_sent'] = r[0]; stats['emails_opened'] = r[1]; stats['emails_replied'] = r[2]
    stats['opportunities'] = tc[0]; stats['meetings_booked'] = tc[1]
    stats['avg_open_rate'] = round(100*r[1]/r[0],1) if r[0]>0 else 0; stats['avg_reply_rate'] = round(100*r[2]/r[0],1) if r[0]>0 else 0
    stats['by_status'] = {row[0] or 'Unknown': row[1] for row in conn.execute("SELECT status, COUNT(*) FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY COUNT(*) DESC")}
    stats['by_campaign'] = [(r[0], r[1]) for r in conn.execute("SELECT name, total_leads FROM campaigns ORDER BY total_leads DESC LIMIT 10")]
    stats['by_list'] = [(r[0], r[1]) for r in conn.execute("SELECT name, contact_count FROM outreach_lists ORDER BY contact_count DESC LIMIT 10")]
    conn.close()
    return stats

@app.get("/api/stats/funnel")
def get_funnel_stats():
    """Get sales funnel conversion metrics"""
    conn = get_db()

    # Define funnel stages in order
    stages = ['Lead', 'Contacted', 'Replied', 'Scheduled', 'Show', 'No-Show', 'Qualified', 'Client']

    # Count contacts at each stage
    funnel = {}
    for stage in stages:
        count = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE status=? AND is_duplicate=0",
            (stage,)
        ).fetchone()[0]
        funnel[stage] = count

    # Also count negative outcomes
    funnel['Not Interested'] = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE status='Not Interested' AND is_duplicate=0"
    ).fetchone()[0]
    funnel['Bounced'] = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE status='Bounced' AND is_duplicate=0"
    ).fetchone()[0]

    # Calculate conversion rates (avoid division by zero)
    total_contacts = sum(funnel[s] for s in stages)
    conversions = {
        'contact_rate': round(funnel['Contacted'] / max(funnel['Lead'] + funnel['Contacted'], 1) * 100, 1),
        'reply_rate': round(funnel['Replied'] / max(funnel['Contacted'], 1) * 100, 1),
        'booked_rate': round(funnel['Scheduled'] / max(funnel['Replied'], 1) * 100, 1),
        'show_rate': round(funnel['Show'] / max(funnel['Scheduled'], 1) * 100, 1),
        'qualified_rate': round(funnel['Qualified'] / max(funnel['Show'], 1) * 100, 1),
        'close_rate': round(funnel['Client'] / max(funnel['Qualified'], 1) * 100, 1),
        'overall_conversion': round(funnel['Client'] / max(total_contacts, 1) * 100, 2),
    }

    # Calculate totals for funnel visualization
    total_in_funnel = sum(funnel[s] for s in stages)
    max_stage = max(funnel[s] for s in stages) if stages else 1

    # Build funnel data with percentages for bar widths
    funnel_data = []
    for stage in stages:
        funnel_data.append({
            'stage': stage,
            'count': funnel[stage],
            'percentage': round(funnel[stage] / max(max_stage, 1) * 100, 1)
        })

    conn.close()
    return {
        'funnel': funnel,
        'conversions': conversions,
        'funnel_data': funnel_data,
        'total': total_in_funnel
    }

@app.get("/api/stats/database")
def get_database_stats():
    conn = get_db()
    insights = {}
    insights['by_country'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT company_country, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY company_country ORDER BY cnt DESC LIMIT 15")]
    insights['by_country_strategy'] = [{"name": r[0] or "Not Assigned", "value": r[1]} for r in
        conn.execute("SELECT country_strategy, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY country_strategy ORDER BY cnt DESC")]
    insights['by_seniority'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT seniority, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY seniority ORDER BY cnt DESC LIMIT 10")]
    insights['by_industry'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT industry, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY industry ORDER BY cnt DESC LIMIT 15")]
    insights['by_company_size'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT employee_bucket, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY employee_bucket ORDER BY cnt DESC")]
    insights['by_status'] = [{"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT status, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY cnt DESC")]
    insights['by_email_status'] = [{"name": r[0] or "Not Verified", "value": r[1]} for r in
        conn.execute("SELECT email_status, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY email_status ORDER BY cnt DESC")]
    insights['top_companies'] = [{"name": r[0], "value": r[1]} for r in
        conn.execute("SELECT company, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 AND company IS NOT NULL AND company != '' GROUP BY company ORDER BY cnt DESC LIMIT 10")]
    # Timeline query - different syntax for PostgreSQL vs SQLite
    if USE_POSTGRES:
        insights['contacts_timeline'] = [{"date": str(r[0]), "value": r[1]} for r in
            conn.execute("SELECT DATE(created_at), COUNT(*) FROM contacts WHERE is_duplicate=0 AND created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY DATE(created_at)")]
    else:
        insights['contacts_timeline'] = [{"date": r[0], "value": r[1]} for r in
            conn.execute("SELECT DATE(created_at), COUNT(*) FROM contacts WHERE is_duplicate=0 AND created_at >= DATE('now', '-30 days') GROUP BY DATE(created_at) ORDER BY DATE(created_at)")]
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
    conn = get_db()
    perf = {}
    campaigns = []
    for r in conn.execute("""
        SELECT id, name, country, status, total_leads, emails_sent, emails_opened, emails_replied,
               emails_bounced, open_rate, click_rate, reply_rate
        FROM campaigns ORDER BY emails_sent DESC
    """):
        c = dict(r)
        tc = conn.execute("SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns WHERE campaign_id=?", (c['id'],)).fetchone()
        c['opportunities'] = tc[0]
        c['meetings_booked'] = tc[1]
        campaigns.append(c)
    perf['campaigns'] = campaigns

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
    perf['by_country'] = [{"country": r[0] or "Not Set", "campaigns": r[1], "sent": r[2], "replied": r[3]} for r in
        conn.execute("SELECT country, COUNT(*), COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_replied),0) FROM campaigns GROUP BY country ORDER BY SUM(emails_sent) DESC")]
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
    templates = conn.execute("""
        SELECT et.*, tc.times_sent as campaign_sent, tc.times_opened as campaign_opened,
               tc.times_replied as campaign_replied, tc.opportunities as campaign_opportunities,
               tc.meetings as campaign_meetings, tc.id as dashcard_id
        FROM template_campaigns tc
        JOIN email_templates et ON tc.template_id = et.id
        WHERE tc.campaign_id = ?
        ORDER BY et.step_type, et.variant
    """, (campaign_id,)).fetchall()
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']
    grouped = {}
    for t in templates:
        template_dict = dict(t)
        step = template_dict['step_type']
        template_dict['sent'] = template_dict['campaign_sent'] or 0
        template_dict['opened'] = template_dict['campaign_opened'] or 0
        template_dict['replied'] = template_dict['campaign_replied'] or 0
        template_dict['opportunities'] = template_dict['campaign_opportunities'] or 0
        template_dict['meetings'] = template_dict['campaign_meetings'] or 0
        template_dict['open_rate'] = round(100 * template_dict['opened'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0
        template_dict['reply_rate'] = round(100 * template_dict['replied'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0
        if step not in grouped: grouped[step] = []
        grouped[step].append(template_dict)
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
            template_breakdown.append({'step_type': step, 'variants': grouped[step], 'step_metrics': step_metrics})
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
            template_breakdown.append({'step_type': step, 'variants': grouped[step], 'step_metrics': step_metrics})
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
    conn = get_db()
    # Also delete from contact_campaigns
    conn.execute("DELETE FROM contact_campaigns WHERE campaign_id=?", (campaign_id,))
    conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
    conn.commit(); conn.close()
    return {"message": "Deleted"}

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
        t['campaign_ids'] = [c['id'] for c in camps]
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
    conn = get_db()
    rows = conn.execute("SELECT * FROM email_templates ORDER BY step_type, variant").fetchall()
    grouped = {}
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']
    for r in rows:
        t = dict(r)
        step = t['step_type']
        camps = conn.execute("""SELECT c.id, c.name, tc.times_sent, tc.times_opened, tc.times_replied, tc.opportunities, tc.meetings
                                FROM template_campaigns tc
                                JOIN campaigns c ON tc.campaign_id=c.id
                                WHERE tc.template_id=?""", (t['id'],)).fetchall()
        t['campaigns'] = [dict(c) for c in camps]
        t['campaign_ids'] = [c['id'] for c in camps]
        t['campaign_names'] = ', '.join(c['name'] for c in camps)
        ts = t['times_sent'] + sum(c['times_sent'] or 0 for c in camps)
        to = t['times_opened'] + sum(c['times_opened'] or 0 for c in camps)
        tr = t['times_replied'] + sum(c['times_replied'] or 0 for c in camps)
        opps = sum(c['opportunities'] or 0 for c in camps)
        meets = sum(c['meetings'] or 0 for c in camps)
        t['total_sent'] = ts; t['total_opened'] = to; t['total_replied'] = tr
        t['opportunities'] = opps; t['meetings'] = meets
        t['total_open_rate'] = round(100 * to / ts, 1) if ts > 0 else 0
        t['total_reply_rate'] = round(100 * tr / ts, 1) if ts > 0 else 0
        if step not in grouped: grouped[step] = []
        grouped[step].append(t)
    result = []
    for step in step_order:
        if step in grouped: result.append({'step_type': step, 'variants': grouped[step], 'total_variants': len(grouped[step])})
    for step in grouped:
        if step not in step_order: result.append({'step_type': step, 'variants': grouped[step], 'total_variants': len(grouped[step])})
    conn.close()
    return {"data": result}

@app.post("/api/templates")
def create_template(template: TemplateCreate):
    conn = get_db()
    conn.execute("INSERT INTO email_templates (name, variant, step_type, subject, body, country) VALUES (?, ?, ?, ?, ?, ?)",
                (template.name, template.variant, template.step_type, template.subject, template.body, template.country))
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

class BulkAssignTemplatesRequest(BaseModel):
    template_ids: List[int]
    campaign_ids: List[int]

@app.post("/api/templates/bulk/assign-campaigns")
def bulk_assign_templates_to_campaigns(request: BulkAssignTemplatesRequest):
    """Bulk assign multiple templates to multiple campaigns"""
    if not request.template_ids or not request.campaign_ids:
        raise HTTPException(400, "template_ids and campaign_ids are required")

    conn = get_db()
    count = 0
    for template_id in request.template_ids:
        for campaign_id in request.campaign_ids:
            # INSERT OR IGNORE to avoid duplicates
            conn.execute("INSERT OR IGNORE INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)",
                        (template_id, campaign_id))
            count += 1
    conn.commit()
    conn.close()
    return {"message": f"Assigned {len(request.template_ids)} templates to {len(request.campaign_ids)} campaigns"}

class TemplateCampaignMetricsUpdate(BaseModel):
    times_sent: Optional[int] = None
    times_opened: Optional[int] = None
    times_replied: Optional[int] = None
    opportunities: Optional[int] = None
    meetings: Optional[int] = None

@app.put("/api/campaigns/{campaign_id}/templates/{template_id}/metrics")
def update_template_campaign_metrics(campaign_id: int, template_id: int, metrics: TemplateCampaignMetricsUpdate):
    conn = get_db()
    tc = conn.execute("SELECT * FROM template_campaigns WHERE template_id=? AND campaign_id=?", (template_id, campaign_id)).fetchone()
    if not tc: raise HTTPException(404, "Template not associated with this campaign")
    data = {k: v for k, v in metrics.dict().items() if v is not None}
    if not data: raise HTTPException(400, "No metrics to update")
    conn.execute(f"UPDATE template_campaigns SET {','.join([f'{k}=?' for k in data.keys()])} WHERE template_id=? AND campaign_id=?",
                list(data.values()) + [template_id, campaign_id])
    conn.commit(); conn.close()
    recalc_rates(campaign_id); recalc_template_rates(template_id)
    return {"message": "Updated"}

@app.get("/api/lists")
def get_lists():
    conn = get_db(); rows = conn.execute("SELECT * FROM outreach_lists ORDER BY name").fetchall(); conn.close(); return {"data": [dict(r) for r in rows]}

@app.delete("/api/lists/{list_id}")
def delete_list(list_id: int):
    conn = get_db()
    conn.execute("DELETE FROM contact_lists WHERE list_id=?", (list_id,))
    conn.execute("DELETE FROM outreach_lists WHERE id=?", (list_id,))
    conn.commit(); conn.close()
    return {"message": "Deleted"}

@app.post("/webhook/reachinbox")
async def reachinbox_webhook(request: Request):
    try: payload = await request.json()
    except: payload = {}
    conn = get_db()
    event_type = payload.get('event', payload.get('type', payload.get('event_type', 'unknown')))
    email = payload.get('email', payload.get('to', payload.get('recipient', payload.get('recipient_email'))))
    campaign_name = payload.get('campaign_name', payload.get('campaign', payload.get('campaignName', payload.get('sequence_name'))))
    template_id = payload.get('template_id', payload.get('templateId', payload.get('step_id')))
    el = event_type.lower().strip()
    normalized_event = 'unknown'
    if 'sent' in el or 'deliver' in el: normalized_event = 'sent'
    elif 'open' in el: normalized_event = 'opened'
    elif 'click' in el: normalized_event = 'clicked'
    elif 'repl' in el or 'response' in el: normalized_event = 'replied'
    elif 'bounce' in el: normalized_event = 'bounced'
    elif 'unsub' in el: normalized_event = 'unsubscribed'
    elif 'fail' in el or 'error' in el: normalized_event = 'failed'
    conn.execute("""INSERT INTO webhook_events (source, event_type, email, campaign_name, template_id, payload, processed) VALUES (?, ?, ?, ?, ?, ?, 1)""",
        ('reachinbox', normalized_event, email, campaign_name, template_id, json.dumps(payload)))
    campaign_id = None
    if campaign_name:
        camp = conn.execute("SELECT id FROM campaigns WHERE LOWER(name) = LOWER(?) OR name LIKE ?", (campaign_name, f"%{campaign_name}%")).fetchone()
        if camp:
            campaign_id = camp[0]
            if normalized_event == 'sent': conn.execute("UPDATE campaigns SET emails_sent=emails_sent+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'opened': conn.execute("UPDATE campaigns SET emails_opened=emails_opened+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'clicked': conn.execute("UPDATE campaigns SET emails_clicked=emails_clicked+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'replied': conn.execute("UPDATE campaigns SET emails_replied=emails_replied+1 WHERE id=?", (campaign_id,))
            elif normalized_event == 'bounced': conn.execute("UPDATE campaigns SET emails_bounced=emails_bounced+1 WHERE id=?", (campaign_id,))
            recalc_rates(campaign_id, conn)
    if template_id and campaign_id:
        tc = conn.execute("SELECT id FROM template_campaigns WHERE template_id=? AND campaign_id=?", (template_id, campaign_id)).fetchone()
        if tc:
            if normalized_event == 'sent': conn.execute("UPDATE template_campaigns SET times_sent=times_sent+1 WHERE template_id=? AND campaign_id=?", (template_id, campaign_id))
            elif normalized_event == 'opened': conn.execute("UPDATE template_campaigns SET times_opened=times_opened+1 WHERE template_id=? AND campaign_id=?", (template_id, campaign_id))
            elif normalized_event == 'replied': conn.execute("UPDATE template_campaigns SET times_replied=times_replied+1 WHERE template_id=? AND campaign_id=?", (template_id, campaign_id))
            recalc_template_rates(template_id, conn)
    if email:
        contact = conn.execute("SELECT id, status FROM contacts WHERE LOWER(email)=?", (email.lower(),)).fetchone()
        if contact:
            if normalized_event == 'sent':
                conn.execute("UPDATE contacts SET times_contacted=times_contacted+1, last_contacted_at=?, status=CASE WHEN status='Lead' THEN 'Contacted' ELSE status END WHERE id=?", (datetime.now().isoformat(), contact[0]))
            elif normalized_event == 'replied':
                conn.execute("UPDATE contacts SET status='Engaged' WHERE id=? AND status IN ('Lead','Contacted')", (contact[0],))
            elif normalized_event == 'bounced':
                conn.execute("UPDATE contacts SET email_status='Invalid', status='Bounced' WHERE id=?", (contact[0],))
            elif normalized_event == 'unsubscribed':
                conn.execute("UPDATE contacts SET status='Unsubscribed' WHERE id=?", (contact[0],))
    conn.commit(); conn.close()
    return {"status": "ok", "message": "Processed", "event": normalized_event, "campaign_matched": campaign_id is not None, "contact_matched": email is not None}

@app.post("/webhook/bulkemailchecker")
async def bulkemailchecker_webhook(request: Request):
    try: payload = await request.json()
    except: payload = {}
    conn = get_db()
    results = payload.get('results', payload.get('data', payload.get('emails', [payload])))
    if not isinstance(results, list): results = [results]
    stats = {'processed': 0, 'valid': 0, 'invalid': 0, 'risky': 0, 'not_found': 0}
    for r in results:
        email = r.get('email', r.get('address', r.get('email_address', r.get('to'))))
        if not email: continue
        raw_status = r.get('status', r.get('result', r.get('state', r.get('verdict', 'unknown'))))
        raw_status_lower = str(raw_status).lower()
        if raw_status_lower in ['valid', 'deliverable', 'safe', 'ok', 'good', 'verified']:
            email_status = 'Valid'; stats['valid'] += 1
        elif raw_status_lower in ['invalid', 'undeliverable', 'bad', 'bounce', 'rejected', 'syntax_error', 'mailbox_not_found']:
            email_status = 'Invalid'; stats['invalid'] += 1
        elif raw_status_lower in ['risky', 'unknown', 'catch_all', 'catch-all', 'role', 'disposable', 'accept_all', 'spamtrap']:
            email_status = 'Risky'; stats['risky'] += 1
        else: email_status = raw_status.capitalize() if raw_status else 'Unknown'
        existing = conn.execute("SELECT id, email_status FROM contacts WHERE LOWER(email)=?", (email.lower(),)).fetchone()
        if existing:
            conn.execute("UPDATE contacts SET email_status=?, updated_at=? WHERE id=?", (email_status, datetime.now().isoformat(), existing[0]))
            if email_status == 'Invalid': conn.execute("UPDATE contacts SET status='Bounced' WHERE id=? AND status NOT IN ('Client', 'Opportunity')", (existing[0],))
            stats['processed'] += 1
        else: stats['not_found'] += 1
    conn.execute("INSERT INTO webhook_events (source, event_type, email, payload, processed) VALUES (?, ?, ?, ?, 1)", ('bulkemailchecker', 'validation', None, json.dumps(payload)))
    conn.commit(); conn.close()
    return {"status": "ok", "message": f"Processed {stats['processed']} emails", "stats": stats}

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

# ==================== DATA CLEANING ENDPOINTS ====================

class CleaningApplyRequest(BaseModel):
    contact_ids: List[int]
    field: str  # 'names' or 'company'

@app.get("/api/cleaning/stats")
def get_cleaning_stats():
    """Get data quality statistics for the contacts database."""
    conn = get_db()
    rows = conn.execute("SELECT id, first_name, last_name, company, domain FROM contacts WHERE is_duplicate=0").fetchall()
    conn.close()
    contacts = [dict(r) for r in rows]
    stats = analyze_data_quality(contacts)
    return stats

@app.get("/api/cleaning/names/preview")
def preview_name_changes(limit: int = 500):
    """Preview name cleaning changes without applying them."""
    conn = get_db()
    # Fetch ALL contacts - let Python filter to match stats calculation
    rows = conn.execute("""
        SELECT id, first_name, last_name FROM contacts
        WHERE is_duplicate=0
        AND (first_name IS NOT NULL OR last_name IS NOT NULL)
    """).fetchall()
    conn.close()

    contacts = [dict(r) for r in rows]
    all_changes = preview_name_cleaning(contacts)
    # Return up to 'limit' changes but report total count
    return {"changes": all_changes[:limit], "total": len(all_changes)}

@app.post("/api/cleaning/names/apply")
def apply_name_cleaning(req: CleaningApplyRequest):
    """Apply name cleaning to selected contacts."""
    if not req.contact_ids:
        raise HTTPException(400, "No contacts selected")

    conn = get_db()
    updated = 0
    now = datetime.now().isoformat()

    for cid in req.contact_ids:
        row = conn.execute("SELECT first_name, last_name FROM contacts WHERE id=?", (cid,)).fetchone()
        if row:
            first_name = clean_name(row['first_name'], preserve_case_if_mixed=False)
            last_name = clean_name(row['last_name'], preserve_case_if_mixed=False)
            conn.execute(
                "UPDATE contacts SET first_name=?, last_name=?, updated_at=? WHERE id=?",
                (first_name, last_name, now, cid)
            )
            updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} contact names"}

@app.get("/api/cleaning/companies/preview")
def preview_company_changes(limit: int = 500):
    """Preview company name cleaning changes without applying them."""
    conn = get_db()
    # Fetch ALL companies - let Python do the filtering to match stats calculation
    rows = conn.execute("""
        SELECT id, company, domain FROM contacts
        WHERE is_duplicate=0
        AND company IS NOT NULL AND company != ''
    """).fetchall()
    conn.close()

    contacts = [dict(r) for r in rows]
    all_changes = preview_company_cleaning(contacts)
    # Return up to 'limit' changes but report total count
    return {"changes": all_changes[:limit], "total": len(all_changes)}

@app.post("/api/cleaning/companies/apply")
def apply_company_cleaning(req: CleaningApplyRequest):
    """Apply company name cleaning to selected contacts."""
    if not req.contact_ids:
        raise HTTPException(400, "No contacts selected")

    conn = get_db()
    updated = 0
    now = datetime.now().isoformat()

    for cid in req.contact_ids:
        row = conn.execute("SELECT company, domain FROM contacts WHERE id=?", (cid,)).fetchone()
        if row:
            cleaned, _ = clean_company_name(row['company'], row['domain'])
            if cleaned and cleaned != row['company']:
                conn.execute(
                    "UPDATE contacts SET company=?, updated_at=? WHERE id=?",
                    (cleaned, now, cid)
                )
                updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} company names"}

@app.post("/api/cleaning/names/apply-all")
def apply_all_name_cleaning(limit: int = 10000):
    """Apply name cleaning to all contacts that need it."""
    conn = get_db()
    # Fetch ALL contacts - let Python filter to match preview
    rows = conn.execute("""
        SELECT id, first_name, last_name FROM contacts
        WHERE is_duplicate=0
        AND (first_name IS NOT NULL OR last_name IS NOT NULL)
    """).fetchall()

    updated = 0
    now = datetime.now().isoformat()

    for row in rows:
        first_name = clean_name(row['first_name'], preserve_case_if_mixed=False) if row['first_name'] else row['first_name']
        last_name = clean_name(row['last_name'], preserve_case_if_mixed=False) if row['last_name'] else row['last_name']
        if first_name != row['first_name'] or last_name != row['last_name']:
            conn.execute(
                "UPDATE contacts SET first_name=?, last_name=?, updated_at=? WHERE id=?",
                (first_name, last_name, now, row['id'])
            )
            updated += 1
            if updated >= limit:
                break

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} contact names"}

@app.post("/api/cleaning/companies/apply-all")
def apply_all_company_cleaning(limit: int = 10000):
    """Apply company cleaning to all contacts that need it."""
    conn = get_db()
    # Fetch ALL companies - let Python filter to match preview
    rows = conn.execute("""
        SELECT id, company, domain FROM contacts
        WHERE is_duplicate=0
        AND company IS NOT NULL AND company != ''
    """).fetchall()

    updated = 0
    now = datetime.now().isoformat()

    for row in rows:
        cleaned, reason = clean_company_name(row['company'], row['domain'])
        if reason and cleaned != row['company']:
            conn.execute(
                "UPDATE contacts SET company=?, updated_at=? WHERE id=?",
                (cleaned, now, row['id'])
            )
            updated += 1
            if updated >= limit:
                break

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} company names"}

# ==================== END DATA CLEANING ====================

# ==================== SETTINGS API ====================

@app.get("/api/settings/{key}")
def get_setting(key: str):
    """Get a setting value. API keys are masked for security."""
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if not row or not row[0]:
        return {"configured": False, "value": None}
    # Mask sensitive values (API keys)
    if "api_key" in key.lower():
        return {"configured": True, "value": "***configured***"}
    return {"configured": True, "value": row[0]}

@app.put("/api/settings/{key}")
async def update_setting(key: str, request: Request):
    """Update a setting value."""
    body = await request.json()
    value = body.get("value")
    conn = get_db()
    now = datetime.now().isoformat()
    # Upsert the setting - PostgreSQL uses ON CONFLICT, SQLite uses OR REPLACE
    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO settings (key, value, updated_at) VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """, (key, value, now))
    else:
        conn.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)", (key, value, now))
    conn.commit()
    conn.close()
    return {"status": "ok", "key": key}

# ==================== EMAIL VERIFICATION ====================

BULKEMAILCHECKER_API_URL = "https://api.bulkemailchecker.com/real-time/"

async def verify_email_realtime(email: str, api_key: str) -> dict:
    """
    Call BulkEmailChecker real-time API for single email verification.
    Returns normalized verification result.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                BULKEMAILCHECKER_API_URL,
                params={"key": api_key, "email": email}
            )
            data = response.json()

            # Normalize status: passed -> Valid, failed -> Invalid, unknown -> Unknown
            raw_status = data.get("status", "unknown").lower()
            if raw_status == "passed":
                status = "Valid"
            elif raw_status == "failed":
                status = "Invalid"
            else:
                status = "Unknown"

            # Get event details
            event = data.get("event", "")

            return {
                "email": email,
                "status": status,
                "event": event,
                "is_disposable": data.get("isDisposable", False),
                "is_free_service": data.get("isFreeService", False),
                "is_role_account": data.get("isRoleAccount", False),
                "email_suggested": data.get("emailSuggested"),
                "credits_remaining": data.get("creditsRemaining")
            }
    except httpx.TimeoutException:
        return {
            "email": email,
            "status": "Unknown",
            "event": "timeout",
            "is_disposable": False,
            "is_free_service": False,
            "is_role_account": False,
            "email_suggested": None
        }
    except Exception as e:
        return {
            "email": email,
            "status": "Unknown",
            "event": f"error: {str(e)}",
            "is_disposable": False,
            "is_free_service": False,
            "is_role_account": False,
            "email_suggested": None
        }

def update_contact_verification(conn, contact_id: int, result: dict):
    """Update a contact with verification results."""
    now = datetime.now().isoformat()
    # Use Python booleans for PostgreSQL compatibility
    is_disposable = bool(result.get("is_disposable", False))
    is_free_service = bool(result.get("is_free_service", False))
    is_role_account = bool(result.get("is_role_account", False))

    conn.execute("""
        UPDATE contacts SET
            email_status = ?,
            email_verification_event = ?,
            email_is_disposable = ?,
            email_is_free_service = ?,
            email_is_role_account = ?,
            email_suggested = ?,
            email_verified_at = ?,
            updated_at = ?
        WHERE id = ?
    """, (
        result["status"],
        result.get("event", ""),
        is_disposable,
        is_free_service,
        is_role_account,
        result.get("email_suggested", ""),
        now,
        now,
        contact_id
    ))

@app.get("/api/verify/status")
def get_verification_status():
    """Check if email verification is configured."""
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
    # Count unverified contacts - if status is 'Not Verified', count them regardless of timestamp
    unverified = conn.execute("""
        SELECT COUNT(*) FROM contacts
        WHERE email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """).fetchone()[0]
    conn.close()
    return {"configured": bool(row and row[0]), "unverified_count": unverified}

@app.post("/api/verify/fix-unknown")
def fix_unknown_contacts():
    """Fix contacts that have 'Unknown' status - reset them to 'Not Verified' so they can be re-verified."""
    conn = get_db()
    # Update ALL contacts with Unknown status to Not Verified, and clear verified_at
    # This allows them to be re-verified
    result = conn.execute("""
        UPDATE contacts
        SET email_status = 'Not Verified',
            email_verified_at = NULL,
            email_verification_event = NULL
        WHERE email_status = 'Unknown'
    """)
    count = result.rowcount
    conn.commit()
    conn.close()
    return {"fixed": count, "message": f"Reset {count} contacts from 'Unknown' to 'Not Verified' - ready for verification"}

@app.post("/api/verify/single")
async def verify_single_email(email: str = Query(...)):
    """Verify a single email address (for testing)."""
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
    conn.close()
    if not row or not row[0]:
        raise HTTPException(400, "BulkEmailChecker API key not configured. Go to Settings > Integrations.")

    result = await verify_email_realtime(email, row[0])
    return result

@app.post("/api/verify/contacts")
async def verify_contacts(contact_ids: List[int] = Query(...)):
    """Verify emails for specific contact IDs (only unverified contacts)."""
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
    if not row or not row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    api_key = row[0]

    # Get contacts that need verification (Not Verified, Unknown, or NULL status)
    placeholders = ','.join(['?'] * len(contact_ids))
    contacts = conn.execute(f"""
        SELECT id, email FROM contacts
        WHERE id IN ({placeholders})
        AND email IS NOT NULL AND email != ''
        AND (email_status IN ('Not Verified', 'Unknown') OR email_status IS NULL)
        AND email_verified_at IS NULL
    """, contact_ids).fetchall()

    stats = {"verified": 0, "valid": 0, "invalid": 0, "unknown": 0}

    for contact in contacts:
        cid, email = contact[0], contact[1]

        # Rate limiting - 200ms between requests (5/sec)
        await asyncio.sleep(0.2)

        result = await verify_email_realtime(email, api_key)
        update_contact_verification(conn, cid, result)

        stats["verified"] += 1
        if result["status"] == "Valid":
            stats["valid"] += 1
        elif result["status"] == "Invalid":
            stats["invalid"] += 1
        elif result["status"] == "Risky":
            stats["risky"] += 1
        else:
            stats["unknown"] += 1

    conn.commit()
    conn.close()
    return stats

@app.post("/api/verify/bulk")
def start_bulk_verification(limit: int = Query(None)):
    """Start a background job to verify all unverified contacts."""
    conn = get_db()

    # Check API key
    api_row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
    if not api_row or not api_row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    # Get all unverified contacts - if status is 'Not Verified', include them regardless of timestamp
    query = """
        SELECT id FROM contacts
        WHERE email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    unverified = conn.execute(query).fetchall()
    unverified_ids = [r[0] for r in unverified]

    if not unverified_ids:
        conn.close()
        return {"job_id": None, "message": "No contacts need verification", "total_contacts": 0}

    # Create job
    conn.execute("""INSERT INTO verification_jobs (status, total_contacts, contact_ids, created_at)
        VALUES ('pending', ?, ?, ?)""",
        (len(unverified_ids), ','.join(map(str, unverified_ids)), datetime.now().isoformat()))
    job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    # Start background thread
    start_verification_thread(job_id)
    background_tasks[job_id] = True

    return {"job_id": job_id, "total_contacts": len(unverified_ids)}

# ==================== BACKGROUND VERIFICATION JOBS ====================

def verify_email_sync(email: str, api_key: str) -> dict:
    """Synchronous email verification using httpx (for thread)."""
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                BULKEMAILCHECKER_API_URL,
                params={"key": api_key, "email": email}
            )
            data = response.json()

            # Check for API errors first (rate limit, invalid key, etc.)
            if "error" in data:
                error_msg = data.get("error", "Unknown error")
                print(f"[VERIFY] API error for {email}: {error_msg}")
                # Return special status to indicate API error (not a real "Unknown")
                return {
                    "status": "API_ERROR",
                    "event": f"api_error: {error_msg}",
                    "is_disposable": False,
                    "is_free_service": False,
                    "is_role_account": False,
                    "email_suggested": "",
                    "error": error_msg
                }

            # Check if status field exists
            if "status" not in data:
                print(f"[VERIFY] No status in response for {email}: {data}")
                return {
                    "status": "API_ERROR",
                    "event": "api_error: no status in response",
                    "is_disposable": False,
                    "is_free_service": False,
                    "is_role_account": False,
                    "email_suggested": "",
                    "error": "No status in response"
                }

            raw_status = data.get("status", "").lower()
            if raw_status == "passed":
                status = "Valid"
            elif raw_status == "failed":
                status = "Invalid"
            elif raw_status == "unknown":
                status = "Unknown"  # Genuine unknown from API
            else:
                print(f"[VERIFY] Unexpected status '{raw_status}' for {email}")
                status = "Unknown"

            return {
                "status": status,
                "event": data.get("event", ""),
                "is_disposable": data.get("isDisposable", False),
                "is_free_service": data.get("isFreeService", False),
                "is_role_account": data.get("isRoleAccount", False),
                "email_suggested": data.get("emailSuggested", "")
            }
    except Exception as e:
        print(f"[VERIFY] Exception verifying {email}: {e}")
        return {"status": "API_ERROR", "event": f"exception: {str(e)}",
                "is_disposable": False, "is_free_service": False,
                "is_role_account": False, "email_suggested": "",
                "error": str(e)}


def run_verification_job_sync(job_id: int):
    """Synchronous background task to verify emails (runs in thread)."""
    import traceback
    print(f"[VERIFY THREAD] Starting job {job_id}")
    conn = None

    # Rate limit settings: 1,500/hour = 2.4 seconds between requests
    # We use 2.5 seconds to be safe (1,440/hour)
    REQUEST_DELAY = 2.5
    RATE_LIMIT_PAUSE = 300  # 5 minutes pause if rate limited
    MAX_CONSECUTIVE_ERRORS = 10  # Stop if too many consecutive errors

    try:
        conn = get_db()
        # Get job details
        job = conn.execute("SELECT contact_ids FROM verification_jobs WHERE id=?", (job_id,)).fetchone()
        if not job:
            print(f"[VERIFY THREAD] Job {job_id} not found")
            return

        contact_ids = [int(x) for x in job[0].split(',') if x]
        print(f"[VERIFY THREAD] Processing {len(contact_ids)} contacts (delay: {REQUEST_DELAY}s between requests)")

        # Get API key
        api_row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
        if not api_row or not api_row[0]:
            conn.execute("UPDATE verification_jobs SET status='failed', error_message='API key not configured' WHERE id=?", (job_id,))
            conn.commit()
            return

        api_key = api_row[0]

        # Mark job as running
        conn.execute("UPDATE verification_jobs SET status='running', started_at=? WHERE id=?",
                    (datetime.now().isoformat(), job_id))
        conn.commit()

        verified = valid = invalid = unknown = skipped = 0
        api_errors = 0
        consecutive_errors = 0

        for cid in contact_ids:
            # Check if job was cancelled
            job_status = conn.execute("SELECT status FROM verification_jobs WHERE id=?", (job_id,)).fetchone()
            if job_status and job_status[0] == 'cancelled':
                print(f"[VERIFY THREAD] Job {job_id} was cancelled")
                break

            # Get contact email and current status
            contact = conn.execute(
                "SELECT email, email_status FROM contacts WHERE id=? AND email IS NOT NULL AND email != ''",
                (cid,)
            ).fetchone()

            if not contact:
                continue

            email, current_status = contact[0], contact[1]

            # Skip if already verified (Valid or Invalid are final states)
            if current_status in ['Valid', 'Invalid']:
                skipped += 1
                conn.execute("""UPDATE verification_jobs SET
                    skipped_count=?, current_email=? WHERE id=?""",
                    (skipped, f"Skipped: {email}", job_id))
                conn.commit()
                continue

            # Update current email being processed
            conn.execute("UPDATE verification_jobs SET current_email=? WHERE id=?", (email, job_id))
            conn.commit()

            # Rate limiting delay between requests
            time.sleep(REQUEST_DELAY)

            # Verify email with retry logic
            result = verify_email_sync(email, api_key)

            # Handle API errors (rate limit, etc.)
            if result.get("status") == "API_ERROR":
                error_msg = result.get("error", "").lower()
                consecutive_errors += 1
                api_errors += 1

                # Check for rate limit
                if "rate" in error_msg or "limit" in error_msg or "too many" in error_msg:
                    print(f"[VERIFY THREAD] Rate limit detected! Pausing for {RATE_LIMIT_PAUSE} seconds...")
                    conn.execute("UPDATE verification_jobs SET current_email=? WHERE id=?",
                                (f"Rate limited - pausing {RATE_LIMIT_PAUSE}s...", job_id))
                    conn.commit()
                    time.sleep(RATE_LIMIT_PAUSE)
                    consecutive_errors = 0  # Reset after pause
                    # Retry this email
                    result = verify_email_sync(email, api_key)
                    if result.get("status") == "API_ERROR":
                        print(f"[VERIFY THREAD] Still getting error after pause, skipping {email}")
                        continue
                elif consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    print(f"[VERIFY THREAD] Too many consecutive errors ({consecutive_errors}), stopping job")
                    conn.execute("UPDATE verification_jobs SET status='failed', error_message=? WHERE id=?",
                                (f"Too many consecutive API errors. Last error: {error_msg}", job_id))
                    conn.commit()
                    return
                else:
                    print(f"[VERIFY THREAD] API error for {email}, skipping: {error_msg}")
                    continue
            else:
                consecutive_errors = 0  # Reset on success

            # Only update contact if we got a real result (not API_ERROR)
            if result.get("status") != "API_ERROR":
                update_contact_verification(conn, cid, result)
                verified += 1

                if result["status"] == "Valid":
                    valid += 1
                elif result["status"] == "Invalid":
                    invalid += 1
                else:
                    unknown += 1

                print(f"[VERIFY THREAD] Verified {email}: {result['status']}")

            # Update job progress
            conn.execute("""UPDATE verification_jobs SET
                verified_count=?, valid_count=?, invalid_count=?, unknown_count=?, skipped_count=?
                WHERE id=?""",
                (verified, valid, invalid, unknown, skipped, job_id))
            conn.commit()

        # Mark job as completed
        conn.execute("""UPDATE verification_jobs SET
            status='completed', current_email=NULL, completed_at=?
            WHERE id=?""",
            (datetime.now().isoformat(), job_id))
        conn.commit()
        print(f"[VERIFY THREAD] Job {job_id} completed: {verified} verified, {valid} valid, {invalid} invalid, {unknown} unknown, {api_errors} API errors")

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[VERIFY THREAD] Job {job_id} failed: {error_msg}")
        try:
            if conn:
                conn.execute("UPDATE verification_jobs SET status='failed', error_message=? WHERE id=?",
                            (str(e)[:500], job_id))
                conn.commit()
        except Exception as db_err:
            print(f"[VERIFY THREAD] Failed to update job status: {db_err}")
    finally:
        try:
            if conn:
                conn.close()
        except:
            pass
        if job_id in background_tasks:
            del background_tasks[job_id]


def start_verification_thread(job_id: int):
    """Start verification job in a background thread."""
    thread = threading.Thread(target=run_verification_job_sync, args=(job_id,), daemon=True)
    thread.start()
    print(f"[VERIFY] Started background thread for job {job_id}")


@app.post("/api/verify/job/start")
async def start_verification_job(contact_ids: List[int] = Query(...)):
    """Start a background verification job for multiple contacts."""

    conn = get_db()

    # Check API key
    api_row = conn.execute("SELECT value FROM settings WHERE key='bulkemailchecker_api_key'").fetchone()
    if not api_row or not api_row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    # Filter to only unverified contacts - if status is 'Not Verified', include them
    placeholders = ','.join(['?'] * len(contact_ids))
    unverified = conn.execute(f"""
        SELECT id FROM contacts
        WHERE id IN ({placeholders})
        AND email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """, contact_ids).fetchall()

    unverified_ids = [r[0] for r in unverified]

    if not unverified_ids:
        conn.close()
        return {"job_id": None, "message": "No contacts need verification"}

    # Create job
    conn.execute("""INSERT INTO verification_jobs (status, total_contacts, contact_ids, created_at)
        VALUES ('pending', ?, ?, ?)""",
        (len(unverified_ids), ','.join(map(str, unverified_ids)), datetime.now().isoformat()))
    job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    # Start background thread (doesn't block response)
    start_verification_thread(job_id)
    background_tasks[job_id] = True

    return {"job_id": job_id, "total_contacts": len(unverified_ids)}


@app.get("/api/verify/job/{job_id}")
def get_verification_job(job_id: int):
    """Get status of a verification job."""
    conn = get_db()
    job = conn.execute("""SELECT id, status, total_contacts, verified_count, valid_count,
        invalid_count, unknown_count, skipped_count, current_email, error_message,
        created_at, started_at, completed_at
        FROM verification_jobs WHERE id=?""", (job_id,)).fetchone()
    conn.close()

    if not job:
        raise HTTPException(404, "Job not found")

    return {
        "id": job[0],
        "status": job[1],
        "total_contacts": job[2],
        "verified_count": job[3],
        "valid_count": job[4],
        "invalid_count": job[5],
        "unknown_count": job[6],
        "skipped_count": job[7],
        "current_email": job[8],
        "error_message": job[9],
        "created_at": job[10],
        "started_at": job[11],
        "completed_at": job[12],
        "progress": round((job[3] + job[7]) / job[2] * 100, 1) if job[2] > 0 else 0
    }


@app.post("/api/verify/job/{job_id}/cancel")
def cancel_verification_job(job_id: int):
    """Cancel a running verification job."""
    conn = get_db()
    conn.execute("UPDATE verification_jobs SET status='cancelled' WHERE id=? AND status='running'", (job_id,))
    conn.commit()
    conn.close()
    return {"status": "cancelled"}


@app.get("/api/verify/jobs/active")
def get_active_verification_jobs():
    """Get all active (pending/running) verification jobs."""
    conn = get_db()
    jobs = conn.execute("""SELECT id, status, total_contacts, verified_count, valid_count,
        invalid_count, unknown_count, skipped_count, current_email, created_at
        FROM verification_jobs WHERE status IN ('pending', 'running')
        ORDER BY created_at DESC""").fetchall()
    conn.close()

    return [{
        "id": j[0],
        "status": j[1],
        "total_contacts": j[2],
        "verified_count": j[3],
        "valid_count": j[4],
        "invalid_count": j[5],
        "unknown_count": j[6],
        "skipped_count": j[7],
        "current_email": j[8],
        "created_at": j[9],
        "progress": round((j[3] + j[7]) / j[2] * 100, 1) if j[2] > 0 else 0
    } for j in jobs]


# ==================== END EMAIL VERIFICATION ====================

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.2"}

@app.get("/api/info")
def info():
    return {"name": "Deduply", "version": "5.2"}

if __name__ == "__main__":
    import uvicorn
    print("Starting Deduply API v5.2")
    print("Login: admin@deduply.io / admin123")
    uvicorn.run(app, host="0.0.0.0", port=8001)
