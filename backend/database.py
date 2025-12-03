"""
Database abstraction layer for Deduply
Supports both SQLite (local development) and PostgreSQL (production/Supabase)
"""
import os
import sqlite3
from contextlib import contextmanager

# Check if we have a PostgreSQL URL (production) or use SQLite (local)
DATABASE_URL = os.getenv("DATABASE_URL")
USE_POSTGRES = DATABASE_URL is not None and DATABASE_URL.startswith("postgresql")

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras

class DatabaseConnection:
    """Wrapper to provide consistent interface for both SQLite and PostgreSQL"""

    def __init__(self, conn, is_postgres=False):
        self.conn = conn
        self.is_postgres = is_postgres
        self._cursor = None

    def execute(self, query, params=None):
        """Execute a query, converting SQLite syntax to PostgreSQL if needed"""
        if self.is_postgres:
            # Convert SQLite ? placeholders to PostgreSQL %s
            query = query.replace("?", "%s")
            # Convert AUTOINCREMENT to SERIAL (already handled in schema)
            # Convert INTEGER PRIMARY KEY to SERIAL PRIMARY KEY
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            # Handle INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
            if "INSERT OR IGNORE" in query:
                query = query.replace("INSERT OR IGNORE", "INSERT")
                query = query.rstrip(";") + " ON CONFLICT DO NOTHING"
            # Handle boolean differences
            query = query.replace(" 0)", " FALSE)").replace(" 1)", " TRUE)")
            query = query.replace("=0", "=FALSE").replace("=1", "=TRUE")

        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def executemany(self, query, params_list):
        """Execute many queries"""
        if self.is_postgres:
            query = query.replace("?", "%s")
        cursor = self.conn.cursor()
        cursor.executemany(query, params_list)
        return cursor

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def cursor(self):
        return self.conn.cursor()


def get_db():
    """Get a database connection (SQLite for local, PostgreSQL for production)"""
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return DatabaseConnection(conn, is_postgres=True)
    else:
        # Local SQLite
        db_path = os.getenv("DATABASE_PATH", "deduply.db")
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return DatabaseConnection(conn, is_postgres=False)


def init_db():
    """Initialize database tables"""
    db = get_db()

    if USE_POSTGRES:
        # For PostgreSQL, we expect tables to be created via schema.sql in Supabase
        # Just verify connection works
        try:
            db.execute("SELECT 1")
            print("PostgreSQL connection successful")
        except Exception as e:
            print(f"PostgreSQL connection error: {e}")
        db.close()
        return

    # SQLite initialization (local development)
    cur = db.conn.cursor()

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

    # Create default admin user
    import hashlib
    import secrets
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        token = secrets.token_urlsafe(32)
        pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
        cur.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                   ("admin@deduply.com", pwd_hash, "Admin", "admin", token))

    db.commit()
    db.close()
    print("SQLite database initialized")


# Export for compatibility
__all__ = ['get_db', 'init_db', 'USE_POSTGRES', 'DATABASE_URL']
