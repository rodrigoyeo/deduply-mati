-- Deduply Database Schema for Supabase/PostgreSQL
-- Run this in Supabase SQL Editor to create all tables

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT,
    role TEXT DEFAULT 'member',
    api_token TEXT UNIQUE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contacts table (main data)
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    title TEXT,
    headline TEXT,
    company TEXT,
    seniority TEXT,
    first_phone TEXT,
    corporate_phone TEXT,
    employees INTEGER,
    employee_bucket TEXT,
    industry TEXT,
    keywords TEXT,
    person_linkedin_url TEXT,
    website TEXT,
    domain TEXT,
    company_linkedin_url TEXT,
    company_city TEXT,
    company_state TEXT,
    company_country TEXT,
    region TEXT,
    country_strategy TEXT,
    outreach_lists TEXT,
    campaigns_assigned TEXT,
    status TEXT DEFAULT 'Lead',
    email_status TEXT DEFAULT 'Unknown',
    times_contacted INTEGER DEFAULT 0,
    last_contacted_at TIMESTAMP,
    opportunities INTEGER DEFAULT 0,
    meetings_booked INTEGER DEFAULT 0,
    notes TEXT,
    source_file TEXT,
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_of INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Campaigns table
CREATE TABLE IF NOT EXISTS campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    country TEXT,
    status TEXT DEFAULT 'Active',
    total_leads INTEGER DEFAULT 0,
    emails_sent INTEGER DEFAULT 0,
    emails_opened INTEGER DEFAULT 0,
    emails_clicked INTEGER DEFAULT 0,
    emails_replied INTEGER DEFAULT 0,
    emails_bounced INTEGER DEFAULT 0,
    opportunities INTEGER DEFAULT 0,
    meetings_booked INTEGER DEFAULT 0,
    open_rate REAL DEFAULT 0,
    click_rate REAL DEFAULT 0,
    reply_rate REAL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Email templates table
CREATE TABLE IF NOT EXISTS email_templates (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    variant TEXT DEFAULT 'A',
    step_type TEXT DEFAULT 'Main',
    subject TEXT,
    body TEXT,
    times_sent INTEGER DEFAULT 0,
    times_opened INTEGER DEFAULT 0,
    times_clicked INTEGER DEFAULT 0,
    times_replied INTEGER DEFAULT 0,
    open_rate REAL DEFAULT 0,
    reply_rate REAL DEFAULT 0,
    is_winner BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Template-Campaign relationship table
CREATE TABLE IF NOT EXISTS template_campaigns (
    id SERIAL PRIMARY KEY,
    template_id INTEGER NOT NULL REFERENCES email_templates(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    times_sent INTEGER DEFAULT 0,
    times_opened INTEGER DEFAULT 0,
    times_replied INTEGER DEFAULT 0,
    opportunities INTEGER DEFAULT 0,
    meetings INTEGER DEFAULT 0,
    UNIQUE(template_id, campaign_id)
);

-- Outreach lists table
CREATE TABLE IF NOT EXISTS outreach_lists (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    contact_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Webhook events table
CREATE TABLE IF NOT EXISTS webhook_events (
    id SERIAL PRIMARY KEY,
    source TEXT,
    event_type TEXT,
    email TEXT,
    campaign_name TEXT,
    template_id INTEGER,
    payload TEXT,
    processed BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company);
CREATE INDEX IF NOT EXISTS idx_contacts_campaigns ON contacts(campaigns_assigned);
CREATE INDEX IF NOT EXISTS idx_contacts_lists ON contacts(outreach_lists);
CREATE INDEX IF NOT EXISTS idx_contacts_status ON contacts(status);
CREATE INDEX IF NOT EXISTS idx_contacts_is_duplicate ON contacts(is_duplicate);
CREATE INDEX IF NOT EXISTS idx_webhook_events_email ON webhook_events(email);

-- Insert default admin user (password: admin123)
-- Change this password after first login!
INSERT INTO users (email, password_hash, name, role, api_token)
VALUES (
    'admin@deduply.com',
    '240be518fabd2724ddb6f04eeb9d5b13e8d07a29d2c8f2a6f8e7c1d9e3b5a4c7',
    'Admin',
    'admin',
    'initial_token_change_me_' || gen_random_uuid()::text
) ON CONFLICT (email) DO NOTHING;
