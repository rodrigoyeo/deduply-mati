-- Migration 009: Campaign Sequences table
-- Stores step-level and variant-level analytics synced from ReachInbox
-- Run in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS campaign_sequences (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
    campaign_name TEXT,
    ri_campaign_id INTEGER,
    workspace TEXT DEFAULT 'US',
    step_number INTEGER NOT NULL,
    step_type TEXT DEFAULT 'initial',
    delay_days INTEGER DEFAULT 0,
    variant_index INTEGER DEFAULT 0,
    variant_subject TEXT,
    variant_body TEXT,
    sent INTEGER DEFAULT 0,
    opened INTEGER DEFAULT 0,
    replied INTEGER DEFAULT 0,
    bounced INTEGER DEFAULT 0,
    open_rate REAL DEFAULT 0,
    reply_rate REAL DEFAULT 0,
    synced_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_cs_campaign ON campaign_sequences(campaign_id);
CREATE INDEX idx_cs_workspace ON campaign_sequences(workspace);
CREATE INDEX idx_cs_ri_campaign ON campaign_sequences(ri_campaign_id);
