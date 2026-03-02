-- Migration 004: Pipeline Foundation
-- Adds market field to campaigns for US/MX workspace routing
-- Adds ReachInbox push tracking fields to contacts
-- Run in Supabase SQL Editor

-- 1. Add market field to campaigns (US or MX workspace routing)
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS market TEXT DEFAULT 'US';

-- 2. Add ReachInbox push tracking to contacts
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS reachinbox_lead_id TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS reachinbox_workspace TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS reachinbox_pushed_at TIMESTAMP;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS reachinbox_campaign_id INTEGER;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS pipeline_stage TEXT DEFAULT 'new';
-- pipeline_stage values: new | validated | queued | pushed | active | finished

-- 3. Add enrichment tracking (for future BlitzAPI / Clay webhook integration)
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS enrichment_source TEXT;

-- 4. Create push_log table to track what was pushed where and when
CREATE TABLE IF NOT EXISTS reachinbox_push_log (
    id SERIAL PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    campaign_id INTEGER REFERENCES campaigns(id),
    reachinbox_campaign_id INTEGER NOT NULL,
    workspace TEXT NOT NULL,
    status TEXT DEFAULT 'pushed',
    error_message TEXT,
    pushed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_push_log_contact ON reachinbox_push_log(contact_id);
CREATE INDEX IF NOT EXISTS idx_push_log_campaign ON reachinbox_push_log(campaign_id);
CREATE INDEX IF NOT EXISTS idx_contacts_pipeline_stage ON contacts(pipeline_stage);
CREATE INDEX IF NOT EXISTS idx_contacts_reachinbox_workspace ON contacts(reachinbox_workspace);

SELECT 'migration 004 complete' as status;
