-- Migration 010: Campaign Strategy Fields
-- Adds strategy_brief, target_vertical, target_icp, hypothesis for agent-created campaigns
-- Run in Supabase SQL Editor

ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS strategy_brief TEXT;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS target_vertical TEXT;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS target_icp TEXT;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS hypothesis TEXT;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT 'human';
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approved_by TEXT;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP;
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS ri_campaign_id INTEGER;
