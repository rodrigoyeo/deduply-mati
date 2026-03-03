-- Migration 006: ReachInbox UI + HubSpot queue flag
-- Run against Supabase (PostgreSQL) after migration 005

ALTER TABLE contacts ADD COLUMN IF NOT EXISTS hubspot_queued BOOLEAN DEFAULT FALSE;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS hubspot_synced_at TIMESTAMP;
