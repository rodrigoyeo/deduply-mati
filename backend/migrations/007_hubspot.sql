-- Migration 007: HubSpot CRM sync columns
-- Run against Supabase (PostgreSQL) after migration 006

ALTER TABLE contacts ADD COLUMN IF NOT EXISTS hubspot_contact_id TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS hubspot_deal_id TEXT;
