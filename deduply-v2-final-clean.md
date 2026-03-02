# Deduply 2.0 — Full Ops Platform: Implementation Roadmap

> **Date:** March 2, 2026
> **Status:** Approved for implementation
> **Principle:** Autonomous outreach engine. AI agents run the pipeline end-to-end. Humans approve and review — they don't click buttons.
> **Non-negotiable:** Speed. Every feature must be fast to load, fast to use. No exceptions.

---

## Context

Deduply already has solid contact management, dedup, BulkEmailChecker, ReachInbox webhook handling, and a dual-workspace foundation (US/MX market field, reachinbox_workspace column, migration 004 merged). PR #1 is open with Clay webhook ingest and ReachInbox push API.

**Clay is cancelled.** We build the full enrichment pipeline natively using BlitzAPI directly. $400/month saved.

The goal: a fully autonomous cold email ops platform where agents discover leads, clean data, verify emails, push to sequences, track replies, sync to HubSpot, and learn from results — all without human intervention in the middle.

---

## Architecture

```
[BlitzAPI — Company Search]
        ↓
[BlitzAPI — Employee Finder / Waterfall ICP]
        ↓
[BlitzAPI — Email Finder]
        ↓
[BulkEmailChecker — Email Validation]
        ↓
[Dedup + Workspace Auto-Routing → Supabase]
        ↓
[ReachInbox Push → US or MX Workspace]
        ↓
[Webhooks → contact status + HubSpot auto-sync]
        ↓
[Agent-Native API] ← AI Agents (Hermes / Otto / OpenClaw)
        ↓
[Learning Loop — what's working, A/B winners]
```

---

## What's Already Built (PR #1 — pending merge)

- Auth on `/api/users` endpoints
- Traceback removed from webhook responses
- `POST /api/reachinbox/push` — dual workspace, email validation gate, push log
- `POST /webhook/clay` — ingest webhook (being repurposed as generic ingest endpoint)
- Campaign `market` field (US/MX)
- Migration 004: pipeline_stage, enrichment_source, reachinbox_* columns, reachinbox_push_log table

---

## Phase 0 — Backend Refactor (Before Any New Feature)

**Why:** `main.py` is 3,500+ lines. Every phase will add 300–500 more. Split into FastAPI routers before touching anything else. This also improves load time, hot reload speed, and testability.

**What to do:**
- Split `main.py` into routers: `routers/contacts.py`, `routers/campaigns.py`, `routers/webhooks.py`, `routers/reachinbox.py`, `routers/leadgen.py`, `routers/settings.py`, `routers/analytics.py`, `routers/agent.py`
- `main.py` becomes app entry point + middleware only (~100 lines)
- No new features, no schema changes, no UI changes

**Files:**
- `backend/main.py` — gutted to entry point
- `backend/routers/` — new directory with all endpoint files

**Verification:** All existing endpoints respond identically before and after the split.

---

## Phase 1 — Dual Workspace Auto-Routing

**Goal:** Every contact that enters Deduply — from any source — gets automatically tagged US or MX. No human decides.

**New file: `backend/workspace_routing.py`**

```python
def detect_workspace(contact: dict) -> str:
    # Priority order (first match wins):
    # 1. company_country == "Mexico" → MX
    # 2. domain TLD ends in .mx → MX
    # 3. Spanish business suffix in name (S.A. de C.V., S.A.P.I., etc.) → MX
    # 4. City/state in known Mexican cities list → MX
    # 5. Default → US
```

**Schema change:** None — `reachinbox_workspace` already added in migration 004.

**Hooks — detect_workspace() called on:**
- Every `POST /api/contacts` (new contact)
- Every BlitzAPI import (Phase 2)
- Every CSV import (batch)

**Frontend changes (`App.jsx`):**
- Workspace badge (🇺🇸 / 🇲🇽) on each contact row — no extra query, from existing field
- Filter by workspace on Contacts page
- Workspace column in duplicates view

**Files to modify:**
- `backend/workspace_routing.py` — new file
- `backend/routers/contacts.py` — call detect_workspace on insert
- `database/schema.sql` — no changes needed (migration 004 covers it)
- `frontend/src/App.jsx` — workspace badge + filter

**Verification:** Create contact with `company_country=Mexico` → assert `reachinbox_workspace=MX`.

---

## Phase 2 — BlitzAPI Enrichment Engine (replaces Clay entirely)

**Goal:** Find companies → find ICP contacts → find emails → import to Deduply. All native. No CSV export, no manual upload, no Clay.

### BlitzAPI Endpoints Used

| Endpoint | Purpose | Cost |
|---|---|---|
| `POST /v2/search/companies` | Find companies by industry, location, size, keywords | 1 credit/result |
| `POST /v2/search/employee-finder` | Find ICP contacts at a company by title/seniority | 1 credit/result |
| `POST /v2/search/waterfall-icp-search` | Find decision-makers with cascade fallback (C-Level → VP → Director) | 1 credit/result |
| `POST /v2/people-enrichment/find-work-email` | LinkedIn URL → verified work email | 1 credit |
| `POST /v2/company-enrichment/domain-to-linkedin-url` | Domain → LinkedIn company URL | 1 credit |
| `GET /v2/account/key-info` | Check credit balance | 0 credits |

**Auth:** `x-api-key` header. Stored as `blitzapi_api_key` in settings table.

### Database Additions (Migration 005)

```sql
CREATE TABLE lead_gen_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,       -- 'company_search' | 'employee_finder' | 'waterfall_icp' | 'email_finder'
    status TEXT DEFAULT 'pending', -- pending | running | completed | failed | cancelled
    parameters JSONB,
    results_count INTEGER DEFAULT 0,
    imported_count INTEGER DEFAULT 0,
    credits_used NUMERIC DEFAULT 0,
    workspace TEXT DEFAULT 'US',
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error TEXT
);

CREATE TABLE lead_gen_companies (
    id SERIAL PRIMARY KEY,
    job_id TEXT REFERENCES lead_gen_jobs(id),
    linkedin_url TEXT,
    linkedin_id BIGINT,
    name TEXT,
    about TEXT,
    industry TEXT,
    type TEXT,
    size TEXT,
    employees_on_linkedin INTEGER,
    followers INTEGER,
    founded_year INTEGER,
    domain TEXT,
    hq_country TEXT,
    hq_city TEXT,
    hq_continent TEXT,
    raw_data JSONB,
    imported BOOLEAN DEFAULT FALSE,
    workspace TEXT,               -- auto-detected on import
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_lead_gen_companies_job ON lead_gen_companies(job_id);
CREATE INDEX idx_lead_gen_companies_domain ON lead_gen_companies(domain);
CREATE INDEX idx_lead_gen_jobs_status ON lead_gen_jobs(status);
```

### New Backend Endpoints

```
POST /api/leadgen/companies/search
  Body: {
    keywords: { include: ["SaaS"], exclude: [] },
    industry: { include: ["Software Development"] },
    hq: { country_code: ["US"], continent: ["North America"] },
    employee_range: ["51-200", "201-500"],
    founded_year: { min: 2015 },
    max_results: 25,
    workspace: "US"   // optional, else auto-detected per company
  }
  → Calls BlitzAPI /v2/search/companies
  → Stores results in lead_gen_companies
  → Returns job_id (background job, non-blocking)

GET  /api/leadgen/jobs
  → List all jobs with status, credit usage, results count

GET  /api/leadgen/jobs/{job_id}
  → Job details + first 10 results preview

POST /api/leadgen/companies/import
  Body: { company_ids: [1, 2, 3], workspace: "US" }
  → For each company: runs employee-finder → email-finder via BlitzAPI
  → Inserts found contacts into contacts table
  → Runs workspace auto-routing + dedup by email
  → Returns import job_id (background, shows progress in sidebar)

POST /api/leadgen/companies/{company_id}/waterfall-icp
  Body: { job_levels: ["C-Level", "VP", "Director"], max_per_company: 3 }
  → Calls BlitzAPI /v2/search/waterfall-icp-search
  → Inserts found contacts directly

POST /api/leadgen/contacts/find-email
  Body: { contact_id: 123 }    -- must have person_linkedin_url
  → Calls BlitzAPI /v2/people-enrichment/find-work-email
  → Updates contact.email + email_status

GET  /api/leadgen/credits
  → Returns current BlitzAPI credit balance
```

### New Frontend — Lead Generation Tab

**Location:** New tab in sidebar, between Dashboard and Contacts. Icon: 🔍

**Sub-tab 1 — Company Search**
- Search form: keywords, industry (multi-select dropdown), country, continent, employee range (checkboxes: 1-10, 11-50, 51-200, 201-500, 500+), max results (10 / 25 / 50 / 100), founded after year
- Credit cost preview before running: "~25 credits"
- Results table: company name, industry, size, country, domain, LinkedIn URL, workspace badge (🇺🇸/🇲🇽), checkbox
- "Import Selected" button → runs employee-finder + email-finder in background, shows progress in sidebar widget
- Active jobs sidebar widget (same pattern as existing import progress)

**Sub-tab 2 — ICP / Waterfall Search**
- Input: company LinkedIn URL + job level priority (C-Level, VP, Director, Manager)
- Returns ranked list of decision-makers with name, title, email, LinkedIn
- One-click "Add to Contacts" per row, or bulk select + import

**Sub-tab 3 — Credit Balance**
- Shows remaining BlitzAPI credits
- Log of recent job credit usage

**Speed rule:** Search submit → job starts < 300ms. Results stream in as they arrive, don't wait for full completion.

### Files to Modify

- `backend/routers/leadgen.py` — new file, all endpoints above
- `backend/requirements.txt` — `httpx` already present (verify)
- `database/schema.sql` — add lead_gen_jobs + lead_gen_companies tables
- `backend/migrations/005_lead_generation.sql` — new migration
- `frontend/src/App.jsx` — add Lead Gen tab + 3 sub-tabs

---

## Phase 3 — Agent-Native API

**Goal:** AI agents (Hermes, Otto, any future agent) call Deduply directly via API. No UI required. This is what makes the platform autonomous.

**Why Phase 3 and not Phase 5:** Once this exists, agents can trigger enrichment, push to ReachInbox, and query analytics immediately — multiplying the value of everything built in Phases 1–2.

**New router: `backend/routers/agent.py` with prefix `/agent/v1`**

```
GET  /agent/v1/status
  → Platform health: workspace configs, contact counts, API key status, credit balance

GET  /agent/v1/contacts
  → Query contacts (workspace-aware, flat response, no nested pagination)
  Query params: workspace, status, email_status, pipeline_stage, campaign_id, limit, offset

GET  /agent/v1/contacts/{id}
  → Contact with full event history (webhook_events for this email)

PATCH /agent/v1/contacts/{id}/status
  Body: { status: "Replied" }
  → Update funnel stage

POST /agent/v1/contacts/ingest
  Body: { contacts: [...], outreach_list: "...", campaign: "..." }
  → Batch insert with dedup (same logic as Clay webhook)

POST /agent/v1/leadgen/search
  → Start a BlitzAPI company search job (same as /api/leadgen/companies/search)

GET  /agent/v1/leadgen/jobs/{id}
  → Poll job status + results count

POST /agent/v1/leadgen/import
  → Import companies → contacts (same as /api/leadgen/companies/import)

POST /agent/v1/reachinbox/push
  → Push contacts to ReachInbox (workspace auto-detected from campaign.market)

POST /agent/v1/hubspot/push
  → Push contacts to HubSpot

GET  /agent/v1/analytics/summary
  → Campaign performance + funnel stats, flat format, workspace-aware

GET  /agent/v1/pipeline/stuck
  → Contacts that haven't changed pipeline_stage in 3+ days

GET  /agent/v1/blitz/credits
  → Current BlitzAPI credit balance
```

**Response format (agent-optimized):**
```json
{
  "contacts": [...],
  "workspace": "US",
  "total": 1247,
  "timestamp": "2026-03-02T...",
  "next_action": "push_to_reachinbox"
}
```

**Auth:** Bearer token — existing `users.api_token` field. No new auth system needed.

**Files to modify:**
- `backend/routers/agent.py` — new file
- `backend/main.py` — register agent router

---

## Phase 4 — ReachInbox Push UI + Status Sync

**Goal:** Add UI for the push API that already exists in the backend. Let humans trigger pushes from the Campaign and Contacts pages.

**New backend endpoints:**
```
GET  /api/reachinbox/campaigns
  → Fetch campaign list from ReachInbox API (note: /campaigns returns 500 in some workspaces — UI falls back to manual ID entry)

POST /api/reachinbox/campaigns/{id}/push-contacts
  Body: { contact_ids: [], workspace: "US", email_status_filter: ["Valid"] }
  → Wrapper around existing /api/reachinbox/push

POST /api/reachinbox/sync-status
  → Pull latest open/click/reply/bounce stats from ReachInbox
  → Update campaign metrics in Deduply DB
```

**Enhance existing webhook handler (`POST /webhook/reachinbox`):**
- `lead_interested` / `meeting_booked` → auto-trigger HubSpot push (Phase 5 prereq)
- Verify contact status field is updating correctly on all event types

**Frontend changes:**
- Campaign detail view: "Push to ReachInbox" button → modal (enter ReachInbox campaign ID, workspace auto-filled from campaign.market, email filter, contact count preview)
- Contacts page bulk action: "Push to ReachInbox Campaign"
- Settings page: ReachInbox section with workspace connection status + "Test Connection" button

**Files to modify:**
- `backend/routers/reachinbox.py` — new endpoints
- `frontend/src/App.jsx` — Push modal, bulk action, settings section

---

## Phase 5 — HubSpot Integration

**Goal:** Interested leads auto-push to HubSpot. No manual CRM entry.

**New backend endpoints:**
```
GET  /api/hubspot/status
  → Verify token + return pipeline stages available

POST /api/hubspot/sync/contact/{id}
  → Push single contact to HubSpot as contact + deal

POST /api/hubspot/sync/bulk
  Body: { filters: { status: "Qualified" } }
  → Push filtered contacts to HubSpot

GET  /api/hubspot/sync/log
  → History of all HubSpot sync operations
```

**Auto-trigger in webhook handler:**
```python
if event_type in ('lead_interested', 'meeting_booked'):
    if hubspot_configured:
        background_tasks.add_task(push_contact_to_hubspot, contact_id)
```

**HubSpot field mapping:**

| Deduply | HubSpot |
|---|---|
| email | email |
| first_name | firstname |
| last_name | lastname |
| company | company |
| title | jobtitle |
| domain | website |
| status | hs_lead_status |
| company_country | country |
| first_phone | phone |
| reachinbox_workspace | outreach_market (custom) |

**Settings:** `hubspot_private_app_token` via existing settings table. No new storage needed.

**Speed rule:** HubSpot push is always async (background task). Webhook returns 200 immediately.

**Files to modify:**
- `backend/routers/hubspot.py` — new file
- `backend/routers/webhooks.py` — add auto-trigger
- `frontend/src/App.jsx` — Settings: HubSpot section; Contacts: HubSpot push action

---

## Phase 6 — Learning Loop Analytics

**Goal:** Know what's working. Surface insights automatically so agents can adjust.

**New backend endpoints:**
```
GET /api/analytics/learning
  → Top templates by reply rate (per workspace)
  → Best days/times to send
  → Reply-to-interested conversion rate
  → Average steps to first reply

GET /api/analytics/ab-winners
  → Templates with statistically significant winner (chi-square, p < 0.05, min 100 sends)
  → Auto-flags winning variant per campaign

GET /api/analytics/workspace-compare
  → US vs MX side by side: sent / open rate / reply rate / interested / meetings booked
```

**Frontend — Dashboard additions:**
- "What's Working" panel: top 3 templates with reply rates
- Workspace comparison card (US 🇺🇸 vs MX 🇲🇽)
- "Suggested Actions" — agent-generated recommendations displayed as read-only cards

**Files to modify:**
- `backend/routers/analytics.py` — new endpoints
- `frontend/src/App.jsx` — Dashboard new panels

---

## Implementation Order

| Phase | Branch | What | Effort |
|---|---|---|---|
| 0 | `otto/backend-routers` | Split main.py into routers | 1 day |
| 1 | `otto/workspace-routing` | Auto-detect US/MX on every contact insert | 1 day |
| 2 | `otto/blitz-enrichment` | BlitzAPI company search + employee finder + email finder + Lead Gen tab | 3 days |
| 3 | `otto/agent-api` | Agent-Native `/agent/v1/` router | 2 days |
| 4 | `otto/reachinbox-ui` | ReachInbox push UI + status sync | 2 days |
| 5 | `otto/hubspot-sync` | HubSpot auto-sync on interested/meeting events | 2 days |
| 6 | `otto/learning-analytics` | A/B winners, workspace comparison, learning tab | 2 days |

**Total: ~13 dev days, 7 branches, 7 PRs.**

---

## Day 0 — Credentials Setup (Before Any Code)

After PR #1 merges, run `backend/migrations/004_pipeline_foundation.sql` in Supabase SQL Editor.

Then add these keys in Deduply Settings UI:

| Key | Value |
|---|---|
| `blitzapi_api_key` | BlitzAPI key |
| `reachinbox_api_key_us` | US workspace key |
| `reachinbox_api_key_mx` | MX workspace key |
| `hubspot_private_app_token` | HubSpot private app token |

---

## Critical Files

| File | Role |
|---|---|
| `backend/main.py` | App entry point only after Phase 0 |
| `backend/routers/` | All endpoints — one file per domain |
| `backend/workspace_routing.py` | New — workspace detection logic |
| `backend/database.py` | DB abstraction — SQLite↔PostgreSQL |
| `database/schema.sql` | PostgreSQL schema — add new tables here |
| `backend/migrations/004_pipeline_foundation.sql` | Already adds workspace/market fields |
| `backend/migrations/005_lead_generation.sql` | New — lead gen tables |
| `frontend/src/App.jsx` | All UI — Lead Gen tab, workspace badges, push modals |

---

## Verification Plan

- **Phase 0:** All existing endpoints respond identically before and after router split
- **Phase 1:** Create contact with `company_country=Mexico` → assert `reachinbox_workspace=MX`
- **Phase 2:** Call `POST /api/leadgen/companies/search` → verify results in `lead_gen_companies`; import one company → verify contact created with correct workspace and no duplicate
- **Phase 3:** Use `api_token` in Bearer auth → call `GET /agent/v1/contacts` → verify agent-formatted response with workspace field
- **Phase 4:** Call `POST /api/reachinbox/campaigns/{id}/push-contacts` → verify `reachinbox_push_log` record created; simulate ReachInbox webhook → verify contact status updated
- **Phase 5:** Push contact to HubSpot → verify in HubSpot dashboard; trigger `lead_interested` webhook → verify auto-push happened in background
- **Phase 6:** After 1 week of campaign data → verify learning tab shows template performance and workspace comparison

---

## Existing Code to Reuse

- `_get_reachinbox_key(workspace)` — already in main.py, reuse for all ReachInbox calls
- `background_tasks` pattern — already used for email verification, reuse for lead gen jobs
- `import_tasks` dict pattern — reuse for lead gen job progress tracking
- `webhook_events` table — extend for HubSpot webhook logging
- `settings` table + `GET/PUT /api/settings` — use for all new API keys
- `users.api_token` — already exists, use for Agent API auth
- Dedup by email — already in import flow, run after every lead gen import

---

*Deduply 2.0 — Final Plan | March 2, 2026 | Otto + Claude Code*
