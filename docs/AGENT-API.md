# Deduply Agent API — Reference for AI Agents

> **Base URL (Production):** `https://deduply.arkode-mati.com`
> **Auth:** `Authorization: Bearer <token>` on all `/agent/v1/*` endpoints
> **Agent Token (Hermes):** `gYGuPoaM7xhdP1L635dKKRn3nrknWIvK0V_2Z0i_cAs`

---

## Overview

Deduply is Arkode's cold email operations platform. It manages:
- **Lead generation** (BlitzAPI → companies → contacts)
- **Campaign management** (synced with ReachInbox)
- **Template/sequence management** (A/B variants per step)
- **Contact database** (47,897+ contacts, US + MX workspaces)

### Workspaces
Every request that returns data is workspace-scoped:
- `US` — United States campaigns, contacts, templates
- `MX` — Mexico campaigns, contacts, templates

Pass `?workspace=US` or `?workspace=MX` on GET endpoints, or include `"workspace": "US"` in POST bodies.

---

## 1. Lead Generation Pipeline

### Run Bulk Pipeline
```
POST /agent/v1/leadgen/bulk-run
```
Runs the full BlitzAPI → company search → contact enrichment → staging pipeline.

**Body:**
```json
{
  "vertical": "roofing",
  "country": "US",
  "max_companies": 500,
  "max_per_company": 1,
  "employee_range": ["11-50", "51-200"],
  "keywords_include": ["roofing", "roof repair"],
  "keywords_exclude": ["saas", "agency", "marketing", "technology", "media", "wholesale", "distributor", "software"],
  "industry_include": ["construction", "building materials", "facilities services"],
  "industry_exclude": ["software", "information technology", "financial services", "food & beverages"],
  "workspace": "US"
}
```

**Available verticals (presets):** `roofing`, `hvac`, `plumbing`, `landscaping`
Custom verticals: pass any string + your own keywords/industry filters.

**Response:**
```json
{
  "job_id": "abc123",
  "status": "running",
  "message": "Bulk run started"
}
```

### Check Job Status
```
GET /agent/v1/leadgen/jobs/{job_id}
```

**Response:**
```json
{
  "id": "abc123",
  "status": "awaiting_approval",
  "results_count": 32,
  "companies_found": 50,
  "parameters": "{\"vertical\": \"roofing\", ...}"
}
```

### Get Staged Contacts for a Job
```
GET /agent/v1/leadgen/jobs/{job_id}/contacts?status=pending
```

**Response:**
```json
{
  "job_id": "abc123",
  "summary": {
    "total": 32,
    "pending": 30,
    "approved": 2,
    "with_email": 28,
    "tier_1": 12,
    "tier_2": 10,
    "tier_3": 10
  },
  "contacts": [
    {
      "id": 1,
      "first_name": "John",
      "last_name": "Smith",
      "email": "john@acmeroofing.com",
      "title": "Owner",
      "company_name": "Acme Roofing",
      "company_domain": "acmeroofing.com",
      "icp_tier": 1,
      "workspace": "US",
      "status": "pending"
    }
  ]
}
```

### Approve Contacts
```
POST /agent/v1/leadgen/approve
```

**Body (approve all in a job):**
```json
{"job_id": "abc123"}
```

**Body (approve specific contacts):**
```json
{"contact_ids": [1, 2, 3, 5, 8]}
```

**Response:**
```json
{
  "imported": 28,
  "skipped_duplicates": 2,
  "message": "28 contacts imported to main database"
}
```

---

## 2. Campaign Management

### List Campaigns
```
GET /api/campaigns?workspace=US
```

**Response:**
```json
{
  "data": [
    {
      "id": 2,
      "name": "CE-SL: Roofing Companies 001 - US - ARKODE",
      "market": "US",
      "status": "Completed",
      "total_leads": 5964,
      "emails_sent": 5964,
      "emails_opened": 2668,
      "emails_replied": 33,
      "emails_bounced": 2043,
      "open_rate": 44.7,
      "reply_rate": 0.55,
      "click_rate": 0
    }
  ]
}
```

### Sync Campaign Stats from ReachInbox
```
POST /api/reachinbox/sync-campaigns?workspace=US
```
Pulls live stats from ReachInbox API into Deduply database.

### Sync Full Analytics (Steps + Variants)
```
POST /api/reachinbox/sync-analytics?workspace=US
```
Pulls step-level and variant-level analytics from ReachInbox.

**Response:**
```json
{
  "workspace": "US",
  "campaigns_synced": 8,
  "total_steps": 24,
  "total_variants": 58,
  "message": "Synced 8 campaigns with 58 sequence variants"
}
```

### Get Campaign Sequences (after sync)
```
GET /api/reachinbox/campaign-sequences?workspace=US
```

**Response:**
```json
{
  "workspace": "US",
  "campaigns": {
    "CE-SL: HVAC Companies 001 - US - ARKODE": {
      "campaign_id": 76075,
      "ri_campaign_id": 99220,
      "steps": {
        "1": {
          "type": "initial",
          "variants": [
            {"variant": 0, "sent": 1107, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 1, "sent": 1156, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 2, "sent": 1199, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 3, "sent": 1177, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 4, "sent": 1169, "opened": 0, "replied": 0, "bounced": 0}
          ]
        },
        "2": {
          "type": "follow-up",
          "variants": [
            {"variant": 0, "sent": 2850, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 1, "sent": 2839, "opened": 0, "replied": 0, "bounced": 0}
          ]
        },
        "3": {
          "type": "follow-up",
          "variants": [
            {"variant": 0, "sent": 2814, "opened": 0, "replied": 0, "bounced": 0},
            {"variant": 1, "sent": 2845, "opened": 0, "replied": 0, "bounced": 0}
          ]
        }
      }
    }
  }
}
```

### List ReachInbox Campaigns (live from API)
```
GET /api/reachinbox/campaigns?workspace=US
```

---

## 3. ReachInbox Push (Contacts → Campaigns)

### Push Contacts to ReachInbox Campaign
```
POST /api/reachinbox/push
```

**Body:**
```json
{
  "contact_ids": [1, 2, 3],
  "campaign_id": 103996,
  "workspace": "US"
}
```

---

## 4. Templates

### List Templates
```
GET /api/templates?workspace=US
```

Templates are workspace-scoped:
- US templates: `country = 'United States'` or `'US'`
- MX templates: `country = 'Mexico'` or `'MX'`

---

## 5. Contacts

### Search/List Contacts
```
GET /api/contacts?workspace=US&page=1&per_page=50&search=roofing
```

### Get Contact Counts
```
GET /api/stats
```

**Response:**
```json
{
  "total_contacts": 47897,
  "unique_contacts": 47897,
  "us_contacts": 38034,
  "mx_contacts": 9863,
  "total_campaigns": 34,
  "pushed_contacts": 0
}
```

---

## 6. ReachInbox API (Direct — for reference)

These are the ReachInbox API endpoints Deduply calls internally. Hermes should use the Deduply endpoints above, NOT call ReachInbox directly.

### Endpoints Available
| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/v1/campaigns/create` | Create new campaign |
| POST | `/api/v1/campaigns/add-sequence` | Push sequences (templates) to campaign |
| PUT | `/api/v1/campaigns/set-schedule` | Set campaign schedule |
| POST | `/api/v1/campaigns/update-details` | Update campaign settings |
| PUT | `/api/v1/campaigns/set-accounts` | Assign sending email accounts |
| POST | `/api/v1/campaigns/start` | Start campaign |
| POST | `/api/v1/campaigns/pause` | Pause campaign |
| GET | `/api/v1/campaigns/all` | List all campaigns + stats |
| POST | `/api/v1/analytics` | Get step + variant analytics |
| POST | `/api/v1/analytics/summary` | Cross-campaign summary |

### Add Sequence to Campaign (ReachInbox Format)
```json
POST /api/v1/campaigns/add-sequence
{
  "campaignId": 103996,
  "sequences": [{
    "steps": [
      {
        "variants": [
          {"subject": "{{firstName}}, quick question about {{companyName}}", "body": "Hi {{firstName}}..."},
          {"subject": "Re: {{companyName}} growth", "body": "Hey {{firstName}}..."}
        ],
        "type": "initial",
        "delay": 0
      },
      {
        "variants": [
          {"subject": "", "body": "Just following up, {{firstName}}."}
        ],
        "type": "follow-up",
        "delay": 2
      }
    ]
  }]
}
```

### Available Template Variables
`{{firstName}}`, `{{lastName}}`, `{{companyName}}`, `{{industry}}`, `{{title}}`, `{{linkedin}}`, `{{website}}`, `{{location}}`, `{{country}}`, `{{address}}`, `{{phoneNumber}}`

---

## 7. Current Campaign Data (as of 2026-03-04)

### US Campaigns (8 total)
| Campaign | RI ID | Leads | Sent | Opened | Replied | Open% | Reply% | Status |
|----------|-------|-------|------|--------|---------|-------|--------|--------|
| Landscaping 001 | 103996 | 5,123 | 5,123 | 3,299 | 46 | 64.4% | 0.90% | Active |
| HVAC 001 | 99220 | 5,808 | 5,808 | 3,695 | 58 | 63.6% | 1.00% | Completed |
| Roofing 001 | 97745 | 5,964 | 5,964 | 2,668 | 33 | 44.7% | 0.55% | Completed |
| Plumbing 001 | 100324 | 3,469 | 3,469 | 2,175 | 31 | 62.7% | 0.89% | Completed |
| SD 1 Texas | 76173 | 3,259 | 82 | — | 9 | — | — | Active |
| SD 2 Texas | 82658 | 3,241 | 63 | — | 0 | — | — | Completed |
| SD 3 Texas | 84296 | 3,169 | 6,686 | — | 17 | — | — | Completed |
| SD 4 Texas | 85249 | 3,223 | 9,552 | — | 39 | — | — | Completed |

### MX Campaigns (6 in ReachInbox)
| Campaign | RI ID | Leads | Sent | Replied | Status |
|----------|-------|-------|------|---------|--------|
| VD 15: Manufacturing | 98432 | 10,243 | 16,525 | 74 | Active |
| VD 16: Servicios Prof | 99687 | 5,101 | 9,487 | 60 | Active |
| Webinar 15 ERP | 103277 | 0 | 0 | 0 | Draft |
| VD-F1 General Followup | 76180 | 45 | 0 | 0 | Draft |
| Webinar 2 Automatización | 38349 | 0 | 0 | 0 | Draft |
| Webinar 1 Financiera | 37977 | 0 | 0 | 0 | Draft |

### US Sequence Structure
All US campaigns follow a **3-step sequence**:

**Landscaping 001:**
- Step 1 (initial): 4 A/B variants, ~1,275 sent each
- Step 2 (follow-up): 2 variants, ~2,450 sent each
- Step 3 (follow-up): 2 variants, ~1,060 sent each

**HVAC 001:**
- Step 1 (initial): 5 A/B variants, ~1,150 sent each
- Step 2 (follow-up): 2 variants, ~2,845 sent each
- Step 3 (follow-up): 2 variants, ~2,830 sent each

**Roofing 001:**
- Step 1 (initial): 4 A/B variants, ~1,490 sent each
- Step 2 (follow-up): 2 variants, ~1,965 sent each
- Step 3 (follow-up): 2 variants, ~1,960 sent each

**Plumbing 001:**
- Step 1 (initial): 4 A/B variants, ~867 sent each
- Step 2 (follow-up): 1 variant, 3,394 sent
- Step 3 (follow-up): 1 variant, 3,383 sent

---

## 8. Typical Agent Workflow

### Overnight Pipeline Run
```
1. POST /agent/v1/leadgen/bulk-run
   → vertical=roofing, country=US, max_companies=500, keywords_exclude=[...]
   
2. Poll: GET /agent/v1/leadgen/jobs/{job_id}
   → Wait for status=awaiting_approval
   
3. Review: GET /agent/v1/leadgen/jobs/{job_id}/contacts?status=pending
   → Check quality, icp_tier distribution
   
4. POST /agent/v1/leadgen/approve
   → {job_id: "..."} to approve all, or {contact_ids: [...]} for selective
```

### Campaign Analytics Sync
```
1. POST /api/reachinbox/sync-analytics?workspace=US
   → Pulls step + variant data from ReachInbox

2. GET /api/reachinbox/campaign-sequences?workspace=US
   → Read the synced sequence data

3. Analyze: which variant has highest sent count relative to opens/replies
   → Use this to decide which templates to scale
```

### Creating a New Campaign (Full Flow)
```
1. POST /api/reachinbox/sync-campaigns?workspace=US
   → Ensure latest campaign list

2. Create campaign in ReachInbox via Deduply:
   POST /api/v1/campaigns/create  (via ReachInbox API)
   
3. Add sequences:
   POST /api/v1/campaigns/add-sequence  (via ReachInbox API)
   
4. Set schedule + accounts:
   PUT /api/v1/campaigns/set-schedule
   PUT /api/v1/campaigns/set-accounts
   
5. Push contacts:
   POST /api/reachinbox/push

6. Start:
   POST /api/v1/campaigns/start
```

---

## 9. ICP Tier System

Contacts are scored by ICP (Ideal Customer Profile) tier from BlitzAPI waterfall:

| Tier | Label | Job Levels | Priority |
|------|-------|------------|----------|
| T1 | Owner | C-Team, Owner, President | Highest — decision maker |
| T2 | GM | Director, VP, General Manager | High — influencer |
| T3 | Ops | Manager, Operations | Medium — implementer |

**Template selection by tier:**
- T1 → Direct, business-outcome focused messaging
- T2 → ROI/efficiency focused messaging
- T3 → Pain-point/process focused messaging

---

## 10. Authentication

### Agent Endpoints (`/agent/v1/*`)
```
Authorization: Bearer <agent_token>
```

Tokens:
- `rodrigo.yeo@arkode.io`: `gYGuPoaM7xhdP1L635dKKRn3nrknWIvK0V_2Z0i_cAs`
- `mati@arkode.io`: `w2DL_eQX09ID5EwhPACSznYaJHhWUse4EBINPvVl4IY`

### User Endpoints (`/api/*`)
Requires session cookie from login:
```
POST /auth/login
{"email": "mati@arkode.io", "password": "admin1234"}
```

---

*Last synced: 2026-03-04 00:30 EST*
*Generated by Otto (Arkode Dev Agent)*
