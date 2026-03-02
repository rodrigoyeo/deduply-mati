"""
Contacts router — /api/contacts/*, /api/duplicates/*, /api/import/*, /api/filters
"""
import io
import json
import os
import threading
import traceback
import uuid
from datetime import datetime
from typing import Optional, List

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse

from database import get_db, USE_POSTGRES
from models import ContactCreate, ContactUpdate, BulkUpdateRequest, MergeRequest
from workspace_routing import detect_workspace
from shared import (
    get_contact_campaigns, get_contact_lists, get_contact_technologies,
    set_contact_campaigns, set_contact_lists,
    add_contact_campaign, add_contact_list, add_contact_technology,
    enrich_contact_with_relations, compute_employee_bucket,
    update_counts, import_tasks, IMPORT_TEMP_DIR,
)

router = APIRouter()


# ==================== CONTACTS ====================

@router.get("/api/contacts")
def get_contacts(
    page: int = 1, page_size: int = 50, search: Optional[str] = None,
    status: Optional[str] = None, campaigns: Optional[str] = None,
    outreach_lists: Optional[str] = None, country: Optional[str] = None,
    country_strategy: Optional[str] = None, seniority: Optional[str] = None,
    industry: Optional[str] = None, email_status: Optional[str] = None,
    keywords: Optional[str] = None, show_duplicates: bool = False,
    sort_by: str = "id", sort_order: str = "desc",
    workspace: Optional[str] = None
):
    conn = get_db()
    where = ["1=1"] if show_duplicates else ["c.is_duplicate=0"]
    params = []
    joins = []

    if search:
        where.append("(c.first_name LIKE ? OR c.last_name LIKE ? OR c.email LIKE ? OR c.company LIKE ? OR c.title LIKE ?)")
        params.extend([f"%{search}%"] * 5)

    if status:
        status_list = [s.strip() for s in status.split(',') if s.strip()]
        if len(status_list) == 1:
            where.append("c.status=?")
            params.append(status_list[0])
        elif len(status_list) > 1:
            placeholders = ','.join(['?'] * len(status_list))
            where.append(f"c.status IN ({placeholders})")
            params.extend(status_list)

    if country:
        where.append("c.company_country=?")
        params.append(country)

    if country_strategy:
        cs_list = [s.strip() for s in country_strategy.split(',') if s.strip()]
        if len(cs_list) == 1:
            where.append("c.country_strategy=?")
            params.append(cs_list[0])
        elif len(cs_list) > 1:
            placeholders = ','.join(['?'] * len(cs_list))
            where.append(f"c.country_strategy IN ({placeholders})")
            params.extend(cs_list)

    if seniority:
        where.append("c.seniority=?")
        params.append(seniority)
    if industry:
        where.append("c.industry LIKE ?")
        params.append(f"%{industry}%")

    if email_status:
        es_list = [s.strip() for s in email_status.split(',') if s.strip()]
        if len(es_list) == 1:
            where.append("c.email_status=?")
            params.append(es_list[0])
        elif len(es_list) > 1:
            placeholders = ','.join(['?'] * len(es_list))
            where.append(f"c.email_status IN ({placeholders})")
            params.extend(es_list)

    if keywords:
        kw_list = [k.strip() for k in keywords.split(',') if k.strip()]
        if kw_list:
            kw_conditions = ' OR '.join(['c.keywords LIKE ?' for _ in kw_list])
            where.append(f"({kw_conditions})")
            params.extend([f"%{kw}%" for kw in kw_list])

    if workspace:
        if workspace.upper() == "MX":
            where.append("c.reachinbox_workspace='MX'")
        elif workspace.upper() == "US":
            # US includes explicitly set US as well as NULL (legacy contacts)
            where.append("(c.reachinbox_workspace='US' OR c.reachinbox_workspace IS NULL)")

    if campaigns:
        camp_list = [c.strip() for c in campaigns.split(',') if c.strip()]
        if '__none__' in camp_list:
            camp_list = [c for c in camp_list if c != '__none__']
            if camp_list:
                joins.append("LEFT JOIN contact_campaigns cc ON c.id = cc.contact_id LEFT JOIN campaigns camp ON cc.campaign_id = camp.id")
                placeholders = ','.join(['?'] * len(camp_list))
                where.append(f"(cc.campaign_id IS NULL OR camp.name IN ({placeholders}))")
                params.extend(camp_list)
            else:
                joins.append("LEFT JOIN contact_campaigns cc ON c.id = cc.contact_id")
                where.append("cc.campaign_id IS NULL")
        else:
            joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
            if len(camp_list) == 1:
                where.append("camp.name = ?")
                params.append(camp_list[0])
            else:
                placeholders = ','.join(['?'] * len(camp_list))
                where.append(f"camp.name IN ({placeholders})")
                params.extend(camp_list)

    if outreach_lists:
        list_list = [l.strip() for l in outreach_lists.split(',') if l.strip()]
        if '__none__' in list_list:
            list_list = [l for l in list_list if l != '__none__']
            if list_list:
                joins.append("LEFT JOIN contact_lists cl ON c.id = cl.contact_id LEFT JOIN outreach_lists ol ON cl.list_id = ol.id")
                placeholders = ','.join(['?'] * len(list_list))
                where.append(f"(cl.list_id IS NULL OR ol.name IN ({placeholders}))")
                params.extend(list_list)
            else:
                joins.append("LEFT JOIN contact_lists cl ON c.id = cl.contact_id")
                where.append("cl.list_id IS NULL")
        else:
            joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
            if len(list_list) == 1:
                where.append("ol.name = ?")
                params.append(list_list[0])
            else:
                placeholders = ','.join(['?'] * len(list_list))
                where.append(f"ol.name IN ({placeholders})")
                params.extend(list_list)

    where_sql = " AND ".join(where)
    joins_sql = " ".join(joins)

    total = conn.execute(
        f"SELECT COUNT(DISTINCT c.id) FROM contacts c {joins_sql} WHERE {where_sql}", params
    ).fetchone()[0]

    valid_sorts = ['id', 'first_name', 'last_name', 'email', 'company', 'status', 'created_at', 'employees']
    sort_by = sort_by if sort_by in valid_sorts else 'id'

    rows = conn.execute(f"""
        SELECT DISTINCT c.* FROM contacts c {joins_sql}
        WHERE {where_sql}
        ORDER BY c.{sort_by} {'DESC' if sort_order.lower() == 'desc' else 'ASC'}
        LIMIT ? OFFSET ?
    """, params + [page_size, (page - 1) * page_size]).fetchall()

    result = []
    for r in rows:
        contact = dict(r)
        enrich_contact_with_relations(conn, contact)
        result.append(contact)

    conn.close()
    return {
        "data": result,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, (total + page_size - 1) // page_size)
    }


@router.get("/api/contacts/export")
def export_contacts(
    columns: Optional[str] = None, search: Optional[str] = None,
    status: Optional[str] = None, campaigns: Optional[str] = None,
    outreach_lists: Optional[str] = None, country: Optional[str] = None,
    country_strategy: Optional[str] = None, seniority: Optional[str] = None,
    industry: Optional[str] = None, email_status: Optional[str] = None,
    keywords: Optional[str] = None, valid_emails_only: bool = False
):
    conn = get_db()
    where = ["c.is_duplicate=0"]
    params = []
    joins = []

    if search:
        where.append("(c.first_name LIKE ? OR c.last_name LIKE ? OR c.email LIKE ? OR c.company LIKE ? OR c.title LIKE ?)")
        params.extend([f"%{search}%"] * 5)
    if status:
        where.append("c.status=?")
        params.append(status)
    if country:
        where.append("c.company_country=?")
        params.append(country)
    if country_strategy:
        where.append("c.country_strategy=?")
        params.append(country_strategy)
    if seniority:
        where.append("c.seniority=?")
        params.append(seniority)
    if industry:
        where.append("c.industry LIKE ?")
        params.append(f"%{industry}%")
    if email_status:
        where.append("c.email_status=?")
        params.append(email_status)
    if valid_emails_only:
        where.append("c.email_status='Valid'")
    if keywords:
        kw_list = [k.strip() for k in keywords.split(',') if k.strip()]
        if kw_list:
            kw_conditions = ' OR '.join(['c.keywords LIKE ?' for _ in kw_list])
            where.append(f"({kw_conditions})")
            params.extend([f"%{kw}%" for kw in kw_list])
    if campaigns:
        joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
        where.append("camp.name=?")
        params.append(campaigns)
    if outreach_lists:
        joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
        where.append("ol.name=?")
        params.append(outreach_lists)

    all_cols = [
        'id', 'first_name', 'last_name', 'email', 'title', 'headline', 'company', 'seniority',
        'first_phone', 'corporate_phone', 'employees', 'employee_bucket', 'industry', 'keywords',
        'person_linkedin_url', 'website', 'domain', 'company_linkedin_url',
        'city', 'state', 'country', 'company_city', 'company_state', 'company_country',
        'company_street_address', 'company_postal_code', 'annual_revenue', 'annual_revenue_text',
        'company_description', 'company_seo_description', 'company_founded_year',
        'region', 'country_strategy', 'status', 'email_status', 'times_contacted', 'last_contacted_at',
        'opportunities', 'meetings_booked', 'notes', 'created_at'
    ]
    selected = [c.strip() for c in (columns or '').split(',') if c.strip() in all_cols] or all_cols

    joins_sql = " ".join(joins)
    where_sql = " AND ".join(where)

    rows = conn.execute(
        f"SELECT DISTINCT c.id, {','.join(['c.' + c for c in selected if c != 'id'])} FROM contacts c {joins_sql} WHERE {where_sql}",
        params
    ).fetchall()

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
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
    )


@router.get("/api/contacts/columns")
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


@router.get("/api/contacts/{contact_id}")
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


@router.post("/api/contacts")
def create_contact(contact: ContactCreate):
    conn = get_db()
    data = contact.dict(exclude_none=True)

    campaigns_str = data.pop('campaigns_assigned', None)
    lists_str = data.pop('outreach_lists', None)

    # Auto-detect workspace if not explicitly provided
    if not data.get('reachinbox_workspace'):
        data['reachinbox_workspace'] = detect_workspace(data)

    fields = list(data.keys())
    conn.execute(
        f"INSERT INTO contacts ({','.join(fields)}) VALUES ({','.join(['?'] * len(fields))})",
        list(data.values())
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    if campaigns_str:
        set_contact_campaigns(conn, cid, campaigns_str)
    if lists_str:
        set_contact_lists(conn, cid, lists_str)

    conn.commit()
    conn.close()
    update_counts()
    return {"id": cid, "message": "Created"}


@router.put("/api/contacts/{contact_id}")
def update_contact(contact_id: int, contact: ContactUpdate):
    conn = get_db()
    data = {k: v for k, v in contact.dict().items() if v is not None}
    if not data:
        raise HTTPException(400, "No fields")

    campaigns_str = data.pop('campaigns_assigned', None)
    lists_str = data.pop('outreach_lists', None)

    # Re-detect workspace when location/domain signals change and workspace
    # is not being explicitly set by the caller.
    _workspace_signal_fields = {'company_country', 'domain', 'website', 'company_city', 'company_state'}
    if _workspace_signal_fields & set(data.keys()) and 'reachinbox_workspace' not in data:
        # Fetch the current full contact to build the merged picture
        current = conn.execute("SELECT * FROM contacts WHERE id=?", (contact_id,)).fetchone()
        if current:
            merged = dict(current)
            merged.update(data)
            data['reachinbox_workspace'] = detect_workspace(merged)

    if data:
        data['updated_at'] = datetime.now().isoformat()
        conn.execute(
            f"UPDATE contacts SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?",
            list(data.values()) + [contact_id]
        )

    if campaigns_str is not None:
        set_contact_campaigns(conn, contact_id, campaigns_str)
    if lists_str is not None:
        set_contact_lists(conn, contact_id, lists_str)

    conn.commit()
    conn.close()
    update_counts()
    return {"message": "Updated"}


@router.delete("/api/contacts/{contact_id}")
def delete_contact(contact_id: int):
    conn = get_db()
    conn.execute("DELETE FROM contacts WHERE id=?", (contact_id,))
    conn.commit()
    conn.close()
    update_counts()
    return {"message": "Deleted"}


@router.post("/api/contacts/bulk")
def bulk_update(req: BulkUpdateRequest):
    conn = get_db()

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
            s = f"%{f['search']}%"
            params.extend([s] * 5)

        if f.get('status'):
            status_val = f['status']
            status_list = [s.strip() for s in status_val.split(',') if s.strip()] if isinstance(status_val, str) else status_val
            if len(status_list) == 1:
                where.append("c.status=?")
                params.append(status_list[0])
            elif len(status_list) > 1:
                ph = ','.join(['?'] * len(status_list))
                where.append(f"c.status IN ({ph})")
                params.extend(status_list)

        if f.get('email_status'):
            es_val = f['email_status']
            es_list = [s.strip() for s in es_val.split(',') if s.strip()] if isinstance(es_val, str) else es_val
            if len(es_list) == 1:
                where.append("c.email_status=?")
                params.append(es_list[0])
            elif len(es_list) > 1:
                ph = ','.join(['?'] * len(es_list))
                where.append(f"c.email_status IN ({ph})")
                params.extend(es_list)

        campaign_filter = f.get('campaign') or f.get('campaigns')
        if campaign_filter:
            camp_list = [c.strip() for c in campaign_filter.split(',') if c.strip()] if isinstance(campaign_filter, str) else campaign_filter
            if '__none__' in camp_list:
                camp_list = [c for c in camp_list if c != '__none__']
                if camp_list:
                    joins.append("LEFT JOIN contact_campaigns cc ON c.id = cc.contact_id LEFT JOIN campaigns camp ON cc.campaign_id = camp.id")
                    ph = ','.join(['?'] * len(camp_list))
                    where.append(f"(cc.campaign_id IS NULL OR camp.name IN ({ph}))")
                    params.extend(camp_list)
                else:
                    joins.append("LEFT JOIN contact_campaigns cc ON c.id = cc.contact_id")
                    where.append("cc.campaign_id IS NULL")
            else:
                joins.append("JOIN contact_campaigns cc ON c.id = cc.contact_id JOIN campaigns camp ON cc.campaign_id = camp.id")
                if len(camp_list) == 1:
                    where.append("camp.name=?")
                    params.append(camp_list[0])
                else:
                    ph = ','.join(['?'] * len(camp_list))
                    where.append(f"camp.name IN ({ph})")
                    params.extend(camp_list)

        list_filter = f.get('outreach_list') or f.get('outreach_lists')
        if list_filter:
            list_list = [l.strip() for l in list_filter.split(',') if l.strip()] if isinstance(list_filter, str) else list_filter
            if '__none__' in list_list:
                list_list = [l for l in list_list if l != '__none__']
                if list_list:
                    joins.append("LEFT JOIN contact_lists cl ON c.id = cl.contact_id LEFT JOIN outreach_lists ol ON cl.list_id = ol.id")
                    ph = ','.join(['?'] * len(list_list))
                    where.append(f"(cl.list_id IS NULL OR ol.name IN ({ph}))")
                    params.extend(list_list)
                else:
                    joins.append("LEFT JOIN contact_lists cl ON c.id = cl.contact_id")
                    where.append("cl.list_id IS NULL")
            else:
                joins.append("JOIN contact_lists cl ON c.id = cl.contact_id JOIN outreach_lists ol ON cl.list_id = ol.id")
                if len(list_list) == 1:
                    where.append("ol.name=?")
                    params.append(list_list[0])
                else:
                    ph = ','.join(['?'] * len(list_list))
                    where.append(f"ol.name IN ({ph})")
                    params.extend(list_list)

        if f.get('country_strategy'):
            cs_val = f['country_strategy']
            cs_list = [s.strip() for s in cs_val.split(',') if s.strip()] if isinstance(cs_val, str) else cs_val
            if len(cs_list) == 1:
                where.append("c.country_strategy=?")
                params.append(cs_list[0])
            elif len(cs_list) > 1:
                ph = ','.join(['?'] * len(cs_list))
                where.append(f"c.country_strategy IN ({ph})")
                params.extend(cs_list)

        if f.get('country'):
            where.append("c.company_country=?")
            params.append(f['country'])
        if f.get('seniority'):
            where.append("c.seniority=?")
            params.append(f['seniority'])
        if f.get('industry'):
            where.append("c.industry=?")
            params.append(f['industry'])
        if f.get('keywords'):
            kw_val = f['keywords']
            kw_list = [k.strip() for k in kw_val.split(',') if k.strip()] if isinstance(kw_val, str) else kw_val
            if kw_list:
                kw_conditions = ' OR '.join(['c.keywords LIKE ?' for _ in kw_list])
                where.append(f"({kw_conditions})")
                params.extend([f"%{kw}%" for kw in kw_list])

        query = f"{query} {' '.join(joins)} WHERE {' AND '.join(where)}"
        if req.select_limit and req.select_limit > 0:
            query += f" LIMIT {int(req.select_limit)}"
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

    if action == 'delete':
        placeholders = ','.join(['?'] * len(contact_ids))
        conn.execute(f"DELETE FROM contacts WHERE id IN ({placeholders})", contact_ids)
        updated = len(contact_ids)
    elif field == 'campaigns_assigned':
        placeholders = ','.join(['?'] * len(contact_ids))
        if action == 'remove' and value:
            camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (value,)).fetchone()
            if camp:
                conn.execute(f"DELETE FROM contact_campaigns WHERE contact_id IN ({placeholders}) AND campaign_id=?", contact_ids + [camp[0]])
        elif action == 'add' and value:
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (value,))
            camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (value,)).fetchone()
            if camp:
                for cid in contact_ids:
                    conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)", (cid, camp[0]))
        elif action == 'set':
            for cid in contact_ids:
                set_contact_campaigns(conn, cid, value)
        conn.execute(f"UPDATE contacts SET updated_at=? WHERE id IN ({placeholders})", [now] + contact_ids)
        updated = len(contact_ids)
    elif field == 'outreach_lists':
        placeholders = ','.join(['?'] * len(contact_ids))
        if action == 'remove' and value:
            lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (value,)).fetchone()
            if lst:
                conn.execute(f"DELETE FROM contact_lists WHERE contact_id IN ({placeholders}) AND list_id=?", contact_ids + [lst[0]])
        elif action == 'add' and value:
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (value,))
            lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (value,)).fetchone()
            if lst:
                for cid in contact_ids:
                    conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)", (cid, lst[0]))
        elif action == 'set':
            for cid in contact_ids:
                set_contact_lists(conn, cid, value)
        conn.execute(f"UPDATE contacts SET updated_at=? WHERE id IN ({placeholders})", [now] + contact_ids)
        updated = len(contact_ids)
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


# ==================== DUPLICATES ====================

@router.get("/api/duplicates")
def get_duplicates():
    conn = get_db()
    rows = conn.execute(
        "SELECT LOWER(email) as email, COUNT(*) as cnt, GROUP_CONCAT(id) as ids FROM contacts WHERE email IS NOT NULL AND email != '' AND is_duplicate=0 GROUP BY LOWER(email) HAVING COUNT(*)>1 ORDER BY cnt DESC LIMIT 100"
    ).fetchall()
    groups = []
    for row in rows:
        ids = [int(i) for i in row['ids'].split(',')]
        contacts = conn.execute(f"SELECT * FROM contacts WHERE id IN ({','.join(['?'] * len(ids))})", ids).fetchall()
        enriched = []
        for c in contacts:
            contact = dict(c)
            enrich_contact_with_relations(conn, contact)
            enriched.append(contact)
        groups.append({"email": row['email'], "count": row['cnt'], "contacts": enriched})
    conn.close()
    return {"groups": groups, "total_groups": len(groups)}


@router.post("/api/duplicates/merge")
def merge_duplicates(req: MergeRequest):
    conn = get_db()
    primary = conn.execute("SELECT * FROM contacts WHERE id=?", (req.primary_id,)).fetchone()
    if not primary:
        raise HTTPException(404, "Not found")

    primary_camps = set(r[0] for r in conn.execute(
        "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
        (req.primary_id,)
    ).fetchall())
    primary_lists = set(r[0] for r in conn.execute(
        "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
        (req.primary_id,)
    ).fetchall())

    for dup_id in req.duplicate_ids:
        dup = conn.execute("SELECT * FROM contacts WHERE id=?", (dup_id,)).fetchone()
        if dup:
            dup_camps = set(r[0] for r in conn.execute(
                "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
                (dup_id,)
            ).fetchall())
            dup_lists = set(r[0] for r in conn.execute(
                "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
                (dup_id,)
            ).fetchall())

            primary_camps.update(dup_camps)
            primary_lists.update(dup_lists)

            conn.execute("UPDATE contacts SET is_duplicate=1, duplicate_of=? WHERE id=?", (req.primary_id, dup_id))
            conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (dup_id,))
            conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (dup_id,))

    set_contact_campaigns(conn, req.primary_id, ', '.join(sorted(primary_camps)) if primary_camps else None)
    set_contact_lists(conn, req.primary_id, ', '.join(sorted(primary_lists)) if primary_lists else None)
    conn.execute("UPDATE contacts SET updated_at=? WHERE id=?", (datetime.now().isoformat(), req.primary_id))

    conn.commit()
    conn.close()
    update_counts()
    return {"message": f"Merged {len(req.duplicate_ids)} contacts"}


@router.post("/api/duplicates/unmerge/{contact_id}")
def unmerge(contact_id: int):
    conn = get_db()
    conn.execute("UPDATE contacts SET is_duplicate=0, duplicate_of=NULL WHERE id=?", (contact_id,))
    conn.commit()
    conn.close()
    return {"message": "Restored"}


@router.get("/api/duplicates/stats")
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


@router.post("/api/duplicates/auto-merge")
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
            f"SELECT * FROM contacts WHERE id IN ({','.join(['?'] * len(ids))}) ORDER BY created_at ASC",
            ids
        ).fetchall()

        if not contacts:
            continue

        primary = contacts[0]
        primary_id = primary['id']
        duplicates = contacts[1:]

        all_camps = set(r[0] for r in conn.execute(
            "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
            (primary_id,)
        ).fetchall())
        all_lists = set(r[0] for r in conn.execute(
            "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
            (primary_id,)
        ).fetchall())

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
            dup_camps = set(r[0] for r in conn.execute(
                "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
                (dup['id'],)
            ).fetchall())
            dup_lists = set(r[0] for r in conn.execute(
                "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
                (dup['id'],)
            ).fetchall())

            all_camps.update(dup_camps)
            all_lists.update(dup_lists)

            for f in ['first_name', 'last_name', 'title', 'company', 'first_phone', 'corporate_phone', 'person_linkedin_url', 'website']:
                if not best_data[f] and dup[f]:
                    best_data[f] = dup[f]

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


@router.post("/api/duplicates/merge-group/{email}")
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
        (primary_id,)
    ).fetchall())
    all_lists = set(r[0] for r in conn.execute(
        "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
        (primary_id,)
    ).fetchall())

    for dup in duplicates:
        dup_camps = set(r[0] for r in conn.execute(
            "SELECT c.name FROM contact_campaigns cc JOIN campaigns c ON cc.campaign_id=c.id WHERE cc.contact_id=?",
            (dup['id'],)
        ).fetchall())
        dup_lists = set(r[0] for r in conn.execute(
            "SELECT ol.name FROM contact_lists cl JOIN outreach_lists ol ON cl.list_id=ol.id WHERE cl.contact_id=?",
            (dup['id'],)
        ).fetchall())

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


# ==================== IMPORT ====================

@router.post("/api/import/preview")
async def preview_import(file: UploadFile = File(...)):
    try:
        content = await file.read()
        df = None
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, nrows=10)
                break
            except Exception:
                continue
        if df is None:
            raise HTTPException(400, "Cannot read file")
        total_df = None
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try:
                total_df = pd.read_csv(io.BytesIO(content), encoding=enc)
                break
            except Exception:
                continue
        total_rows = len(total_df) if total_df is not None else 0

        target_columns = [
            "first_name", "last_name", "email", "title", "headline", "company", "seniority",
            "first_phone", "corporate_phone", "employees", "industry", "keywords", "person_linkedin_url",
            "website", "domain", "company_linkedin_url",
            "city", "state", "country",
            "company_city", "company_state", "company_country", "company_street_address", "company_postal_code",
            "annual_revenue", "annual_revenue_text", "company_description", "company_seo_description",
            "company_technologies", "company_founded_year",
            "outreach_lists", "campaigns_assigned", "notes"
        ]

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
            elif 'company_city' in cl: suggestions[col] = 'company_city'
            elif 'company_state' in cl: suggestions[col] = 'company_state'
            elif 'company_country' in cl: suggestions[col] = 'company_country'
            elif 'company_street' in cl: suggestions[col] = 'company_street_address'
            elif 'company_postal' in cl: suggestions[col] = 'company_postal_code'
            elif cl == 'city' or cl == 'person_city': suggestions[col] = 'city'
            elif cl == 'state' or cl == 'person_state': suggestions[col] = 'state'
            elif cl == 'country' or cl == 'person_country': suggestions[col] = 'country'
            elif 'revenue_clean' in cl: suggestions[col] = 'annual_revenue'
            elif 'annual_revenue' in cl and 'clean' not in cl: suggestions[col] = 'annual_revenue_text'
            elif 'short_description' in cl: suggestions[col] = 'company_description'
            elif 'seo_description' in cl: suggestions[col] = 'company_seo_description'
            elif 'technologies' in cl: suggestions[col] = 'company_technologies'
            elif 'founded_year' in cl: suggestions[col] = 'company_founded_year'
            elif 'outreach' in cl: suggestions[col] = 'outreach_lists'
            elif 'campaign' in cl or 'assigned' in cl: suggestions[col] = 'campaigns_assigned'

        return {
            "filename": file.filename,
            "total_rows": total_rows,
            "columns": list(df.columns),
            "preview": df.head(5).fillna('').to_dict(orient='records'),
            "target_columns": target_columns,
            "suggested_mapping": suggestions
        }
    except Exception as e:
        raise HTTPException(400, f"Error: {str(e)}")


@router.post("/api/import/execute")
async def execute_import(
    file: UploadFile = File(...),
    column_mapping: str = Query(...),
    outreach_list: str = Query(None),
    campaigns: str = Query(None),
    country_strategy: str = Query(None),
    check_duplicates: bool = Query(True),
    merge_duplicates: bool = Query(True),
    verify_emails: bool = Query(False)
):
    """Start a background import job. Returns job_id immediately."""
    content = await file.read()
    df = None
    for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(io.BytesIO(content), encoding=enc)
            break
        except Exception:
            continue
    if df is None:
        raise HTTPException(400, "Cannot read CSV")

    total_rows = len(df)

    job_uuid = str(uuid.uuid4())
    temp_file_path = os.path.join(IMPORT_TEMP_DIR, f"{job_uuid}.csv")
    with open(temp_file_path, 'wb') as f:
        f.write(content)

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
        merge_duplicates_flag = bool(job[6])
        file_name = job[7]

        mapping = json.loads(column_mapping)

        df = None
        with open(file_path, 'rb') as f:
            content = f.read()
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc)
                break
            except Exception:
                continue

        if df is None:
            conn.execute(
                "UPDATE import_jobs SET status='failed', error_message='Cannot read CSV file' WHERE id=?",
                (job_id,)
            )
            conn.commit()
            return

        conn.execute(
            "UPDATE import_jobs SET status='running', started_at=? WHERE id=?",
            (datetime.now().isoformat(), job_id)
        )
        conn.commit()

        if outreach_list:
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (outreach_list,))

        campaign_list = [c.strip() for c in (campaigns or '').split(',') if c.strip()]
        for c in campaign_list:
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (c,))

        email_index = {}
        if check_duplicates:
            for row in conn.execute("SELECT id, LOWER(email) FROM contacts WHERE email IS NOT NULL AND is_duplicate=0"):
                email_index[row[1]] = row[0]

        stats = {'imported': 0, 'merged': 0, 'duplicates_found': 0, 'failed': 0}
        processed = 0

        for idx, row in df.iterrows():
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
                            if tgt_col in ['employees', 'annual_revenue', 'company_founded_year']:
                                try:
                                    data[tgt_col] = int(float(str(val).replace(',', '').replace('$', '')))
                                except Exception:
                                    pass
                            elif tgt_col == 'campaigns_assigned':
                                csv_campaigns.update(c.strip() for c in str(val).split(',') if c.strip())
                            elif tgt_col == 'outreach_lists':
                                csv_lists.update(l.strip() for l in str(val).split(',') if l.strip())
                            elif tgt_col == 'company_technologies':
                                csv_technologies.update(t.strip() for t in str(val).split(',') if t.strip())
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

                csv_campaigns.update(campaign_list)
                if outreach_list:
                    csv_lists.add(outreach_list)

                if country_strategy and not data.get('country_strategy'):
                    data['country_strategy'] = country_strategy

                data['employee_bucket'] = compute_employee_bucket(data.get('employees'))
                data['source_file'] = file_name
                email = (data.get('email') or '').lower().strip()

                conn.execute("UPDATE import_jobs SET current_row=? WHERE id=?", (email or f"Row {idx + 1}", job_id))
                conn.commit()

                if check_duplicates and email and email in email_index:
                    stats['duplicates_found'] += 1
                    if merge_duplicates_flag:
                        existing_id = email_index[email]
                        for camp in csv_campaigns:
                            add_contact_campaign(conn, existing_id, camp)
                        for lst in csv_lists:
                            add_contact_list(conn, existing_id, lst)
                        for tech in csv_technologies:
                            add_contact_technology(conn, existing_id, tech)
                        if data.get('country_strategy'):
                            conn.execute(
                                "UPDATE contacts SET country_strategy=?, updated_at=? WHERE id=?",
                                (data['country_strategy'], datetime.now().isoformat(), existing_id)
                            )
                        stats['merged'] += 1
                else:
                    if data:
                        # Auto-detect workspace if not already set
                        if not data.get('reachinbox_workspace'):
                            data['reachinbox_workspace'] = detect_workspace(data)

                        fields = list(data.keys())
                        if USE_POSTGRES:
                            placeholders = ','.join(['%s'] * len(fields))
                            result = conn.execute(
                                f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders}) RETURNING id",
                                list(data.values())
                            )
                            cid = result.fetchone()[0]
                        else:
                            placeholders = ','.join(['?'] * len(fields))
                            conn.execute(
                                f"INSERT INTO contacts ({','.join(fields)}) VALUES ({placeholders})",
                                list(data.values())
                            )
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

            except Exception:
                stats['failed'] += 1

            processed += 1

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

        try:
            os.remove(file_path)
        except Exception:
            pass

        update_counts()
        print(f"[IMPORT THREAD] Job {job_id} completed: {stats}")

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[IMPORT THREAD] Job {job_id} failed: {error_msg}")
        try:
            if conn:
                conn.execute(
                    "UPDATE import_jobs SET status='failed', error_message=? WHERE id=?",
                    (str(e)[:500], job_id)
                )
                conn.commit()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        if job_id in import_tasks:
            del import_tasks[job_id]


@router.get("/api/import/job/{job_id}")
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
        "id": job[0], "status": job[1], "total_rows": job[2],
        "processed_count": job[3], "imported_count": job[4],
        "merged_count": job[5], "duplicates_found": job[6],
        "failed_count": job[7], "current_row": job[8],
        "error_message": job[9], "file_name": job[10],
        "created_at": job[11], "started_at": job[12], "completed_at": job[13]
    }


@router.post("/api/import/job/{job_id}/cancel")
def cancel_import_job(job_id: int):
    """Cancel a running import job."""
    conn = get_db()
    conn.execute("UPDATE import_jobs SET status='cancelled' WHERE id=? AND status='running'", (job_id,))
    conn.commit()
    conn.close()
    return {"status": "cancelled"}


@router.get("/api/import/jobs/active")
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


# ==================== FILTERS ====================

@router.get("/api/filters")
def get_filters():
    conn = get_db()
    opts = {
        'statuses': ['Lead', 'Contacted', 'Replied', 'Interested', 'Meeting Booked', 'No-Show', 'Qualified', 'Client', 'Not Interested', 'Bounced'],
        'countries': [r[0] for r in conn.execute("SELECT DISTINCT company_country FROM contacts WHERE company_country IS NOT NULL AND company_country != '' ORDER BY company_country")],
        'country_strategies': ['Mexico', 'United States', 'Germany', 'Spain'],
        'seniorities': [r[0] for r in conn.execute("SELECT DISTINCT seniority FROM contacts WHERE seniority IS NOT NULL AND seniority != '' ORDER BY seniority")],
        'industries': [r[0] for r in conn.execute("SELECT DISTINCT industry FROM contacts WHERE industry IS NOT NULL AND industry != '' ORDER BY industry LIMIT 50")],
        'campaigns': [r[0] for r in conn.execute("SELECT name FROM campaigns ORDER BY name")],
        'outreach_lists': [r[0] for r in conn.execute("SELECT name FROM outreach_lists ORDER BY name")]
    }
    conn.close()
    return opts
