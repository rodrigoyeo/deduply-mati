"""
Campaigns router — /api/campaigns/*, /api/templates/*, /api/lists/*
"""
import sqlite3
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Depends

from database import get_db
from models import (
    CampaignCreate, CampaignUpdate,
    TemplateCreate, TemplateUpdate,
    BulkAssignTemplatesRequest, TemplateCampaignMetricsUpdate,
)
from shared import recalc_rates, recalc_template_rates

router = APIRouter()


# ==================== CAMPAIGNS ====================

@router.get("/api/campaigns")
def get_campaigns(search: Optional[str] = None, status: Optional[str] = None,
                  workspace: Optional[str] = None):
    conn = get_db()
    where, params = ["1=1"], []
    if search:
        where.append("(name LIKE ? OR description LIKE ?)")
        params.extend([f"%{search}%"] * 2)
    if status:
        where.append("status=?")
        params.append(status)
    if workspace:
        if workspace.upper() == "MX":
            where.append("(market='MX' OR country='Mexico')")
        elif workspace.upper() == "US":
            where.append("(market='US' OR market IS NULL OR (country != 'Mexico' OR country IS NULL))")
    rows = conn.execute(
        f"SELECT * FROM campaigns WHERE {' AND '.join(where)} ORDER BY created_at DESC", params
    ).fetchall()
    result = []
    for r in rows:
        c = dict(r)
        tc_metrics = conn.execute(
            "SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns WHERE campaign_id=?",
            (c['id'],)
        ).fetchone()
        c['opportunities'] = tc_metrics[0]
        c['meetings_booked'] = tc_metrics[1]
        result.append(c)
    conn.close()
    return {"data": result}


@router.get("/api/campaigns/{campaign_id}")
def get_campaign(campaign_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Not found")
    campaign = dict(row)
    templates = conn.execute("""
        SELECT et.*, tc.times_sent as campaign_sent, tc.times_opened as campaign_opened,
               tc.times_replied as campaign_replied, tc.opportunities as campaign_opportunities,
               tc.meetings as campaign_meetings, tc.id as dashcard_id
        FROM template_campaigns tc
        JOIN email_templates et ON tc.template_id = et.id
        WHERE tc.campaign_id = ?
        ORDER BY et.step_type, et.variant
    """, (campaign_id,)).fetchall()
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']
    grouped = {}
    for t in templates:
        template_dict = dict(t)
        step = template_dict['step_type']
        template_dict['sent'] = template_dict['campaign_sent'] or 0
        template_dict['opened'] = template_dict['campaign_opened'] or 0
        template_dict['replied'] = template_dict['campaign_replied'] or 0
        template_dict['opportunities'] = template_dict['campaign_opportunities'] or 0
        template_dict['meetings'] = template_dict['campaign_meetings'] or 0
        template_dict['open_rate'] = round(100 * template_dict['opened'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0
        template_dict['reply_rate'] = round(100 * template_dict['replied'] / template_dict['sent'], 1) if template_dict['sent'] > 0 else 0
        if step not in grouped:
            grouped[step] = []
        grouped[step].append(template_dict)
    template_breakdown = []
    for step in step_order:
        if step in grouped:
            step_metrics = {
                'sent': sum(t['sent'] for t in grouped[step]),
                'opened': sum(t['opened'] for t in grouped[step]),
                'replied': sum(t['replied'] for t in grouped[step]),
                'opportunities': sum(t['opportunities'] for t in grouped[step]),
                'meetings': sum(t['meetings'] for t in grouped[step])
            }
            step_metrics['open_rate'] = round(100 * step_metrics['opened'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            step_metrics['reply_rate'] = round(100 * step_metrics['replied'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            template_breakdown.append({'step_type': step, 'variants': grouped[step], 'step_metrics': step_metrics})
    for step in grouped:
        if step not in step_order:
            step_metrics = {
                'sent': sum(t['sent'] for t in grouped[step]),
                'opened': sum(t['opened'] for t in grouped[step]),
                'replied': sum(t['replied'] for t in grouped[step]),
                'opportunities': sum(t['opportunities'] for t in grouped[step]),
                'meetings': sum(t['meetings'] for t in grouped[step])
            }
            step_metrics['open_rate'] = round(100 * step_metrics['opened'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            step_metrics['reply_rate'] = round(100 * step_metrics['replied'] / step_metrics['sent'], 1) if step_metrics['sent'] > 0 else 0
            template_breakdown.append({'step_type': step, 'variants': grouped[step], 'step_metrics': step_metrics})
    campaign['template_breakdown'] = template_breakdown
    conn.close()
    return campaign


@router.post("/api/campaigns")
def create_campaign(campaign: CampaignCreate):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO campaigns (name, description, country, status, market) VALUES (?, ?, ?, ?, ?)",
            (campaign.name, campaign.description, campaign.country, campaign.status, campaign.market)
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        return {"id": cid, "message": "Created"}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Name exists")
    finally:
        conn.close()


@router.put("/api/campaigns/{campaign_id}")
def update_campaign(campaign_id: int, campaign: CampaignUpdate):
    conn = get_db()
    data = {k: v for k, v in campaign.dict().items() if v is not None}
    if not data:
        raise HTTPException(400, "No fields")
    conn.execute(
        f"UPDATE campaigns SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?",
        list(data.values()) + [campaign_id]
    )
    conn.commit()
    conn.close()
    recalc_rates(campaign_id)
    return {"message": "Updated"}


@router.delete("/api/campaigns/{campaign_id}")
def delete_campaign(campaign_id: int):
    conn = get_db()
    conn.execute("DELETE FROM contact_campaigns WHERE campaign_id=?", (campaign_id,))
    conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
    conn.commit()
    conn.close()
    return {"message": "Deleted"}


# ==================== TEMPLATES ====================

@router.get("/api/templates")
def get_templates(campaign_id: Optional[int] = None, search: Optional[str] = None):
    conn = get_db()
    where, params = ["1=1"], []
    if search:
        where.append("(name LIKE ? OR subject LIKE ?)")
        params.extend([f"%{search}%"] * 2)
    rows = conn.execute(
        f"SELECT * FROM email_templates WHERE {' AND '.join(where)} ORDER BY created_at DESC", params
    ).fetchall()
    result = []
    for r in rows:
        t = dict(r)
        camps = conn.execute(
            "SELECT c.id, c.name, c.country, tc.times_sent, tc.times_opened, tc.times_replied, tc.opportunities, tc.meetings FROM template_campaigns tc JOIN campaigns c ON tc.campaign_id=c.id WHERE tc.template_id=?",
            (t['id'],)
        ).fetchall()
        t['campaigns'] = [dict(c) for c in camps]
        t['campaign_ids'] = [c['id'] for c in camps]
        t['campaign_names'] = ', '.join(c['name'] for c in camps)
        ts = t['times_sent'] + sum(c['times_sent'] or 0 for c in camps)
        to = t['times_opened'] + sum(c['times_opened'] or 0 for c in camps)
        tr = t['times_replied'] + sum(c['times_replied'] or 0 for c in camps)
        opps = sum(c['opportunities'] or 0 for c in camps)
        meets = sum(c['meetings'] or 0 for c in camps)
        t['total_sent'] = ts
        t['total_opened'] = to
        t['total_replied'] = tr
        t['opportunities'] = opps
        t['meetings'] = meets
        t['total_open_rate'] = round(100 * to / ts, 1) if ts > 0 else 0
        t['total_reply_rate'] = round(100 * tr / ts, 1) if ts > 0 else 0
        result.append(t)
    conn.close()
    return {"data": result}


@router.get("/api/templates/grouped/by-step")
def get_templates_grouped_by_step():
    conn = get_db()
    rows = conn.execute("SELECT * FROM email_templates ORDER BY step_type, variant").fetchall()
    grouped = {}
    step_order = ['Main', 'Step 1', 'Step 2', 'Step 3', 'Follow-up']
    for r in rows:
        t = dict(r)
        step = t['step_type']
        camps = conn.execute("""SELECT c.id, c.name, c.country, tc.times_sent, tc.times_opened, tc.times_replied, tc.opportunities, tc.meetings
                                FROM template_campaigns tc
                                JOIN campaigns c ON tc.campaign_id=c.id
                                WHERE tc.template_id=?""", (t['id'],)).fetchall()
        t['campaigns'] = [dict(c) for c in camps]
        t['campaign_ids'] = [c['id'] for c in camps]
        t['campaign_names'] = ', '.join(c['name'] for c in camps)
        ts = t['times_sent'] + sum(c['times_sent'] or 0 for c in camps)
        to = t['times_opened'] + sum(c['times_opened'] or 0 for c in camps)
        tr = t['times_replied'] + sum(c['times_replied'] or 0 for c in camps)
        opps = sum(c['opportunities'] or 0 for c in camps)
        meets = sum(c['meetings'] or 0 for c in camps)
        t['total_sent'] = ts
        t['total_opened'] = to
        t['total_replied'] = tr
        t['opportunities'] = opps
        t['meetings'] = meets
        t['total_open_rate'] = round(100 * to / ts, 1) if ts > 0 else 0
        t['total_reply_rate'] = round(100 * tr / ts, 1) if ts > 0 else 0
        if step not in grouped:
            grouped[step] = []
        grouped[step].append(t)
    result = []
    for step in step_order:
        if step in grouped:
            result.append({'step_type': step, 'variants': grouped[step], 'total_variants': len(grouped[step])})
    for step in grouped:
        if step not in step_order:
            result.append({'step_type': step, 'variants': grouped[step], 'total_variants': len(grouped[step])})
    conn.close()
    return {"data": result}


@router.get("/api/templates/{template_id}")
def get_template(template_id: int):
    conn = get_db()
    row = conn.execute("SELECT * FROM email_templates WHERE id=?", (template_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Not found")
    t = dict(row)
    camps = conn.execute(
        "SELECT c.id, c.name FROM template_campaigns tc JOIN campaigns c ON tc.campaign_id=c.id WHERE tc.template_id=?",
        (template_id,)
    ).fetchall()
    t['campaign_ids'] = [c['id'] for c in camps]
    t['campaign_names'] = [c['name'] for c in camps]
    conn.close()
    return t


@router.post("/api/templates")
def create_template(template: TemplateCreate):
    conn = get_db()
    conn.execute(
        "INSERT INTO email_templates (name, variant, step_type, subject, body, country) VALUES (?, ?, ?, ?, ?, ?)",
        (template.name, template.variant, template.step_type, template.subject, template.body, template.country)
    )
    tid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    if template.campaign_ids:
        for cid in template.campaign_ids:
            conn.execute("INSERT OR IGNORE INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)", (tid, cid))
    conn.commit()
    conn.close()
    return {"id": tid, "message": "Created"}


@router.put("/api/templates/{template_id}")
def update_template(template_id: int, template: TemplateUpdate):
    conn = get_db()
    data = {k: v for k, v in template.dict().items() if v is not None and k != 'campaign_ids'}
    if data:
        data['updated_at'] = datetime.now().isoformat()
        conn.execute(
            f"UPDATE email_templates SET {','.join([f'{k}=?' for k in data.keys()])} WHERE id=?",
            list(data.values()) + [template_id]
        )
    if template.campaign_ids is not None:
        existing = conn.execute(
            "SELECT campaign_id FROM template_campaigns WHERE template_id=?", (template_id,)
        ).fetchall()
        existing_ids = set(r[0] for r in existing)
        new_ids = set(template.campaign_ids)
        to_remove = existing_ids - new_ids
        for cid in to_remove:
            conn.execute(
                "DELETE FROM template_campaigns WHERE template_id=? AND campaign_id=?",
                (template_id, cid)
            )
        to_add = new_ids - existing_ids
        for cid in to_add:
            conn.execute(
                "INSERT OR IGNORE INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)",
                (template_id, cid)
            )
    conn.commit()
    conn.close()
    recalc_template_rates(template_id)
    return {"message": "Updated"}


@router.delete("/api/templates/{template_id}")
def delete_template(template_id: int):
    conn = get_db()
    conn.execute("DELETE FROM template_campaigns WHERE template_id=?", (template_id,))
    conn.execute("DELETE FROM email_templates WHERE id=?", (template_id,))
    conn.commit()
    conn.close()
    return {"message": "Deleted"}


@router.post("/api/templates/bulk/assign-campaigns")
def bulk_assign_templates_to_campaigns(request: BulkAssignTemplatesRequest):
    """Bulk assign multiple templates to multiple campaigns."""
    if not request.template_ids or not request.campaign_ids:
        raise HTTPException(400, "template_ids and campaign_ids are required")

    conn = get_db()
    count = 0
    for template_id in request.template_ids:
        for campaign_id in request.campaign_ids:
            conn.execute(
                "INSERT OR IGNORE INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)",
                (template_id, campaign_id)
            )
            count += 1
    conn.commit()
    conn.close()
    return {"message": f"Assigned {len(request.template_ids)} templates to {len(request.campaign_ids)} campaigns"}


@router.put("/api/campaigns/{campaign_id}/templates/{template_id}/metrics")
def update_template_campaign_metrics(
    campaign_id: int, template_id: int, metrics: TemplateCampaignMetricsUpdate
):
    conn = get_db()
    tc = conn.execute(
        "SELECT * FROM template_campaigns WHERE template_id=? AND campaign_id=?",
        (template_id, campaign_id)
    ).fetchone()
    if not tc:
        raise HTTPException(404, "Template not associated with this campaign")
    data = {k: v for k, v in metrics.dict().items() if v is not None}
    if not data:
        raise HTTPException(400, "No metrics to update")
    conn.execute(
        f"UPDATE template_campaigns SET {','.join([f'{k}=?' for k in data.keys()])} WHERE template_id=? AND campaign_id=?",
        list(data.values()) + [template_id, campaign_id]
    )
    conn.commit()
    conn.close()
    recalc_rates(campaign_id)
    recalc_template_rates(template_id)
    return {"message": "Updated"}


# ==================== LISTS ====================

@router.get("/api/lists")
def get_lists():
    conn = get_db()
    rows = conn.execute("SELECT * FROM outreach_lists ORDER BY name").fetchall()
    conn.close()
    return {"data": [dict(r) for r in rows]}


@router.delete("/api/lists/{list_id}")
def delete_list(list_id: int):
    conn = get_db()
    conn.execute("DELETE FROM contact_lists WHERE list_id=?", (list_id,))
    conn.execute("DELETE FROM outreach_lists WHERE id=?", (list_id,))
    conn.commit()
    conn.close()
    return {"message": "Deleted"}
