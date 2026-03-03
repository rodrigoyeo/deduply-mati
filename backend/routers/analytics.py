"""
Analytics router — /api/stats/*, /api/analytics/*, /api/cleaning/*
"""
import json
from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException

from database import get_db, USE_POSTGRES
from models import CleaningApplyRequest
from data_cleaning import (
    clean_name, clean_company_name, clean_title,
    preview_name_cleaning, preview_company_cleaning, preview_title_cleaning,
    analyze_data_quality,
)

router = APIRouter()


# ==================== STATS ====================

@router.get("/api/stats")
def get_stats():
    conn = get_db()
    stats = {
        'total_contacts': conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        'unique_contacts': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0").fetchone()[0],
        'duplicates': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=1").fetchone()[0],
        'total_campaigns': conn.execute("SELECT COUNT(*) FROM campaigns").fetchone()[0],
        'total_lists': conn.execute("SELECT COUNT(*) FROM outreach_lists").fetchone()[0],
        'total_templates': conn.execute("SELECT COUNT(*) FROM email_templates").fetchone()[0]
    }
    r = conn.execute(
        "SELECT COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_opened),0), COALESCE(SUM(emails_replied),0) FROM campaigns"
    ).fetchone()
    tc = conn.execute(
        "SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns"
    ).fetchone()
    stats['emails_sent'] = r[0]
    stats['emails_opened'] = r[1]
    stats['emails_replied'] = r[2]
    stats['opportunities'] = tc[0]
    stats['meetings_booked'] = tc[1]
    stats['avg_open_rate'] = round(100 * r[1] / r[0], 1) if r[0] > 0 else 0
    stats['avg_reply_rate'] = round(100 * r[2] / r[0], 1) if r[0] > 0 else 0
    stats['by_status'] = {
        row[0] or 'Unknown': row[1]
        for row in conn.execute(
            "SELECT status, COUNT(*) FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY COUNT(*) DESC"
        )
    }
    stats['by_campaign'] = [
        (r[0], r[1]) for r in conn.execute(
            "SELECT name, total_leads FROM campaigns ORDER BY total_leads DESC LIMIT 10"
        )
    ]
    stats['by_list'] = [
        (r[0], r[1]) for r in conn.execute(
            "SELECT name, contact_count FROM outreach_lists ORDER BY contact_count DESC LIMIT 10"
        )
    ]
    # Workspace breakdown
    try:
        stats['us_contacts'] = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE reachinbox_workspace='US' OR country_strategy NOT IN ('Mexico') OR country_strategy IS NULL"
        ).fetchone()[0]
        stats['mx_contacts'] = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE reachinbox_workspace='MX' OR country_strategy='Mexico'"
        ).fetchone()[0]
        stats['pushed_contacts'] = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE reachinbox_lead_id IS NOT NULL AND reachinbox_lead_id != ''"
        ).fetchone()[0]
        stats['hubspot_synced'] = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE hubspot_contact_id IS NOT NULL AND hubspot_contact_id != ''"
        ).fetchone()[0]
        stats['duplicate_contacts'] = stats['duplicates']
    except Exception:
        pass
    conn.close()
    return stats


@router.get("/api/stats/funnel")
def get_funnel_stats():
    """Get sales funnel conversion metrics."""
    conn = get_db()

    stages = ['Lead', 'Contacted', 'Replied', 'Scheduled', 'Show', 'No-Show', 'Qualified', 'Client']

    funnel = {}
    for stage in stages:
        count = conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE status=? AND is_duplicate=0", (stage,)
        ).fetchone()[0]
        funnel[stage] = count

    funnel['Not Interested'] = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE status='Not Interested' AND is_duplicate=0"
    ).fetchone()[0]
    funnel['Bounced'] = conn.execute(
        "SELECT COUNT(*) FROM contacts WHERE status='Bounced' AND is_duplicate=0"
    ).fetchone()[0]

    total_contacts = sum(funnel[s] for s in stages)
    conversions = {
        'contact_rate': round(funnel['Contacted'] / max(funnel['Lead'] + funnel['Contacted'], 1) * 100, 1),
        'reply_rate': round(funnel['Replied'] / max(funnel['Contacted'], 1) * 100, 1),
        'booked_rate': round(funnel['Scheduled'] / max(funnel['Replied'], 1) * 100, 1),
        'show_rate': round(funnel['Show'] / max(funnel['Scheduled'], 1) * 100, 1),
        'qualified_rate': round(funnel['Qualified'] / max(funnel['Show'], 1) * 100, 1),
        'close_rate': round(funnel['Client'] / max(funnel['Qualified'], 1) * 100, 1),
        'overall_conversion': round(funnel['Client'] / max(total_contacts, 1) * 100, 2),
    }

    total_in_funnel = sum(funnel[s] for s in stages)
    max_stage = max(funnel[s] for s in stages) if stages else 1

    funnel_data = []
    for stage in stages:
        funnel_data.append({
            'stage': stage,
            'count': funnel[stage],
            'percentage': round(funnel[stage] / max(max_stage, 1) * 100, 1)
        })

    conn.close()
    return {
        'funnel': funnel,
        'conversions': conversions,
        'funnel_data': funnel_data,
        'total': total_in_funnel
    }


@router.get("/api/stats/database")
def get_database_stats():
    conn = get_db()
    insights = {}
    insights['by_country'] = [
        {"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT company_country, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY company_country ORDER BY cnt DESC LIMIT 15")
    ]
    insights['by_country_strategy'] = [
        {"name": r[0] or "Not Assigned", "value": r[1]} for r in
        conn.execute("SELECT country_strategy, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY country_strategy ORDER BY cnt DESC")
    ]
    insights['by_seniority'] = [
        {"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT seniority, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY seniority ORDER BY cnt DESC LIMIT 10")
    ]
    insights['by_industry'] = [
        {"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT industry, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY industry ORDER BY cnt DESC LIMIT 15")
    ]
    insights['by_company_size'] = [
        {"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT employee_bucket, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY employee_bucket ORDER BY cnt DESC")
    ]
    insights['by_status'] = [
        {"name": r[0] or "Unknown", "value": r[1]} for r in
        conn.execute("SELECT status, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY status ORDER BY cnt DESC")
    ]
    insights['by_email_status'] = [
        {"name": r[0] or "Not Verified", "value": r[1]} for r in
        conn.execute("SELECT email_status, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 GROUP BY email_status ORDER BY cnt DESC")
    ]
    insights['top_companies'] = [
        {"name": r[0], "value": r[1]} for r in
        conn.execute("SELECT company, COUNT(*) as cnt FROM contacts WHERE is_duplicate=0 AND company IS NOT NULL AND company != '' GROUP BY company ORDER BY cnt DESC LIMIT 10")
    ]
    if USE_POSTGRES:
        insights['contacts_timeline'] = [
            {"date": str(r[0]), "value": r[1]} for r in
            conn.execute("SELECT DATE(created_at), COUNT(*) FROM contacts WHERE is_duplicate=0 AND created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY DATE(created_at)")
        ]
    else:
        insights['contacts_timeline'] = [
            {"date": r[0], "value": r[1]} for r in
            conn.execute("SELECT DATE(created_at), COUNT(*) FROM contacts WHERE is_duplicate=0 AND created_at >= DATE('now', '-30 days') GROUP BY DATE(created_at) ORDER BY DATE(created_at)")
        ]
    insights['data_quality'] = {
        'with_email': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND email IS NOT NULL AND email != ''").fetchone()[0],
        'with_phone': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND (first_phone IS NOT NULL OR corporate_phone IS NOT NULL)").fetchone()[0],
        'with_linkedin': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND person_linkedin_url IS NOT NULL AND person_linkedin_url != ''").fetchone()[0],
        'with_company': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0 AND company IS NOT NULL AND company != ''").fetchone()[0],
        'total': conn.execute("SELECT COUNT(*) FROM contacts WHERE is_duplicate=0").fetchone()[0]
    }
    conn.close()
    return insights


@router.get("/api/stats/performance")
def get_performance_stats():
    conn = get_db()
    perf = {}
    campaigns = []
    for r in conn.execute("""
        SELECT id, name, country, status, total_leads, emails_sent, emails_opened, emails_replied,
               emails_bounced, open_rate, click_rate, reply_rate
        FROM campaigns ORDER BY emails_sent DESC
    """):
        c = dict(r)
        tc = conn.execute(
            "SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns WHERE campaign_id=?",
            (c['id'],)
        ).fetchone()
        c['opportunities'] = tc[0]
        c['meetings_booked'] = tc[1]
        campaigns.append(c)
    perf['campaigns'] = campaigns

    row = conn.execute("""
        SELECT COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_opened),0), COALESCE(SUM(emails_clicked),0),
               COALESCE(SUM(emails_replied),0), COALESCE(SUM(emails_bounced),0)
        FROM campaigns
    """).fetchone()
    tc_totals = conn.execute(
        "SELECT COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0) FROM template_campaigns"
    ).fetchone()
    perf['totals'] = {
        'sent': row[0], 'opened': row[1], 'clicked': row[2], 'replied': row[3],
        'bounced': row[4], 'opportunities': tc_totals[0], 'meetings': tc_totals[1],
        'open_rate': round(100 * row[1] / row[0], 1) if row[0] > 0 else 0,
        'click_rate': round(100 * row[2] / row[0], 1) if row[0] > 0 else 0,
        'reply_rate': round(100 * row[3] / row[0], 1) if row[0] > 0 else 0,
        'bounce_rate': round(100 * row[4] / row[0], 1) if row[0] > 0 else 0
    }
    perf['by_country'] = [
        {"country": r[0] or "Not Set", "campaigns": r[1], "sent": r[2], "replied": r[3]} for r in
        conn.execute("SELECT country, COUNT(*), COALESCE(SUM(emails_sent),0), COALESCE(SUM(emails_replied),0) FROM campaigns GROUP BY country ORDER BY SUM(emails_sent) DESC")
    ]
    perf['top_templates'] = []
    for t in conn.execute("SELECT id, name, variant, step_type FROM email_templates"):
        tc = conn.execute("""
            SELECT COALESCE(SUM(times_sent),0), COALESCE(SUM(times_opened),0), COALESCE(SUM(times_replied),0),
                   COALESCE(SUM(opportunities),0), COALESCE(SUM(meetings),0)
            FROM template_campaigns WHERE template_id=?
        """, (t['id'],)).fetchone()
        if tc[0] > 0:
            perf['top_templates'].append({
                'id': t['id'], 'name': t['name'], 'variant': t['variant'], 'step_type': t['step_type'],
                'times_sent': tc[0], 'times_opened': tc[1], 'times_replied': tc[2],
                'opportunities': tc[3], 'meetings': tc[4],
                'open_rate': round(100 * tc[1] / tc[0], 1) if tc[0] > 0 else 0,
                'reply_rate': round(100 * tc[2] / tc[0], 1) if tc[0] > 0 else 0
            })
    perf['top_templates'].sort(key=lambda x: x['reply_rate'], reverse=True)
    perf['top_templates'] = perf['top_templates'][:10]
    conn.close()
    return perf


# ==================== DATA CLEANING ====================

@router.get("/api/cleaning/stats")
def get_cleaning_stats():
    """Get data quality statistics for the contacts database."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, first_name, last_name, title, company, domain FROM contacts WHERE is_duplicate=0"
    ).fetchall()
    conn.close()
    contacts = [dict(r) for r in rows]
    stats = analyze_data_quality(contacts)
    return stats


@router.get("/api/cleaning/names/preview")
def preview_name_changes(limit: int = 500):
    """Preview name cleaning changes without applying them."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, first_name, last_name FROM contacts
        WHERE is_duplicate=0
        AND (first_name IS NOT NULL OR last_name IS NOT NULL)
    """).fetchall()
    conn.close()

    contacts = [dict(r) for r in rows]
    all_changes = preview_name_cleaning(contacts)
    return {"changes": all_changes[:limit], "total": len(all_changes)}


@router.post("/api/cleaning/names/apply")
def apply_name_cleaning(req: CleaningApplyRequest):
    """Apply name cleaning to selected contacts."""
    if not req.contact_ids:
        raise HTTPException(400, "No contacts selected")

    conn = get_db()
    updated = 0
    now = datetime.now().isoformat()

    for cid in req.contact_ids:
        row = conn.execute("SELECT first_name, last_name FROM contacts WHERE id=?", (cid,)).fetchone()
        if row:
            first_name = clean_name(row['first_name'], preserve_case_if_mixed=False)
            last_name = clean_name(row['last_name'], preserve_case_if_mixed=False)
            conn.execute(
                "UPDATE contacts SET first_name=?, last_name=?, updated_at=? WHERE id=?",
                (first_name, last_name, now, cid)
            )
            updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} contact names"}


@router.get("/api/cleaning/companies/preview")
def preview_company_changes(limit: int = 500):
    """Preview company name cleaning changes without applying them."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, company, domain FROM contacts
        WHERE is_duplicate=0
        AND company IS NOT NULL AND company != ''
    """).fetchall()
    conn.close()

    contacts = [dict(r) for r in rows]
    all_changes = preview_company_cleaning(contacts)
    return {"changes": all_changes[:limit], "total": len(all_changes)}


@router.post("/api/cleaning/companies/apply")
def apply_company_cleaning(req: CleaningApplyRequest):
    """Apply company name cleaning to selected contacts."""
    if not req.contact_ids:
        raise HTTPException(400, "No contacts selected")

    conn = get_db()
    updated = 0
    now = datetime.now().isoformat()

    for cid in req.contact_ids:
        row = conn.execute("SELECT company, domain FROM contacts WHERE id=?", (cid,)).fetchone()
        if row:
            cleaned, _ = clean_company_name(row['company'], row['domain'])
            if cleaned and cleaned != row['company']:
                conn.execute(
                    "UPDATE contacts SET company=?, updated_at=? WHERE id=?",
                    (cleaned, now, cid)
                )
                updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} company names"}


@router.post("/api/cleaning/names/apply-all")
def apply_all_name_cleaning(limit: int = 10000):
    """Apply name cleaning to all contacts that need it."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, first_name, last_name FROM contacts
        WHERE is_duplicate=0
        AND (first_name IS NOT NULL OR last_name IS NOT NULL)
    """).fetchall()

    updated = 0
    now = datetime.now().isoformat()

    for row in rows:
        first_name = clean_name(row['first_name'], preserve_case_if_mixed=False) if row['first_name'] else row['first_name']
        last_name = clean_name(row['last_name'], preserve_case_if_mixed=False) if row['last_name'] else row['last_name']
        if first_name != row['first_name'] or last_name != row['last_name']:
            conn.execute(
                "UPDATE contacts SET first_name=?, last_name=?, updated_at=? WHERE id=?",
                (first_name, last_name, now, row['id'])
            )
            updated += 1
            if updated >= limit:
                break

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} contact names"}


@router.post("/api/cleaning/companies/apply-all")
def apply_all_company_cleaning(limit: int = 10000):
    """Apply company cleaning to all contacts that need it."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, company, domain FROM contacts
        WHERE is_duplicate=0
        AND company IS NOT NULL AND company != ''
    """).fetchall()

    updated = 0
    now = datetime.now().isoformat()

    for row in rows:
        cleaned, reason = clean_company_name(row['company'], row['domain'])
        if reason and cleaned != row['company']:
            conn.execute(
                "UPDATE contacts SET company=?, updated_at=? WHERE id=?",
                (cleaned, now, row['id'])
            )
            updated += 1
            if updated >= limit:
                break

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} company names"}


@router.get("/api/cleaning/titles/preview")
def preview_title_changes(limit: int = 500):
    """Preview title cleaning changes without applying them."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, title FROM contacts
        WHERE is_duplicate=0
        AND title IS NOT NULL AND title != ''
    """).fetchall()
    conn.close()

    contacts = [dict(r) for r in rows]
    all_changes = preview_title_cleaning(contacts)
    return {"changes": all_changes[:limit], "total": len(all_changes)}


@router.post("/api/cleaning/titles/apply")
def apply_title_cleaning(req: CleaningApplyRequest):
    """Apply title cleaning to selected contacts."""
    if not req.contact_ids:
        raise HTTPException(400, "No contacts selected")

    conn = get_db()
    updated = 0
    now = datetime.now().isoformat()

    for cid in req.contact_ids:
        row = conn.execute("SELECT title FROM contacts WHERE id=?", (cid,)).fetchone()
        if row and row['title']:
            cleaned = clean_title(row['title'])
            if cleaned and cleaned != row['title']:
                conn.execute(
                    "UPDATE contacts SET title=?, updated_at=? WHERE id=?",
                    (cleaned, now, cid)
                )
                updated += 1

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} titles"}


@router.post("/api/cleaning/titles/apply-all")
def apply_all_title_cleaning(limit: int = 10000):
    """Apply title cleaning to all contacts that need it."""
    conn = get_db()
    rows = conn.execute("""
        SELECT id, title FROM contacts
        WHERE is_duplicate=0
        AND title IS NOT NULL AND title != ''
    """).fetchall()

    updated = 0
    now = datetime.now().isoformat()

    for row in rows:
        cleaned = clean_title(row['title'])
        if cleaned and cleaned != row['title']:
            conn.execute(
                "UPDATE contacts SET title=?, updated_at=? WHERE id=?",
                (cleaned, now, row['id'])
            )
            updated += 1
            if updated >= limit:
                break

    conn.commit()
    conn.close()
    return {"updated": updated, "message": f"Cleaned {updated} titles"}


# ==================== LEARNING LOOP ====================

_DAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']


@router.get("/api/analytics/learning")
def get_learning_analytics():
    """
    Learning loop insights:
    - Top 5 templates by reply_rate per workspace (US + MX separately)
    - Best days to send (grouped by day-of-week from webhook_events replies)
    - Reply-to-interested conversion rate
    - Avg steps to first reply
    """
    conn = get_db()

    # --- Top templates per workspace ---
    rows = conn.execute("""
        SELECT et.id, et.name, et.variant, et.step_type,
               COALESCE(c.market, 'US') AS workspace,
               SUM(tc.times_sent)    AS sends,
               SUM(tc.times_replied) AS replies
        FROM template_campaigns tc
        JOIN campaigns c ON tc.campaign_id = c.id
        JOIN email_templates et ON tc.template_id = et.id
        GROUP BY et.id, et.name, et.variant, et.step_type, c.market
        HAVING SUM(tc.times_sent) >= 5
        ORDER BY replies DESC
    """).fetchall()

    workspace_templates: dict = {"US": [], "MX": []}
    for r in rows:
        sends = r[5] or 0
        replies = r[6] or 0
        rr = round(100.0 * replies / sends, 1) if sends > 0 else 0
        ws = (r[4] or "US").upper()
        if ws not in workspace_templates:
            workspace_templates[ws] = []
        workspace_templates[ws].append({
            "id": r[0], "name": r[1], "variant": r[2], "step_type": r[3],
            "workspace": ws, "sends": sends, "replies": replies, "reply_rate": rr
        })
    for ws in workspace_templates:
        workspace_templates[ws].sort(key=lambda x: x["reply_rate"], reverse=True)
        workspace_templates[ws] = workspace_templates[ws][:5]

    # --- Best days to send (from webhook_events replies) ---
    if USE_POSTGRES:
        day_rows = conn.execute("""
            SELECT EXTRACT(DOW FROM created_at)::int AS dow, COUNT(*) AS cnt
            FROM webhook_events
            WHERE event_type = 'replied'
            GROUP BY dow ORDER BY dow
        """).fetchall()
    else:
        day_rows = conn.execute("""
            SELECT CAST(strftime('%w', created_at) AS INTEGER) AS dow, COUNT(*) AS cnt
            FROM webhook_events
            WHERE event_type = 'replied'
            GROUP BY dow ORDER BY dow
        """).fetchall()

    total_reply_events = sum(r[1] for r in day_rows) or 1
    best_days = [
        {"day": _DAY_NAMES[int(r[0])], "replies": r[1],
         "pct": round(100.0 * r[1] / total_reply_events, 1)}
        for r in day_rows
    ]
    best_days.sort(key=lambda x: x["replies"], reverse=True)

    # --- Reply-to-interested conversion ---
    reply_count = conn.execute(
        "SELECT COUNT(*) FROM webhook_events WHERE event_type='replied'"
    ).fetchone()[0] or 0
    interested_count = conn.execute(
        "SELECT COUNT(*) FROM webhook_events WHERE event_type='lead_interested'"
    ).fetchone()[0] or 0
    reply_to_interested = round(100.0 * interested_count / reply_count, 1) if reply_count > 0 else 0

    # --- Avg steps to first reply (parsed from payload JSON) ---
    payload_rows = conn.execute(
        "SELECT payload FROM webhook_events WHERE event_type='replied' AND payload IS NOT NULL"
    ).fetchall()
    step_numbers = []
    for row in payload_rows:
        try:
            p = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
            sn = p.get("step_number") or p.get("step_id")
            if sn is not None and str(sn).isdigit():
                step_numbers.append(int(sn))
        except Exception:
            pass
    avg_steps = round(sum(step_numbers) / len(step_numbers), 1) if step_numbers else None

    conn.close()
    return {
        "top_templates": workspace_templates,
        "best_days": best_days,
        "reply_to_interested_rate": reply_to_interested,
        "avg_steps_to_reply": avg_steps,
        "total_replies": reply_count,
        "total_interested": interested_count,
    }


@router.get("/api/analytics/ab-winners")
def get_ab_winners():
    """
    For each campaign compare template variants A vs B.
    Winner = variant with higher reply_rate AND at least 20 sends.
    """
    conn = get_db()

    rows = conn.execute("""
        SELECT c.id AS campaign_id, c.name AS campaign_name, et.variant,
               SUM(tc.times_sent)    AS sends,
               SUM(tc.times_replied) AS replies
        FROM template_campaigns tc
        JOIN campaigns c ON tc.campaign_id = c.id
        JOIN email_templates et ON tc.template_id = et.id
        WHERE et.variant IN ('A', 'B')
        GROUP BY c.id, c.name, et.variant
        ORDER BY c.name, et.variant
    """).fetchall()

    # Group by campaign
    campaigns: dict = {}
    for r in rows:
        cid = r[0]
        if cid not in campaigns:
            campaigns[cid] = {"campaign_id": cid, "campaign_name": r[1], "variants": {}}
        sends = r[3] or 0
        replies = r[4] or 0
        rr = round(100.0 * replies / sends, 1) if sends > 0 else 0
        campaigns[cid]["variants"][r[2]] = {"sends": sends, "replies": replies, "reply_rate": rr}

    winners = []
    for cid, camp in campaigns.items():
        a = camp["variants"].get("A")
        b = camp["variants"].get("B")
        if not a or not b:
            continue
        if a["sends"] < 20 and b["sends"] < 20:
            continue
        if a["reply_rate"] == b["reply_rate"]:
            continue
        winner_v, loser_v = ("A", "B") if a["reply_rate"] > b["reply_rate"] else ("B", "A")
        winner, loser = camp["variants"][winner_v], camp["variants"][loser_v]
        winners.append({
            "campaign_id": cid,
            "campaign_name": camp["campaign_name"],
            "winner_variant": winner_v,
            "winner_reply_rate": winner["reply_rate"],
            "winner_sends": winner["sends"],
            "loser_variant": loser_v,
            "loser_reply_rate": loser["reply_rate"],
            "loser_sends": loser["sends"],
        })

    conn.close()
    winners.sort(key=lambda x: x["winner_reply_rate"] - x["loser_reply_rate"], reverse=True)
    return {"winners": winners}


@router.get("/api/analytics/workspace-compare")
def get_workspace_compare():
    """US vs MX side-by-side: sent, open_rate, reply_rate, interested_count, meetings_booked."""
    conn = get_db()

    camp_rows = conn.execute("""
        SELECT COALESCE(market, 'US') AS workspace,
               COUNT(*)               AS campaign_count,
               COALESCE(SUM(emails_sent), 0)     AS sent,
               COALESCE(SUM(emails_opened), 0)   AS opened,
               COALESCE(SUM(emails_replied), 0)  AS replied,
               COALESCE(SUM(opportunities), 0)   AS interested,
               COALESCE(SUM(meetings_booked), 0) AS meetings
        FROM campaigns
        GROUP BY workspace
    """).fetchall()

    result = {}
    for r in camp_rows:
        ws = r[0].upper()
        sent = r[2] or 0
        result[ws] = {
            "workspace": ws,
            "campaign_count": r[1],
            "sent": sent,
            "opened": r[3],
            "replied": r[4],
            "interested": r[5],
            "meetings": r[6],
            "open_rate": round(100.0 * r[3] / sent, 1) if sent > 0 else 0,
            "reply_rate": round(100.0 * r[4] / sent, 1) if sent > 0 else 0,
            "interested_rate": round(100.0 * r[5] / max(r[4], 1), 1),
        }

    # Ensure both workspaces are present even if empty
    for ws in ("US", "MX"):
        if ws not in result:
            result[ws] = {
                "workspace": ws, "campaign_count": 0, "sent": 0,
                "opened": 0, "replied": 0, "interested": 0, "meetings": 0,
                "open_rate": 0, "reply_rate": 0, "interested_rate": 0,
            }

    conn.close()
    return {"workspaces": result}
