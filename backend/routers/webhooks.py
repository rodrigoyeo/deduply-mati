"""
Webhooks router — /webhook/*, /api/webhooks
"""
import json
import re
from datetime import datetime

from fastapi import APIRouter, Request

from database import get_db
from shared import recalc_rates, recalc_template_rates

router = APIRouter()


@router.get("/webhook/reachinbox")
async def reachinbox_webhook_verify():
    """Verification endpoint for ReachInbox webhook setup."""
    return {"status": "ok", "message": "Webhook endpoint active"}


@router.post("/webhook/reachinbox")
async def reachinbox_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    try:
        conn = get_db()
        event_type = payload.get('event', payload.get('type', payload.get('event_type', 'unknown')))
        email = payload.get('lead_email', payload.get('email', payload.get('to', payload.get('recipient', payload.get('recipient_email')))))
        campaign_name = payload.get('campaign_name', payload.get('campaign', payload.get('campaignName', payload.get('sequence_name'))))
        template_id = payload.get('template_id', payload.get('templateId'))
        step_number = payload.get('step_number', payload.get('step_id', payload.get('sequence_step')))
        el = event_type.lower().strip()
        normalized_event = 'unknown'
        if 'sent' in el or 'deliver' in el: normalized_event = 'sent'
        elif 'open' in el: normalized_event = 'opened'
        elif 'click' in el: normalized_event = 'clicked'
        elif 'repl' in el or 'response' in el: normalized_event = 'replied'
        elif 'bounce' in el: normalized_event = 'bounced'
        elif 'unsub' in el: normalized_event = 'unsubscribed'
        elif 'fail' in el or 'error' in el: normalized_event = 'failed'
        elif el == 'lead_interested' or el == 'interested': normalized_event = 'lead_interested'
        elif el == 'lead_not_interested' or el == 'not_interested': normalized_event = 'lead_not_interested'
        elif 'meeting' in el or 'scheduled' in el or 'booked' in el or 'calendar' in el: normalized_event = 'meeting_booked'
        conn.execute(
            """INSERT INTO webhook_events (source, event_type, email, campaign_name, template_id, payload, processed) VALUES (?, ?, ?, ?, ?, ?, TRUE)""",
            ('reachinbox', normalized_event, email, campaign_name, template_id, json.dumps(payload))
        )

        step_type = None
        if step_number:
            step_map = {1: 'Main', 2: 'Followup 1', 3: 'Followup 2', 4: 'Followup 3', 5: 'Followup 4'}
            step_type = step_map.get(int(step_number), f'Followup {int(step_number) - 1}' if int(step_number) > 1 else 'Main')

        if not step_number and email and campaign_name and normalized_event in ('lead_interested', 'meeting_booked', 'replied', 'opened', 'clicked'):
            last_sent = conn.execute("""
                SELECT payload FROM webhook_events
                WHERE email=? AND campaign_name=? AND event_type='sent'
                ORDER BY created_at DESC LIMIT 1
            """, (email, campaign_name)).fetchone()
            if last_sent and last_sent[0]:
                try:
                    sent_payload = json.loads(last_sent[0]) if isinstance(last_sent[0], str) else last_sent[0]
                    step_number = sent_payload.get('step_number')
                    if step_number:
                        step_map = {1: 'Main', 2: 'Followup 1', 3: 'Followup 2', 4: 'Followup 3', 5: 'Followup 4'}
                        step_type = step_map.get(int(step_number), f'Followup {int(step_number) - 1}')
                except Exception:
                    pass

        campaign_id = None
        if campaign_name:
            camp = conn.execute(
                "SELECT id FROM campaigns WHERE LOWER(name) = LOWER(?) OR name LIKE ?",
                (campaign_name, f"%{campaign_name}%")
            ).fetchone()
            if camp:
                campaign_id = camp[0]
                if normalized_event == 'sent': conn.execute("UPDATE campaigns SET emails_sent=emails_sent+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'opened': conn.execute("UPDATE campaigns SET emails_opened=emails_opened+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'clicked': conn.execute("UPDATE campaigns SET emails_clicked=emails_clicked+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'replied': conn.execute("UPDATE campaigns SET emails_replied=emails_replied+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'bounced': conn.execute("UPDATE campaigns SET emails_bounced=emails_bounced+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'lead_interested': conn.execute("UPDATE campaigns SET opportunities=opportunities+1 WHERE id=?", (campaign_id,))
                elif normalized_event == 'meeting_booked':
                    conn.execute("UPDATE campaigns SET meetings_booked=meetings_booked+1, opportunities=opportunities+1 WHERE id=?", (campaign_id,))
                recalc_rates(campaign_id, conn)

        email_subject = payload.get('email_subject', payload.get('subject', ''))
        clean_subject = re.sub(r'^(Re|Fwd|RE|FWD|re|fwd):\s*', '', email_subject).strip() if email_subject else ''

        matched_template_id = None
        if campaign_id and clean_subject:
            templates = conn.execute("""
                SELECT et.id, et.subject FROM email_templates et
                JOIN template_campaigns tc ON et.id = tc.template_id
                WHERE tc.campaign_id = ? AND et.subject IS NOT NULL AND et.subject != ''
            """, (campaign_id,)).fetchall()
            for t in templates:
                template_subject = t[1] or ''
                static_subject = re.sub(r'\{\{[^}]+\}\}', '', template_subject).strip()
                if static_subject and static_subject.lower() in clean_subject.lower():
                    matched_template_id = t[0]
                    break

        if not matched_template_id and campaign_id and step_type:
            templates = conn.execute("""
                SELECT et.id FROM email_templates et
                JOIN template_campaigns tc ON et.id = tc.template_id
                WHERE tc.campaign_id = ? AND et.step_type = ?
            """, (campaign_id, step_type)).fetchall()

            if not templates:
                step_templates = conn.execute(
                    "SELECT id FROM email_templates WHERE step_type = ?", (step_type,)
                ).fetchall()
                for t in step_templates:
                    existing = conn.execute(
                        "SELECT id FROM template_campaigns WHERE template_id=? AND campaign_id=?",
                        (t[0], campaign_id)
                    ).fetchone()
                    if not existing:
                        conn.execute(
                            "INSERT INTO template_campaigns (template_id, campaign_id) VALUES (?, ?)",
                            (t[0], campaign_id)
                        )
                templates = step_templates

            for t in templates:
                matched_template_id = t[0]
                break

        if matched_template_id and campaign_id:
            if normalized_event == 'sent':
                conn.execute("UPDATE template_campaigns SET times_sent=times_sent+1 WHERE template_id=? AND campaign_id=?", (matched_template_id, campaign_id))
            elif normalized_event == 'opened':
                conn.execute("UPDATE template_campaigns SET times_opened=times_opened+1 WHERE template_id=? AND campaign_id=?", (matched_template_id, campaign_id))
            elif normalized_event == 'replied':
                conn.execute("UPDATE template_campaigns SET times_replied=times_replied+1 WHERE template_id=? AND campaign_id=?", (matched_template_id, campaign_id))
            elif normalized_event == 'lead_interested':
                conn.execute("UPDATE template_campaigns SET opportunities=opportunities+1 WHERE template_id=? AND campaign_id=?", (matched_template_id, campaign_id))
            elif normalized_event == 'meeting_booked':
                conn.execute("UPDATE template_campaigns SET meetings=meetings+1, opportunities=opportunities+1 WHERE template_id=? AND campaign_id=?", (matched_template_id, campaign_id))
            recalc_template_rates(matched_template_id, conn)

        if email:
            contact = conn.execute(
                "SELECT id, status FROM contacts WHERE LOWER(email)=?", (email.lower(),)
            ).fetchone()
            if contact:
                contact_id = contact[0]
                if campaign_id:
                    existing_assoc = conn.execute(
                        "SELECT id FROM contact_campaigns WHERE contact_id=? AND campaign_id=?",
                        (contact_id, campaign_id)
                    ).fetchone()
                    if not existing_assoc:
                        conn.execute(
                            "INSERT INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)",
                            (contact_id, campaign_id)
                        )

                if normalized_event == 'sent':
                    conn.execute(
                        "UPDATE contacts SET times_contacted=times_contacted+1, last_contacted_at=?, status=CASE WHEN status='Lead' THEN 'Contacted' ELSE status END WHERE id=?",
                        (datetime.now().isoformat(), contact_id)
                    )
                elif normalized_event == 'replied':
                    conn.execute(
                        "UPDATE contacts SET status='Replied', updated_at=? WHERE id=? AND status IN ('Lead','Contacted')",
                        (datetime.now().isoformat(), contact_id)
                    )
                elif normalized_event == 'bounced':
                    conn.execute(
                        "UPDATE contacts SET email_status='Invalid', status='Bounced' WHERE id=?",
                        (contact_id,)
                    )
                elif normalized_event == 'lead_interested':
                    conn.execute(
                        "UPDATE contacts SET status='Interested', opportunities=opportunities+1, updated_at=? WHERE id=?",
                        (datetime.now().isoformat(), contact_id)
                    )
                elif normalized_event == 'lead_not_interested':
                    conn.execute(
                        "UPDATE contacts SET status='Not Interested', updated_at=? WHERE id=?",
                        (datetime.now().isoformat(), contact_id)
                    )
                elif normalized_event == 'meeting_booked':
                    conn.execute(
                        "UPDATE contacts SET status='Scheduled', opportunities=opportunities+1, meetings_booked=meetings_booked+1, updated_at=? WHERE id=?",
                        (datetime.now().isoformat(), contact_id)
                    )
        conn.commit()
        conn.close()
        return {
            "status": "ok",
            "message": "Processed",
            "event": normalized_event,
            "campaign_matched": campaign_id is not None,
            "campaign_id": campaign_id,
            "step_number": step_number,
            "step_type": step_type,
            "template_matched": matched_template_id is not None,
            "template_id": matched_template_id,
            "contact_matched": email is not None
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/webhook/bulkemailchecker")
async def bulkemailchecker_webhook(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    conn = get_db()
    results = payload.get('results', payload.get('data', payload.get('emails', [payload])))
    if not isinstance(results, list):
        results = [results]
    stats = {'processed': 0, 'valid': 0, 'invalid': 0, 'risky': 0, 'not_found': 0}
    for r in results:
        email = r.get('email', r.get('address', r.get('email_address', r.get('to'))))
        if not email:
            continue
        raw_status = r.get('status', r.get('result', r.get('state', r.get('verdict', 'unknown'))))
        raw_status_lower = str(raw_status).lower()
        if raw_status_lower in ['valid', 'deliverable', 'safe', 'ok', 'good', 'verified']:
            email_status = 'Valid'
            stats['valid'] += 1
        elif raw_status_lower in ['invalid', 'undeliverable', 'bad', 'bounce', 'rejected', 'syntax_error', 'mailbox_not_found']:
            email_status = 'Invalid'
            stats['invalid'] += 1
        elif raw_status_lower in ['risky', 'unknown', 'catch_all', 'catch-all', 'role', 'disposable', 'accept_all', 'spamtrap']:
            email_status = 'Risky'
            stats['risky'] += 1
        else:
            email_status = raw_status.capitalize() if raw_status else 'Unknown'
        existing = conn.execute(
            "SELECT id, email_status FROM contacts WHERE LOWER(email)=?", (email.lower(),)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE contacts SET email_status=?, updated_at=? WHERE id=?",
                (email_status, datetime.now().isoformat(), existing[0])
            )
            if email_status == 'Invalid':
                conn.execute(
                    "UPDATE contacts SET status='Bounced' WHERE id=? AND status NOT IN ('Client', 'Opportunity')",
                    (existing[0],)
                )
            stats['processed'] += 1
        else:
            stats['not_found'] += 1
    conn.execute(
        "INSERT INTO webhook_events (source, event_type, email, payload, processed) VALUES (?, ?, ?, ?, TRUE)",
        ('bulkemailchecker', 'validation', None, json.dumps(payload))
    )
    conn.commit()
    conn.close()
    return {"status": "ok", "message": f"Processed {stats['processed']} emails", "stats": stats}


@router.post("/webhook/clay")
async def clay_ingest_webhook(request: Request):
    """
    Accept contacts from a Clay webhook. POST a JSON payload:
    {
        "contacts": [{"email": "...", "first_name": "...", "company": "...", ...}],
        "outreach_list": "optional-list-name",
        "campaign": "optional-campaign-name",
        "country_strategy": "optional"
    }
    Contacts are inserted or merged by email.
    """
    try:
        payload = await request.json()
    except Exception:
        from fastapi import HTTPException
        raise HTTPException(400, "Invalid JSON payload")

    contacts_data = payload.get("contacts", [])
    if not contacts_data:
        return {"status": "ok", "message": "No contacts in payload", "imported": 0, "merged": 0}

    outreach_list_name = payload.get("outreach_list")
    campaign_name = payload.get("campaign")
    country_strategy = payload.get("country_strategy")
    conn = get_db()
    imported, merged = 0, 0
    now = datetime.now().isoformat()

    for c in contacts_data:
        email = (c.get("email") or "").strip().lower()
        if not email:
            continue

        existing = conn.execute("SELECT id FROM contacts WHERE LOWER(email)=?", (email,)).fetchone()
        fields = {
            "first_name": c.get("first_name") or c.get("firstName"),
            "last_name": c.get("last_name") or c.get("lastName"),
            "email": email,
            "title": c.get("title") or c.get("job_title"),
            "company": c.get("company") or c.get("company_name"),
            "person_linkedin_url": c.get("linkedin_url") or c.get("person_linkedin_url"),
            "domain": c.get("company_domain") or c.get("domain"),
            "website": c.get("website"),
            "company_country": c.get("company_country") or c.get("country"),
            "seniority": c.get("seniority"),
            "industry": c.get("industry"),
            "enrichment_source": c.get("enrichment_source", "clay"),
            "country_strategy": country_strategy,
            "source_file": "clay_webhook",
            "updated_at": now,
        }
        fields = {k: v for k, v in fields.items() if v is not None}

        if existing:
            contact_id = existing[0]
            non_email = [k for k in fields if k not in ("email", "source_file", "updated_at")]
            if non_email:
                set_clauses = [f"{k}=?" for k in non_email]
                set_clauses.append("updated_at=?")
                values = [fields[k] for k in non_email] + [now, contact_id]
                conn.execute(f"UPDATE contacts SET {','.join(set_clauses)} WHERE id=?", values)
            merged += 1
        else:
            cols = ", ".join(fields.keys())
            ph = ", ".join(["?"] * len(fields))
            conn.execute(f"INSERT INTO contacts ({cols}) VALUES ({ph})", list(fields.values()))
            contact_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            imported += 1

        if outreach_list_name:
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (outreach_list_name,))
            lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (outreach_list_name,)).fetchone()
            if lst:
                conn.execute("INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)", (contact_id, lst[0]))

        if campaign_name:
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (campaign_name,))
            camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (campaign_name,)).fetchone()
            if camp:
                conn.execute("INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)", (contact_id, camp[0]))

    conn.commit()
    conn.close()
    from shared import update_counts
    update_counts()
    return {"status": "ok", "imported": imported, "merged": merged, "total": imported + merged}


@router.post("/webhook/{source}")
async def generic_webhook(source: str, request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    conn = get_db()
    conn.execute(
        "INSERT INTO webhook_events (source, event_type, payload, processed) VALUES (?, ?, ?, TRUE)",
        (source, 'generic', json.dumps(payload))
    )
    conn.commit()
    conn.close()
    return {"status": "received"}


@router.get("/api/webhooks")
def get_webhooks(limit: int = 50):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM webhook_events ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return {"data": [dict(r) for r in rows]}
