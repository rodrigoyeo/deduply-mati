"""
Pydantic models (request/response schemas) for Deduply API.
"""
from pydantic import BaseModel
from typing import Optional, List


class UserLogin(BaseModel):
    email: str
    password: str


class UserCreate(BaseModel):
    email: str
    password: str
    name: Optional[str] = None
    role: str = "member"


class ChangePassword(BaseModel):
    current_password: str
    new_password: str


class ContactCreate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    seniority: Optional[str] = None
    first_phone: Optional[str] = None
    company_country: Optional[str] = None
    outreach_lists: Optional[str] = None
    campaigns_assigned: Optional[str] = None
    status: str = "Lead"
    notes: Optional[str] = None


class ContactUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    title: Optional[str] = None
    company: Optional[str] = None
    seniority: Optional[str] = None
    first_phone: Optional[str] = None
    company_country: Optional[str] = None
    outreach_lists: Optional[str] = None
    campaigns_assigned: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None


class BulkUpdateRequest(BaseModel):
    contact_ids: Optional[List[int]] = None
    filters: Optional[dict] = None
    field: str
    value: Optional[str] = None
    action: Optional[str] = None
    select_limit: Optional[int] = None


class CampaignCreate(BaseModel):
    name: str
    description: Optional[str] = None
    country: Optional[str] = None
    status: str = "Active"
    market: str = "US"  # US or MX - determines ReachInbox workspace
    strategy_brief: Optional[str] = None
    target_vertical: Optional[str] = None
    target_icp: Optional[str] = None
    hypothesis: Optional[str] = None
    created_by: str = "human"


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    country: Optional[str] = None
    status: Optional[str] = None
    market: Optional[str] = None  # US or MX
    emails_sent: Optional[int] = None
    emails_opened: Optional[int] = None
    emails_clicked: Optional[int] = None
    emails_replied: Optional[int] = None
    emails_bounced: Optional[int] = None
    opportunities: Optional[int] = None
    meetings_booked: Optional[int] = None
    strategy_brief: Optional[str] = None
    target_vertical: Optional[str] = None
    target_icp: Optional[str] = None
    hypothesis: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[str] = None


class TemplateCreate(BaseModel):
    name: str
    variant: str = "A"
    step_type: str = "Main"
    subject: Optional[str] = None
    body: Optional[str] = None
    country: Optional[str] = None
    campaign_ids: Optional[List[int]] = None


class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    variant: Optional[str] = None
    step_type: Optional[str] = None
    country: Optional[str] = None
    times_sent: Optional[int] = None
    times_opened: Optional[int] = None
    times_clicked: Optional[int] = None
    times_replied: Optional[int] = None
    is_winner: Optional[bool] = None
    campaign_ids: Optional[List[int]] = None


class MergeRequest(BaseModel):
    primary_id: int
    duplicate_ids: List[int]


class BulkAssignTemplatesRequest(BaseModel):
    template_ids: List[int]
    campaign_ids: List[int]


class TemplateCampaignMetricsUpdate(BaseModel):
    times_sent: Optional[int] = None
    times_opened: Optional[int] = None
    times_replied: Optional[int] = None
    opportunities: Optional[int] = None
    meetings: Optional[int] = None


class CleaningApplyRequest(BaseModel):
    contact_ids: List[int]
    field: str  # 'names' or 'company'


class ReachInboxPushRequest(BaseModel):
    contact_ids: List[int]
    reachinbox_campaign_id: int  # The numeric campaign ID inside ReachInbox
    workspace: str = "US"        # "US" or "MX"
    deduply_campaign_id: Optional[int] = None
    email_status_filter: Optional[List[str]] = None  # e.g. ["Valid"] - skip others


class PushCampaignContactsRequest(BaseModel):
    deduply_campaign_id: int
    email_status_filter: Optional[List[str]] = None  # default ["Valid"]
