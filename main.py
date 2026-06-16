"""
main.py — NEXUS Deal Intelligence Engine
Phase 1: pipeline + qualification + lender matching + signal engine + outreach drafting.

Single-service FastAPI app. SQLite local, Postgres in prod via DATABASE_URL.
Reuses the proven patterns from TexWholesale Engine.
"""

from __future__ import annotations

import csv
import io
import json
import os
import statistics
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text, create_engine, func, select, inspect as sa_inspect, text as sa_text,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker

import collectors as signal_engine

APP_NAME = "NEXUS Deal Intelligence Engine"
APP_VERSION = "0.2.0"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./nexus.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
HTTP_TIMEOUT = 8.0

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uid() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------
PIPELINE_STAGES = ["Signal", "Contacted", "Qualifying", "Submitted", "Approved", "Funded", "Lost"]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class Deal(Base):
    __tablename__ = "deals"

    id = Column(String, primary_key=True, default=_uid)
    business_name = Column(String, default="")
    contact_name = Column(String, default="")
    email = Column(String, default="")
    phone = Column(String, default="")
    website = Column(String, default="")
    location = Column(String, default="")
    industry = Column(String, default="")

    # Capital need
    amount_requested = Column(Float, default=0.0)
    use_of_funds = Column(String, default="")
    timeline = Column(String, default="")          # ASAP | 1-2 weeks | this month | exploring

    # Qualification
    monthly_revenue = Column(Float, default=0.0)
    time_in_business_months = Column(Integer, default=0)
    existing_positions = Column(Integer, default=0)  # existing MCAs
    credit_range = Column(String, default="")
    nsf_count = Column(Integer, default=0)
    negative_days = Column(Integer, default=0)
    avg_daily_balance = Column(Float, default=0.0)
    revenue_trend = Column(String, default="")       # growing | flat | declining
    qual_score = Column(Float, default=0.0)
    paper_grade = Column(String, default="")         # A | B | C | D

    # Deal management
    stage = Column(String, default="Signal")
    signal_source = Column(String, default="")
    signal_type = Column(String, default="")
    signal_score = Column(Float, default=0.0)
    matched_lenders = Column(Text, default="[]")     # JSON list of lender match results
    submitted_to = Column(String, default="")
    estimated_commission = Column(Float, default=0.0)
    actual_commission = Column(Float, default=0.0)
    notes = Column(Text, default="")

    # Outcome tracking (the guarantee engine)
    contacted_at = Column(DateTime, nullable=True)
    submitted_at = Column(DateTime, nullable=True)
    funded_at = Column(DateTime, nullable=True)
    next_follow_up = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=_now)
    updated_at = Column(DateTime, default=_now)

    def matches(self) -> list:
        try:
            return json.loads(self.matched_lenders or "[]")
        except (ValueError, TypeError):
            return []

    def to_dict(self) -> dict:
        today = _now().date()

        def days_until(dt):
            if not dt:
                return None
            d = dt.date() if isinstance(dt, datetime) else dt
            return (d - today).days

        time_to_fund = None
        if self.funded_at and self.contacted_at:
            time_to_fund = (self.funded_at.date() - self.contacted_at.date()).days

        return {
            "id": self.id,
            "business_name": self.business_name,
            "contact_name": self.contact_name,
            "email": self.email,
            "phone": self.phone,
            "website": self.website,
            "location": self.location,
            "industry": self.industry,
            "amount_requested": self.amount_requested,
            "use_of_funds": self.use_of_funds,
            "timeline": self.timeline,
            "monthly_revenue": self.monthly_revenue,
            "time_in_business_months": self.time_in_business_months,
            "existing_positions": self.existing_positions,
            "credit_range": self.credit_range,
            "nsf_count": self.nsf_count,
            "negative_days": self.negative_days,
            "avg_daily_balance": self.avg_daily_balance,
            "revenue_trend": self.revenue_trend,
            "qual_score": round(self.qual_score, 1),
            "paper_grade": self.paper_grade,
            "stage": self.stage,
            "signal_source": self.signal_source,
            "signal_type": self.signal_type,
            "signal_score": round(self.signal_score, 1),
            "matched_lenders": self.matches(),
            "submitted_to": self.submitted_to,
            "estimated_commission": self.estimated_commission,
            "actual_commission": self.actual_commission,
            "notes": self.notes,
            "next_follow_up": self.next_follow_up.isoformat() if self.next_follow_up else None,
            "days_until_follow_up": days_until(self.next_follow_up),
            "time_to_fund_days": time_to_fund,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Lender(Base):
    __tablename__ = "lenders"

    id = Column(String, primary_key=True, default=_uid)
    name = Column(String, default="")
    paper_grades = Column(String, default="A,B")      # which grades they fund
    min_revenue = Column(Float, default=0.0)           # monthly
    min_tib_months = Column(Integer, default=6)
    min_credit = Column(Integer, default=500)
    max_amount = Column(Float, default=500000.0)
    min_amount = Column(Float, default=5000.0)
    industries = Column(Text, default="")              # comma-sep; empty = all
    excluded_industries = Column(Text, default="")
    commission_pct = Column(Float, default=8.0)        # your points
    funds_same_day = Column(Boolean, default=False)
    notes = Column(Text, default="")
    active = Column(Boolean, default=True)

    # Learned performance (the brain)
    submissions = Column(Integer, default=0)
    approvals = Column(Integer, default=0)
    fundings = Column(Integer, default=0)
    total_commission = Column(Float, default=0.0)

    def _csv(self, raw) -> list[str]:
        return [s.strip().lower() for s in (raw or "").split(",") if s.strip()]

    def approval_rate(self) -> float:
        return round(self.approvals / self.submissions, 2) if self.submissions else 0.0

    def to_dict(self) -> dict:
        return {
            "id": self.id, "name": self.name,
            "paper_grades": self._csv(self.paper_grades),
            "min_revenue": self.min_revenue, "min_tib_months": self.min_tib_months,
            "min_credit": self.min_credit, "max_amount": self.max_amount, "min_amount": self.min_amount,
            "industries": self._csv(self.industries), "excluded_industries": self._csv(self.excluded_industries),
            "commission_pct": self.commission_pct, "funds_same_day": self.funds_same_day,
            "notes": self.notes, "active": self.active,
            "submissions": self.submissions, "approvals": self.approvals, "fundings": self.fundings,
            "approval_rate": self.approval_rate(), "total_commission": self.total_commission,
        }


class SignalRecord(Base):
    __tablename__ = "signal_records"

    id = Column(String, primary_key=True, default=_uid)
    business_name = Column(String, default="")
    location = Column(String, default="")
    signal_type = Column(String, default="")
    score = Column(Float, default=0.0)
    source = Column(String, default="")
    source_url = Column(String, default="")
    raw_data = Column(Text, default="{}")
    contact_hints = Column(Text, default="{}")
    dedup_key = Column(String, default="")
    worked = Column(Boolean, default=False)            # have you acted on it?
    converted_to_deal = Column(String, default="")     # deal id if converted
    created_at = Column(DateTime, default=_now)

    def to_dict(self) -> dict:
        try:
            raw = json.loads(self.raw_data or "{}")
        except (ValueError, TypeError):
            raw = {}
        try:
            hints = json.loads(self.contact_hints or "{}")
        except (ValueError, TypeError):
            hints = {}
        return {
            "id": self.id, "business_name": self.business_name, "location": self.location,
            "signal_type": self.signal_type, "score": round(self.score, 1), "source": self.source,
            "source_url": self.source_url, "raw_data": raw, "contact_hints": hints,
            "dedup_key": self.dedup_key, "worked": self.worked,
            "converted_to_deal": self.converted_to_deal,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class ReferralPartner(Base):
    __tablename__ = "referral_partners"

    id = Column(String, primary_key=True, default=_uid)
    name = Column(String, default="")
    partner_type = Column(String, default="accountant")  # accountant | bookkeeper | lawyer | other
    email = Column(String, default="")
    phone = Column(String, default="")
    split_pct = Column(Float, default=25.0)
    referrals_sent = Column(Integer, default=0)
    referrals_funded = Column(Integer, default=0)
    total_paid = Column(Float, default=0.0)
    last_contact = Column(DateTime, nullable=True)
    notes = Column(Text, default="")
    created_at = Column(DateTime, default=_now)

    def to_dict(self) -> dict:
        return {
            "id": self.id, "name": self.name, "partner_type": self.partner_type,
            "email": self.email, "phone": self.phone, "split_pct": self.split_pct,
            "referrals_sent": self.referrals_sent, "referrals_funded": self.referrals_funded,
            "total_paid": self.total_paid,
            "last_contact": self.last_contact.isoformat() if self.last_contact else None,
            "notes": self.notes,
        }


Base.metadata.create_all(engine)


def ensure_schema():
    """Forward-only migration: add missing columns to existing tables."""
    dialect = engine.dialect.name

    def sql_type(col):
        t = col.type.__class__.__name__.lower()
        if "boolean" in t:
            return "BOOLEAN"
        if "integer" in t:
            return "INTEGER"
        if "float" in t:
            return "DOUBLE PRECISION" if dialect == "postgresql" else "FLOAT"
        if "datetime" in t:
            return "TIMESTAMP"
        return "TEXT"

    insp = sa_inspect(engine)
    existing = set(insp.get_table_names())
    with engine.begin() as conn:
        for tname, model in Base.metadata.tables.items():
            if tname not in existing:
                continue
            have = {c["name"] for c in insp.get_columns(tname)}
            for col in model.columns:
                if col.name not in have:
                    try:
                        conn.execute(sa_text(f"ALTER TABLE {tname} ADD COLUMN {col.name} {sql_type(col)}"))
                    except Exception as e:
                        print(f"[migrate] skip {tname}.{col.name}: {e}")


ensure_schema()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Seed default lender panel (your starter ISO relationships)
# ---------------------------------------------------------------------------
DEFAULT_LENDERS = [
    {"name": "Credibly", "paper_grades": "A,B,C", "min_revenue": 15000, "min_tib_months": 6,
     "min_credit": 500, "max_amount": 400000, "commission_pct": 8, "funds_same_day": True,
     "notes": "Broad coverage, 4-hour approvals, both white-label and referral tracks."},
    {"name": "Fora Financial", "paper_grades": "A,B", "min_revenue": 20000, "min_tib_months": 6,
     "min_credit": 570, "max_amount": 1500000, "commission_pct": 10, "funds_same_day": False,
     "notes": "No personal guarantee, pays renewals for lifetime of client."},
    {"name": "Rapid Finance", "paper_grades": "A,B,C", "min_revenue": 15000, "min_tib_months": 12,
     "min_credit": 550, "max_amount": 500000, "commission_pct": 9, "funds_same_day": False,
     "notes": "$2B+ funded, factor 1.15-1.50, also SBA/term/bridge."},
    {"name": "Greenbox Capital", "paper_grades": "B,C,D", "min_revenue": 10000, "min_tib_months": 5,
     "min_credit": 500, "max_amount": 250000, "commission_pct": 15, "funds_same_day": True,
     "notes": "Highest commissions (up to 19%). Zero-tolerance for double-funding."},
    {"name": "OnDeck", "paper_grades": "A", "min_revenue": 8333, "min_tib_months": 12,
     "min_credit": 600, "max_amount": 250000, "commission_pct": 4, "funds_same_day": True,
     "notes": "A-paper term loans + LOC. Lower commission, stronger borrowers."},
    {"name": "Forward Financing", "paper_grades": "B,C", "min_revenue": 10000, "min_tib_months": 6,
     "min_credit": 500, "max_amount": 300000, "commission_pct": 12, "funds_same_day": True,
     "notes": "Solid B-paper funder, fast."},
]


def seed_lenders(db: Session):
    if db.execute(select(func.count(Lender.id))).scalar():
        return
    for L in DEFAULT_LENDERS:
        db.add(Lender(**L))
    db.commit()


with SessionLocal() as _db:
    seed_lenders(_db)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title=APP_NAME, version=APP_VERSION)


@app.get("/api/health")
def health():
    return {
        "status": "ok", "app": APP_NAME, "version": APP_VERSION,
        "time": _now().isoformat(),
        "database": "postgresql" if DATABASE_URL.startswith("postgresql") else "sqlite",
        "integrations": {
            "claude": bool(ANTHROPIC_API_KEY),
            "sam_gov": bool(os.getenv("SAM_GOV_API_KEY", "").strip()),
            "google_maps": bool(os.getenv("GOOGLE_MAPS_API_KEY", "").strip()),
            "yelp": bool(os.getenv("YELP_API_KEY", "").strip()),
        },
        "signal_collectors": signal_engine.registry_stats()["total_collectors"],
    }


# ===========================================================================
# QUALIFICATION ENGINE
# ===========================================================================

class QualifyInput(BaseModel):
    industry: str = ""
    time_in_business_months: int = 0
    monthly_revenue: float = 0.0
    existing_positions: int = 0
    use_of_funds: str = ""
    amount_requested: float = 0.0
    timeline: str = ""
    credit_range: str = ""           # "<550" | "550-600" | "600-650" | "650-700" | "700+"
    # Optional bank-statement-derived metrics
    nsf_count: int = 0
    negative_days: int = 0
    avg_daily_balance: float = 0.0
    revenue_trend: str = ""


RESTRICTED_INDUSTRIES = {"adult", "gambling", "firearms", "cannabis", "nonprofit", "real estate investment"}


def _credit_floor(credit_range: str) -> int:
    return {"<550": 500, "550-600": 550, "600-650": 600, "650-700": 650, "700+": 700}.get(credit_range, 500)


def qualify_deal(d: QualifyInput) -> dict:
    """
    Score a deal 0-100 and assign a paper grade. Returns disqualifiers if any.
    This is the pre-screen that protects your lender relationships.
    """
    score = 0.0
    reasons = []
    disqualifiers = []

    # Hard disqualifiers
    if d.time_in_business_months < 6:
        disqualifiers.append("Under 6 months in business")
    if d.monthly_revenue < 10000:
        disqualifiers.append("Monthly revenue under $10K")
    if d.industry.lower() in RESTRICTED_INDUSTRIES:
        disqualifiers.append(f"Restricted industry: {d.industry}")
    if d.existing_positions >= 3:
        disqualifiers.append("3+ existing MCA positions (stacked)")
    if d.revenue_trend == "declining":
        disqualifiers.append("Declining revenue trend")
    if d.negative_days >= 10:
        disqualifiers.append("10+ negative days per month")
    if d.nsf_count > 5:
        disqualifiers.append("More than 5 NSFs per month")

    # Scoring (only meaningful if not disqualified)
    # Time in business (0-20)
    if d.time_in_business_months >= 24:
        score += 20; reasons.append("Established 2+ years")
    elif d.time_in_business_months >= 12:
        score += 14; reasons.append("1+ year in business")
    elif d.time_in_business_months >= 6:
        score += 8

    # Revenue (0-25)
    if d.monthly_revenue >= 100000:
        score += 25; reasons.append("Strong revenue ($100K+/mo)")
    elif d.monthly_revenue >= 50000:
        score += 20; reasons.append("Solid revenue ($50K+/mo)")
    elif d.monthly_revenue >= 25000:
        score += 14
    elif d.monthly_revenue >= 10000:
        score += 8

    # Revenue trend (0-15)
    if d.revenue_trend == "growing":
        score += 15; reasons.append("Growing revenue")
    elif d.revenue_trend == "flat":
        score += 9

    # Existing positions (0-15)
    if d.existing_positions == 0:
        score += 15; reasons.append("No existing advances")
    elif d.existing_positions == 1:
        score += 8

    # Bank health (0-15)
    if d.nsf_count <= 2 and d.negative_days <= 2:
        score += 15; reasons.append("Clean bank statements")
    elif d.nsf_count <= 5 and d.negative_days < 10:
        score += 7

    # Credit (0-10)
    cf = _credit_floor(d.credit_range)
    if cf >= 700:
        score += 10; reasons.append("Strong credit")
    elif cf >= 600:
        score += 6
    elif cf >= 550:
        score += 3

    # Paper grade
    if disqualifiers:
        grade = "DECLINE"
    elif score >= 75 and cf >= 650 and d.nsf_count <= 2:
        grade = "A"
    elif score >= 55:
        grade = "B"
    elif score >= 40:
        grade = "C"
    else:
        grade = "D"

    return {
        "qual_score": round(score, 1),
        "paper_grade": grade,
        "qualified": not disqualifiers,
        "reasons": reasons,
        "disqualifiers": disqualifiers,
    }


@app.post("/api/qualify")
def qualify(d: QualifyInput, db: Session = Depends(get_db)):
    """Self-serve qualification — the 8-question widget backend."""
    result = qualify_deal(d)

    # Estimate amount range and lender matches if qualified
    if result["qualified"]:
        # Typical: advance ~ 1x monthly revenue (range 0.5x-1.5x)
        est_low = round(d.monthly_revenue * 0.5, -3)
        est_high = round(d.monthly_revenue * 1.25, -3)
        result["estimated_amount"] = {"low": est_low, "high": est_high}
        result["estimated_factor"] = {"A": "1.15-1.25", "B": "1.25-1.40", "C": "1.35-1.49", "D": "1.40-1.49"}.get(result["paper_grade"], "1.25-1.40")

        # Match lenders
        matches = match_lenders(d, result["paper_grade"], db)
        result["lender_matches"] = matches[:3]
    return result


# ===========================================================================
# LENDER MATCHING ENGINE
# ===========================================================================

def match_lenders(d: QualifyInput, paper_grade: str, db: Session) -> list[dict]:
    lenders = db.execute(select(Lender).where(Lender.active == True)).scalars().all()
    cf = _credit_floor(d.credit_range)
    industry = (d.industry or "").lower()
    results = []

    for L in lenders:
        score = 0.0
        reasons = []
        grades = L._csv(L.paper_grades)

        # Paper grade fit (0-25) — must be able to fund this grade
        if paper_grade.lower() in [g.lower() for g in grades] or paper_grade == "DECLINE":
            score += 25
        else:
            continue  # lender can't fund this paper grade — skip

        # Industry fit (0-20)
        inds = L._csv(L.industries)
        excl = L._csv(L.excluded_industries)
        if industry and industry in excl:
            continue  # excluded
        if not inds or (industry and industry in inds):
            score += 20
            if inds:
                reasons.append("Industry match")
        else:
            score += 10  # neutral

        # Revenue fit (0-15)
        if d.monthly_revenue >= L.min_revenue:
            score += 15
        else:
            continue  # doesn't meet minimum

        # TIB fit (0-10)
        if d.time_in_business_months >= L.min_tib_months:
            score += 10
        else:
            continue

        # Credit fit (0-10)
        if cf >= L.min_credit:
            score += 10
            reasons.append("Credit qualifies")

        # Amount fit (0-10)
        amt = d.amount_requested or d.monthly_revenue
        if L.min_amount <= amt <= L.max_amount:
            score += 10
        elif amt > L.max_amount:
            score += 3
            reasons.append("Amount near lender max")

        # Speed fit (0-5)
        if d.timeline == "ASAP" and L.funds_same_day:
            score += 5; reasons.append("Same-day funding")
        elif L.funds_same_day:
            score += 3

        # Learned relationship bonus (0-5)
        ar = L.approval_rate()
        score += ar * 5

        est_commission = round((d.amount_requested or d.monthly_revenue) * (L.commission_pct / 100), 0)

        results.append({
            "lender_id": L.id,
            "lender_name": L.name,
            "match_score": round(min(score, 100), 1),
            "commission_pct": L.commission_pct,
            "estimated_commission": est_commission,
            "funds_same_day": L.funds_same_day,
            "approval_rate": ar,
            "reasons": reasons,
            "notes": L.notes,
        })

    results.sort(key=lambda x: x["match_score"], reverse=True)
    return results


# ===========================================================================
# DEALS / PIPELINE
# ===========================================================================

class DealCreate(BaseModel):
    business_name: str
    contact_name: str = ""
    email: str = ""
    phone: str = ""
    website: str = ""
    location: str = ""
    industry: str = ""
    amount_requested: float = 0.0
    use_of_funds: str = ""
    timeline: str = ""
    monthly_revenue: float = 0.0
    time_in_business_months: int = 0
    existing_positions: int = 0
    credit_range: str = ""
    signal_source: str = ""
    signal_type: str = ""
    signal_score: float = 0.0
    notes: str = ""


class StageUpdate(BaseModel):
    stage: str


class DealUpdate(BaseModel):
    contact_name: str | None = None
    email: str | None = None
    phone: str | None = None
    monthly_revenue: float | None = None
    time_in_business_months: int | None = None
    existing_positions: int | None = None
    credit_range: str | None = None
    nsf_count: int | None = None
    negative_days: int | None = None
    avg_daily_balance: float | None = None
    revenue_trend: str | None = None
    amount_requested: float | None = None
    submitted_to: str | None = None
    actual_commission: float | None = None
    notes: str | None = None


@app.post("/api/deals")
def create_deal(d: DealCreate, db: Session = Depends(get_db)):
    deal = Deal(**d.model_dump())
    db.add(deal)
    db.commit()
    return deal.to_dict()


@app.get("/api/deals")
def list_deals(stage: str = "", db: Session = Depends(get_db)):
    stmt = select(Deal).order_by(Deal.created_at.desc())
    if stage:
        stmt = stmt.where(Deal.stage == stage)
    deals = db.execute(stmt).scalars().all()
    return {"count": len(deals), "deals": [x.to_dict() for x in deals]}


@app.get("/api/deals/{deal_id}")
def get_deal(deal_id: str, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(404, "Deal not found")
    return deal.to_dict()


@app.put("/api/deals/{deal_id}")
def update_deal(deal_id: str, payload: DealUpdate, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(404, "Deal not found")
    data = payload.model_dump(exclude_unset=True)
    for k, v in data.items():
        setattr(deal, k, v)
    # Re-qualify if financials changed
    if any(k in data for k in ["monthly_revenue", "time_in_business_months", "existing_positions",
                                "credit_range", "nsf_count", "negative_days", "revenue_trend"]):
        q = qualify_deal(QualifyInput(
            industry=deal.industry, time_in_business_months=deal.time_in_business_months or 0,
            monthly_revenue=deal.monthly_revenue or 0, existing_positions=deal.existing_positions or 0,
            credit_range=deal.credit_range or "", nsf_count=deal.nsf_count or 0,
            negative_days=deal.negative_days or 0, revenue_trend=deal.revenue_trend or "",
            amount_requested=deal.amount_requested or 0, timeline=deal.timeline or "",
        ))
        deal.qual_score = q["qual_score"]
        deal.paper_grade = q["paper_grade"]
        matches = match_lenders(QualifyInput(
            industry=deal.industry, time_in_business_months=deal.time_in_business_months or 0,
            monthly_revenue=deal.monthly_revenue or 0, existing_positions=deal.existing_positions or 0,
            credit_range=deal.credit_range or "", amount_requested=deal.amount_requested or 0,
            timeline=deal.timeline or "",
        ), q["paper_grade"], db)
        deal.matched_lenders = json.dumps(matches[:5])
        if matches:
            deal.estimated_commission = matches[0]["estimated_commission"]
    deal.updated_at = _now()
    db.commit()
    return deal.to_dict()


@app.put("/api/deals/{deal_id}/stage")
def update_stage(deal_id: str, payload: StageUpdate, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(404, "Deal not found")
    if payload.stage not in PIPELINE_STAGES:
        raise HTTPException(400, f"stage must be one of {PIPELINE_STAGES}")
    deal.stage = payload.stage
    now = _now()
    # Timestamp milestones for the outcome guarantee tracker
    if payload.stage == "Contacted" and not deal.contacted_at:
        deal.contacted_at = now
    if payload.stage == "Submitted" and not deal.submitted_at:
        deal.submitted_at = now
    if payload.stage == "Funded" and not deal.funded_at:
        deal.funded_at = now
    deal.updated_at = now
    db.commit()
    return deal.to_dict()


# ===========================================================================
# LENDERS
# ===========================================================================

class LenderInput(BaseModel):
    name: str
    paper_grades: str = "A,B"
    min_revenue: float = 0.0
    min_tib_months: int = 6
    min_credit: int = 500
    max_amount: float = 500000.0
    min_amount: float = 5000.0
    industries: str = ""
    excluded_industries: str = ""
    commission_pct: float = 8.0
    funds_same_day: bool = False
    notes: str = ""


@app.get("/api/lenders")
def list_lenders(db: Session = Depends(get_db)):
    lenders = db.execute(select(Lender).order_by(Lender.name)).scalars().all()
    return {"count": len(lenders), "lenders": [L.to_dict() for L in lenders]}


@app.post("/api/lenders")
def add_lender(d: LenderInput, db: Session = Depends(get_db)):
    L = Lender(**d.model_dump())
    db.add(L)
    db.commit()
    return L.to_dict()


@app.put("/api/lenders/{lender_id}/record-outcome")
def record_lender_outcome(lender_id: str, outcome: str = Query(...), commission: float = 0.0, db: Session = Depends(get_db)):
    """Feed the learning loop: record submitted/approved/funded for a lender."""
    L = db.get(Lender, lender_id)
    if not L:
        raise HTTPException(404, "Lender not found")
    if outcome == "submitted":
        L.submissions = (L.submissions or 0) + 1
    elif outcome == "approved":
        L.approvals = (L.approvals or 0) + 1
    elif outcome == "funded":
        L.fundings = (L.fundings or 0) + 1
        L.total_commission = (L.total_commission or 0) + commission
    db.commit()
    return L.to_dict()


# ===========================================================================
# SIGNAL ENGINE
# ===========================================================================

@app.get("/api/signals/sources")
def signal_sources():
    """List all collectors and their status."""
    return signal_engine.registry_stats()


class RunSignalsInput(BaseModel):
    collectors: list[str] = Field(default_factory=list)   # empty = run all enabled
    states: list[str] = Field(default_factory=lambda: ["MN"])
    cities: list[str] = Field(default_factory=lambda: ["Minneapolis, MN", "St Paul, MN"])
    location: str = "Minneapolis, MN"


@app.post("/api/signals/run")
def run_signals(payload: RunSignalsInput, db: Session = Depends(get_db)):
    """
    Run the signal collectors, score the results, and store them.
    In production this runs on a schedule; this endpoint triggers it manually.
    """
    registry = signal_engine.build_registry()
    if payload.collectors:
        registry = [c for c in registry if c.name in payload.collectors]

    all_signals = []
    errors = []
    ran = []
    kwargs = {"states": payload.states, "cities": payload.cities, "location": payload.location}
    for c in registry:
        if not c.enabled:
            continue
        try:
            sigs = c.collect(**kwargs)
            all_signals.extend(sigs)
            ran.append(c.name)
        except Exception as e:
            errors.append(f"{c.name}: {e}")

    # Score and rank
    known = {d.business_name.lower() for d in db.execute(select(Deal)).scalars().all()}
    funded = {d.business_name.lower() for d in db.execute(select(Deal).where(Deal.stage == "Funded")).scalars().all()}
    ranked = signal_engine.rank_signals(all_signals, known_businesses=known, past_clients=funded)

    # Store (dedup against existing)
    existing_keys = {s.dedup_key for s in db.execute(select(SignalRecord)).scalars().all()}
    stored = 0
    for r in ranked:
        for sig in r["signals"]:
            if sig["dedup_key"] in existing_keys:
                continue
            db.add(SignalRecord(
                business_name=r["business_name"], location=r["location"],
                signal_type=sig["signal_type"], score=r["score"], source=sig["source"],
                source_url=sig.get("source_url", ""), raw_data=json.dumps(sig.get("raw_data", {})),
                contact_hints=json.dumps(r.get("contact_hints", {})), dedup_key=sig["dedup_key"],
            ))
            existing_keys.add(sig["dedup_key"])
            stored += 1
    db.commit()

    return {
        "collectors_run": len(ran),
        "signals_found": len(all_signals),
        "unique_businesses": len(ranked),
        "new_signals_stored": stored,
        "errors": errors,
        "top_signals": ranked[:10],
    }


@app.get("/api/signals/today")
def signals_today(limit: int = 20, db: Session = Depends(get_db)):
    """The 'Act Today' list — top unworked signals by score."""
    stmt = select(SignalRecord).where(SignalRecord.worked == False).order_by(SignalRecord.score.desc()).limit(limit)
    sigs = db.execute(stmt).scalars().all()
    return {"count": len(sigs), "signals": [s.to_dict() for s in sigs]}


@app.post("/api/signals/{signal_id}/convert")
def convert_signal(signal_id: str, db: Session = Depends(get_db)):
    """Turn a signal into a deal in the pipeline."""
    sig = db.get(SignalRecord, signal_id)
    if not sig:
        raise HTTPException(404, "Signal not found")
    try:
        hints = json.loads(sig.contact_hints or "{}")
    except (ValueError, TypeError):
        hints = {}
    deal = Deal(
        business_name=sig.business_name, location=sig.location,
        phone=hints.get("phone", ""), email=hints.get("email", ""), website=hints.get("website", ""),
        signal_source=sig.source, signal_type=sig.signal_type, signal_score=sig.score,
        stage="Signal",
    )
    db.add(deal)
    sig.worked = True
    sig.converted_to_deal = deal.id
    db.commit()
    return deal.to_dict()


@app.put("/api/signals/{signal_id}/dismiss")
def dismiss_signal(signal_id: str, db: Session = Depends(get_db)):
    sig = db.get(SignalRecord, signal_id)
    if not sig:
        raise HTTPException(404, "Signal not found")
    sig.worked = True
    db.commit()
    return {"dismissed": signal_id}


# ===========================================================================
# BANK STATEMENT ANALYZER (Phase 2) — the deal-packaging engine
# ===========================================================================

class BankStatementInput(BaseModel):
    """Accept either raw text (pasted/extracted) or pre-computed metrics."""
    deal_id: str = ""
    # Raw text path: paste bank statement text, Claude structures it
    raw_text: str = ""
    # Manual metrics path: enter numbers directly
    months: list[dict] = Field(default_factory=list)  # [{month, deposits, withdrawals, ending_balance, nsf_count, negative_days}]
    # If neither raw_text nor months, just re-analyze from deal's existing metrics


class MonthMetrics(BaseModel):
    month: str = ""
    total_deposits: float = 0.0
    total_withdrawals: float = 0.0
    ending_balance: float = 0.0
    avg_daily_balance: float = 0.0
    nsf_count: int = 0
    negative_days: int = 0
    largest_deposit: float = 0.0
    deposit_count: int = 0
    identified_mca_debits: float = 0.0  # daily/weekly ACH to known funders


def _claude_parse_bank_statement(raw_text: str) -> list[dict] | None:
    """Use Claude to extract structured monthly metrics from raw bank statement text."""
    if not ANTHROPIC_API_KEY or not raw_text:
        return None
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = (
            "You are a bank statement analyst. Extract monthly financial metrics from this bank statement text.\n\n"
            "For EACH month present, return a JSON array of objects with these exact fields:\n"
            "- month (string, e.g. '2026-01')\n"
            "- total_deposits (float)\n"
            "- total_withdrawals (float)\n"
            "- ending_balance (float)\n"
            "- avg_daily_balance (float, estimate from beginning/ending if not explicit)\n"
            "- nsf_count (int, count of NSF/overdraft fees)\n"
            "- negative_days (int, days balance was below zero)\n"
            "- largest_deposit (float)\n"
            "- deposit_count (int)\n"
            "- identified_mca_debits (float, total of any recurring daily/weekly fixed ACH debits that look like MCA payments)\n\n"
            "Return ONLY the JSON array, no markdown, no explanation.\n\n"
            f"BANK STATEMENT TEXT:\n{raw_text[:8000]}"
        )
        resp = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text").strip()
        text = text.strip("`").strip()
        if text.startswith("json"):
            text = text[4:].strip()
        return json.loads(text)
    except Exception as e:
        print(f"[analyzer] Claude parse failed: {e}")
        return None


def analyze_bank_statements(months: list[dict]) -> dict:
    """
    Compute all underwriting metrics from monthly bank statement data.
    This is what lenders actually look at — the four dimensions.
    """
    if not months:
        return {"error": "No monthly data provided", "metrics": {}}

    deposits = [m.get("total_deposits", 0) for m in months]
    balances = [m.get("avg_daily_balance", 0) or m.get("ending_balance", 0) for m in months]
    nsfs = [m.get("nsf_count", 0) for m in months]
    neg_days = [m.get("negative_days", 0) for m in months]
    mca_debits = [m.get("identified_mca_debits", 0) for m in months]

    avg_monthly_deposits = statistics.mean(deposits) if deposits else 0
    avg_daily_balance = statistics.mean(balances) if balances else 0
    total_nsfs = sum(nsfs)
    avg_nsfs_per_month = round(total_nsfs / len(months), 1) if months else 0
    max_neg_days = max(neg_days) if neg_days else 0
    avg_neg_days = round(sum(neg_days) / len(months), 1) if months else 0
    total_mca_debits = sum(mca_debits)

    # Revenue trend: simple linear direction
    if len(deposits) >= 2:
        first_half = statistics.mean(deposits[:len(deposits)//2])
        second_half = statistics.mean(deposits[len(deposits)//2:])
        if second_half > first_half * 1.05:
            trend = "growing"
        elif second_half < first_half * 0.95:
            trend = "declining"
        else:
            trend = "flat"
    else:
        trend = "insufficient_data"

    # Deposit consistency (coefficient of variation)
    cv = round(statistics.pstdev(deposits) / statistics.mean(deposits), 3) if len(deposits) >= 2 and statistics.mean(deposits) else 0

    # Health ratings
    nsf_rating = "green" if avg_nsfs_per_month < 1 else ("yellow" if avg_nsfs_per_month <= 3 else "red")
    neg_rating = "green" if max_neg_days == 0 else ("yellow" if max_neg_days <= 5 else "red")
    balance_rating = "green" if avg_daily_balance > 5000 else ("yellow" if avg_daily_balance > 1000 else "red")
    trend_rating = "green" if trend == "growing" else ("yellow" if trend == "flat" else "red")

    # Estimated available capacity for new MCA payment
    est_monthly_operating_costs = avg_monthly_deposits * 0.85  # rough: 85% goes to operations
    current_mca_burden = (total_mca_debits / len(months)) if months else 0
    available_monthly = avg_monthly_deposits - est_monthly_operating_costs - current_mca_burden
    estimated_capacity = max(available_monthly * 0.5, 0)  # conservative: only use 50% of available

    return {
        "month_count": len(months),
        "months": months,
        "avg_monthly_deposits": round(avg_monthly_deposits, 0),
        "avg_daily_balance": round(avg_daily_balance, 0),
        "total_nsfs": total_nsfs,
        "avg_nsfs_per_month": avg_nsfs_per_month,
        "max_negative_days_in_month": max_neg_days,
        "avg_negative_days": avg_neg_days,
        "revenue_trend": trend,
        "deposit_consistency_cv": cv,
        "existing_mca_debits_monthly": round(current_mca_burden, 0),
        "estimated_daily_payment_capacity": round(estimated_capacity / 22, 0),  # ~22 business days
        "ratings": {
            "nsf": nsf_rating,
            "negative_days": neg_rating,
            "balance": balance_rating,
            "trend": trend_rating,
            "overall": "green" if all(r == "green" for r in [nsf_rating, neg_rating, balance_rating, trend_rating])
                       else ("red" if any(r == "red" for r in [nsf_rating, neg_rating, trend_rating]) else "yellow"),
        },
    }


@app.post("/api/analyze-statements")
def analyze_statements(d: BankStatementInput, db: Session = Depends(get_db)):
    """
    Analyze bank statements. Accepts raw text (Claude extracts metrics) or
    pre-computed monthly metrics. Returns the full underwriting analysis +
    auto-qualification + lender matches.
    """
    months = d.months

    # Claude extraction path
    if d.raw_text and not months:
        parsed = _claude_parse_bank_statement(d.raw_text)
        if parsed:
            months = parsed
        else:
            return {"error": "Couldn't parse bank statement text. Paste cleaner text or enter metrics manually.",
                    "claude_available": bool(ANTHROPIC_API_KEY)}

    if not months:
        return {"error": "Provide bank statement text (raw_text) or monthly metrics (months)."}

    analysis = analyze_bank_statements(months)

    # Auto-qualify based on the analysis
    deal = db.get(Deal, d.deal_id) if d.deal_id else None
    q = qualify_deal(QualifyInput(
        industry=deal.industry if deal else "",
        time_in_business_months=deal.time_in_business_months if deal else 12,
        monthly_revenue=analysis["avg_monthly_deposits"],
        existing_positions=1 if analysis["existing_mca_debits_monthly"] > 0 else 0,
        credit_range=deal.credit_range if deal else "600-650",
        nsf_count=round(analysis["avg_nsfs_per_month"]),
        negative_days=analysis["max_negative_days_in_month"],
        revenue_trend=analysis["revenue_trend"],
        amount_requested=deal.amount_requested if deal else analysis["avg_monthly_deposits"],
        timeline=deal.timeline if deal else "",
    ))
    analysis["qualification"] = q

    # Match lenders if qualified
    if q["qualified"]:
        matches = match_lenders(QualifyInput(
            industry=deal.industry if deal else "",
            time_in_business_months=deal.time_in_business_months if deal else 12,
            monthly_revenue=analysis["avg_monthly_deposits"],
            existing_positions=1 if analysis["existing_mca_debits_monthly"] > 0 else 0,
            credit_range=deal.credit_range if deal else "600-650",
            amount_requested=deal.amount_requested if deal else analysis["avg_monthly_deposits"],
            timeline=deal.timeline if deal else "",
        ), q["paper_grade"], db)
        analysis["lender_matches"] = matches[:3]

    # Update the deal if linked
    if deal:
        deal.monthly_revenue = analysis["avg_monthly_deposits"]
        deal.nsf_count = round(analysis["avg_nsfs_per_month"])
        deal.negative_days = analysis["max_negative_days_in_month"]
        deal.avg_daily_balance = analysis["avg_daily_balance"]
        deal.revenue_trend = analysis["revenue_trend"]
        deal.qual_score = q["qual_score"]
        deal.paper_grade = q["paper_grade"]
        deal.matched_lenders = json.dumps(analysis.get("lender_matches", []))
        if analysis.get("lender_matches"):
            deal.estimated_commission = analysis["lender_matches"][0]["estimated_commission"]
        deal.updated_at = _now()
        db.commit()
        analysis["deal_updated"] = deal.id

    return analysis


# ===========================================================================
# REFERRAL PARTNERS
# ===========================================================================

class PartnerInput(BaseModel):
    name: str
    partner_type: str = "accountant"
    email: str = ""
    phone: str = ""
    split_pct: float = 25.0
    notes: str = ""


@app.get("/api/partners")
def list_partners(db: Session = Depends(get_db)):
    partners = db.execute(select(ReferralPartner).order_by(ReferralPartner.name)).scalars().all()
    return {"count": len(partners), "partners": [p.to_dict() for p in partners]}


@app.post("/api/partners")
def add_partner(d: PartnerInput, db: Session = Depends(get_db)):
    p = ReferralPartner(**d.model_dump())
    db.add(p)
    db.commit()
    return p.to_dict()


@app.put("/api/partners/{partner_id}/record-referral")
def record_referral(partner_id: str, funded: bool = False, commission: float = 0.0, db: Session = Depends(get_db)):
    p = db.get(ReferralPartner, partner_id)
    if not p:
        raise HTTPException(404, "Partner not found")
    p.referrals_sent = (p.referrals_sent or 0) + 1
    if funded:
        p.referrals_funded = (p.referrals_funded or 0) + 1
        split = round(commission * (p.split_pct / 100), 2)
        p.total_paid = (p.total_paid or 0) + split
    p.last_contact = _now()
    db.commit()
    return p.to_dict()


# ===========================================================================
# COMMISSION TRACKER
# ===========================================================================

@app.get("/api/commissions")
def commission_tracker(db: Session = Depends(get_db)):
    """Full commission breakdown: earned, pending, projected renewals."""
    deals = db.execute(select(Deal)).scalars().all()
    earned = sum(d.actual_commission or 0 for d in deals if d.stage == "Funded")
    pending_deals = [d for d in deals if d.stage in ("Submitted", "Approved")]
    pending = sum(d.estimated_commission or 0 for d in pending_deals)

    # Revenue by lender
    by_lender = {}
    for d in deals:
        if d.stage == "Funded" and d.submitted_to:
            by_lender[d.submitted_to] = by_lender.get(d.submitted_to, 0) + (d.actual_commission or 0)

    # Monthly trend
    monthly = {}
    for d in deals:
        if d.stage == "Funded" and d.funded_at:
            key = d.funded_at.strftime("%Y-%m")
            monthly[key] = monthly.get(key, 0) + (d.actual_commission or 0)

    # Partner splits owed
    partners = db.execute(select(ReferralPartner)).scalars().all()
    partner_splits = [{"name": p.name, "referrals_funded": p.referrals_funded or 0,
                       "total_paid": p.total_paid or 0, "split_pct": p.split_pct} for p in partners if p.referrals_funded]

    funded_deals = [d for d in deals if d.stage == "Funded"]
    ttfs = [(d.funded_at.date() - d.contacted_at.date()).days for d in funded_deals if d.funded_at and d.contacted_at]

    return {
        "earned": round(earned, 2),
        "pending": round(pending, 2),
        "total_deals_funded": len(funded_deals),
        "avg_commission": round(earned / len(funded_deals), 2) if funded_deals else 0,
        "avg_time_to_fund": round(statistics.mean(ttfs), 1) if ttfs else None,
        "by_lender": by_lender,
        "monthly": monthly,
        "partner_splits": partner_splits,
        "pending_deals": [{"id": d.id, "business": d.business_name, "estimated": d.estimated_commission,
                           "submitted_to": d.submitted_to, "stage": d.stage} for d in pending_deals],
    }


# ===========================================================================
# FOLLOW-UP ENGINE
# ===========================================================================

FOLLOWUP_CADENCE = [1, 3, 7, 14, 30]


@app.post("/api/deals/{deal_id}/schedule-followup")
def schedule_followup(deal_id: str, days: int = 0, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(404, "Deal not found")
    if days <= 0:
        # Auto-cadence based on stage
        stage_defaults = {"Signal": 1, "Contacted": 3, "Qualifying": 2, "Submitted": 1, "Approved": 1}
        days = stage_defaults.get(deal.stage, 3)
    deal.next_follow_up = _now() + timedelta(days=days)
    deal.updated_at = _now()
    db.commit()
    return deal.to_dict()


@app.get("/api/followups/due")
def followups_due(db: Session = Depends(get_db)):
    now = _now()
    deals = db.execute(select(Deal).where(Deal.next_follow_up.isnot(None)).where(
        Deal.stage.notin_(["Funded", "Lost"])
    )).scalars().all()
    due = []
    for d in deals:
        fu = d.next_follow_up
        if fu.tzinfo is None:
            fu = fu.replace(tzinfo=timezone.utc)
        days = (fu.date() - now.date()).days
        if days <= 1:  # due today or overdue
            row = d.to_dict()
            row["overdue_days"] = abs(days) if days < 0 else 0
            due.append(row)
    due.sort(key=lambda x: x.get("overdue_days", 0), reverse=True)
    return {"count": len(due), "deals": due}


# ===========================================================================
# OUTREACH DRAFTING (drafts only; you send from your own mail app)
# ===========================================================================

import urllib.parse as _url

OUTREACH_TEMPLATES = {
    "gov_contract_award": {
        "subject": "Congrats on the contract — quick question on working capital",
        "body": "Hi {contact},\n\nI saw {business} recently landed a government contract — congratulations. "
                "Fulfilling a new contract often means fronting payroll and materials before you get paid.\n\n"
                "I work with lenders who advance working capital against receivables — no equity, fast funding, "
                "flexible repayment. If a capital cushion would help you deliver on this contract smoothly, "
                "I'd be glad to walk you through options. No obligation.\n\n{signoff}",
    },
    "hiring_surge": {
        "subject": "Scaling up at {business}?",
        "body": "Hi {contact},\n\nNoticed {business} is hiring — growth is exciting but it stretches cash flow "
                "(payroll hits before new revenue does).\n\nI help businesses get non-dilutive working capital to "
                "bridge growth phases — fast, flexible, no equity. Worth a quick chat if runway would help?\n\n{signoff}",
    },
    "default": {
        "subject": "Working capital for {business}",
        "body": "Hi {contact},\n\nI work with lenders who provide fast, flexible working capital to growing "
                "businesses — no equity, repayment that flexes with your revenue. If you ever need capital "
                "between milestones, I'd be glad to help you find the right fit. No obligation.\n\n{signoff}",
    },
}


class OutreachInput(BaseModel):
    deal_id: str = ""
    signal_type: str = ""
    business_name: str = ""
    contact_name: str = ""
    email: str = ""
    your_name: str = ""
    your_phone: str = ""
    your_email: str = ""


@app.post("/api/outreach/draft")
def draft_outreach(d: OutreachInput, db: Session = Depends(get_db)):
    business = d.business_name
    contact = d.contact_name or "there"
    email = d.email
    sig_type = d.signal_type

    if d.deal_id:
        deal = db.get(Deal, d.deal_id)
        if deal:
            business = business or deal.business_name
            contact = deal.contact_name or contact
            email = email or deal.email
            sig_type = sig_type or deal.signal_type

    tpl = OUTREACH_TEMPLATES.get(sig_type, OUTREACH_TEMPLATES["default"])
    signoff = f"{d.your_name or '[Your name]'}\n{d.your_phone or '[phone]'}\n{d.your_email or '[email]'}"
    subject = tpl["subject"].format(business=business or "your business", contact=contact)
    body = tpl["body"].format(business=business or "your business", contact=contact, signoff=signoff)

    mailto = ""
    if email:
        mailto = f"mailto:{email}?" + _url.urlencode({"subject": subject, "body": body}, quote_via=_url.quote)

    return {"subject": subject, "body": body, "mailto": mailto, "has_email": bool(email)}


# ===========================================================================
# DASHBOARD STATS
# ===========================================================================

@app.get("/api/stats")
def stats(db: Session = Depends(get_db)):
    total_deals = db.execute(select(func.count(Deal.id))).scalar() or 0
    by_stage = {}
    for s in PIPELINE_STAGES:
        by_stage[s] = db.execute(select(func.count(Deal.id)).where(Deal.stage == s)).scalar() or 0

    funded = db.execute(select(Deal).where(Deal.stage == "Funded")).scalars().all()
    total_commission = sum(d.actual_commission or 0 for d in funded)
    pending = db.execute(select(Deal).where(Deal.stage.in_(["Submitted", "Approved"]))).scalars().all()
    pending_commission = sum(d.estimated_commission or 0 for d in pending)

    # Time to fund
    ttfs = [(d.funded_at.date() - d.contacted_at.date()).days for d in funded if d.funded_at and d.contacted_at]
    avg_ttf = round(statistics.mean(ttfs), 1) if ttfs else None

    unworked_signals = db.execute(select(func.count(SignalRecord.id)).where(SignalRecord.worked == False)).scalar() or 0

    return {
        "total_deals": total_deals,
        "pipeline": by_stage,
        "funded_count": len(funded),
        "total_commission": total_commission,
        "pending_commission": pending_commission,
        "avg_time_to_fund_days": avg_ttf,
        "unworked_signals": unworked_signals,
        "total_lenders": db.execute(select(func.count(Lender.id))).scalar() or 0,
    }


# ===========================================================================
# DASHBOARD STATIC
# ===========================================================================
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "dashboard")
if os.path.isdir(DASHBOARD_DIR):
    @app.get("/")
    def root():
        return RedirectResponse("/dashboard/")
    app.mount("/dashboard", StaticFiles(directory=DASHBOARD_DIR, html=True), name="dashboard")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
