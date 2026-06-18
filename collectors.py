"""
collectors.py — NEXUS Signal Engine
50+ data collectors that detect business capital-need signals.

Each collector is a small class with a standard interface:
  .collect() -> list[Signal]

The registry handles scheduling, dedup, and feeds into the scoring engine.
All collectors degrade gracefully: missing API keys disable, network errors skip.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus, urlencode

import httpx

# Make config.py importable regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from config import TARGET_STATES, TARGET_CITIES, TARGET_INDUSTRIES, cities_as_list
    # Limit render-heavy scrapers to the top 2 states to conserve Firecrawl credits
    RENDER_STATES = TARGET_STATES[:2]
except Exception:
    TARGET_STATES = ["MN", "WI", "IA", "SD", "ND"]
    TARGET_CITIES = ["Minneapolis,MN", "St Paul,MN", "Milwaukee,WI"]
    TARGET_INDUSTRIES = []
    RENDER_STATES = ["MN", "WI"]
    def cities_as_list():
        return ["Minneapolis, MN", "St Paul, MN", "Milwaukee, WI"]

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HTTP_TIMEOUT = 8.0
SCRAPE_DELAY = 1.5  # seconds between scrape requests (courtesy)
USER_AGENT = "NexusSignalEngine/1.0 (deal-intelligence)"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _log(src: str, msg: str):
    print(f"[{_now().isoformat()[:19]}] [{src}] {msg}")


def _extract_contact_hints(text: str, url: str = "") -> dict:
    """Pull phone, email, website from page text/markdown."""
    hints = {}
    if not text:
        return hints
    phones = re.findall(r'\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}', text)
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    websites = re.findall(r'https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s"\'<>\)]*)?', text)
    # Filter out common junk domains
    junk = ("indeed.com", "craigslist.org", "bizbuysell.com", "google.com", "facebook.com",
            "twitter.com", "linkedin.com", "schema.org", "w3.org", "gstatic.com",
            "googleapis.com", "cloudflare", "firecrawl")
    if phones:
        hints["phone"] = phones[0]
    if emails:
        clean = [e for e in emails if not any(j in e.lower() for j in junk)]
        if clean:
            hints["email"] = clean[0]
    if websites:
        clean = [w for w in websites if not any(j in w.lower() for j in junk)]
        if clean:
            hints["website"] = clean[0]
    return hints


class _ScrapeResult:
    """
    Lightweight stand-in for an httpx.Response so collectors that read `.text`
    keep working when the page comes from Firecrawl (markdown + html).
    Exposes .text (combined), .markdown, .html, and a no-op .json().
    """
    def __init__(self, text: str = "", markdown: str = "", html: str = "", status_code: int = 200):
        self.text = text
        self.markdown = markdown
        self.html = html
        self.status_code = status_code

    def json(self):
        try:
            return json.loads(self.text)
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# Signal data class
# ---------------------------------------------------------------------------
@dataclass
class Signal:
    """One detected intent signal from any source."""
    business_name: str
    location: str = ""                  # city, state or address
    signal_type: str = ""               # e.g. "hiring_surge", "gov_contract_award"
    strength: float = 0.0               # 0-40 base strength
    source: str = ""                    # collector name
    source_url: str = ""                # link to the raw data
    raw_data: dict = field(default_factory=dict)
    contact_hints: dict = field(default_factory=dict)  # phone, email, website, linkedin
    timestamp: datetime = field(default_factory=_now)
    dedup_key: str = ""                 # auto-generated if empty
    # RBF/MCA deal-sourcing fields
    revenue_estimate: str = ""          # "$50K-$100K/mo", "est. $2M/yr"
    urgency: str = ""                   # growth | cashflow_gap | debt_refi | expansion
    contact_availability: str = ""      # phone+email | linkedin_only | form_only | research_required
    data_source_quality: str = ""       # public_record | api | scraped | estimated | stub

    def __post_init__(self):
        if not self.dedup_key:
            raw = f"{self.business_name}|{self.signal_type}|{self.source}".lower()
            self.dedup_key = hashlib.md5(raw.encode()).hexdigest()[:16]
        # Auto-derive contact_availability from contact_hints if not set
        if not self.contact_availability:
            hints = self.contact_hints or {}
            has_phone = bool(hints.get("phone"))
            has_email = bool(hints.get("email"))
            has_li = bool(hints.get("linkedin"))
            has_web = bool(hints.get("website"))
            if has_phone and has_email:
                self.contact_availability = "phone+email"
            elif has_phone:
                self.contact_availability = "phone_only"
            elif has_email:
                self.contact_availability = "email_only"
            elif has_li:
                self.contact_availability = "linkedin_only"
            elif has_web:
                self.contact_availability = "website_only"
            else:
                self.contact_availability = "research_required"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


# ---------------------------------------------------------------------------
# Base collector
# ---------------------------------------------------------------------------
class Collector:
    """Base class for all signal collectors."""
    name: str = "base"
    description: str = ""
    source_type: str = "api"            # api | scrape | bulk | manual
    schedule: str = "daily"             # daily | weekly | monthly
    requires_key: str = ""              # env var name, or "" if no key needed
    market: str = "rbf"                 # rbf | micro_acq | notes | wholesale | all
    tier: str = "B"                     # A (strongest) | B (moderate) | C (contextual)
    default_strength: float = 20.0
    enabled: bool = True

    def __init__(self):
        if self.requires_key and not os.getenv(self.requires_key, "").strip():
            self.enabled = False

    def _get(self, url: str, params: dict = None, headers: dict = None) -> httpx.Response | None:
        hdrs = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if headers:
            hdrs.update(headers)
        try:
            with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True) as c:
                resp = c.get(url, params=params, headers=hdrs)
            if resp.status_code != 200:
                _log(self.name, f"HTTP {resp.status_code} from {url}")
                return None
            return resp
        except Exception as e:
            _log(self.name, f"request failed: {e}")
            return None

    def _scrape_get(self, url: str, headers: dict = None, render: bool = False):
        """
        Fetch a page through the best available method:
          1. Firecrawl (if FIRECRAWL_API_KEY set) — returns clean markdown + html,
             handles JS rendering, proxies, and anti-bot automatically.
          2. Scrape.do (if SCRAPEDO_API_KEY set) — proxy/anti-ban, returns html.
          3. Direct request (fallback) — works only on unprotected sites.

        Returns an object with a `.text` attribute (the page content) so existing
        collectors keep working. With Firecrawl, `.text` is markdown+html combined
        and `.markdown` holds clean markdown for easier parsing.
        """
        time.sleep(SCRAPE_DELAY)

        firecrawl_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
        scrapedo_key = os.getenv("SCRAPEDO_API_KEY", "").strip()

        # ---- Option 1: Firecrawl ----
        if firecrawl_key:
            try:
                body = {
                    "url": url,
                    "formats": ["markdown", "html"],
                    "onlyMainContent": False,
                    "location": {"country": "US", "languages": ["en-US"]},
                    "timeout": 45000,
                    "blockAds": True,
                    "proxy": "auto",
                }
                if render:
                    body["waitFor"] = 2500
                with httpx.Client(timeout=60.0) as c:
                    r = c.post("https://api.firecrawl.dev/v2/scrape",
                               json=body,
                               headers={"Authorization": f"Bearer {firecrawl_key}",
                                        "Content-Type": "application/json"})
                if r.status_code != 200:
                    _log(self.name, f"firecrawl HTTP {r.status_code} for {url[:60]}")
                    return None
                data = (r.json() or {}).get("data", {})
                md = data.get("markdown", "") or ""
                html = data.get("html", "") or ""
                return _ScrapeResult(text=(md + "\n" + html), markdown=md, html=html)
            except Exception as e:
                _log(self.name, f"firecrawl failed: {e}")
                return None

        # ---- Option 2: Scrape.do ----
        if scrapedo_key:
            try:
                params = {"token": scrapedo_key, "url": url, "geoCode": "us"}
                if render:
                    params["render"] = "true"
                with httpx.Client(timeout=HTTP_TIMEOUT * 3, follow_redirects=True) as c:
                    resp = c.get("https://api.scrape.do", params=params)
                if resp.status_code != 200:
                    _log(self.name, f"scrape.do HTTP {resp.status_code} for {url[:60]}")
                    return None
                return resp
            except Exception as e:
                _log(self.name, f"scrape.do failed: {e}")
                return None

        # ---- Option 3: Direct (fallback) ----
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if headers:
            hdrs.update(headers)
        try:
            with httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True) as c:
                resp = c.get(url, headers=hdrs)
            return resp if resp.status_code == 200 else None
        except Exception as e:
            _log(self.name, f"scrape failed: {e}")
            return None

    def collect(self, **kwargs) -> list[Signal]:
        """Override in subclass. Returns signals found in this run."""
        return []

    def status(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "source_type": self.source_type,
            "schedule": self.schedule,
            "market": self.market,
            "tier": self.tier,
            "enabled": self.enabled,
            "requires_key": self.requires_key or None,
        }


# ===========================================================================
# TIER A — Strongest intent signals (high conversion probability)
# ===========================================================================

# ---- 1. SAM.gov Government Contract Awards ----
class SamGovContracts(Collector):
    name = "sam_gov_contracts"
    description = "Federal contract awards — businesses winning contracts often need capital to fulfill them"
    source_type = "api"
    schedule = "daily"
    requires_key = "SAM_GOV_API_KEY"
    tier = "A"
    default_strength = 35.0

    def collect(self, **kwargs) -> list[Signal]:
        key = os.getenv("SAM_GOV_API_KEY", "").strip()
        if not key:
            return []
        # Search recent awards in target states
        states = kwargs.get("states", ["MN", "WI", "IA", "ND", "SD"])
        signals = []
        posted_from = (_now() - timedelta(days=7)).strftime("%m/%d/%Y")
        posted_to = _now().strftime("%m/%d/%Y")
        for state in states[:3]:  # limit to avoid rate cap (10/day on free tier)
            resp = self._get("https://api.sam.gov/prod/opportunities/v2/search", params={
                "api_key": key, "limit": 25, "offset": 0,
                "postedFrom": posted_from, "postedTo": posted_to,
                "placeOfPerformanceStateCode": state,
                "ptype": "a",  # award notices
            })
            if not resp:
                continue
            data = resp.json()
            for opp in (data.get("opportunitiesData") or data.get("opportunities") or []):
                title = opp.get("title", "")
                org = opp.get("organizationName") or opp.get("department", "")
                signals.append(Signal(
                    business_name=title[:120],
                    location=state,
                    signal_type="gov_contract_award",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=f"https://sam.gov/opp/{opp.get('noticeId', '')}",
                    raw_data={"title": title, "agency": org, "type": opp.get("type", "")},
                ))
        _log(self.name, f"found {len(signals)} contract signals")
        return signals


# ---- 2. USASpending.gov Recent Awards ----
class USASpendingAwards(Collector):
    name = "usaspending_awards"
    description = "Federal spending data — recently awarded contracts to small businesses"
    source_type = "api"
    schedule = "weekly"
    tier = "A"
    default_strength = 33.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # USASpending API is free, no key needed — POST endpoint
        try:
            with httpx.Client(timeout=HTTP_TIMEOUT) as c:
                body = {
                    "filters": {
                        "time_period": [{"start_date": (_now() - timedelta(days=14)).strftime("%Y-%m-%d"),
                                         "end_date": _now().strftime("%Y-%m-%d")}],
                        "recipient_locations": [{"country": "USA", "state": st} for st in kwargs.get("states", ["MN"])],
                    },
                    "fields": ["Award ID", "Recipient Name", "Award Amount", "Description", "Place of Performance State Code"],
                    "limit": 50, "page": 1, "sort": "Award Amount", "order": "desc",
                }
                r = c.post("https://api.usaspending.gov/api/v2/search/spending_by_award/",
                           json=body, headers={"Content-Type": "application/json"}, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                for row in (r.json().get("results") or []):
                    name = row.get("Recipient Name", "")
                    amt = row.get("Award Amount", 0)
                    if name and amt and float(amt) > 50000:
                        signals.append(Signal(
                            business_name=name,
                            location=row.get("Place of Performance State Code", ""),
                            signal_type="fed_award_received",
                            strength=self.default_strength,
                            source=self.name,
                            raw_data={"amount": amt, "description": row.get("Description", "")[:200]},
                        ))
        except Exception as e:
            _log(self.name, f"error: {e}")
        _log(self.name, f"found {len(signals)} award signals")
        return signals


# ---- 3. Indeed Job Postings (scrape) ----
class IndeedJobs(Collector):
    name = "indeed_jobs"
    description = "Companies posting 3+ jobs simultaneously are growing fast and probably cash-constrained"
    source_type = "scrape"
    schedule = "daily"
    tier = "A"
    default_strength = 30.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        cities = cities_as_list()[:2]  # conserve credits
        for city in cities:
            q = quote_plus("hiring multiple positions")
            loc = quote_plus(city)
            resp = self._scrape_get(f"https://www.indeed.com/jobs?q={q}&l={loc}&sort=date&limit=25", render=True)
            if not resp:
                _log(self.name, "Indeed blocked — requires Indeed Publisher API or headless browser")
                continue
            text = getattr(resp, "text", "") or ""
            # Primary: data-testid company name
            companies = re.findall(r'data-testid="company-name"[^>]*>([^<]+)<', text)
            # Fallback 1: companyName JSON field
            if not companies:
                companies = re.findall(r'"companyName"\s*:\s*"([^"]+)"', text)
            # Fallback 2: JSON-LD JobPosting hiringOrganization
            if not companies:
                companies = re.findall(r'"hiringOrganization"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"', text)
            counts: dict[str, int] = {}
            for co in companies:
                co = co.strip()
                if co:
                    counts[co] = counts.get(co, 0) + 1
            for co, cnt in counts.items():
                if cnt >= 2:
                    hints = _extract_contact_hints(text)
                    signals.append(Signal(
                        business_name=co,
                        location=city,
                        signal_type="hiring_surge",
                        strength=self.default_strength + min(cnt * 2, 10),
                        source=self.name,
                        source_url=f"https://www.indeed.com/cmp/{quote_plus(co)}/jobs",
                        urgency="growth",
                        data_source_quality="scraped",
                        contact_hints=hints,
                        raw_data={"job_count": cnt, "city": city},
                    ))
        _log(self.name, f"found {len(signals)} Indeed hiring signals")
        return signals


# ---- 4. SBA Loan Data (bulk) ----
class SBALoanData(Collector):
    name = "sba_loan_data"
    description = "SBA 7(a) and 504 loan data — businesses that got SBA loans may need additional working capital"
    source_type = "bulk"
    schedule = "monthly"
    tier = "A"
    default_strength = 28.0

    def collect(self, **kwargs) -> list[Signal]:
        # SBA publishes bulk CSV data at data.sba.gov — too large for runtime scraping
        # This collector is a stub that would process pre-downloaded SBA data
        _log(self.name, "SBA bulk data requires pre-download from data.sba.gov/dataset/ — stub only")
        return []


# ---- 5. SEC EDGAR New Filings ----
class SECEdgarFilings(Collector):
    name = "sec_edgar_filings"
    description = "Recent SEC filings — 8-K events, new registrations signal business activity"
    source_type = "api"
    schedule = "daily"
    tier = "A"
    default_strength = 25.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # EDGAR full-text search is free, no key needed, 10 req/sec
        # Search for recent 8-K filings (material events) from small companies
        resp = self._get("https://efts.sec.gov/LATEST/search-index", params={
            "q": "working capital",
            "dateRange": "custom",
            "startdt": (_now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "enddt": _now().strftime("%Y-%m-%d"),
            "forms": "8-K",
        }, headers={"User-Agent": "Nexus research@nexus.dev"})
        if not resp:
            # Try the EDGAR full-text search endpoint
            resp = self._get("https://efts.sec.gov/LATEST/search-index", params={
                "q": "financing", "forms": "8-K",
            }, headers={"User-Agent": "Nexus research@nexus.dev"})
        if resp:
            try:
                data = resp.json()
                for hit in (data.get("hits", {}).get("hits", []) or [])[:20]:
                    src = hit.get("_source", {})
                    signals.append(Signal(
                        business_name=src.get("display_names", [""])[0] if src.get("display_names") else src.get("entity_name", ""),
                        signal_type="sec_filing_event",
                        strength=self.default_strength,
                        source=self.name,
                        source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={src.get('entity_id', '')}",
                        raw_data={"form_type": src.get("form_type", ""), "file_date": src.get("file_date", "")},
                    ))
            except Exception:
                pass
        _log(self.name, f"found {len(signals)} SEC signals")
        return signals


# ===========================================================================
# TIER B — Moderate intent signals
# ===========================================================================

# ---- 8. Minnesota Secretary of State — New Business Filings ----
class MNSosNewBusinesses(Collector):
    name = "mn_sos_new_businesses"
    description = "New business registrations in MN — fresh businesses often need startup capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # MN SOS business search: https://mblsportal.sos.state.mn.us/Business/Search
        resp = self._scrape_get("https://mblsportal.sos.state.mn.us/Business/Search")
        if not resp:
            _log(self.name, "MN SOS portal not reachable — stub only")
            return signals
        # The MN SOS portal requires form-based interaction; this would need
        # a headless browser (Playwright) for full implementation.
        # For now, log and return empty.
        _log(self.name, "MN SOS requires headless browser — planned for v0.2")
        return signals


# ---- 9. UCC Filings — existing MCA positions / renewal windows ----
class MNSosUCCFilings(Collector):
    name = "ucc_filings"
    description = "UCC-1/UCC-3 filings — businesses with existing MCA positions (renewal/refi window)"
    source_type = "scrape"
    schedule = "weekly"
    tier = "A"
    default_strength = 38.0

    UCC_PORTALS = {
        "MN": "https://mblsportal.sos.state.mn.us/UCC/Search",
        "WI": "https://www.wdfi.org/apps/ucc/",
    }

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        for st in RENDER_STATES:
            url = self.UCC_PORTALS.get(st)
            if not url:
                continue
            resp = self._scrape_get(url, render=True)
            if not resp:
                _log(self.name, f"UCC data available at {url} — automation blocked, search manually")
                continue
            text = getattr(resp, "text", "") or ""
            # Look for debtor/secured-party pairs in the rendered content
            # Filings list a debtor (the business) and secured party (the lender they owe)
            debtors = re.findall(r'(?:Debtor|Borrower)[:\s]+([A-Z][A-Za-z0-9&.,\' ]{3,50})', text)
            secured = re.findall(r'(?:Secured Party|Lender)[:\s]+([A-Z][A-Za-z0-9&.,\' ]{3,50})', text)
            dates = re.findall(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', text)
            if not debtors:
                _log(self.name, f"{st}: UCC portal reachable but no parseable filings — search {url}")
                continue
            for i, deb in enumerate(debtors[:15]):
                deb = deb.strip()
                sp = secured[i].strip() if i < len(secured) else ""
                fdate = dates[i] if i < len(dates) else ""
                # Recency → new vs existing
                is_new = True
                try:
                    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y"):
                        try:
                            d = datetime.strptime(fdate, fmt)
                            is_new = (datetime.now() - d).days < 90
                            break
                        except ValueError:
                            continue
                except Exception:
                    pass
                hints = _extract_contact_hints(text)
                signals.append(Signal(
                    business_name=deb,
                    location=st,
                    signal_type="ucc_filing_new" if is_new else "ucc_filing_existing",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=url,
                    urgency="debt_refi",
                    data_source_quality="scraped",
                    contact_hints=hints,
                    contact_availability="research_required" if not hints else "",
                    raw_data={"secured_party": sp, "filing_date": fdate, "state": st},
                ))
        _log(self.name, f"found {len(signals)} UCC signals")
        return signals


# ---- 10. Wisconsin SOS — Business Filings ----
class WISosBusinesses(Collector):
    name = "wi_sos_businesses"
    description = "New WI business filings — neighboring state deal flow"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 16.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "WI SOS search requires headless browser — planned for v0.2")
        return []


# ---- 11. Iowa SOS — Business Filings ----
class IASosBusinesses(Collector):
    name = "ia_sos_businesses"
    description = "New IA business filings — neighboring state deal flow"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 16.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "IA SOS search requires headless browser — planned for v0.2")
        return []


# ---- 12. North Dakota SOS ----
class NDSosBusinesses(Collector):
    name = "nd_sos_businesses"
    description = "New ND business filings"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "ND SOS planned for v0.2")
        return []


# ---- 13. South Dakota SOS ----
class SDSosBusinesses(Collector):
    name = "sd_sos_businesses"
    description = "New SD business filings"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "SD SOS planned for v0.2")
        return []


# ---- 14. OpenCorporates — Company Data ----
class OpenCorporatesSearch(Collector):
    name = "opencorporates"
    description = "Global company registry — cross-reference and enrich business data"
    source_type = "api"
    schedule = "weekly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # OpenCorporates free API (limited, but usable for enrichment)
        query = kwargs.get("query", "")
        state = kwargs.get("state", "mn")
        if not query:
            return signals
        resp = self._get(f"https://api.opencorporates.com/v0.4/companies/search", params={
            "q": query, "jurisdiction_code": f"us_{state}", "per_page": 10,
        })
        if resp:
            data = resp.json()
            for co in (data.get("results", {}).get("companies") or []):
                c = co.get("company", {})
                signals.append(Signal(
                    business_name=c.get("name", ""),
                    location=c.get("registered_address_in_full", ""),
                    signal_type="company_registry",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=c.get("opencorporates_url", ""),
                    raw_data={"status": c.get("current_status"), "type": c.get("company_type"),
                              "incorporation_date": c.get("incorporation_date")},
                ))
        _log(self.name, f"found {len(signals)} OpenCorporates signals")
        return signals


# ---- 15. BizBuySell Listings ----
class BizBuySellListings(Collector):
    name = "bizbuysell"
    description = "Businesses for sale — owners ready to exit, may need bridge capital to close"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "B"
    default_strength = 32.0

    STATE_URLS = {
        "MN": "https://www.bizbuysell.com/minnesota-businesses-for-sale/",
        "WI": "https://www.bizbuysell.com/wisconsin-businesses-for-sale/",
        "IA": "https://www.bizbuysell.com/iowa-businesses-for-sale/",
    }

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        for st in RENDER_STATES:
            url = self.STATE_URLS.get(st)
            if not url:
                continue
            resp = None
            for attempt in range(2):
                resp = self._scrape_get(url, render=True)
                if resp:
                    break
            if not resp:
                _log(self.name, f"{st}: blocked after retries — browse manually at {url}")
                continue
            text = getattr(resp, "text", "") or ""
            md = getattr(resp, "markdown", "") or text

            # In markdown, listings often appear as headers/links with prices nearby
            titles = re.findall(r'(?:^|\n)#{1,4}\s*([A-Z][^\n#]{8,70})', md)
            if not titles:
                titles = re.findall(r'\[([A-Z][^\]]{8,70})\]\(', md)  # markdown links
            if not titles:
                titles = re.findall(r'class="listing-title[^"]*"[^>]*>([^<]+)<', text)

            prices = re.findall(r'(?:Asking Price|Price)[:\s]*\$([0-9,]+)', text)
            revenues = re.findall(r'(?:Revenue|Gross Revenue)[:\s]*\$([0-9,]+)', text)
            cashflows = re.findall(r'(?:Cash Flow|Cash flow)[:\s]*\$([0-9,]+)', text)

            count = 0
            seen = set()
            for i, title in enumerate(titles[:25]):
                t = title.strip()
                tl = t.lower()
                if not t or t in seen:
                    continue
                if any(skip in tl for skip in ["businesses for sale", "search", "filter", "sign in",
                                                "create account", "saved", "franchise opportunit"]):
                    continue
                seen.add(t)
                price = prices[i] if i < len(prices) else ""
                rev = revenues[i] if i < len(revenues) else ""
                cf = cashflows[i] if i < len(cashflows) else ""
                hints = _extract_contact_hints(text)
                rev_est = f"${rev}/yr" if rev else ""
                signals.append(Signal(
                    business_name=t,
                    location=st,
                    signal_type="business_for_sale",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=url,
                    urgency="expansion",
                    revenue_estimate=rev_est,
                    data_source_quality="scraped" if (price or rev) else "partial",
                    contact_hints=hints,
                    raw_data={"asking_price": price, "revenue": rev, "cash_flow": cf, "state": st},
                ))
                count += 1
            if count < 3:
                _log(self.name, f"{st}: only {count} parsed — listings page may need selector update ({url})")
        _log(self.name, f"found {len(signals)} BizBuySell signals")
        return signals


# ---- 16. Craigslist Business-for-Sale ----
class CraigslistBFS(Collector):
    name = "craigslist_bfs"
    description = "Craigslist business-for-sale — informal sellers, often motivated, contact in body"
    source_type = "scrape"
    schedule = "daily"
    market = "micro_acq"
    tier = "B"
    default_strength = 24.0

    CITIES = {"MN": "minneapolis", "WI": "milwaukee"}

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        for st in RENDER_STATES:
            city = self.CITIES.get(st)
            if not city:
                continue
            url = f"https://{city}.craigslist.org/search/bfs"
            resp = None
            for attempt in range(2):
                resp = self._scrape_get(url, render=True)
                if resp:
                    break
            if not resp:
                _log(self.name, f"{city}: blocked after retry — {url}")
                continue
            text = getattr(resp, "text", "") or ""
            md = getattr(resp, "markdown", "") or text
            # Markdown listings: [Title](url) $price
            items = re.findall(r'\[([^\]]{8,80})\]\((https?://[^\)]+)\)', md)
            seen = set()
            for title, link in items[:20]:
                t = title.strip()
                if not t or t in seen or t.lower() in ("reply", "favorite", "next", "prev"):
                    continue
                seen.add(t)
                hints = _extract_contact_hints(text)
                signals.append(Signal(
                    business_name=t,
                    location=f"{city.title()}, {st}",
                    signal_type="business_for_sale_informal",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=link,
                    urgency="expansion",
                    data_source_quality="scraped",
                    contact_hints=hints,
                ))
        _log(self.name, f"found {len(signals)} Craigslist BFS signals")
        return signals


# ---- 16b. Craigslist Jobs — hiring surge (2+ ads from same company) ----
class CraigslistJobs(Collector):
    name = "craigslist_jobs"
    description = "Craigslist job postings — companies posting multiple roles = hiring surge / growth"
    source_type = "scrape"
    schedule = "daily"
    tier = "B"
    default_strength = 25.0

    CITIES = {"MN": "minneapolis", "WI": "milwaukee"}

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        for st in RENDER_STATES:
            city = self.CITIES.get(st)
            if not city:
                continue
            url = f"https://{city}.craigslist.org/search/jjj"
            resp = self._scrape_get(url, render=True)
            if not resp:
                _log(self.name, f"{city} jobs: blocked — {url}")
                continue
            text = getattr(resp, "text", "") or ""
            md = getattr(resp, "markdown", "") or text
            # Count company mentions — a company with multiple ads = hiring surge
            items = re.findall(r'\[([^\]]{8,90})\]\(', md)
            # Try to extract company names from titles (often "Role - Company" or "Company: ...")
            companies = {}
            for title in items:
                # crude: titles with a dash often have company after the dash
                m = re.search(r'[-–]\s*([A-Z][A-Za-z0-9&.\' ]{3,40})$', title.strip())
                if m:
                    co = m.group(1).strip()
                    companies[co] = companies.get(co, 0) + 1
            for co, cnt in companies.items():
                if cnt >= 2:
                    hints = _extract_contact_hints(text)
                    signals.append(Signal(
                        business_name=co,
                        location=f"{city.title()}, {st}",
                        signal_type="hiring_surge",
                        strength=self.default_strength + min(cnt * 2, 10),
                        source=self.name,
                        source_url=url,
                        urgency="growth",
                        data_source_quality="scraped",
                        contact_hints=hints,
                        raw_data={"posting_count": cnt},
                    ))
        _log(self.name, f"found {len(signals)} Craigslist jobs signals")
        return signals


# ---- 17. Google Trends — Rising Business Queries ----
class GoogleTrends(Collector):
    name = "google_trends"
    description = "Trending business-related searches in your area — signals demand shifts"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        # Google Trends doesn't have a free API; would need pytrends or scraping
        _log(self.name, "Google Trends requires pytrends library — planned for v0.2")
        return []


# ---- 20. USPTO Trademark Filings ----
class USPTOTrademarks(Collector):
    name = "uspto_trademarks"
    description = "New trademark filings — businesses investing in IP are growing and may need capital"
    source_type = "api"
    schedule = "weekly"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # USPTO bulk data is available at https://bulkdata.uspto.gov/
        # The TSDR API can search recent filings
        state = kwargs.get("state", "MN")
        resp = self._get("https://tsdr.uspto.gov/search", params={
            "searchType": "statusSearch",
            "ownerState": state,
        })
        # USPTO search API is complex; this is a simplified stub
        _log(self.name, "USPTO search requires specialized parsing — planned for v0.2")
        return signals


# ---- 21. Construction/Building Permits ----
class BuildingPermits(Collector):
    name = "building_permits"
    description = "Construction permits — businesses expanding physical space need build-out capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        # Minneapolis permits: https://www.minneapolismn.gov/government/programs-initiatives/development-projects/
        _log(self.name, "Building permits require city-specific scraper — planned for v0.2")
        return []


# ---- 23. Liquor License Applications ----
class LiquorLicenses(Collector):
    name = "liquor_licenses"
    description = "New liquor license applications — bar/restaurant opening, needs startup capital"
    source_type = "scrape"
    schedule = "monthly"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Liquor licenses require state-specific portal — planned for v0.2")
        return []


# ---- 24. LinkedIn Company Hiring ----
class LinkedInHiring(Collector):
    name = "linkedin_hiring"
    description = "LinkedIn job postings — companies posting multiple roles are growing"
    source_type = "scrape"
    schedule = "daily"
    tier = "A"
    default_strength = 28.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        # LinkedIn public job search (no auth needed for public listings)
        keywords = kwargs.get("keywords", "hiring")
        location = kwargs.get("location", "Minneapolis, Minnesota")
        resp = self._scrape_get(
            f"https://www.linkedin.com/jobs/search?keywords={quote_plus(keywords)}&location={quote_plus(location)}&sortBy=DD"
        )
        if not resp:
            return signals
        # Extract company names from public listing HTML
        companies = re.findall(r'"companyName"\s*:\s*"([^"]+)"', resp.text)
        if not companies:
            companies = re.findall(r'class="base-search-card__subtitle"[^>]*>([^<]+)<', resp.text)
        counts: dict[str, int] = {}
        for co in companies:
            co = co.strip()
            if co:
                counts[co] = counts.get(co, 0) + 1
        for co, cnt in counts.items():
            if cnt >= 2:
                signals.append(Signal(
                    business_name=co,
                    location=location,
                    signal_type="linkedin_hiring_surge",
                    strength=self.default_strength + min(cnt * 2, 10),
                    source=self.name,
                    raw_data={"posting_count": cnt},
                ))
        _log(self.name, f"found {len(signals)} LinkedIn hiring signals")
        return signals


# ---- 26-30. Industry-Specific Job Board Scrapers ----

# ---- 31. LoopNet Commercial Leases ----
class LoopNetLeases(Collector):
    name = "loopnet_leases"
    description = "New commercial leases — businesses signing leases need build-out capital"
    source_type = "scrape"
    schedule = "weekly"
    market = "rbf"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "LoopNet scraping requires headless browser — planned for v0.2")
        return []


# ---- 34. Kickstarter/Indiegogo ----
class KickstarterProjects(Collector):
    name = "kickstarter"
    description = "Kickstarter projects nearing funding — may need additional working capital post-campaign"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 15.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.kickstarter.com/discover/advanced?state=live&sort=newest&region=US-MN")
        if not resp:
            return signals
        names = re.findall(r'"name"\s*:\s*"([^"]+)"', resp.text)
        for n in names[:10]:
            signals.append(Signal(
                business_name=n.strip(),
                location="Minnesota",
                signal_type="crowdfunding_active",
                strength=self.default_strength,
                source=self.name,
            ))
        _log(self.name, f"found {len(signals)} Kickstarter signals")
        return signals


# ---- 35. Shopify Store Detector ----
class ShopifyStoreDetector(Collector):
    name = "shopify_stores"
    description = "E-commerce stores on Shopify — subscription businesses that qualify for RBF"
    source_type = "scrape"
    schedule = "weekly"
    market = "rbf"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        # Would use myip.ms or BuiltWith to find Shopify stores in a region
        _log(self.name, "Shopify store detection requires BuiltWith or similar — planned for v0.2")
        return []


# ---- 36. GitHub Activity (SaaS companies) ----
class GitHubActivity(Collector):
    name = "github_activity"
    description = "GitHub repo activity drops — SaaS founders losing steam, may need capital or exit"
    source_type = "api"
    schedule = "weekly"
    market = "micro_acq"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        # GitHub API is free, no key needed for public repos
        _log(self.name, "GitHub activity tracking requires a curated list of SaaS repos — enrichment collector")
        return []


# ---- 38. Acquire.com Listings (micro-acq) ----
class AcquireComListings(Collector):
    name = "acquire_com"
    description = "Acquire.com listings — SaaS/e-comm businesses for sale, owner wants to exit"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Acquire.com requires auth for listings — planned for v0.2")
        return []


# ---- 39-43. Additional State SOS (UCC + Entities) ----

class _StateSosStub(Collector):
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 16.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, f"State SOS scraper requires headless browser — planned for v0.2")
        return []


class ILSosBusinesses(_StateSosStub):
    name = "il_sos"
    description = "Illinois SOS — Chicago metro deal flow"

class MOSosBusinesses(_StateSosStub):
    name = "mo_sos"
    description = "Missouri SOS business filings"

class NESosBusinesses(_StateSosStub):
    name = "ne_sos"
    description = "Nebraska SOS business filings"

class MISosBusinesses(_StateSosStub):
    name = "mi_sos"
    description = "Michigan SOS business filings"

class OHSosBusinesses(_StateSosStub):
    name = "oh_sos"
    description = "Ohio SOS business filings"


# ---- 44. Federal Register — New Regulations ----
class FederalRegister(Collector):
    name = "federal_register"
    description = "New regulations — industries facing new rules often need compliance capital"
    source_type = "api"
    schedule = "weekly"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._get("https://www.federalregister.gov/api/v1/documents.json", params={
            "conditions[type][]": "RULE",
            "conditions[publication_date][gte]": (_now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            "per_page": 20, "order": "newest",
        })
        if resp:
            for doc in (resp.json().get("results") or []):
                signals.append(Signal(
                    business_name=doc.get("agencies", [{"name": ""}])[0].get("name", "") if doc.get("agencies") else "",
                    signal_type="new_regulation",
                    strength=self.default_strength,
                    source=self.name,
                    source_url=doc.get("html_url", ""),
                    raw_data={"title": doc.get("title", "")[:200], "type": doc.get("type", "")},
                ))
        _log(self.name, f"found {len(signals)} regulation signals")
        return signals


# ---- 45. Minnesota DEED (Dept of Employment) ----
class MNDEED(Collector):
    name = "mn_deed"
    description = "MN employment data — growing industries and new business formations"
    source_type = "scrape"
    schedule = "monthly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "MN DEED data requires bulk download — planned for v0.2")
        return []


# ---- 46. Chamber of Commerce Directories ----
class ChamberOfCommerce(Collector):
    name = "chamber_of_commerce"
    description = "Local chamber members — established businesses, good referral targets"
    source_type = "scrape"
    schedule = "monthly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Chamber directories require per-city scraper — planned for v0.2")
        return []


# ---- 47. Equipment Auctions (IronPlanet/Ritchie Bros) ----
class EquipmentAuctions(Collector):
    name = "equipment_auctions"
    description = "Equipment auctions — businesses buying equipment need capital; selling = possible exit"
    source_type = "scrape"
    schedule = "weekly"
    market = "all"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Equipment auction scraping planned for v0.2")
        return []


# ---- 48. App Store / Google Play (for app businesses) ----
class AppStoreRankings(Collector):
    name = "app_store_rankings"
    description = "Rising apps — app businesses growing fast may need capital"
    source_type = "scrape"
    schedule = "weekly"
    market = "rbf"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "App store ranking tracking planned for v0.2")
        return []


# ---- 49. Seasonal Calendar (internal) ----
class SeasonalCalendar(Collector):
    name = "seasonal_calendar"
    description = "Industry seasonal patterns — restaurants need capital before summer, retail before Q4"
    source_type = "manual"
    schedule = "daily"
    tier = "C"
    default_strength = 10.0

    SEASONS = {
        1: [("restaurants", "post-holiday slow season — may need bridge"), ("tax_prep", "peak season — growth capital")],
        2: [("construction", "pre-season equipment and hiring"), ("landscaping", "pre-season prep")],
        3: [("landscaping", "season starting — equipment + payroll"), ("construction", "season ramping")],
        4: [("restaurants", "patio season prep — renovation capital"), ("retail", "spring inventory")],
        5: [("tourism", "summer season prep"), ("restaurants", "peak season approaching")],
        6: [("construction", "peak season — payroll crunch"), ("landscaping", "peak — crew expansion")],
        7: [("retail", "back-to-school inventory"), ("construction", "peak season continuing")],
        8: [("retail", "Q4 inventory pre-order"), ("ecommerce", "holiday prep begins")],
        9: [("ecommerce", "Q4 inventory + ad spend"), ("restaurants", "fall menu transition")],
        10: [("retail", "holiday inventory arriving"), ("ecommerce", "Black Friday/CM prep")],
        11: [("retail", "peak holiday — staffing"), ("restaurants", "catering season")],
        12: [("tax_prep", "approaching tax season — hiring"), ("restaurants", "holiday event capital")],
    }

    def collect(self, **kwargs) -> list[Signal]:
        month = _now().month
        signals = []
        for industry, reason in self.SEASONS.get(month, []):
            signals.append(Signal(
                business_name=f"[{industry} businesses]",
                signal_type="seasonal_opportunity",
                strength=self.default_strength,
                source=self.name,
                raw_data={"industry": industry, "reason": reason, "month": month},
            ))
        _log(self.name, f"seasonal signals for month {month}: {len(signals)}")
        return signals


# ---- 50. Personal Home Listing (owner exit signal) ----
class OwnerHomeListing(Collector):
    name = "owner_home_listing"
    description = "Business owner listing personal home — life transition signal, may be ready to sell business"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "A"
    default_strength = 30.0

    def collect(self, **kwargs) -> list[Signal]:
        # Would cross-reference known business owner addresses with Zillow/Realtor listings
        _log(self.name, "Owner home listing detection requires business-owner address database — enrichment collector")
        return []


# ---- 51. Glassdoor Reviews ----
class GlassdoorReviews(Collector):
    name = "glassdoor_reviews"
    description = "Glassdoor reviews mentioning uncertainty or leadership changes — exit signal"
    source_type = "scrape"
    schedule = "monthly"
    market = "micro_acq"
    tier = "C"
    default_strength = 15.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Glassdoor scraping requires headless browser — planned for v0.2")
        return []


# ---- 52. SSL Certificate Expiration ----
class SSLCertExpiry(Collector):
    name = "ssl_cert_expiry"
    description = "SSL certs expiring — business neglecting their web presence, possible exit candidate"
    source_type = "api"
    schedule = "monthly"
    market = "micro_acq"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "SSL expiry tracking requires domain watchlist — enrichment collector")
        return []


# ---- 54. AngelList / Wellfound Startups ----
class AngelListStartups(Collector):
    name = "angellist_startups"
    description = "Early-stage startups — may need non-dilutive capital"
    source_type = "scrape"
    schedule = "weekly"
    market = "rbf"
    tier = "C"
    default_strength = 16.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Wellfound/AngelList requires auth — planned for v0.2")
        return []


# ---- 55. County Court Records (liens, judgments) ----
class CountyCourtRecords(Collector):
    name = "county_court_records"
    description = "Tax liens and judgments — businesses under financial stress may need capital solutions"
    source_type = "scrape"
    schedule = "monthly"
    market = "all"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        url = "https://publicaccess.courts.state.mn.us/CaseSearch"
        resp = self._scrape_get(url, render=True)
        if not resp:
            _log(self.name, f"Court records: search manually at {url} (automation blocked)")
            return signals
        text = getattr(resp, "text", "") or ""
        # Look for business-like party names (contains LLC/Inc/Corp)
        biz = re.findall(r'\b([A-Z][A-Za-z0-9&.,\' ]{3,45}(?:LLC|Inc|Corp|Co|Ltd|Company))\b', text)
        seen = set()
        for b in biz[:15]:
            b = b.strip()
            if b in seen:
                continue
            seen.add(b)
            signals.append(Signal(
                business_name=b,
                location="MN",
                signal_type="court_filing_business",
                strength=self.default_strength,
                source=self.name,
                source_url=url,
                urgency="litigation",
                data_source_quality="scraped",
                contact_availability="research_required",
            ))
        _log(self.name, f"found {len(signals)} court filing signals")
        return signals


# ---- 56. Hunter.io Email Finder ----
class HunterEmailFinder(Collector):
    name = "hunter_email_finder"
    description = "Find business owner emails for outreach enrichment"
    source_type = "api"
    schedule = "daily"
    requires_key = "HUNTER_API_KEY"
    tier = "C"
    default_strength = 10.0

    def collect(self, **kwargs) -> list[Signal]:
        # Enrichment collector — called per-business, not in bulk scan
        _log(self.name, "Hunter.io is an enrichment tool — use via /api/enrich endpoint")
        return []


# ---- 57. Google Jobs API ----
class GoogleJobsSearch(Collector):
    name = "google_jobs"
    description = "Google Jobs aggregation — catches postings from multiple job boards"
    source_type = "scrape"
    schedule = "daily"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        location = kwargs.get("location", "Minneapolis, MN")
        queries = ["hiring multiple positions", "urgently hiring", "immediate start"]
        for q in queries[:2]:
            resp = self._scrape_get(f"https://www.google.com/search?q={quote_plus(q + ' ' + location)}&ibp=htl;jobs")
            if not resp:
                continue
            companies = re.findall(r'"([^"]{3,40})"\s*,\s*"(?:Full-time|Part-time|Contract)', resp.text)
            for co in set(companies[:10]):
                signals.append(Signal(
                    business_name=co.strip(), location=location,
                    signal_type="hiring_urgently", strength=self.default_strength,
                    source=self.name, raw_data={"query": q},
                ))
        _log(self.name, f"found {len(signals)} Google Jobs signals")
        return signals


# ---- 58. Bing News — Business Expansion ----
class BingNewsExpansion(Collector):
    name = "bing_news_expansion"
    description = "Local news about business expansions, new locations, funding rounds"
    source_type = "scrape"
    schedule = "daily"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        queries = ["business expansion Minneapolis", "new restaurant opening Minnesota", "company funding Minnesota"]
        for q in queries:
            resp = self._scrape_get(f"https://www.bing.com/news/search?q={quote_plus(q)}&qft=sortbydate%3d%221%22")
            if not resp:
                continue
            titles = re.findall(r'class="title"[^>]*>([^<]+)<', resp.text)
            for t in titles[:5]:
                signals.append(Signal(
                    business_name=t.strip()[:80], location="Minnesota",
                    signal_type="business_expansion_news", strength=self.default_strength,
                    source=self.name, raw_data={"headline": t.strip()},
                ))
        _log(self.name, f"found {len(signals)} news signals")
        return signals


# ---- 59. Minnesota Business Licenses ----
class MNBusinessLicenses(Collector):
    name = "mn_business_licenses"
    description = "New business license applications in MN cities"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "MN city license portals require per-city scraper — planned for v0.3")
        return []


# ---- 60. SBA Disaster Loans ----
class SBADisasterLoans(Collector):
    name = "sba_disaster_loans"
    description = "SBA disaster loan data — affected businesses may need additional capital"
    source_type = "api"
    schedule = "monthly"
    tier = "B"
    default_strength = 24.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._get("https://data.sba.gov/api/views/nsei-6cj5/rows.json?accessType=DOWNLOAD")
        # SBA disaster loan data is large; this is a lightweight check
        _log(self.name, "SBA disaster data requires bulk processing — planned for v0.3")
        return signals


# ---- 61. Facebook Marketplace Businesses ----
class FacebookMarketplace(Collector):
    name = "facebook_marketplace"
    description = "Businesses for sale on Facebook Marketplace — informal, motivated sellers"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Facebook Marketplace requires auth — planned for v0.3")
        return []


# ---- 62. Thumbtack / HomeAdvisor Pros ----
class ThumbTackPros(Collector):
    name = "thumbtack_pros"
    description = "Service pros on Thumbtack — active contractors who may need working capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        categories = ["plumbing", "hvac", "electrical", "roofing", "painting"]
        for cat in categories[:3]:
            resp = self._scrape_get(f"https://www.thumbtack.com/mn/minneapolis/{cat}/")
            if not resp:
                continue
            names = re.findall(r'"name"\s*:\s*"([^"]{3,60})"', resp.text)
            for n in set(list(names)[:5]):
                signals.append(Signal(
                    business_name=n.strip(), location="Minneapolis, MN",
                    signal_type=f"active_service_pro_{cat}", strength=self.default_strength,
                    source=self.name, raw_data={"category": cat},
                ))
        _log(self.name, f"found {len(signals)} Thumbtack signals")
        return signals


# ---- 63. Foursquare New Businesses ----
class FoursquareNewBusinesses(Collector):
    name = "foursquare_new"
    description = "Recently opened businesses via Foursquare — startups needing capital"
    source_type = "api"
    schedule = "weekly"
    requires_key = "FOURSQUARE_API_KEY"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        key = os.getenv("FOURSQUARE_API_KEY", "").strip().strip('"').strip("'")
        if key.lower().startswith("bearer "):
            key = key[7:].strip()
        if not key:
            return []
        signals = []
        location = kwargs.get("location", "Minneapolis, MN")
        resp = self._get(
            "https://api.foursquare.com/v3/places/search",
            params={"near": location, "sort": "DISTANCE", "limit": 20},
            headers={"Authorization": key, "Accept": "application/json"},
        )
        if not resp:
            return signals
        for place in (resp.json().get("results") or []):
            name = place.get("name", "")
            if not name:
                continue
            addr_parts = place.get("location") or {}
            addr = ", ".join(filter(None, [addr_parts.get("address"), addr_parts.get("locality")]))
            signals.append(Signal(
                business_name=name,
                location=addr or location,
                signal_type="new_business_opened",
                strength=self.default_strength,
                source=self.name,
                raw_data={"fsq_id": place.get("fsq_id", "")},
            ))
        _log(self.name, f"found {len(signals)} Foursquare new business signals")
        return signals


# ---- 64. DoorDash / UberEats Restaurant Growth ----
class FoodDeliveryGrowth(Collector):
    name = "food_delivery_growth"
    description = "Restaurants active on delivery platforms — growing revenue, may need capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 14.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Delivery platform scraping requires specialized approach — planned for v0.3")
        return []


# ---- 65. Minnesota DEED Job Vacancy Survey ----
class MNDEEDJobVacancies(Collector):
    name = "mn_deed_vacancies"
    description = "MN DEED job vacancy data — industries with labor shortages need capital to compete"
    source_type = "api"
    schedule = "monthly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "MN DEED vacancy data requires bulk download — planned for v0.3")
        return []


# ---- 66. Commercial Truck Sales ----
class CommercialTruckSales(Collector):
    name = "commercial_truck_sales"
    description = "Commercial truck listings — trucking companies buying trucks need capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.commercialtrucktrader.com/listing?state=MN")
        if not resp:
            return signals
        # Look for dealers posting multiple listings (they have buyer customers)
        dealers = re.findall(r'"dealerName"\s*:\s*"([^"]+)"', resp.text)
        counts: dict[str, int] = {}
        for d in dealers:
            counts[d] = counts.get(d, 0) + 1
        for dealer, cnt in counts.items():
            if cnt >= 3:
                signals.append(Signal(
                    business_name=dealer, location="Minnesota",
                    signal_type="commercial_vehicle_activity", strength=self.default_strength,
                    source=self.name, raw_data={"listing_count": cnt},
                ))
        _log(self.name, f"found {len(signals)} truck signals")
        return signals


# ---- 67. Startup MN / MN Cup ----
class StartupMN(Collector):
    name = "startup_mn"
    description = "Minnesota startup ecosystem — early companies needing capital"
    source_type = "scrape"
    schedule = "monthly"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.startribune.com/business/technology/")
        if not resp:
            return signals
        headlines = re.findall(r'<h[23][^>]*>.*?<a[^>]*>([^<]+)</a>', resp.text)
        for h in headlines[:10]:
            if any(kw in h.lower() for kw in ["startup", "raises", "funding", "launch", "opens", "expands"]):
                signals.append(Signal(
                    business_name=h.strip()[:80], location="Minnesota",
                    signal_type="startup_news", strength=self.default_strength,
                    source=self.name, raw_data={"headline": h.strip()},
                ))
        _log(self.name, f"found {len(signals)} startup MN signals")
        return signals


# ---- 68. Minneapolis / St Paul Business Journal ----
class MSPBizJournal(Collector):
    name = "msp_biz_journal"
    description = "MSP Business Journal — local business news, expansions, new openings"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.bizjournals.com/twincities/news")
        if not resp:
            return signals
        titles = re.findall(r'data-title="([^"]+)"', resp.text)
        if not titles:
            titles = re.findall(r'<h[23][^>]*>([^<]{10,80})</h[23]>', resp.text)
        for t in titles[:10]:
            if any(kw in t.lower() for kw in ["opens", "expands", "hires", "grows", "raises", "acquires", "launch"]):
                signals.append(Signal(
                    business_name=t.strip()[:80], location="Twin Cities, MN",
                    signal_type="local_biz_news", strength=self.default_strength,
                    source=self.name, raw_data={"headline": t.strip()},
                ))
        _log(self.name, f"found {len(signals)} biz journal signals")
        return signals


# ---- 69. IRS Tax Exempt Org Search ----
class IRSTaxExemptOrgs(Collector):
    name = "irs_tax_exempt"
    description = "Recently registered nonprofits/orgs — some have fundable subsidiaries or need bridge capital"
    source_type = "api"
    schedule = "monthly"
    tier = "C"
    default_strength = 10.0

    def collect(self, **kwargs) -> list[Signal]:
        # IRS Exempt Org search has a public endpoint
        _log(self.name, "IRS exempt org search requires bulk CSV processing — planned for v0.3")
        return []


# ---- 70. Medical/Dental Practice Openings ----
class MedicalPracticeOpenings(Collector):
    name = "medical_practice_openings"
    description = "New medical/dental practices — high-revenue businesses needing equipment + buildout capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "A"
    default_strength = 28.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        for specialty in ["dentist", "medical practice", "veterinary clinic"]:
            resp = self._scrape_get(f"https://www.indeed.com/jobs?q={quote_plus(specialty + ' hiring')}&l={quote_plus('Minneapolis, MN')}&sort=date&limit=10")
            if not resp:
                continue
            companies = re.findall(r'data-testid="company-name"[^>]*>([^<]+)<', resp.text)
            if not companies:
                companies = re.findall(r'"companyName"\s*:\s*"([^"]+)"', resp.text)
            for co in set(companies[:5]):
                signals.append(Signal(
                    business_name=co.strip(), location="Minneapolis, MN",
                    signal_type=f"medical_practice_hiring", strength=self.default_strength,
                    source=self.name, raw_data={"specialty": specialty},
                ))
        _log(self.name, f"found {len(signals)} medical practice signals")
        return signals


# ===========================================================================
# COLLECTOR REGISTRY
# ===========================================================================

ALL_COLLECTORS: list[type[Collector]] = [
    # Tier A — Strongest
    SamGovContracts,           # 1
    USASpendingAwards,         # 2
    IndeedJobs,                # 3
    SBALoanData,               # 4
    SECEdgarFilings,           # 5
    MNSosUCCFilings,           # 9
    LinkedInHiring,            # 24
    OwnerHomeListing,          # 50

    # Tier B — Moderate
    MNSosNewBusinesses,        # 8
    WISosBusinesses,           # 10
    IASosBusinesses,           # 11
    OpenCorporatesSearch,      # 14
    BizBuySellListings,        # 15
    CraigslistBFS,             # 16
    CraigslistJobs,            # 16b (hiring surge via Craigslist)
    USPTOTrademarks,           # 20
    BuildingPermits,           # 21
    LiquorLicenses,            # 23
    LoopNetLeases,             # 31
    ShopifyStoreDetector,      # 35
    AcquireComListings,        # 38
    EquipmentAuctions,         # 47
    CountyCourtRecords,        # 55

    # Tier C — Contextual / enrichment
    NDSosBusinesses,           # 12
    SDSosBusinesses,           # 13
    GoogleTrends,              # 17
    KickstarterProjects,       # 34
    GitHubActivity,            # 36
    ILSosBusinesses,           # 39
    MOSosBusinesses,           # 40
    NESosBusinesses,           # 41
    MISosBusinesses,           # 42
    OHSosBusinesses,           # 43
    FederalRegister,           # 44
    MNDEED,                    # 45
    ChamberOfCommerce,         # 46
    AppStoreRankings,          # 48
    SeasonalCalendar,          # 49
    GlassdoorReviews,          # 51
    SSLCertExpiry,             # 52
    AngelListStartups,         # 54

    # v0.2 additions
    HunterEmailFinder,         # 56
    GoogleJobsSearch,          # 57
    BingNewsExpansion,         # 58
    MNBusinessLicenses,        # 59
    SBADisasterLoans,          # 60
    FacebookMarketplace,       # 61
    ThumbTackPros,             # 62
    FoursquareNewBusinesses,   # 63 (replaces Yelp new businesses)
    FoodDeliveryGrowth,        # 64
    MNDEEDJobVacancies,        # 65
    CommercialTruckSales,      # 66
    StartupMN,                 # 67
    MSPBizJournal,             # 68
    IRSTaxExemptOrgs,          # 69
    MedicalPracticeOpenings,   # 70
]


def build_registry() -> list[Collector]:
    """Instantiate all collectors and return them."""
    return [cls() for cls in ALL_COLLECTORS]


# ===========================================================================
# SCORING ENGINE
# ===========================================================================

# ---------------------------------------------------------------------------
# Job title classifier — scores job postings by how strongly the TITLE predicts
# a real, ROI-positive capital need (revenue-generating roles score highest)
# ---------------------------------------------------------------------------

# Revenue-generating / capital-intensive roles → strong capital-need signal
_HIGH_VALUE_TITLES = {
    # Trucking / logistics (capital-intensive, fuel + equipment + payroll gaps)
    "cdl driver": 30, "truck driver": 30, "owner operator": 28, "dispatcher": 18,
    "diesel mechanic": 24, "fleet": 22,
    # Skilled trades (high revenue per head, growth = equipment + payroll)
    "hvac": 28, "hvac technician": 30, "plumber": 28, "electrician": 28,
    "journeyman": 26, "pipefitter": 26, "welder": 24, "roofer": 24,
    "mason": 22, "carpenter": 22, "installer": 22,
    # Medical / dental (very high revenue per head, equipment heavy)
    "dental hygienist": 28, "dentist": 30, "physician": 30, "nurse practitioner": 26,
    "physical therapist": 28, "veterinarian": 30, "optometrist": 28, "chiropractor": 26,
    "dental assistant": 20, "medical assistant": 18, "registered nurse": 22,
    # Sales / revenue roles (direct revenue generation)
    "sales representative": 25, "sales manager": 26, "account executive": 25,
    "business development": 24, "outside sales": 26,
    # Construction / contracting (project capital needs)
    "project manager": 22, "estimator": 22, "foreman": 22, "superintendent": 22,
    "heavy equipment operator": 24, "crane operator": 24,
    # Manufacturing / production (capacity expansion)
    "machinist": 22, "cnc": 22, "production manager": 20, "fabricator": 22,
    # Food service at scale (expansion signal)
    "kitchen manager": 18, "line cook": 14, "restaurant manager": 18, "chef": 18,
}

# Support / overhead roles → weak signal (not direct revenue, less urgent capital need)
_LOW_VALUE_TITLES = {
    "administrative assistant": 5, "receptionist": 5, "office manager": 6,
    "data entry": 4, "intern": 3, "customer service": 8, "front desk": 5,
    "bookkeeper": 7, "human resources": 6, "social media": 6, "marketing coordinator": 8,
    "video editing": 3, "graphic designer": 6, "content writer": 5,
}


def classify_job_title(title: str) -> dict:
    """
    Score a job title 0-30 by how strongly it predicts a real, ROI-positive
    capital need. Revenue-generating and capital-intensive roles score high;
    overhead/support roles score low.
    """
    t = (title or "").lower().strip()
    if not t:
        return {"score": 10, "category": "unknown", "reason": "no title"}

    # Check high-value titles (longest match wins for specificity)
    best_high = 0
    best_high_key = ""
    for key, score in _HIGH_VALUE_TITLES.items():
        if key in t and score > best_high:
            best_high = score
            best_high_key = key

    # Check low-value titles
    best_low = 0
    best_low_key = ""
    for key, score in _LOW_VALUE_TITLES.items():
        if key in t and (best_low == 0 or score < best_low):
            best_low = score
            best_low_key = key

    if best_high:
        return {"score": best_high, "category": "revenue_generating",
                "reason": f"'{best_high_key}' is a revenue-generating/capital-intensive role", "matched": best_high_key}
    if best_low:
        return {"score": best_low, "category": "overhead",
                "reason": f"'{best_low_key}' is an overhead role — weaker capital-need signal", "matched": best_low_key}

    return {"score": 12, "category": "neutral", "reason": "title not classified", "matched": ""}


def score_hiring_signal(sig: Signal) -> float:
    """
    Re-score a hiring signal using the job title classifier.
    Pulls the title from raw_data if available.
    """
    raw = sig.raw_data or {}
    title = raw.get("job_title") or raw.get("title") or ""
    if not title:
        # Fall back to default strength if no title to classify
        return sig.strength
    cls = classify_job_title(title)
    # Blend: base 15 + title score (capped at 40)
    return min(15 + cls["score"], 40)


# Base signal strength weights by signal_type (0-40 scale)
SIGNAL_WEIGHTS: dict[str, float] = {
    "gov_contract_award": 35,
    "fed_award_received": 33,
    "ucc_payoff_approaching": 32,
    "hiring_surge": 30,
    "linkedin_hiring_surge": 28,
    "owner_home_listing": 30,
    "sba_loan_activity": 28,
    "sec_filing_event": 25,
    "crowdfunding_capital_need": 25,
    "business_for_sale": 22,
    "franchise_resale": 22,
    "financing_inquiry": 22,
    "building_permit": 22,
    "new_liquor_license": 20,
    "new_or_changed_business": 20,
    "business_for_sale_informal": 20,
    "product_launch": 20,
    "company_registry": 12,
    "high_review_activity": 18,
    "bbb_accredited": 15,
    "seasonal_opportunity": 10,
    "new_regulation": 14,
    "crowdfunding_active": 15,
    # Industry-specific
    "hiring_hvac_hiring": 22,
    "hiring_plumbing_hiring": 22,
    "hiring_electrical_hiring": 22,
    "hiring_landscaping_hiring": 20,
    "hiring_trucking_hiring": 24,
}

# Learned adjustments (initialized at 1.0, updated by the learning loop)
LEARNED_WEIGHTS: dict[str, float] = {}

# Industry close rates (learned over time, start at baseline)
INDUSTRY_CLOSE_RATES: dict[str, float] = {
    "construction": 0.08,
    "restaurants": 0.06,
    "trucking": 0.10,
    "hvac": 0.09,
    "retail": 0.07,
    "ecommerce": 0.08,
    "saas": 0.07,
    "landscaping": 0.08,
    "default": 0.07,
}


def score_signal(sig: Signal, all_signals_for_business: list[Signal] = None,
                 is_past_client: bool = False, was_contacted: bool = False) -> float:
    """
    Score a signal from 0-100. Higher = act sooner.

    Components:
      base_strength (0-40)  — from signal type weight
      stacking     (0-15)   — multiple signals on same business
      recency      (0-15)   — newer signals score higher
      industry     (0-10)   — learned close rate for this industry
      relationship (0-20)   — past client or previous contact
    """
    # Base strength
    base = SIGNAL_WEIGHTS.get(sig.signal_type, sig.strength)
    learned_mult = LEARNED_WEIGHTS.get(sig.signal_type, 1.0)
    base = min(base * learned_mult, 40)

    # Stacking bonus
    others = all_signals_for_business or []
    stacking = min(15, len(others) * 5)

    # Recency bonus
    age_days = max(0, (_now() - sig.timestamp).days) if sig.timestamp else 30
    recency = max(0, 15 - (age_days * 0.5))

    # Industry bonus (learned)
    industry = "default"
    for ind in INDUSTRY_CLOSE_RATES:
        if ind in sig.signal_type or ind in sig.business_name.lower() or ind in json.dumps(sig.raw_data).lower():
            industry = ind
            break
    industry_bonus = INDUSTRY_CLOSE_RATES.get(industry, 0.07) * 100  # scale to 0-10ish

    # Relationship bonus
    relationship = 0
    if is_past_client:
        relationship = 20
    elif was_contacted:
        relationship = 10

    total = base + stacking + recency + min(industry_bonus, 10) + relationship
    return round(min(total, 100), 1)


# ---------------------------------------------------------------------------
# Signal quality filter — catches garbage before it hits the dashboard
# ---------------------------------------------------------------------------

# Generic words that aren't real business names
_GARBAGE_NAMES = {
    "video editing", "photo editing", "web design", "graphic design", "social media",
    "marketing", "consulting", "freelance", "remote work", "side hustle", "online business",
    "make money", "work from home", "passive income", "dropshipping", "affiliate",
    "crypto", "nft", "bitcoin", "forex", "trading", "investment opportunity",
    "click here", "sign up", "free trial", "download", "subscribe", "follow",
    "loading", "error", "undefined", "null", "none", "test", "example",
    "untitled", "no title", "new post", "draft", "placeholder",
    "sec", "commission", "united states", "department", "agency",
    "amendment", "modification", "solicitation", "notice", "combined synopsis",
    "", " ",
}

# Government procurement codes / categories (not business names)
_GOV_CODE_PATTERNS = [
    r'^\d{2}--',           # FSC codes like "43--RING,WEARING"
    r'^\d{4,6}\s',         # NAICS codes like "541330 Engineering"
    r'^[A-Z]--',           # PSC letter codes
    r'--\w+,\w+',          # Double-dash category patterns
    r'^COMBINED SYNOPSIS',
    r'^AMENDMENT\s',
    r'^MODIFICATION\s',
    r'^SOURCES\s+SOUGHT',
    r'^JUSTIFICATION\s',
    r'^SPECIAL\s+NOTICE',
    r'^AWARD\s+NOTICE',
    r'^INTENT\s+TO\s',
    r'^PRE.?SOLICITATION',
    r'^SOLICITATION\b',
]

# Minimum name length
_MIN_NAME_LENGTH = 4


def _is_valid_signal(sig: Signal) -> bool:
    """
    Returns True if this signal looks like a real, actionable business lead.
    Filters out: procurement codes, public companies, generic names, spam, artifacts.
    """
    name = (sig.business_name or "").strip()

    # Too short
    if len(name) < _MIN_NAME_LENGTH:
        return False

    # Known garbage
    if name.lower().strip() in _GARBAGE_NAMES:
        return False

    words = name.split()

    # Government procurement codes (e.g. "43--RING,WEARING", "5340--HARDWARE")
    for pat in _GOV_CODE_PATTERNS:
        if re.match(pat, name, re.IGNORECASE):
            return False

    # All-caps names with commas = probably a procurement category, not a business
    # "RING,WEARING" or "BOLT,MACHINE" patterns
    if name.isupper() and "," in name and len(name) < 40:
        return False

    # All-caps with no lowercase at all and under 30 chars = probably a code or abbreviation
    # Real business names have mixed case ("Acme Trucking") or are longer if all-caps
    if name.isupper() and len(name) < 25 and not any(c.isdigit() for c in name):
        # Allow common business suffixes
        if not any(s in name for s in ["LLC", "INC", "CORP", "LTD", "CO", "SERVICES", "GROUP"]):
            return False

    # All lowercase single word under 15 chars = probably a generic term
    if len(words) == 1 and len(name) < 15 and name.islower():
        return False

    # Starts with common HTML/JSON artifacts
    if name.startswith(("{", "[", "<", "http", "www.", "/", "#")):
        return False

    # All caps single short word = probably an acronym from HTML
    if len(words) == 1 and name.isupper() and len(name) < 5:
        return False

    # Contains obvious non-business patterns
    lower = name.lower()
    spam_patterns = ["click here", "sign up", "free", "discount", "limited time",
                     "act now", "subscribe", "unsubscribe", "cookie", "privacy policy",
                     "page not found", "access denied", "error 404", "javascript"]
    if any(p in lower for p in spam_patterns):
        return False

    # Looks like a government solicitation title, not a business name
    gov_title_words = ["solicitation", "amendment", "modification", "combined synopsis",
                       "sources sought", "justification", "presolicitation",
                       "request for proposal", "request for quote", "rfp", "rfq",
                       "task order", "delivery order", "blanket purchase"]
    if any(g in lower for g in gov_title_words):
        return False

    # Product Hunt: require 2+ words or internal capital
    if sig.source == "product_hunt":
        if len(words) < 2 and not any(c.isupper() for c in name[1:]):
            return False

    # SEC EDGAR: these are PUBLIC companies — mark but allow through with lower priority
    # (the scoring will handle deprioritization)
    if sig.source == "sec_edgar_filings":
        if len(name) < 5:
            return False
        # Let them through but they'll be deprioritized in scoring

    # SAM.gov: filter out solicitation TITLES (we want awardee NAMES)
    if sig.source == "sam_gov_contracts":
        raw = sig.raw_data or {}
        # If the "business_name" looks like a solicitation title not a company name, reject
        if len(name) > 60:
            return False  # real company names are rarely this long
        if any(w in lower for w in ["contract", "acquisition", "procurement", "maintenance of"]):
            return False

    # Seasonal/internal signals always pass
    if sig.source in ("seasonal_calendar",):
        return True

    return True


def _is_likely_public_company(sig: Signal) -> bool:
    """
    Detect if a signal is likely a public company (not our target market).
    Public companies have SEC filings, are usually large, and don't need MCA.
    """
    if sig.source == "sec_edgar_filings":
        return True

    raw = sig.raw_data or {}
    name = (sig.business_name or "").lower()

    # Common public company indicators
    public_suffixes = ["inc.", "incorporated", "corporation", "plc", "n.v."]
    if any(s in name for s in public_suffixes):
        return True

    # If we know they have SEC filings from other data
    if raw.get("form_type") in ("10-K", "10-Q", "8-K", "S-1"):
        return True

    return False


def dedupe_and_stack(signals: list[Signal]) -> list[Signal]:
    """
    Merge signals about the same business+type. When a business shows up across
    multiple sources, that's a STRONGER lead — stack them and boost strength.
    """
    buckets: dict[str, list[Signal]] = defaultdict(list)
    for s in signals:
        # Stack by business name (normalized), not just dedup_key, so cross-source
        # signals on the same business merge
        key = (s.business_name or "").lower().strip()
        buckets[key].append(s)

    merged = []
    for key, group in buckets.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        # Keep the strongest as base, stack the rest
        base = max(group, key=lambda x: x.strength)
        sources = sorted(set(s.source for s in group))
        base.raw_data = dict(base.raw_data or {})
        base.raw_data["stacked_sources"] = sources
        base.raw_data["stack_count"] = len(group)
        # Each additional distinct source adds strength (capped)
        base.strength = min(base.strength + (len(sources) - 1) * 6, 100)
        if len(sources) > 1 and not base.signal_type.startswith("stacked_"):
            base.signal_type = f"stacked_{base.signal_type}"
        merged.append(base)
    return merged


def rank_signals(signals: list[Signal], known_businesses: set[str] = None,
                 past_clients: set[str] = None) -> list[dict]:
    """
    Score and rank all signals, grouping by business name.
    Filters out garbage/generic signals before ranking.
    Returns a sorted list of dicts ready for the dashboard.
    """
    known = known_businesses or set()
    clients = past_clients or set()

    # ---- Quality filter: remove garbage signals ----
    filtered = []
    for s in signals:
        if not _is_valid_signal(s):
            continue
        filtered.append(s)
    signals = filtered

    # Group signals by business name
    grouped: dict[str, list[Signal]] = {}
    for s in signals:
        key = s.business_name.lower().strip()
        grouped.setdefault(key, []).append(s)

    ranked = []
    for key, sigs in grouped.items():
        best = max(sigs, key=lambda s: s.strength)
        is_client = key in clients
        was_known = key in known
        score = score_signal(best, all_signals_for_business=sigs,
                             is_past_client=is_client, was_contacted=was_known)
        ranked.append({
            "business_name": best.business_name,
            "location": best.location,
            "score": score,
            "signal_count": len(sigs),
            "signals": [s.to_dict() for s in sigs],
            "primary_signal": best.signal_type,
            "source": best.source,
            "source_url": best.source_url,
            "contact_hints": best.contact_hints,
            "is_past_client": is_client,
            "was_contacted": was_known,
            "dedup_key": best.dedup_key,
        })

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


# ===========================================================================
# REGISTRY STATS
# ===========================================================================

def registry_stats() -> dict:
    collectors = build_registry()
    total = len(collectors)
    enabled = sum(1 for c in collectors if c.enabled)
    by_tier = {"A": 0, "B": 0, "C": 0, "D": 0}
    by_market = {}
    by_type = {}
    for c in collectors:
        by_tier[c.tier] = by_tier.get(c.tier, 0) + 1
        by_market[c.market] = by_market.get(c.market, 0) + 1
        by_type[c.source_type] = by_type.get(c.source_type, 0) + 1
    return {
        "total_collectors": total,
        "total": total,
        "enabled": enabled,
        "active_collectors": enabled,
        "disabled_missing_key": total - enabled,
        "by_tier": by_tier,
        "by_source_type": by_type,
        "by_market": by_market,
        "collectors": [c.status() for c in collectors],
    }


# ===========================================================================
# ENRICHMENT PIPELINE
# Turns a raw signal (name + location) into a pre-call dossier by looking up
# public data: business age, estimated revenue, existing MCA, owner contact.
# ===========================================================================

def enrich_business(business_name: str, location: str = "", website: str = "",
                    raw_data: dict = None) -> dict:
    """
    Build a pre-call dossier for a business. Each lookup degrades gracefully —
    a failed lookup just leaves that field empty rather than blocking the others.

    Returns a dict with whatever could be found:
      - estimated_tib_months (from any incorporation date in raw_data)
      - estimated_monthly_revenue (from review count, employee count heuristics)
      - has_existing_mca (from UCC hints if available)
      - owner_name, owner_linkedin, phone, email, website
      - enrichment_notes (what was found / not found)
    """
    raw = raw_data or {}
    dossier = {
        "business_name": business_name,
        "location": location,
        "estimated_tib_months": None,
        "estimated_monthly_revenue": None,
        "revenue_basis": "",
        "has_existing_mca": None,
        "owner_name": "",
        "owner_linkedin": "",
        "phone": raw.get("phone", "") or (raw.get("contact_hints", {}) or {}).get("phone", ""),
        "email": "",
        "website": website,
        "enrichment_notes": [],
        "enriched_at": _now().isoformat(),
    }

    # --- Business age from incorporation date if present in signal raw_data ---
    inc_date = raw.get("incorporation_date") or raw.get("file_date") or raw.get("sale_date")
    if inc_date:
        try:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
                try:
                    d = datetime.strptime(str(inc_date)[:10], fmt)
                    months = max(0, int((_now().replace(tzinfo=None) - d).days / 30))
                    dossier["estimated_tib_months"] = months
                    dossier["enrichment_notes"].append(f"Business age ~{months} months (from filing date)")
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # --- Estimated revenue from review count (rough heuristic) ---
    review_count = raw.get("review_count") or raw.get("total_ratings") or 0
    if review_count and int(review_count) > 0:
        rc = int(review_count)
        # Very rough: more reviews ~ more volume. Industry-dependent, so wide bands.
        if rc > 500:
            est_rev = 150000
        elif rc > 200:
            est_rev = 90000
        elif rc > 100:
            est_rev = 60000
        elif rc > 50:
            est_rev = 40000
        else:
            est_rev = 25000
        dossier["estimated_monthly_revenue"] = est_rev
        dossier["revenue_basis"] = f"~{rc} reviews (rough proxy)"
        dossier["enrichment_notes"].append(f"Est. revenue ${est_rev:,}/mo based on {rc} reviews (very rough)")

    # --- Job count as a growth proxy ---
    job_count = raw.get("job_count") or raw.get("posting_count") or 0
    if job_count and int(job_count) >= 2:
        dossier["enrichment_notes"].append(f"{job_count} simultaneous job postings — active growth")

    # --- Existing MCA hint (would come from UCC collector when active) ---
    if raw.get("ucc_filing") or raw.get("existing_mca"):
        dossier["has_existing_mca"] = True
        dossier["enrichment_notes"].append("UCC filing detected — likely existing MCA position")

    # --- Contract award = strong fundability signal ---
    if raw.get("amount") or raw.get("award_amount"):
        amt = raw.get("amount") or raw.get("award_amount")
        dossier["enrichment_notes"].append(f"Recent contract/award: ${amt} — strong fulfillment-capital need")

    if not dossier["enrichment_notes"]:
        dossier["enrichment_notes"].append("Limited public data available — manual research recommended before outreach")

    return dossier


def enrichment_completeness(dossier: dict) -> int:
    """Score 0-100 how complete the dossier is (how ready for a call)."""
    fields = ["estimated_tib_months", "estimated_monthly_revenue", "phone", "website", "owner_name"]
    have = sum(1 for f in fields if dossier.get(f))
    return round((have / len(fields)) * 100)
