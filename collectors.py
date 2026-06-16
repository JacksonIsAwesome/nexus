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
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus, urlencode

import httpx

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

    def __post_init__(self):
        if not self.dedup_key:
            raw = f"{self.business_name}|{self.signal_type}|{self.source}".lower()
            self.dedup_key = hashlib.md5(raw.encode()).hexdigest()[:16]

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

    def _scrape_get(self, url: str, headers: dict = None) -> httpx.Response | None:
        """GET with scraping courtesy delay and browser-like headers."""
        time.sleep(SCRAPE_DELAY)
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
        # USASpending API is free, no key needed
        resp = self._get("https://api.usaspending.gov/api/v2/search/spending_by_award/", params=None, headers={"Content-Type": "application/json"})
        # POST endpoint — use httpx directly
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
        cities = kwargs.get("cities", ["Minneapolis, MN", "St Paul, MN"])
        for city in cities:
            # Indeed's public search (no API needed)
            q = quote_plus("hiring multiple positions")
            loc = quote_plus(city)
            resp = self._scrape_get(f"https://www.indeed.com/jobs?q={q}&l={loc}&sort=date&limit=25")
            if not resp:
                continue
            # Extract company names from job cards (basic pattern matching)
            text = resp.text
            # Indeed uses data-company-name or companyName in their HTML
            companies = re.findall(r'data-testid="company-name"[^>]*>([^<]+)<', text)
            if not companies:
                companies = re.findall(r'"companyName"\s*:\s*"([^"]+)"', text)
            # Count postings per company
            counts: dict[str, int] = {}
            for co in companies:
                co = co.strip()
                if co:
                    counts[co] = counts.get(co, 0) + 1
            for co, cnt in counts.items():
                if cnt >= 2:  # 2+ postings on one page = hiring surge
                    signals.append(Signal(
                        business_name=co,
                        location=city,
                        signal_type="hiring_surge",
                        strength=self.default_strength + min(cnt * 2, 10),
                        source=self.name,
                        source_url=f"https://www.indeed.com/cmp/{quote_plus(co)}/jobs",
                        raw_data={"job_count": cnt, "city": city},
                    ))
        _log(self.name, f"found {len(signals)} hiring signals")
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

# ---- 6. Google Places — New/Changed Businesses ----
class GooglePlacesChanges(Collector):
    name = "google_places_changes"
    description = "Google Business Profile changes — reduced hours, new locations, name changes"
    source_type = "api"
    schedule = "daily"
    requires_key = "GOOGLE_MAPS_API_KEY"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
        if not key:
            return []
        signals = []
        # Search for recently opened businesses in target area
        queries = kwargs.get("queries", ["new business Minneapolis", "now open Minneapolis"])
        for q in queries[:3]:
            resp = self._get("https://maps.googleapis.com/maps/api/place/textsearch/json", params={
                "query": q, "key": key,
            })
            if not resp:
                continue
            for place in (resp.json().get("results") or [])[:10]:
                if place.get("business_status") == "OPERATIONAL":
                    signals.append(Signal(
                        business_name=place.get("name", ""),
                        location=place.get("formatted_address", ""),
                        signal_type="new_or_changed_business",
                        strength=self.default_strength,
                        source=self.name,
                        raw_data={"rating": place.get("rating"), "types": place.get("types", [])[:5]},
                        contact_hints={"address": place.get("formatted_address", "")},
                    ))
        _log(self.name, f"found {len(signals)} places signals")
        return signals


# ---- 7. Yelp Business Activity ----
class YelpActivity(Collector):
    name = "yelp_activity"
    description = "Yelp review velocity — businesses with sudden review spikes are growing fast"
    source_type = "api"
    schedule = "weekly"
    requires_key = "YELP_API_KEY"
    tier = "B"
    default_strength = 18.0

    def collect(self, **kwargs) -> list[Signal]:
        key = os.getenv("YELP_API_KEY", "").strip()
        if not key:
            return []
        signals = []
        location = kwargs.get("location", "Minneapolis, MN")
        categories = kwargs.get("categories", ["contractors", "restaurants", "auto", "landscaping"])
        for cat in categories[:4]:
            resp = self._get("https://api.yelp.com/v3/businesses/search", params={
                "location": location, "categories": cat, "sort_by": "review_count", "limit": 10,
            }, headers={"Authorization": f"Bearer {key}"})
            if not resp:
                continue
            for biz in (resp.json().get("businesses") or []):
                rc = biz.get("review_count", 0)
                if rc > 50:  # established business
                    signals.append(Signal(
                        business_name=biz.get("name", ""),
                        location=", ".join(biz.get("location", {}).get("display_address", [])),
                        signal_type="high_review_activity",
                        strength=self.default_strength + min(rc // 50, 10),
                        source=self.name,
                        source_url=biz.get("url", ""),
                        raw_data={"review_count": rc, "rating": biz.get("rating"), "category": cat},
                        contact_hints={"phone": biz.get("phone", "")},
                    ))
        _log(self.name, f"found {len(signals)} Yelp signals")
        return signals


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


# ---- 9. Minnesota SOS — UCC Filings ----
class MNSosUCCFilings(Collector):
    name = "mn_sos_ucc"
    description = "UCC filings approaching payoff — existing MCA about to be paid off = renewal window"
    source_type = "scrape"
    schedule = "weekly"
    tier = "A"
    default_strength = 32.0

    def collect(self, **kwargs) -> list[Signal]:
        # MN UCC search: https://mblsportal.sos.state.mn.us/UCC/Search
        _log(self.name, "MN SOS UCC search requires headless browser — planned for v0.2")
        return []


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
    description = "Businesses for sale — owners ready to exit, may need bridge capital or buyer matching"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        state = kwargs.get("state", "minnesota")
        resp = self._scrape_get(f"https://www.bizbuysell.com/minnesota-businesses-for-sale/")
        if not resp:
            return signals
        # Extract listing titles and asking prices
        titles = re.findall(r'class="listing-title[^"]*"[^>]*>([^<]+)<', resp.text)
        prices = re.findall(r'Asking Price:\s*\$([0-9,]+)', resp.text)
        for i, title in enumerate(titles[:20]):
            price = prices[i] if i < len(prices) else ""
            signals.append(Signal(
                business_name=title.strip(),
                location=state.title(),
                signal_type="business_for_sale",
                strength=self.default_strength,
                source=self.name,
                source_url=f"https://www.bizbuysell.com/minnesota-businesses-for-sale/",
                raw_data={"asking_price": price, "title": title.strip()},
            ))
        _log(self.name, f"found {len(signals)} BizBuySell signals")
        return signals


# ---- 16. Craigslist Business-for-Sale ----
class CraigslistBFS(Collector):
    name = "craigslist_bfs"
    description = "Craigslist business-for-sale — informal sellers, often motivated"
    source_type = "scrape"
    schedule = "daily"
    market = "micro_acq"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        city = kwargs.get("cl_city", "minneapolis")
        resp = self._scrape_get(f"https://{city}.craigslist.org/search/bfs")
        if not resp:
            return signals
        titles = re.findall(r'class="posting-title"[^>]*>\s*<span[^>]*>([^<]+)<', resp.text)
        if not titles:
            titles = re.findall(r'<a[^>]*class="titlestring"[^>]*>([^<]+)<', resp.text)
        for t in titles[:15]:
            signals.append(Signal(
                business_name=t.strip(),
                location=f"{city.title()}, MN",
                signal_type="business_for_sale_informal",
                strength=self.default_strength,
                source=self.name,
            ))
        _log(self.name, f"found {len(signals)} Craigslist signals")
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


# ---- 18. BBB — Recently Accredited Businesses ----
class BBBAccredited(Collector):
    name = "bbb_accredited"
    description = "BBB accredited businesses — established, credible, likely bankable"
    source_type = "scrape"
    schedule = "weekly"
    tier = "C"
    default_strength = 15.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.bbb.org/search?find_country=US&find_loc=Minneapolis%2C+MN&find_type=Category&page=1")
        if not resp:
            return signals
        names = re.findall(r'"businessName"\s*:\s*"([^"]+)"', resp.text)
        for name in names[:15]:
            signals.append(Signal(
                business_name=name.strip(),
                location="Minneapolis, MN",
                signal_type="bbb_accredited",
                strength=self.default_strength,
                source=self.name,
            ))
        _log(self.name, f"found {len(signals)} BBB signals")
        return signals


# ---- 19. WHOIS Domain Expiration ----
class WHOISDomainExpiry(Collector):
    name = "whois_domain_expiry"
    description = "Business domains nearing expiration — owner losing interest, possible exit candidate"
    source_type = "api"
    schedule = "monthly"
    market = "micro_acq"
    tier = "C"
    default_strength = 15.0

    def collect(self, **kwargs) -> list[Signal]:
        # Would check WHOIS for domains of known businesses
        _log(self.name, "WHOIS lookup requires domain list input — enrichment collector")
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


# ---- 22. Health Department Inspections ----
class HealthInspections(Collector):
    name = "health_inspections"
    description = "Restaurant health inspections — active restaurants are fundable; new ones need capital"
    source_type = "scrape"
    schedule = "monthly"
    tier = "C"
    default_strength = 12.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "Health inspections require county-specific portal — planned for v0.2")
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


# ---- 25. Reddit r/smallbusiness ----
class RedditSmallBusiness(Collector):
    name = "reddit_smallbusiness"
    description = "Reddit posts from business owners asking about financing — direct intent signal"
    source_type = "api"
    schedule = "daily"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        subreddits = ["smallbusiness", "Entrepreneur", "ecommerce"]
        keywords = ["working capital", "business loan", "cash flow", "need funding", "financing options", "MCA", "line of credit"]
        for sub in subreddits:
            resp = self._get(f"https://www.reddit.com/r/{sub}/search.json", params={
                "q": " OR ".join(keywords[:3]), "sort": "new", "t": "week", "limit": 10,
                "restrict_sr": "on",
            }, headers={"User-Agent": USER_AGENT})
            if not resp:
                continue
            try:
                posts = resp.json().get("data", {}).get("children", [])
                for post in posts:
                    d = post.get("data", {})
                    title = d.get("title", "")
                    author = d.get("author", "")
                    signals.append(Signal(
                        business_name=f"Reddit user: {author}",
                        signal_type="financing_inquiry",
                        strength=self.default_strength,
                        source=self.name,
                        source_url=f"https://www.reddit.com{d.get('permalink', '')}",
                        raw_data={"title": title[:200], "subreddit": sub, "score": d.get("score", 0)},
                    ))
            except Exception:
                pass
        _log(self.name, f"found {len(signals)} Reddit signals")
        return signals


# ---- 26-30. Industry-Specific Job Board Scrapers ----

class _IndustryJobCollector(Collector):
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 18.0

    def _search_indeed_industry(self, industry_q: str, location: str) -> list[Signal]:
        signals = []
        q = quote_plus(industry_q)
        loc = quote_plus(location)
        resp = self._scrape_get(f"https://www.indeed.com/jobs?q={q}&l={loc}&sort=date&limit=15")
        if not resp:
            return signals
        companies = re.findall(r'data-testid="company-name"[^>]*>([^<]+)<', resp.text)
        if not companies:
            companies = re.findall(r'"companyName"\s*:\s*"([^"]+)"', resp.text)
        seen = set()
        for co in companies:
            co = co.strip()
            if co and co not in seen:
                seen.add(co)
                signals.append(Signal(
                    business_name=co,
                    location=location,
                    signal_type=f"hiring_{self.name}",
                    strength=self.default_strength,
                    source=self.name,
                    raw_data={"industry_query": industry_q},
                ))
        return signals


class HVACHiring(_IndustryJobCollector):
    name = "hvac_hiring"
    description = "HVAC companies hiring — boring cash-flow businesses that need capital to grow"

    def collect(self, **kwargs) -> list[Signal]:
        s = self._search_indeed_industry("HVAC technician", kwargs.get("location", "Minneapolis, MN"))
        _log(self.name, f"found {len(s)} HVAC hiring signals")
        return s


class PlumbingHiring(_IndustryJobCollector):
    name = "plumbing_hiring"
    description = "Plumbing companies hiring — service businesses with strong recurring revenue"

    def collect(self, **kwargs) -> list[Signal]:
        s = self._search_indeed_industry("plumber", kwargs.get("location", "Minneapolis, MN"))
        _log(self.name, f"found {len(s)} plumbing signals")
        return s


class ElectricalHiring(_IndustryJobCollector):
    name = "electrical_hiring"
    description = "Electrical contractors hiring"

    def collect(self, **kwargs) -> list[Signal]:
        s = self._search_indeed_industry("electrician", kwargs.get("location", "Minneapolis, MN"))
        _log(self.name, f"found {len(s)} electrical signals")
        return s


class LandscapingHiring(_IndustryJobCollector):
    name = "landscaping_hiring"
    description = "Landscaping companies hiring — seasonal businesses that need bridge capital"

    def collect(self, **kwargs) -> list[Signal]:
        s = self._search_indeed_industry("landscaping crew", kwargs.get("location", "Minneapolis, MN"))
        _log(self.name, f"found {len(s)} landscaping signals")
        return s


class TruckingHiring(_IndustryJobCollector):
    name = "trucking_hiring"
    description = "Trucking companies hiring drivers — capital-intensive, perfect MCA candidates"

    def collect(self, **kwargs) -> list[Signal]:
        s = self._search_indeed_industry("CDL driver", kwargs.get("location", "Minneapolis, MN"))
        _log(self.name, f"found {len(s)} trucking signals")
        return s


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


# ---- 32. Franchise Disclosure Documents ----
class FranchiseDisclosures(Collector):
    name = "franchise_disclosures"
    description = "Franchise FDDs — new franchisees need startup capital"
    source_type = "scrape"
    schedule = "monthly"
    market = "all"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        _log(self.name, "FDD scraping from state regulators — planned for v0.2")
        return []


# ---- 33. GoFundMe Business Campaigns ----
class GoFundMeBusiness(Collector):
    name = "gofundme_business"
    description = "GoFundMe business campaigns — owners publicly seeking capital = direct intent"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 25.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        location = kwargs.get("location", "Minneapolis")
        resp = self._scrape_get(f"https://www.gofundme.com/discover/small-business-fundraiser?location={quote_plus(location)}")
        if not resp:
            return signals
        titles = re.findall(r'"title"\s*:\s*"([^"]+)"', resp.text)
        for t in titles[:15]:
            if any(kw in t.lower() for kw in ["business", "shop", "store", "restaurant", "startup", "company"]):
                signals.append(Signal(
                    business_name=t.strip(),
                    location=location,
                    signal_type="crowdfunding_capital_need",
                    strength=self.default_strength,
                    source=self.name,
                ))
        _log(self.name, f"found {len(signals)} GoFundMe signals")
        return signals


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


# ---- 37. Product Hunt Launches ----
class ProductHuntLaunches(Collector):
    name = "product_hunt"
    description = "Recent Product Hunt launches — startups that just launched need growth capital"
    source_type = "scrape"
    schedule = "daily"
    market = "rbf"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.producthunt.com/")
        if not resp:
            return signals
        # Extract product names from the homepage
        names = re.findall(r'"name"\s*:\s*"([^"]{3,60})"', resp.text)
        seen = set()
        for n in names[:20]:
            n = n.strip()
            if n and n not in seen and len(n) > 3:
                seen.add(n)
                signals.append(Signal(
                    business_name=n,
                    signal_type="product_launch",
                    strength=self.default_strength,
                    source=self.name,
                    source_url="https://www.producthunt.com",
                ))
        _log(self.name, f"found {len(signals)} Product Hunt signals")
        return signals


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


# ---- 53. Franchise Resale Listings ----
class FranchiseResale(Collector):
    name = "franchise_resale"
    description = "Franchise resale listings — franchisee wants to exit"
    source_type = "scrape"
    schedule = "weekly"
    market = "micro_acq"
    tier = "B"
    default_strength = 22.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        resp = self._scrape_get("https://www.franchisegator.com/resales/")
        if not resp:
            return signals
        titles = re.findall(r'<h[23][^>]*>([^<]+)</h[23]>', resp.text)
        for t in titles[:15]:
            if len(t.strip()) > 5:
                signals.append(Signal(
                    business_name=t.strip(),
                    signal_type="franchise_resale",
                    strength=self.default_strength,
                    source=self.name,
                ))
        _log(self.name, f"found {len(signals)} franchise resale signals")
        return signals


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
        _log(self.name, "County court records require per-county scraper — planned for v0.2")
        return []


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


# ---- 63. Yelp "New Business" Filter ----
class YelpNewBusinesses(Collector):
    name = "yelp_new_businesses"
    description = "Recently opened businesses on Yelp — startups needing capital"
    source_type = "scrape"
    schedule = "weekly"
    tier = "B"
    default_strength = 20.0

    def collect(self, **kwargs) -> list[Signal]:
        signals = []
        location = kwargs.get("location", "Minneapolis, MN")
        resp = self._scrape_get(f"https://www.yelp.com/search?find_desc=new+business&find_loc={quote_plus(location)}&attrs=BusinessOpenedRecently")
        if not resp:
            return signals
        names = re.findall(r'"name"\s*:\s*"([^"]{3,60})"', resp.text)
        for n in set(list(names)[:10]):
            signals.append(Signal(
                business_name=n.strip(), location=location,
                signal_type="new_business_opened", strength=self.default_strength,
                source=self.name,
            ))
        _log(self.name, f"found {len(signals)} new business signals")
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
    GooglePlacesChanges,       # 6
    YelpActivity,              # 7
    MNSosNewBusinesses,        # 8
    WISosBusinesses,           # 10
    IASosBusinesses,           # 11
    OpenCorporatesSearch,      # 14
    BizBuySellListings,        # 15
    CraigslistBFS,             # 16
    USPTOTrademarks,           # 20
    BuildingPermits,           # 21
    LiquorLicenses,            # 23
    RedditSmallBusiness,       # 25
    HVACHiring,                # 26
    PlumbingHiring,            # 27
    ElectricalHiring,          # 28
    LandscapingHiring,         # 29
    TruckingHiring,            # 30
    LoopNetLeases,             # 31
    FranchiseDisclosures,      # 32
    GoFundMeBusiness,          # 33
    ShopifyStoreDetector,      # 35
    ProductHuntLaunches,       # 37
    AcquireComListings,        # 38
    EquipmentAuctions,         # 47
    FranchiseResale,           # 53
    CountyCourtRecords,        # 55

    # Tier C — Contextual / enrichment
    NDSosBusinesses,           # 12
    SDSosBusinesses,           # 13
    GoogleTrends,              # 17
    BBBAccredited,             # 18
    WHOISDomainExpiry,         # 19
    HealthInspections,         # 22
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
    YelpNewBusinesses,         # 63
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


def rank_signals(signals: list[Signal], known_businesses: set[str] = None,
                 past_clients: set[str] = None) -> list[dict]:
    """
    Score and rank all signals, grouping by business name.
    Returns a sorted list of dicts ready for the dashboard.
    """
    known = known_businesses or set()
    clients = past_clients or set()

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
    by_tier = {"A": 0, "B": 0, "C": 0}
    by_market = {}
    by_type = {}
    for c in collectors:
        by_tier[c.tier] = by_tier.get(c.tier, 0) + 1
        by_market[c.market] = by_market.get(c.market, 0) + 1
        by_type[c.source_type] = by_type.get(c.source_type, 0) + 1
    return {
        "total_collectors": total,
        "enabled": enabled,
        "disabled_missing_key": total - enabled,
        "by_tier": by_tier,
        "by_market": by_market,
        "by_source_type": by_type,
        "collectors": [c.status() for c in collectors],
    }
