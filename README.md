# NEXUS — Deal Intelligence Engine (Phase 1)

A signal-based deal-sourcing, qualification, and lender-matching engine for RBF/MCA
brokering. Built market-agnostic so it expands into micro-acquisitions, note brokering,
and wholesaling through the same architecture.

## What's in Phase 1

- **Signal engine** — 55 collectors that detect business capital-need signals (gov
  contracts, hiring surges, UCC filings, business-for-sale listings, financing inquiries,
  seasonal patterns, and more). Each is an independent module; adding source #56 is one
  small class. Signals are scored 0-100 (strength + stacking + recency + industry + relationship).
- **Qualification engine** — the 8-question pre-screen. Grades deals A/B/C/D, flags instant
  disqualifiers (stacking, declining revenue, excessive NSFs), and protects your lender
  relationships by catching bad deals before submission.
- **Lender matching** — ranks your lender panel by paper grade, revenue, credit, amount,
  speed, and your learned approval rate with each. Ships with 6 real lenders pre-seeded.
- **Pipeline** — Signal → Contacted → Qualifying → Submitted → Approved → Funded, with
  time-to-fund tracking for your speed guarantee.
- **Outreach drafting** — signal-aware email templates with pre-filled mailto: links. You
  send from your own mail app (no automated-messaging / TCPA exposure).
- **Learning loop foundation** — every lender outcome (submitted/approved/funded) is
  recorded and feeds back into matching.

## Run locally
```bash
cd nexus
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```
Open http://localhost:8000/dashboard/

## Deploy to Railway
Push to GitHub, create a Railway project from the repo. The Dockerfile and railway.toml
(with the `sh -c` PORT wrapper baked in) are picked up automatically. Add a Postgres
plugin for persistence. Health check is at /api/health.

## API keys (all optional)
- `SAM_GOV_API_KEY` — free at sam.gov, enables federal contract award signals
- `GOOGLE_MAPS_API_KEY` — enables Google Business Profile signals
- `YELP_API_KEY` — free, enables review-velocity signals
- `ANTHROPIC_API_KEY` — enables AI outreach personalization (later phase)

Without keys, ~52 of 55 collectors still run (the scrapers and free APIs).

## Signal sources note
The scraper collectors include a 1.5s courtesy delay and browser headers. Some sources
(state SOS portals, LinkedIn, Glassdoor) need a headless browser and are stubbed for v0.2.
Always respect each site's terms of service before enabling a scraper. If you add an
anti-ban proxy/rotation service, wire it into the `_scrape_get` method in collectors.py.

## What's next (later phases)
- Phase 2: bank statement analyzer (PDF parsing + metrics extraction)
- Phase 3: relationship engine (lifecycle tracking, renewal prediction, referral partners)
- Phase 4: learning loop (signal weights auto-tune from your close data)
- Phase 5: market expansion (micro-acquisitions, notes)
