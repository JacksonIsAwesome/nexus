"""
config.py — NEXUS targeting configuration
Region, city, and industry focus for deal sourcing. Override via env vars.
"""
import os

# Target regions for deal sourcing (your geographic footprint)
TARGET_STATES = os.getenv("NEXUS_STATES", "MN,WI,IA,SD,ND").split(",")
TARGET_CITIES = os.getenv(
    "NEXUS_CITIES",
    "Minneapolis,MN;St Paul,MN;Milwaukee,WI;Madison,WI;Des Moines,IA"
).split(";")

# Industry focus for RBF/MCA — businesses with steady receivables / card volume
TARGET_INDUSTRIES = [
    "restaurant", "food_service", "construction", "contractor",
    "trucking", "logistics", "medical", "healthcare", "retail",
    "ecommerce", "saas", "technology", "manufacturing", "wholesale",
    "auto_repair", "personal_service", "fitness", "beauty", "landscaping",
]

# Lead quality filters
MIN_MONTHLY_REVENUE_ESTIMATE = 10000   # below this = not worth RBF
MAX_DEBT_STACK_PREFERRED = 2           # prefer 0-2 existing positions, flag 3+


def cities_as_list() -> list[str]:
    """Return cities as clean 'City, ST' strings."""
    out = []
    for c in TARGET_CITIES:
        c = c.strip()
        if "," in c:
            city, st = c.rsplit(",", 1)
            out.append(f"{city.strip()}, {st.strip()}")
        elif c:
            out.append(c)
    return out
