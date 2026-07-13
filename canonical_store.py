"""
Dual-write shim: mirror scraped auction records into the canonical store.

Phase 2 of the auction-data consolidation plan (cc-market-survey:
auction-data-consolidation-plan.md). While the S3/CSV pipeline keeps running
unchanged, each scraper run also pushes its new records through the canonical
store's one door — rpc public.auction_upsert_listings — so the two stores can
be diffed for a week before cutover.

Entirely env-gated and failure-isolated:
  * SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY unset  -> no-op (logs one line)
  * any error                                       -> logged, never raises,
                                                       the CSV path is
                                                       unaffected either way

Mapping rules mirror auction-store/backfill/backfill.py in cc-market-survey
(the one-shot historical loader); this module is the live-path equivalent.
Key decisions shared with the backfill:
  * BaT rows are keyed by URL slug (matches how garage-draft rows dedupe).
  * Reserve-not-met amounts land in current_bid, never price.
  * Non-USD amounts keep their currency and set needs_review.
"""

import datetime as dt
import json
import os
import re
import time
import urllib.error
import urllib.request

BATCH_SIZE = 200
PLAUSIBLE_MIN, PLAUSIBLE_MAX = 100, 10_000_000

CURRENCY_PATTERNS = [
    ("USD", re.compile(r"\bUSD\b")),
    ("EUR", re.compile(r"\bEUR\b|€")),
    ("GBP", re.compile(r"\bGBP\b|£")),
    ("CAD", re.compile(r"\bCAD\b")),
    ("AUD", re.compile(r"\bAUD\b")),
    ("CHF", re.compile(r"\bCHF\b")),
]


def configured():
    return bool(os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))


# --------------------------------------------------------------------------
# Parsers (kept in lockstep with backfill.py)
# --------------------------------------------------------------------------

def parse_money(raw):
    """Raw sale string -> (amount, currency, suspicious)."""
    if raw is None:
        return None, None, False
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none"):
        return None, None, False

    currency = "USD"
    for code, pat in CURRENCY_PATTERNS:
        if pat.search(s):
            currency = code
            break
    else:
        if "$" not in s and not re.search(r"\d", s):
            return None, None, False

    m = re.search(r"[\d][\d.,'’]*", s)
    if not m:
        return None, currency, False
    num = m.group(0).replace("’", "'")

    suspicious = False
    if currency == "CHF":
        num = num.replace("'", "")
    if currency in ("EUR", "CHF"):
        if "." in num and "," in num:
            num = num.replace(".", "").replace(",", ".")
        elif re.fullmatch(r"\d{1,3}(\.\d{3})+", num):
            num = num.replace(".", "")
        else:
            num = num.replace(",", "")
    elif "," in num and not re.fullmatch(r"\d{1,3}(,\d{3})*(\.\d+)?", num):
        # Cars & Bids corruption: '$38,75012' — junk digits after the comma
        # group. Keep the first 3 digits after the first comma, flag it.
        parts = num.split(",")
        num = re.sub(r"[^\d]", "", parts[0]) + re.sub(r"[^\d]", "", "".join(parts[1:]))[:3]
        suspicious = True
    else:
        num = num.replace(",", "")

    try:
        amount = float(num)
    except ValueError:
        return None, currency, True
    if amount != amount:  # NaN
        return None, currency, True
    if PLAUSIBLE_MIN <= amount <= PLAUSIBLE_MAX:
        return round(amount, 2), currency, suspicious
    return None, currency, True


def parse_count(raw):
    if raw is None:
        return None
    digits = re.sub(r"[^\d]", "", str(raw))
    return int(digits) if digits else None


DATE_FORMATS = ("%m/%d/%y", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d")


def parse_date(raw):
    if raw is None:
        return None
    s = str(raw).strip().replace("on ", "")
    if not s or s.lower() == "nan":
        return None
    for fmt in DATE_FORMATS:
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_epoch(raw):
    try:
        v = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return dt.datetime.fromtimestamp(v, dt.timezone.utc).isoformat()


def bat_slug(url):
    if not url:
        return None
    m = re.search(r"/listing/([^/?#]+)", str(url))
    return m.group(1).strip("/") if m else None


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "null") else None


# --------------------------------------------------------------------------
# Record mappers
# --------------------------------------------------------------------------

def map_bat_record(row):
    """One bat_scraper.py record dict -> upsert batch item (or None to skip)."""
    url = _clean(row.get("auction_url"))
    slug = bat_slug(url)
    if not slug:
        return None

    sale_type = (_clean(row.get("sale_type")) or "").lower()
    if sale_type == "sold":
        outcome = "sold"
    elif sale_type in ("high bid", "reserve not met", "bid to"):
        outcome = "reserve_not_met"
    elif _clean(row.get("error")) and not _clean(row.get("title")):
        return None
    else:
        outcome = "unknown"

    amount, currency, suspicious = parse_money(row.get("sale_amount"))
    ended_at = (parse_date(row.get("sale_date"))
                or parse_epoch(row.get("end_timestamp"))
                or parse_date(row.get("end_date")))

    payload = {
        "url": url,
        "raw_title": _clean(row.get("title")),
        "make": _clean(row.get("make")),
        "model": _clean(row.get("model")),
        "year": parse_count(row.get("year")),
        "status": "ended",
        "outcome": outcome,
        "currency": currency or "USD",
        "bid_count": parse_count(row.get("bids")),
        "views": parse_count(row.get("views")),
        "watchers": parse_count(row.get("watchers")),
        "comments": parse_count(row.get("comments")),
        "ended_at": ended_at,
        "raw": {k: v for k, v in row.items() if _clean(v) is not None},
    }
    if outcome == "sold":
        payload["price"] = amount
    elif amount is not None:
        payload["current_bid"] = amount
    if suspicious or (currency and currency != "USD"):
        payload["needs_review"] = True

    return {
        "source_id": "bat",
        "source_listing_id": slug,
        "entered_by": "scraper",
        "payload": {k: v for k, v in payload.items() if v is not None},
    }


def map_cnb_record(row):
    """One cnb_scraper.py record dict -> upsert batch item (or None to skip)."""
    url = _clean(row.get("auction_url"))
    if not url:
        return None
    m = re.search(r"/auctions/([^/?#]+)", url)
    slug = m.group(1).strip("/") if m else None
    if not slug:
        return None

    sale_type = (_clean(row.get("sale_type")) or "").lower()
    if "sold" in sale_type:
        outcome = "sold"
    elif "reserve" in sale_type or "bid to" in sale_type:
        outcome = "reserve_not_met"
    else:
        outcome = "unknown"

    amount, currency, suspicious = parse_money(row.get("sale_amount"))
    payload = {
        "url": url,
        "raw_title": _clean(row.get("model")),  # cnb 'model' holds the title
        "make": _clean(row.get("make")),
        "year": parse_count(row.get("year")),
        "vin": _clean(row.get("vin")),
        "mileage": parse_count(row.get("mileage")),
        "status": "ended",
        "outcome": outcome,
        "currency": currency or "USD",
        "bid_count": parse_count(row.get("bids")),
        "views": parse_count(row.get("views")),
        "comments": parse_count(row.get("comments")),
        "ended_at": parse_date(row.get("sale_date")),
        "raw": {k: v for k, v in row.items() if _clean(v) is not None},
    }
    if outcome == "sold":
        payload["price"] = amount
    elif amount is not None:
        payload["current_bid"] = amount
    if suspicious or (currency and currency != "USD"):
        payload["needs_review"] = True

    return {
        "source_id": "carsandbids",
        "source_listing_id": slug,
        "entered_by": "scraper",
        "payload": {k: v for k, v in payload.items() if v is not None},
    }


# --------------------------------------------------------------------------
# Push
# --------------------------------------------------------------------------

def _post_batch(url, key, batch):
    req = urllib.request.Request(
        f"{url.rstrip('/')}/rest/v1/rpc/auction_upsert_listings",
        data=json.dumps({"p_batch": batch}).encode(),
        method="POST",
    )
    req.add_header("apikey", key)
    req.add_header("Authorization", f"Bearer {key}")
    req.add_header("Content-Type", "application/json")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                resp.read()
            return True
        except urllib.error.HTTPError as e:
            detail = e.read().decode()[:300]
            if e.code >= 500 and attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f"   [canonical] HTTP {e.code}: {detail}")
            return False
        except urllib.error.URLError as e:
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f"   [canonical] network error: {e}")
            return False


def push_records(records, mapper, label):
    """Mirror scraped records into the canonical store. Never raises."""
    try:
        if not configured():
            print(f"[canonical] SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY not set — "
                  f"skipping canonical-store dual-write ({label})")
            return 0
        items, seen = [], set()
        for row in records:
            item = mapper(row)
            if item is None:
                continue
            k = (item["source_id"], item["source_listing_id"])
            if k in seen:
                continue
            seen.add(k)
            items.append(item)
        if not items:
            print(f"[canonical] no mappable {label} records to mirror")
            return 0

        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        ok = 0
        for i in range(0, len(items), BATCH_SIZE):
            chunk = items[i:i + BATCH_SIZE]
            if _post_batch(url, key, chunk):
                ok += len(chunk)
        print(f"[canonical] mirrored {ok}/{len(items)} {label} listings to canonical store")
        return ok
    except Exception as e:  # dual-write must never break the CSV pipeline
        print(f"[canonical] dual-write failed (non-fatal): {e}")
        return 0


def push_bat_records(records):
    return push_records(records, map_bat_record, "BaT")


def push_cnb_records(records):
    return push_records(records, map_cnb_record, "C&B")


if __name__ == "__main__":
    # Parser/mapper self-tests (no network).
    assert parse_money("USD $38,250") == (38250.0, "USD", False)
    assert parse_money("EUR €120.000") == (120000.0, "EUR", False)
    assert parse_money("$38,75012") == (38750.0, "USD", True)
    assert parse_money("$10") == (None, "USD", True)
    item = map_bat_record({
        "auction_url": "https://bringatrailer.com/listing/1990-bmw-m3-77/",
        "title": "1990 BMW M3", "make": "BMW", "model": "M3", "year": 1990,
        "sale_type": "sold", "sale_amount": "USD $88,000",
        "sale_date": "6/15/26", "bids": "51", "views": "22,411",
        "watchers": "812", "comments": "140",
    })
    assert item["source_listing_id"] == "1990-bmw-m3-77"
    assert item["payload"]["price"] == 88000.0
    assert item["payload"]["views"] == 22411
    rnm = map_bat_record({
        "auction_url": "https://bringatrailer.com/listing/rnm-1/",
        "title": "RNM", "sale_type": "high bid", "sale_amount": "USD $61,500",
    })
    assert "price" not in rnm["payload"] and rnm["payload"]["current_bid"] == 61500.0
    cnb = map_cnb_record({
        "auction_url": "https://carsandbids.com/auctions/abc123/2020-supra",
        "model": "2020 Toyota Supra", "make": "Toyota", "year": 2020,
        "sale_type": "sold", "sale_amount": "$41,000", "vin": "JT1234",
        "mileage": "23,000 mi", "bids": 30, "views": 9000, "comments": 55,
        "sale_date": "7/1/26", "scraped_date": "2026-07-01 12:00:00",
    })
    assert cnb["source_id"] == "carsandbids"
    assert cnb["payload"]["vin"] == "JT1234" and cnb["payload"]["mileage"] == 23000
    print("canonical_store selftest: all assertions passed")
