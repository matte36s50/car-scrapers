#!/usr/bin/env python3
"""Fail loudly when a scraped feed has stopped collecting.

Cars & Bids stopped adding rows in October 2025 and nobody noticed for eleven
months, because every layer of the pipeline reported success:

  * cnb_scraper.py exits 1 when auction discovery returns nothing, but the
    workflow ran it as `python cnb_scraper.py || echo "..."`, so the non-zero
    exit became a successful echo;
  * the step also carried continue-on-error: true;
  * the upload step runs `if: always()` and re-pushed the unchanged cnb.csv,
    so the S3 object's Last-Modified advanced every single day.

The result was a file that looked fresh to every downstream consumer while its
newest record stayed eleven months old. This script checks the thing that
actually matters — how recent the newest RECORD is, not the file — so a dead
feed is visible the day it dies.

Usage:
  python check_feed_freshness.py cnb.csv --max-age-days 14 --label "Cars & Bids"
  python check_feed_freshness.py bat.csv --max-age-days 3  --label "Bring a Trailer"

Exit codes: 0 fresh, 1 stale, 2 unreadable.
"""
import argparse
import csv
import datetime
import os
import re
import sys

DATE_PATTERNS = (
    (re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2})'), lambda m: (int(m.group(1)), int(m.group(2)), int(m.group(3)))),
    (re.compile(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$'), lambda m: (
        int(m.group(3)) + 2000 if int(m.group(3)) < 100 else int(m.group(3)),
        int(m.group(1)), int(m.group(2)))),
)


def parse_date(value):
    value = (value or '').strip()
    for pattern, unpack in DATE_PATTERNS:
        m = pattern.match(value)
        if not m:
            continue
        try:
            year, month, day = unpack(m)
            if not (1990 <= year <= datetime.date.today().year + 1):
                return None
            return datetime.date(year, month, day)
        except ValueError:
            return None
    return None


def newest_record(path, column):
    newest = None
    rows = 0
    with open(path, newline='', encoding='utf-8', errors='replace') as fh:
        for row in csv.DictReader(fh):
            rows += 1
            parsed = parse_date(row.get(column))
            if parsed and (newest is None or parsed > newest):
                newest = parsed
    return newest, rows


def annotate(level, message):
    """GitHub Actions annotation, so the failure is visible in the run summary."""
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print(f"::{level} ::{message}")
    print(message)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('path')
    ap.add_argument('--column', default='sale_date')
    ap.add_argument('--max-age-days', type=int, default=14)
    ap.add_argument('--label', default=None)
    args = ap.parse_args()
    label = args.label or os.path.basename(args.path)

    if not os.path.exists(args.path):
        annotate('error', f"{label}: {args.path} does not exist — nothing was collected.")
        return 2

    newest, rows = newest_record(args.path, args.column)
    if newest is None:
        annotate('error', f"{label}: no parseable '{args.column}' in {rows:,} rows.")
        return 2

    age = (datetime.date.today() - newest).days
    summary = f"{label}: {rows:,} rows, newest record {newest.isoformat()} ({age} days old)"

    if age > args.max_age_days:
        annotate('error',
                 f"{summary} — STALE, the feed has added nothing for longer than "
                 f"the {args.max_age_days}-day limit. The upload may still be "
                 f"refreshing the object's timestamp, which is why this checks "
                 f"records rather than file age.")
        return 1

    print(f"✓ {summary}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
