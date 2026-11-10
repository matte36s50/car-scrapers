#!/usr/bin/env python3
"""Offline tests for the Cars & Bids extraction helpers.

Covers the two regressions that put 52 malformed rows into cnb.csv: model
names carrying year and make, and comments silently reading 0.

Run: python test_cnb_extraction.py
"""
import io
import json
import os
import re
import sys
import tempfile
from unittest import mock

import pandas as pd

import canonical_store
import cnb_scraper
from cnb_scraper import derive_model_from_title, merge_store_rows

failures = 0


def check(name, actual, expected):
    global failures
    if actual != expected:
        failures += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       actual   {actual!r}")
    else:
        print(f"  ok   {name}")


print("derive_model_from_title — fallback when the spec list has no Model row")
# The four titles that actually landed in cnb.csv on 2026-09-17.
check('strips year and make', derive_model_from_title('2022 Ford Bronco', 'Ford', 2022), 'Bronco')
check('multi-word make', derive_model_from_title('2012 Land Rover LR4 HSE', 'Land Rover', 2012), 'LR4 HSE')
check('keeps trim words', derive_model_from_title('2001 Nissan Pathfinder LE 4x4', 'Nissan', 2001), 'Pathfinder LE 4x4')
check('porsche', derive_model_from_title('2014 Porsche Cayenne', 'Porsche', 2014), 'Cayenne')
check('make casing ignored', derive_model_from_title('2014 PORSCHE Cayenne', 'porsche', 2014), 'Cayenne')
check('year alone when make unknown', derive_model_from_title('1988 BMW M3', None, None), 'BMW M3')
check('no year prefix is left alone', derive_model_from_title('M3 Sport Evolution', 'BMW', None), 'M3 Sport Evolution')
check('empty title', derive_model_from_title('', 'BMW', 1988), '')
check('none title', derive_model_from_title(None, 'BMW', 1988), '')
check('make appearing mid-name is not stripped',
      derive_model_from_title('2005 Ford Ford GT', 'Ford', 2005), 'Ford GT')

print("\ncomment-count recovery from page text")
# The scraper falls back to the comments heading when ul.stats no longer
# carries the row. Empty (not 0) when absent, so the MII drops the input
# instead of ranking the car worst on it.
def comments_from(body):
    m = re.search(r'(\d[\d,]*)\s+Comments?\b', body or '', re.IGNORECASE)
    return int(m.group(1).replace(',', '')) if m else ''

check('plural heading', comments_from('Bid history   47 Comments   Seller'), 47)
check('singular', comments_from('1 Comment'), 1)
check('thousands separator', comments_from('1,204 Comments'), 1204)
check('case insensitive', comments_from('12 COMMENTS'), 12)
check('absent -> empty, not zero', comments_from('no such section here'), '')
check('empty body -> empty', comments_from(''), '')
check('does not match a bare word', comments_from('Comments'), '')

print("\nthe regression these guard against")
# Every one of the 52 bad rows had a year-prefixed model AND comments == 0.
bad = ['2022 Ford Bronco', '2014 Porsche Cayenne',
       '2012 Land Rover LR4 HSE', '2001 Nissan Pathfinder LE 4x4']
makes = ['Ford', 'Porsche', 'Land Rover', 'Nissan']
fixed = [derive_model_from_title(t, mk, None) for t, mk in zip(bad, makes)]
check('no repaired model starts with a year',
      any(re.match(r'^(19|20)\d{2}\s', m) for m in fixed), False)
check('no repaired model still contains its make',
      any(mk.lower() in m.lower() for m, mk in zip(fixed, makes)), False)

print("\nthe daily pipeline keeps what it scrapes")
# main.yml downloads cnb.csv before the scraper runs and uploads that local
# file after it. The scraper used to leave the local copy as downloaded, so
# its own upload was overwritten a minute later, every run from 2025-10-24.
with tempfile.TemporaryDirectory() as d:
    here = os.getcwd()
    os.chdir(d)
    try:
        pd.DataFrame([{"auction_url": "https://carsandbids.com/auctions/old/2019-bmw-m2", "year": 2019}]
                     ).to_csv("cnb.csv", index=False)            # as main.yml downloaded it
        updated = pd.DataFrame([
            {"auction_url": "https://carsandbids.com/auctions/old/2019-bmw-m2", "year": 2019},
            {"auction_url": "https://carsandbids.com/auctions/new/2004-bmw-m3", "year": 2004}])
        s3 = mock.Mock()
        with mock.patch.object(cnb_scraper.boto3, "client", return_value=s3), \
                mock.patch("sys.stdout", new=io.StringIO()):
            ok = cnb_scraper.upload_updated_cnb_csv(updated)
        local = pd.read_csv("cnb.csv")
        check('upload reports success', ok, True)
        check('the upload goes to cnb.csv in the bucket', s3.upload_file.call_args[0][1:], ("my-mii-reports", "cnb.csv"))
        check('the local cnb.csv is what was uploaded, not the stale download',
              list(local["auction_url"]), list(updated["auction_url"]))
        check('no temp file is left behind', os.path.exists(cnb_scraper.TEMP_LOCAL_FILE), False)
    finally:
        os.chdir(here)

print("\nturning a page of past auctions")
check("listing ids, ignoring slugs, queries and other pages",
      cnb_scraper.auction_ids(["https://carsandbids.com/auctions/3v0W1D5b/2004-porsche-911?x=1",
                               "https://carsandbids.com/auctions/KYlz5JEw/2006-bmw-z4",
                               "https://carsandbids.com/past-auctions/", None]),
      {"3v0W1D5b", "KYlz5JEw"})

print("\nsaving never drops what another run saved meanwhile")
mine = pd.DataFrame([{"auction_url": "u/a", "sale_type": "sold"}, {"auction_url": "u/b", "sale_type": "sold"}])
theirs = pd.DataFrame([{"auction_url": "u/a", "sale_type": "OLD"}, {"auction_url": "u/c", "sale_type": "sold", "watchers": 3}])
merged = cnb_scraper.merge_with_current(mine, theirs)
check("a listing only the other run has is kept", sorted(merged["auction_url"]), ["u/a", "u/b", "u/c"])
check("where both have one, this run's row wins", merged.set_index("auction_url").loc["u/a", "sale_type"], "sold")
check("nothing to merge leaves the frame as it was", cnb_scraper.merge_with_current(mine, None) is mine, True)

with tempfile.TemporaryDirectory() as d:
    here = os.getcwd()
    os.chdir(d)
    try:
        s3 = mock.Mock()
        s3.download_file.side_effect = lambda b, k, f: theirs.to_csv(f, index=False)
        with mock.patch.object(cnb_scraper.boto3, "client", return_value=s3), \
                mock.patch("sys.stdout", new=io.StringIO()):
            cnb_scraper.upload_updated_cnb_csv(mine)
        check("the uploaded file includes the other run's listing",
              sorted(pd.read_csv("cnb.csv")["auction_url"]), ["u/a", "u/b", "u/c"])
    finally:
        os.chdir(here)

print("\nrestoring what the canonical store kept")
existing = pd.DataFrame([
    {"model": "M2", "make": "BMW", "auction_url": "https://carsandbids.com/auctions/a/2019-bmw-m2",
     "year": 2019.0, "comments": 31, "watchers": 5},
])
store = [
    # already in cnb.csv: left exactly as it is
    {"model": "CHANGED", "make": "BMW", "auction_url": "https://carsandbids.com/auctions/a/2019-bmw-m2", "year": 2019},
    # scraped after the 2026-09-17 fix: restored untouched
    {"model": "M3", "make": "BMW", "auction_url": "https://carsandbids.com/auctions/b/2004-bmw-m3",
     "year": 2004, "comments": 0, "scraped_date": "2026-10-02 06:10:00"},
    # scraped before it: the title sits in `model`, comments were never read
    {"model": "2022 Ford Bronco Badlands", "make": "Ford", "comments": 0, "year": 2022,
     "auction_url": "https://carsandbids.com/auctions/c/2022-ford-bronco", "scraped_date": "2026-08-05 06:10:00"},
    # the same listing pushed twice: the later scrape wins
    {"model": "Cayenne", "make": "Porsche", "sale_type": "reserve not met", "year": 2014,
     "auction_url": "https://carsandbids.com/auctions/d/2014-porsche-cayenne", "scraped_date": "2026-10-01 06:00:00"},
    {"model": "Cayenne", "make": "Porsche", "sale_type": "sold", "year": 2014,
     "auction_url": "https://carsandbids.com/auctions/d/2014-porsche-cayenne", "scraped_date": "2026-10-03 06:00:00"},
    # not a listing row
    {"model": "junk"}, None,
]
merged, restored, repaired = merge_store_rows(existing, store)
by_url = {r["auction_url"].rsplit("/", 1)[-1]: r for r in merged.to_dict("records")}
check('three rows come back', (restored, len(merged)), (3, 4))
check('a row cnb.csv already has is not touched', by_url["2019-bmw-m2"]["model"], "M2")
check('a good row is restored as scraped', (by_url["2004-bmw-m3"]["model"], by_url["2004-bmw-m3"]["comments"]), ("M3", 0))
check('a title in `model` loses its year and make', by_url["2022-ford-bronco"]["model"], "Bronco Badlands")
check('and its unread comment count is empty, not zero', by_url["2022-ford-bronco"]["comments"], "")
check('one row was repaired', repaired, 1)
check('the later scrape of a listing wins', by_url["2014-porsche-cayenne"]["sale_type"], "sold")
check("cnb.csv's columns keep their order, new ones go last",
      list(merged.columns), ["model", "make", "auction_url", "year", "comments", "watchers", "scraped_date", "sale_type"])
same, n, _ = merge_store_rows(merged, store)
check('running it again restores nothing', (n, len(same)), (0, 4))

print("\nreading the store back")
pages = [[{"raw": {"auction_url": "u1"}}, {"raw": None}], [{"raw": {"auction_url": "u2"}}], []]
asked = []


def fake_urlopen(req, timeout=None):
    asked.append(req.full_url)
    return io.BytesIO(json.dumps(pages[len(asked) - 1]).encode())


with mock.patch.dict(os.environ, {"SUPABASE_URL": "https://store.example", "SUPABASE_SERVICE_ROLE_KEY": "k"}), \
        mock.patch.object(canonical_store.urllib.request, "urlopen", fake_urlopen):
    got = canonical_store.fetch_cnb_scraped_rows(page_size=2)
check('pages until one comes back empty, skipping rows with no scraped record',
      [r["auction_url"] for r in got], ["u1", "u2"])
check('each page starts where the last one ended',
      [re.search(r"offset=(\d+)", u).group(1) for u in asked], ["0", "2", "3"])
check('only this scraper\'s Cars & Bids rows are asked for',
      all("source_id=eq.carsandbids" in u and "entered_by=eq.scraper" in u for u in asked), True)
with mock.patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_SERVICE_ROLE_KEY": ""}):
    try:
        canonical_store.fetch_cnb_scraped_rows()
        refused = False
    except RuntimeError:
        refused = True
check('without credentials it refuses rather than restore nothing', refused, True)

print(f"\n{failures} test(s) failed" if failures else "\nAll tests passed")
sys.exit(1 if failures else 0)
