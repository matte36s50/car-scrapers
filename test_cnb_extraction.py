#!/usr/bin/env python3
"""Offline tests for the Cars & Bids extraction helpers.

Covers the two regressions that put 52 malformed rows into cnb.csv: model
names carrying year and make, and comments silently reading 0.

Run: python test_cnb_extraction.py
"""
import re
import sys

from cnb_scraper import derive_model_from_title

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

print(f"\n{failures} test(s) failed" if failures else "\nAll tests passed")
sys.exit(1 if failures else 0)
