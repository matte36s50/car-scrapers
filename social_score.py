"""
Social Score Composite for MII Calculation
==========================================

Replaces the legacy hardcoded brand->score lookup (the source of the broken
`social_score`: only 19 distinct values, pinned per brand, frozen over time)
with a *measured* composite computed per ``manufacturer x model x quarter``.

The composite is a weighted blend of five percentile-ranked sub-signals:

    social_mentions        ~0.30  Reddit + enthusiast forum mention volume
    social_engagement_rate ~0.25  interactions / reach (Reddit + IG/TikTok)
    social_sov             ~0.20  model mentions / segment mentions that quarter
    social_video_uploads   ~0.15  new videos that quarter (YouTube + TikTok uploads)
    social_sentiment       ~0.10  share of positive+neutral mentions (VADER NLP)

Design principles (see docs/social-score-methodology.md in the app repo):

  * Mid-rank percentile across ALL (model x quarter) observations, matching the
    pattern the MII front-end already uses.
  * Avoid double-counting existing MII inputs: we use YouTube *upload counts*
    (not view totals, which are a separate MII input) and Reddit/forum/IG
    *mentions* (not the on-listing BaT comment count, also a separate input).
  * Missing sub-signals are DROPPED from a row's weighted sum and the remaining
    weights are renormalized. We never impute a brand default.
  * Everything is keyed on the same manufacturer + model + quarter grain as the
    rest of the pipeline, reusing the upstream model-name normalization so keys
    line up with mii_results.

Collectors degrade gracefully: when a source's credentials/libraries are absent
the collector returns ``None`` and that sub-signal simply drops out (and the
remaining weights renormalize). Results are cached per ``source:model:quarter``
so coverage accumulates across runs.
"""

import os
import json
import time
import hashlib
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Composite weights (renormalized per-row over whichever sub-signals exist)
# ---------------------------------------------------------------------------
SUBSIGNAL_WEIGHTS = {
    "social_mentions": 0.30,
    "social_engagement_rate": 0.25,
    "social_sov": 0.20,
    "social_video_views": 0.15,
    "social_sentiment": 0.10,
}

# Rate-limit spacing (seconds)
REDDIT_DELAY = 1.0
YOUTUBE_DELAY = 0.5

# YouTube Data API quota. A search.list call costs 100 units; the free tier is
# 10,000 units/day. We cap spend per run (env-overridable, with headroom under
# the daily limit) and back off cleanly when exhausted - the persistent cache
# means each subsequent run fills in more uncached (model, quarter) keys.
YOUTUBE_SEARCH_UNIT_COST = 100
YOUTUBE_VIDEOS_UNIT_COST = 1   # videos.list (statistics) to total views of the quarter's uploads
YOUTUBE_DEFAULT_QUOTA_BUDGET = 9000
YOUTUBE_MAX_VIDEOS_PER_PERIOD = 50  # videos sampled per (model, quarter) for the view total


# ---------------------------------------------------------------------------
# Period helpers - the pipeline labels its grain "quarter" but actually uses
# monthly periods ("YYYY-MM"). Handle monthly, quarterly and yearly labels.
# ---------------------------------------------------------------------------
def period_bounds(period_str: str):
    """Return (start_dt, end_dt) datetimes spanning a period label.

    Accepts 'YYYY-MM' (monthly), 'YYYYQn' / 'YYYY-Qn' (quarterly), 'YYYY'.
    Returns (None, None) if it cannot be parsed.
    """
    if period_str is None or (isinstance(period_str, float) and pd.isna(period_str)):
        return None, None
    s = str(period_str).strip()
    try:
        if "Q" in s:  # quarterly: 2025Q3 or 2025-Q3
            year = int(s.split("Q")[0].rstrip("-"))
            q = int(s.split("Q")[1])
            start_month = (q - 1) * 3 + 1
            start = datetime(year, start_month, 1)
            end = datetime(year + (start_month + 3 > 12), (start_month + 3 - 1) % 12 + 1, 1)
            return start, end
        if "-" in s:  # monthly: 2025-03
            year, month = (int(x) for x in s.split("-")[:2])
            start = datetime(year, month, 1)
            end = datetime(year + (month == 12), (month % 12) + 1, 1)
            return start, end
        # yearly: 2025
        year = int(s)
        return datetime(year, 1, 1), datetime(year + 1, 1, 1)
    except (ValueError, IndexError):
        return None, None


def to_rfc3339(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Segment map for Share of Voice. SoV = model mentions / total mentions in the
# model's segment that quarter. Segments are stable manufacturer-class buckets;
# falls back to 'other' so every model lands somewhere.
# ---------------------------------------------------------------------------
SEGMENT_BY_MANUFACTURER = {
    "Porsche": "german_sports", "BMW": "german_sports", "Mercedes-Benz": "german_sports",
    "Audi": "german_sports",
    "Ferrari": "exotic", "Lamborghini": "exotic", "McLaren": "exotic",
    "Bugatti": "exotic", "Pagani": "exotic", "Koenigsegg": "exotic",
    "Aston Martin": "gt_luxury", "Bentley": "gt_luxury", "Rolls-Royce": "gt_luxury",
    "Maserati": "gt_luxury", "Jaguar": "gt_luxury",
    "Toyota": "jdm", "Nissan": "jdm", "Honda": "jdm", "Mazda": "jdm",
    "Subaru": "jdm", "Mitsubishi": "jdm", "Lexus": "jdm", "Acura": "jdm",
    "Infiniti": "jdm",
    "Ford": "american", "Chevrolet": "american", "Dodge": "american",
    "Plymouth": "american", "Pontiac": "american", "Buick": "american",
    "Cadillac": "american", "Lincoln": "american", "Chrysler": "american",
    "GMC": "american", "Shelby": "american",
}


def get_segment(manufacturer: str) -> str:
    return SEGMENT_BY_MANUFACTURER.get(manufacturer, "other")


# ---------------------------------------------------------------------------
# Cache (file-based, keyed by source:query:period). Persisted to disk so social
# coverage accumulates across runs; load from / save to S3 alongside the cache
# file if desired by the caller.
# ---------------------------------------------------------------------------
class SocialCache:
    def __init__(self, cache_file: str = "social_score_cache.json", ttl_days: int = 30):
        self.cache_file = cache_file
        self.ttl = timedelta(days=ttl_days)
        self.cache = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save(self):
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.cache, f)
        except IOError as e:
            print(f"  Warning: could not save social cache: {e}")

    def _key(self, source: str, query: str, period: str) -> str:
        return hashlib.md5(f"{source}:{query}:{period}".encode()).hexdigest()

    def get(self, source, query, period):
        entry = self.cache.get(self._key(source, query, period))
        if not entry:
            return None
        cached_at = datetime.fromisoformat(entry.get("cached_at", "2000-01-01"))
        # Historical periods never change, so only expire the in-progress period.
        start, end = period_bounds(period)
        is_closed = end is not None and end <= datetime.now()
        if is_closed or (datetime.now() - cached_at) < self.ttl:
            return entry.get("data")
        return None

    def set(self, source, query, period, data):
        self.cache[self._key(source, query, period)] = {
            "data": data,
            "cached_at": datetime.now().isoformat(),
        }
        self._save()


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------
class RedditForumCollector:
    """Reddit + enthusiast forum mention volume, interactions and reach.

    Uses PRAW when REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET are configured.
    Returns ``None`` (sub-signal drops) when unavailable, rather than guessing.

    Returns per (make, model, period):
        {'mentions': int, 'interactions': int, 'reach': int, 'texts': [str, ...]}
    """

    # Subreddits searched for mentions. Brand subs improve recall for marques.
    BASE_SUBS = ["cars", "whatcarshouldIbuy", "Autos", "classiccars", "spotted"]
    BRAND_SUBS = {
        "BMW": ["BMW", "E30", "E90"], "Porsche": ["Porsche", "porsche911"],
        "Toyota": ["Toyota", "supra"], "Nissan": ["Nissan", "NissanGTR"],
        "Honda": ["Honda", "S2000"], "Mazda": ["mazda", "rx7"],
        "Ford": ["Ford", "Mustang"], "Chevrolet": ["chevy", "Corvette"],
        "Ferrari": ["ferrari"], "Lamborghini": ["Lamborghini"],
        "Mercedes-Benz": ["mercedes_benz", "AMG"],
    }

    def __init__(self, cache: SocialCache):
        self.cache = cache
        self.reddit = None
        self._init_reddit()

    def _init_reddit(self):
        cid = os.environ.get("REDDIT_CLIENT_ID")
        secret = os.environ.get("REDDIT_CLIENT_SECRET")
        if not (cid and secret):
            print("  Reddit: no credentials (set REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET) - mentions sub-signal will drop")
            return
        try:
            import praw
            self.reddit = praw.Reddit(
                client_id=cid,
                client_secret=secret,
                user_agent=os.environ.get("REDDIT_USER_AGENT", "mii-social-score/1.0"),
                check_for_async=False,
            )
            self.reddit.read_only = True
            print("  Reddit: initialized successfully")
        except ImportError:
            print("  Warning: praw not installed (pip install praw) - mentions sub-signal will drop")
        except Exception as e:
            print(f"  Warning: could not initialize Reddit: {e}")

    def collect(self, make: str, model: str, period: str) -> Optional[dict]:
        if not self.reddit:
            return None
        query = f"{make} {model}".strip()
        cached = self.cache.get("reddit", query, period)
        if cached is not None:
            return cached

        start, end = period_bounds(period)
        if start is None:
            return None

        subs = self.BASE_SUBS + self.BRAND_SUBS.get(make, [])
        mentions = interactions = reach = 0
        texts = []
        try:
            for sub_name in subs:
                time.sleep(REDDIT_DELAY)
                try:
                    subreddit = self.reddit.subreddit(sub_name)
                    sub_reach = int(getattr(subreddit, "subscribers", 0) or 0)
                    for post in subreddit.search(query, sort="new", time_filter="all", limit=100):
                        created = datetime.utcfromtimestamp(post.created_utc)
                        if not (start <= created < end):
                            continue
                        mentions += 1
                        interactions += int(getattr(post, "score", 0) or 0)
                        interactions += int(getattr(post, "num_comments", 0) or 0)
                        reach += sub_reach
                        title = getattr(post, "title", "") or ""
                        body = getattr(post, "selftext", "") or ""
                        if title or body:
                            texts.append(f"{title}. {body}"[:1000])
                except Exception:
                    continue  # private/banned/missing sub - skip
        except Exception as e:
            print(f"    Reddit error for '{query}' {period}: {e}")
            return None

        if mentions == 0:
            result = {"mentions": 0, "interactions": 0, "reach": 0, "texts": []}
        else:
            result = {"mentions": mentions, "interactions": interactions,
                      "reach": reach, "texts": texts[:50]}
        self.cache.set("reddit", query, period, result)
        return result


class YouTubeVideoCollector:
    """Social video impact for a model in a given period.

    The scored sub-signal is *views on the videos uploaded that period* - i.e.
    how much attention a model's NEW content pulled in that quarter. This is
    genuinely per-period (the upload window gives recency) and measures impact
    (views), unlike an all-time view total which is constant across quarters.

    Uses search.list (publishedAfter/Before bounded to the period) to find up to
    YOUTUBE_MAX_VIDEOS_PER_PERIOD of the period's uploads, then videos.list to
    total their view counts. Also returns the upload count for auditability.

    Returns per (make, model, period): {'uploads': int, 'views': int}
    """

    def __init__(self, api_key: Optional[str], cache: SocialCache,
                 quota_budget: Optional[int] = None):
        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")
        self.cache = cache
        self.youtube = None
        # Per-run quota budget (units). Env override wins, else the default.
        if quota_budget is None:
            quota_budget = int(os.environ.get("YOUTUBE_QUOTA_BUDGET",
                                              YOUTUBE_DEFAULT_QUOTA_BUDGET))
        self.quota_budget = quota_budget
        self.quota_used = 0
        self._exhausted_warned = False
        self._init_youtube()

    def _init_youtube(self):
        if not self.api_key:
            print("  YouTube: no API key (set YOUTUBE_API_KEY) - video sub-signal will drop")
            return
        try:
            from googleapiclient.discovery import build
            self.youtube = build("youtube", "v3", developerKey=self.api_key)
            print("  YouTube: initialized successfully")
        except ImportError:
            print("  Warning: google-api-python-client not installed - video sub-signal will drop")
        except Exception as e:
            print(f"  Warning: could not initialize YouTube API: {e}")

    def collect(self, make: str, model: str, period: str) -> Optional[dict]:
        if not self.youtube:
            return None
        query = f"{make} {model}".strip()
        cached = self.cache.get("youtube_video", query, period)
        if cached is not None:
            return cached  # cache hits never spend quota

        # Quota guard: stop calling once the per-run budget is spent. Remaining
        # uncached keys simply drop this run and get picked up next run.
        if self.quota_used + YOUTUBE_SEARCH_UNIT_COST > self.quota_budget:
            if not self._exhausted_warned:
                print(f"  YouTube: quota budget reached ({self.quota_used}/{self.quota_budget} units) "
                      f"- remaining uncached models deferred to next run")
                self._exhausted_warned = True
            return None

        start, end = period_bounds(period)
        if start is None:
            return None

        try:
            time.sleep(YOUTUBE_DELAY)
            search = self.youtube.search().list(
                q=query,
                part="id",
                type="video",
                order="relevance",
                maxResults=YOUTUBE_MAX_VIDEOS_PER_PERIOD,
                publishedAfter=to_rfc3339(start),
                publishedBefore=to_rfc3339(end),
            ).execute()
            self.quota_used += YOUTUBE_SEARCH_UNIT_COST
            uploads = int(search.get("pageInfo", {}).get("totalResults", 0))

            video_ids = [
                it["id"]["videoId"]
                for it in search.get("items", [])
                if it.get("id", {}).get("videoId")
            ]

            # Total the view counts of those uploads (1 quota unit, batched).
            views = 0
            if video_ids:
                time.sleep(YOUTUBE_DELAY)
                stats = self.youtube.videos().list(
                    part="statistics", id=",".join(video_ids)
                ).execute()
                self.quota_used += YOUTUBE_VIDEOS_UNIT_COST
                for v in stats.get("items", []):
                    views += int(v.get("statistics", {}).get("viewCount", 0) or 0)

            result = {"uploads": uploads, "views": views}
            self.cache.set("youtube_video", query, period, result)
            return result
        except Exception as e:
            # quotaExceeded surfaces here too - count it as spent and stop trying.
            msg = str(e)
            if "quota" in msg.lower():
                self.quota_used = self.quota_budget
                if not self._exhausted_warned:
                    print(f"    YouTube: API reported quota exhausted - deferring remaining models")
                    self._exhausted_warned = True
            else:
                print(f"    YouTube error for '{query}' {period}: {e}")
            return None


class InstagramTikTokCollector:
    """Instagram/TikTok interactions, reach and new-video uploads.

    No official low-friction API exists; this collector activates only when a
    provider is wired via SOCIAL_IGTT_PROVIDER (and its token). Until then it
    returns ``None`` and the IG/TikTok contribution to engagement + video simply
    drops and the remaining weights renormalize.

    Returns per (make, model, period):
        {'interactions': int, 'reach': int, 'video_uploads': int}
    """

    def __init__(self, cache: SocialCache):
        self.cache = cache
        self.provider = os.environ.get("SOCIAL_IGTT_PROVIDER")
        if not self.provider:
            print("  Instagram/TikTok: not configured (set SOCIAL_IGTT_PROVIDER) - IG/TikTok sub-signals will drop")

    def collect(self, make: str, model: str, period: str) -> Optional[dict]:
        if not self.provider:
            return None
        query = f"{make} {model}".strip()
        cached = self.cache.get("igtt", query, period)
        if cached is not None:
            return cached
        # Provider integrations plug in here (hashtag/profile pulls). Each must
        # populate interactions / reach / video_uploads for the period window.
        return None


class SentimentAnalyzer:
    """VADER sentiment over collected mention text.

    Sub-signal value = share of mentions that are positive OR neutral
    (compound >= -0.05). Lightweight, local, no API.
    """

    def __init__(self):
        self.analyzer = None
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self.analyzer = SentimentIntensityAnalyzer()
            print("  Sentiment: VADER initialized")
        except ImportError:
            print("  Warning: vaderSentiment not installed (pip install vaderSentiment) - sentiment sub-signal will drop")

    def share_positive_neutral(self, texts) -> Optional[float]:
        if not self.analyzer or not texts:
            return None
        non_negative = 0
        for t in texts:
            if not t:
                continue
            compound = self.analyzer.polarity_scores(t)["compound"]
            if compound >= -0.05:  # positive or neutral
                non_negative += 1
        total = sum(1 for t in texts if t)
        if total == 0:
            return None
        return non_negative / total


# ---------------------------------------------------------------------------
# Percentile + blend
# ---------------------------------------------------------------------------
def mid_rank_percentile(series: pd.Series) -> pd.Series:
    """Mid-rank percentile in [0, 100] across all observations (NaN preserved).

    Matches the MII front-end pattern: average-method rank, expressed as a
    percentile of the population.
    """
    return series.rank(method="average", pct=True) * 100.0


def blend_with_renormalization(row, ranked_cols) -> float:
    """Weighted sum over present sub-signals, weights renormalized to those
    present. Returns NaN when no sub-signal is available (never a brand default).
    """
    num = 0.0
    wsum = 0.0
    for col, weight in SUBSIGNAL_WEIGHTS.items():
        val = row.get(ranked_cols[col])
        if pd.notna(val):
            num += val * weight
            wsum += weight
    if wsum == 0:
        return float("nan")
    return num / wsum


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def compute_social_scores(
    grouped_df: pd.DataFrame,
    youtube_api_key: Optional[str] = None,
    cache_file: str = "social_score_cache.json",
    sample_size: Optional[int] = None,
) -> pd.DataFrame:
    """Compute the measured social_score composite on the pipeline grain.

    Args:
        grouped_df: must contain columns ['manufacturer', 'model', 'quarter']
            (the same normalized keys the rest of the pipeline uses).
        youtube_api_key: optional; falls back to YOUTUBE_API_KEY env var.
        cache_file: path for the persistent per-signal cache.
        sample_size: if set, only process this many unique (manufacturer, model,
            quarter) keys (for testing).

    Returns:
        DataFrame keyed on [manufacturer, model, quarter] with columns:
            social_mentions, social_engagement_rate, social_sov,
            social_video_views, social_sentiment, social_score
        plus social_video_uploads (audit-only: upload count behind the views).
        Raw sub-signals are emitted for auditability; social_score is the
        percentile-ranked, renormalized composite in [0, 100] (NaN where no
        sub-signal could be measured). The scored video sub-signal is views on
        the period's uploads (impact), not the all-time view total.
    """
    print("\n" + "=" * 80)
    print("COMPUTING MEASURED SOCIAL SCORE COMPOSITE")
    print("=" * 80)

    keys = grouped_df[["manufacturer", "model", "quarter"]].drop_duplicates().copy()
    if sample_size:
        keys = keys.head(sample_size)
    print(f"Unique (manufacturer, model, quarter) keys: {len(keys):,}")

    cache = SocialCache(cache_file=cache_file)
    reddit = RedditForumCollector(cache)
    youtube = YouTubeVideoCollector(youtube_api_key, cache)
    igtt = InstagramTikTokCollector(cache)
    sentiment = SentimentAnalyzer()

    rows = []
    total = len(keys)
    for i, (_, k) in enumerate(keys.iterrows()):
        make, model, period = k["manufacturer"], k["model"], k["quarter"]
        if (i + 1) % 250 == 0 or i + 1 == total:
            print(f"  collecting {i+1}/{total}")

        r = reddit.collect(make, model, period)
        y = youtube.collect(make, model, period)
        ig = igtt.collect(make, model, period)

        # --- raw sub-signals (NaN where the source could not be measured) ---
        mentions = float("nan")
        interactions = reach = 0.0
        texts = []
        if r is not None:
            mentions = r.get("mentions", 0)
            interactions += r.get("interactions", 0)
            reach += r.get("reach", 0)
            texts = r.get("texts", []) or []
        if ig is not None:
            interactions += ig.get("interactions", 0)
            reach += ig.get("reach", 0)

        engagement = (interactions / reach) if reach > 0 else float("nan")

        # Scored video sub-signal = views on the period's uploads (impact).
        # Upload count is retained alongside it purely for auditability.
        views = float("nan")
        uploads = float("nan")
        yt_views = y.get("views") if y is not None else None
        ig_views = ig.get("video_views") if ig is not None else None
        if yt_views is not None or ig_views is not None:
            views = (yt_views or 0) + (ig_views or 0)
        yt_up = y.get("uploads") if y is not None else None
        ig_up = ig.get("video_uploads") if ig is not None else None
        if yt_up is not None or ig_up is not None:
            uploads = (yt_up or 0) + (ig_up or 0)

        sent = sentiment.share_positive_neutral(texts) if texts else float("nan")

        rows.append({
            "manufacturer": make, "model": model, "quarter": period,
            "segment": get_segment(make),
            "social_mentions": mentions,
            "social_engagement_rate": engagement,
            "social_video_views": views,
            "social_video_uploads": uploads,
            "social_sentiment": sent,
            "_mention_count": 0 if pd.isna(mentions) else mentions,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # --- Share of Voice: model mentions / segment mentions that quarter -------
    seg_totals = df.groupby(["segment", "quarter"])["_mention_count"].transform("sum")
    sov = df["_mention_count"] / seg_totals.replace(0, pd.NA)
    # SoV only meaningful where the row itself has a measured mention count.
    df["social_sov"] = sov.where(df["social_mentions"].notna()).astype(float)
    df = df.drop(columns=["_mention_count"])

    # --- Mid-rank percentile each sub-signal across all (model x quarter) -----
    ranked_cols = {}
    for col in SUBSIGNAL_WEIGHTS:
        rcol = f"_rank_{col}"
        df[rcol] = mid_rank_percentile(df[col])
        ranked_cols[col] = rcol

    # --- Weighted blend with per-row renormalization --------------------------
    df["social_score"] = df.apply(lambda row: blend_with_renormalization(row, ranked_cols), axis=1)
    df["social_score"] = df["social_score"].round(2)
    df = df.drop(columns=list(ranked_cols.values()) + ["segment"])

    measured = df["social_score"].notna().sum()
    print(f"\nMeasured social_score for {measured:,}/{len(df):,} keys")
    if measured:
        print(f"  distinct values: {df['social_score'].nunique():,}")
        print(f"  range: {df['social_score'].min():.2f} - {df['social_score'].max():.2f}")
    print("  sub-signal coverage:")
    for col in SUBSIGNAL_WEIGHTS:
        print(f"    {col:<24} {df[col].notna().sum():>6,} / {len(df):,}")

    return df


# ---------------------------------------------------------------------------
# Smoke test (offline - exercises blend/percentile/SoV with synthetic signals)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import numpy as np

    print("Smoke-testing social_score composite (synthetic data)...")
    rng = np.random.default_rng(0)
    makes = [("BMW", "M3 (E30)"), ("BMW", "M3 (E36)"), ("BMW", "M3 (E46)"),
             ("Porsche", "911"), ("Toyota", "Supra"), ("Honda", "S2000")]
    quarters = ["2024-01", "2024-02", "2024-03", "2024-04"]
    recs = []
    for mk, md in makes:
        for q in quarters:
            recs.append({"manufacturer": mk, "model": md, "quarter": q})
    grouped = pd.DataFrame(recs)

    # Monkeypatch collectors with synthetic values to exercise the math path.
    df = grouped.copy()
    df["segment"] = df["manufacturer"].map(get_segment)
    df["social_mentions"] = rng.integers(1, 500, len(df)).astype(float)
    df["social_engagement_rate"] = rng.random(len(df))
    df["social_video_views"] = rng.integers(0, 5_000_000, len(df)).astype(float)
    df["social_sentiment"] = rng.random(len(df))
    seg_totals = df.groupby(["segment", "quarter"])["social_mentions"].transform("sum")
    df["social_sov"] = df["social_mentions"] / seg_totals

    ranked_cols = {}
    for col in SUBSIGNAL_WEIGHTS:
        rcol = f"_rank_{col}"
        df[rcol] = mid_rank_percentile(df[col])
        ranked_cols[col] = rcol
    df["social_score"] = df.apply(lambda r: blend_with_renormalization(r, ranked_cols), axis=1).round(2)

    print(df[["manufacturer", "model", "quarter", "social_score"]].to_string(index=False))
    print(f"\ndistinct social_score values: {df['social_score'].nunique()} (was 19 in legacy)")
    e30 = df[df.model == "M3 (E30)"]["social_score"].tolist()
    e36 = df[df.model == "M3 (E36)"]["social_score"].tolist()
    print(f"E30 varies across quarters: {len(set(e30)) > 1}")
    print(f"E30 != E36 generations:    {e30 != e36}")
    print("Smoke test complete.")
