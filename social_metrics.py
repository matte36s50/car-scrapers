"""
Social Metrics Collector for MII Calculation

Collects social interest data from:
- Google Trends (via pytrends - fully free)
- YouTube Data API (10k units/day free tier)

All data is aggregated monthly for long-term trend analysis.
"""

import os
import time
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError

# Rate limiting settings
GOOGLE_TRENDS_DELAY = 2.0  # seconds between requests
YOUTUBE_DELAY = 0.5  # seconds between requests


class SocialMetricsCache:
    """Simple file-based cache for social metrics to avoid redundant API calls"""

    def __init__(self, cache_file: str = "social_metrics_cache.json"):
        self.cache_file = cache_file
        self.cache = self._load_cache()

    def _load_cache(self) -> dict:
        """Load cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_cache(self):
        """Save cache to file"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except IOError as e:
            print(f"  Warning: Could not save cache: {e}")

    def _get_cache_key(self, source: str, query: str, month: str) -> str:
        """Generate a unique cache key"""
        return hashlib.md5(f"{source}:{query}:{month}".encode()).hexdigest()

    def get(self, source: str, query: str, month: str) -> Optional[dict]:
        """Get cached value if exists and not expired (30 days)"""
        key = self._get_cache_key(source, query, month)
        if key in self.cache:
            entry = self.cache[key]
            cached_time = datetime.fromisoformat(entry.get('cached_at', '2000-01-01'))
            if datetime.now() - cached_time < timedelta(days=30):
                return entry.get('data')
        return None

    def set(self, source: str, query: str, month: str, data: dict):
        """Cache a value"""
        key = self._get_cache_key(source, query, month)
        self.cache[key] = {
            'data': data,
            'cached_at': datetime.now().isoformat()
        }
        self._save_cache()


class GoogleTrendsCollector:
    """
    Collects search interest data from Google Trends using pytrends.

    Free tier: Unlimited (unofficial API, rate limited)
    Best for: Monthly interest trends by car make/model
    """

    def __init__(self, cache: SocialMetricsCache):
        self.cache = cache
        self.pytrends = None
        self._init_pytrends()

    def _init_pytrends(self):
        """Initialize pytrends with retry logic"""
        try:
            from pytrends.request import TrendReq
            self.pytrends = TrendReq(
                hl='en-US',
                tz=360,
                timeout=(10, 25),
                retries=2,
                backoff_factor=0.5
            )
            print("  Google Trends: Initialized successfully")
        except ImportError:
            print("  Warning: pytrends not installed. Run: pip install pytrends")
            self.pytrends = None
        except Exception as e:
            print(f"  Warning: Could not initialize pytrends: {e}")
            self.pytrends = None

    def get_interest_over_time(
        self,
        make: str,
        model: str,
        months_back: int = 12
    ) -> dict:
        """
        Get Google Trends interest data for a car make/model combination.

        Returns:
            dict with 'avg_interest', 'trend_direction', 'peak_interest', 'data_points'
        """
        if not self.pytrends:
            return self._get_fallback_estimate(make, model)

        # Build search query
        query = f"{make} {model}".strip()
        current_month = datetime.now().strftime("%Y-%m")

        # Check cache first
        cached = self.cache.get("google_trends", query, current_month)
        if cached:
            return cached

        try:
            # Calculate timeframe (last N months)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=months_back * 30)
            timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"

            # Build payload and get data
            self.pytrends.build_payload(
                kw_list=[query],
                cat=47,  # Auto & Vehicles category
                timeframe=timeframe,
                geo='US'
            )

            time.sleep(GOOGLE_TRENDS_DELAY)  # Rate limiting

            interest_data = self.pytrends.interest_over_time()

            if interest_data.empty:
                print(f"    No Google Trends data for '{query}'")
                result = self._get_fallback_estimate(make, model)
            else:
                # Calculate metrics
                values = interest_data[query].values
                avg_interest = float(values.mean())
                peak_interest = float(values.max())

                # Calculate trend direction (comparing last 3 months to previous 3 months)
                if len(values) >= 6:
                    recent = values[-3:].mean()
                    previous = values[-6:-3].mean()
                    if previous > 0:
                        trend_pct = ((recent - previous) / previous) * 100
                    else:
                        trend_pct = 0
                else:
                    trend_pct = 0

                result = {
                    'avg_interest': round(avg_interest, 2),
                    'peak_interest': round(peak_interest, 2),
                    'trend_direction': 'up' if trend_pct > 5 else ('down' if trend_pct < -5 else 'stable'),
                    'trend_pct': round(trend_pct, 1),
                    'data_points': len(values),
                    'source': 'google_trends'
                }

            # Cache the result
            self.cache.set("google_trends", query, current_month, result)
            return result

        except Exception as e:
            print(f"    Google Trends error for '{query}': {e}")
            return self._get_fallback_estimate(make, model)

    def _get_fallback_estimate(self, make: str, model: str) -> dict:
        """Provide estimated values when API fails"""
        # Base estimates by make popularity
        make_estimates = {
            'porsche': 75, 'ferrari': 70, 'lamborghini': 68,
            'bmw': 65, 'mercedes-benz': 63, 'audi': 55,
            'ford': 50, 'chevrolet': 48, 'dodge': 45,
            'toyota': 55, 'honda': 50, 'nissan': 45,
            'mazda': 40, 'subaru': 42, 'volkswagen': 38,
        }

        make_lower = make.lower() if make else ''
        base = make_estimates.get(make_lower, 30)

        # Boost for iconic models
        model_lower = (model or '').lower()
        iconic_models = ['911', 'gt3', 'm3', 'supra', 'nsx', 'corvette', 'mustang', 'gtr']
        if any(m in model_lower for m in iconic_models):
            base = min(base + 15, 100)

        return {
            'avg_interest': base,
            'peak_interest': base + 10,
            'trend_direction': 'stable',
            'trend_pct': 0,
            'data_points': 0,
            'source': 'estimate'
        }


class YouTubeCollector:
    """
    Collects video statistics from YouTube Data API.

    Free tier: 10,000 quota units/day
    - Search: 100 units per request
    - Video list: 1 unit per request

    Best for: Monthly video count and view trends
    """

    def __init__(self, api_key: Optional[str], cache: SocialMetricsCache):
        self.api_key = api_key or os.environ.get('YOUTUBE_API_KEY')
        self.cache = cache
        self.youtube = None
        self._init_youtube()

    def _init_youtube(self):
        """Initialize YouTube API client"""
        if not self.api_key:
            print("  YouTube: No API key provided (set YOUTUBE_API_KEY env var)")
            return

        try:
            from googleapiclient.discovery import build
            self.youtube = build('youtube', 'v3', developerKey=self.api_key)
            print("  YouTube: Initialized successfully")
        except ImportError:
            print("  Warning: google-api-python-client not installed.")
            print("          Run: pip install google-api-python-client")
        except Exception as e:
            print(f"  Warning: Could not initialize YouTube API: {e}")

    def get_video_metrics(
        self,
        make: str,
        model: str,
        max_results: int = 10
    ) -> dict:
        """
        Get YouTube video metrics for a car make/model.

        Returns:
            dict with 'total_videos', 'avg_views', 'total_views', 'recent_uploads'
        """
        if not self.youtube:
            return self._get_fallback_estimate(make, model)

        query = f"{make} {model} review".strip()
        current_month = datetime.now().strftime("%Y-%m")

        # Check cache first
        cached = self.cache.get("youtube", query, current_month)
        if cached:
            return cached

        try:
            time.sleep(YOUTUBE_DELAY)  # Rate limiting

            # Search for videos (costs 100 quota units)
            search_response = self.youtube.search().list(
                q=query,
                part='id,snippet',
                maxResults=max_results,
                type='video',
                order='relevance',
                publishedAfter=(datetime.now() - timedelta(days=365)).isoformat() + 'Z'
            ).execute()

            video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]

            if not video_ids:
                result = self._get_fallback_estimate(make, model)
                self.cache.set("youtube", query, current_month, result)
                return result

            # Get video statistics (costs 1 unit per video)
            time.sleep(YOUTUBE_DELAY)
            videos_response = self.youtube.videos().list(
                part='statistics',
                id=','.join(video_ids)
            ).execute()

            # Calculate metrics
            total_views = 0
            view_counts = []

            for video in videos_response.get('items', []):
                stats = video.get('statistics', {})
                views = int(stats.get('viewCount', 0))
                total_views += views
                view_counts.append(views)

            avg_views = total_views / len(view_counts) if view_counts else 0

            # Count recent uploads (last 3 months)
            recent_count = sum(
                1 for item in search_response.get('items', [])
                if 'snippet' in item and
                datetime.fromisoformat(item['snippet']['publishedAt'].replace('Z', '+00:00')).replace(tzinfo=None)
                > datetime.now() - timedelta(days=90)
            )

            result = {
                'total_videos': len(video_ids),
                'total_views': total_views,
                'avg_views': round(avg_views, 0),
                'recent_uploads': recent_count,
                'source': 'youtube_api'
            }

            self.cache.set("youtube", query, current_month, result)
            return result

        except Exception as e:
            print(f"    YouTube API error for '{query}': {e}")
            return self._get_fallback_estimate(make, model)

    def _get_fallback_estimate(self, make: str, model: str) -> dict:
        """Provide estimated values when API fails"""
        make_lower = (make or '').lower()
        model_lower = (model or '').lower()

        # Base estimates
        base_videos = 10
        base_views = 50000

        # Popular makes get more videos
        popular_makes = ['porsche', 'ferrari', 'bmw', 'mercedes', 'lamborghini']
        if any(m in make_lower for m in popular_makes):
            base_videos = 25
            base_views = 150000

        # Iconic models get more views
        iconic = ['911', 'gt3', 'm3', 'supra', 'corvette', 'mustang', 'gtr', 'nsx']
        if any(m in model_lower for m in iconic):
            base_views = int(base_views * 1.5)

        return {
            'total_videos': base_videos,
            'total_views': base_views,
            'avg_views': base_views // base_videos,
            'recent_uploads': base_videos // 3,
            'source': 'estimate'
        }


class SocialMetricsCollector:
    """
    Main collector class that orchestrates all social metric sources.

    Usage:
        collector = SocialMetricsCollector(youtube_api_key='YOUR_KEY')
        metrics = collector.get_metrics_for_model('Porsche', '911')
    """

    def __init__(self, youtube_api_key: Optional[str] = None):
        print("\nInitializing Social Metrics Collector...")
        self.cache = SocialMetricsCache()
        self.google_trends = GoogleTrendsCollector(self.cache)
        self.youtube = YouTubeCollector(youtube_api_key, self.cache)
        print("Social Metrics Collector ready.\n")

    def get_metrics_for_model(
        self,
        make: str,
        model: str,
        include_youtube: bool = True,
        include_trends: bool = True
    ) -> dict:
        """
        Get all social metrics for a car make/model combination.

        Args:
            make: Car manufacturer (e.g., 'Porsche')
            model: Car model (e.g., '911')
            include_youtube: Whether to fetch YouTube data
            include_trends: Whether to fetch Google Trends data

        Returns:
            dict with combined metrics from all sources
        """
        result = {
            'make': make,
            'model': model,
            'collected_at': datetime.now().isoformat(),
            'google_trends': {},
            'youtube': {},
            'combined_score': 0
        }

        if include_trends:
            result['google_trends'] = self.google_trends.get_interest_over_time(make, model)

        if include_youtube:
            result['youtube'] = self.youtube.get_video_metrics(make, model)

        # Calculate combined social score (0-100 scale)
        result['combined_score'] = self._calculate_combined_score(result)

        return result

    def _calculate_combined_score(self, metrics: dict) -> float:
        """
        Calculate a combined social interest score (0-100).

        Weights:
        - Google Trends interest: 60%
        - YouTube presence: 40%
        """
        score = 0

        # Google Trends component (0-60 points)
        trends = metrics.get('google_trends', {})
        if trends:
            avg_interest = trends.get('avg_interest', 0)
            trend_bonus = 5 if trends.get('trend_direction') == 'up' else 0
            trends_score = min((avg_interest / 100) * 60 + trend_bonus, 60)
            score += trends_score

        # YouTube component (0-40 points)
        youtube = metrics.get('youtube', {})
        if youtube:
            # Normalize views (log scale since views vary widely)
            import math
            total_views = youtube.get('total_views', 0)
            if total_views > 0:
                # Log scale: 100 views = ~5, 10K = ~10, 1M = ~15, 10M = ~17
                log_views = math.log10(total_views + 1)
                views_score = min((log_views / 7) * 30, 30)  # Max 30 points from views
            else:
                views_score = 0

            # Recent activity bonus (max 10 points)
            recent = youtube.get('recent_uploads', 0)
            activity_score = min(recent * 2, 10)

            score += views_score + activity_score

        return round(score, 2)

    def get_metrics_batch(
        self,
        models: list,
        progress_callback=None
    ) -> pd.DataFrame:
        """
        Get metrics for multiple models.

        Args:
            models: List of dicts with 'make' and 'model' keys
            progress_callback: Optional function(current, total) for progress updates

        Returns:
            DataFrame with all metrics
        """
        results = []
        total = len(models)

        for i, item in enumerate(models):
            make = item.get('make', item.get('manufacturer', ''))
            model = item.get('model', '')

            if progress_callback:
                progress_callback(i + 1, total)
            else:
                print(f"  Processing {i+1}/{total}: {make} {model}")

            metrics = self.get_metrics_for_model(make, model)

            # Flatten the nested dict for DataFrame
            flat = {
                'make': make,
                'model': model,
                'google_trends_interest': metrics['google_trends'].get('avg_interest', 0),
                'google_trends_peak': metrics['google_trends'].get('peak_interest', 0),
                'google_trends_direction': metrics['google_trends'].get('trend_direction', 'unknown'),
                'google_trends_pct': metrics['google_trends'].get('trend_pct', 0),
                'youtube_videos': metrics['youtube'].get('total_videos', 0),
                'youtube_total_views': metrics['youtube'].get('total_views', 0),
                'youtube_avg_views': metrics['youtube'].get('avg_views', 0),
                'youtube_recent': metrics['youtube'].get('recent_uploads', 0),
                'social_score': metrics['combined_score'],
                'collected_at': metrics['collected_at']
            }
            results.append(flat)

        return pd.DataFrame(results)

    def save_metrics_to_s3(self, df: pd.DataFrame, bucket: str = 'my-mii-reports'):
        """Save collected metrics to S3"""
        filename = f"social_metrics_{datetime.now().strftime('%Y%m')}.csv"
        df.to_csv(filename, index=False)

        try:
            s3 = boto3.client('s3')
            s3.upload_file(filename, bucket, filename)
            print(f"  Uploaded {filename} to S3")

            # Also save as latest
            s3.upload_file(filename, bucket, 'social_metrics_latest.csv')
            print(f"  Uploaded social_metrics_latest.csv to S3")
            return True
        except NoCredentialsError:
            print("  Warning: AWS credentials not available, saved locally only")
            return False
        except Exception as e:
            print(f"  Warning: S3 upload failed: {e}")
            return False


def collect_social_metrics_for_mii(
    models_df: pd.DataFrame,
    youtube_api_key: Optional[str] = None,
    sample_size: Optional[int] = None
) -> pd.DataFrame:
    """
    Convenience function to collect social metrics for MII calculation.

    Args:
        models_df: DataFrame with 'manufacturer' and 'model' columns
        youtube_api_key: Optional YouTube API key
        sample_size: If set, only process this many unique models (for testing)

    Returns:
        DataFrame with social metrics that can be merged with MII data
    """
    print("\n" + "=" * 80)
    print("COLLECTING SOCIAL METRICS FOR MII")
    print("=" * 80)

    # Get unique make/model combinations
    unique_models = models_df[['manufacturer', 'model']].drop_duplicates()

    if sample_size:
        unique_models = unique_models.head(sample_size)

    print(f"Total unique models to process: {len(unique_models)}")

    # Initialize collector
    collector = SocialMetricsCollector(youtube_api_key=youtube_api_key)

    # Convert to list of dicts
    models_list = unique_models.to_dict('records')

    # Collect metrics
    print("\nCollecting metrics...")
    metrics_df = collector.get_metrics_batch(models_list)

    # Rename columns for merging
    metrics_df = metrics_df.rename(columns={'make': 'manufacturer'})

    print(f"\nCollected metrics for {len(metrics_df)} models")
    print(f"Social score range: {metrics_df['social_score'].min():.1f} - {metrics_df['social_score'].max():.1f}")

    return metrics_df


# Example usage and testing
if __name__ == "__main__":
    print("Testing Social Metrics Collector...")
    print("=" * 80)

    # Test with a few models
    test_models = [
        {'make': 'Porsche', 'model': '911'},
        {'make': 'BMW', 'model': 'M3'},
        {'make': 'Toyota', 'model': 'Supra'},
        {'make': 'Ford', 'model': 'Mustang'},
        {'make': 'Ferrari', 'model': '458'},
    ]

    collector = SocialMetricsCollector()

    print("\nTesting individual model lookup:")
    for item in test_models[:2]:
        metrics = collector.get_metrics_for_model(item['make'], item['model'])
        print(f"\n{item['make']} {item['model']}:")
        print(f"  Google Trends Interest: {metrics['google_trends'].get('avg_interest', 'N/A')}")
        print(f"  Google Trends Direction: {metrics['google_trends'].get('trend_direction', 'N/A')}")
        print(f"  YouTube Videos: {metrics['youtube'].get('total_videos', 'N/A')}")
        print(f"  YouTube Total Views: {metrics['youtube'].get('total_views', 'N/A'):,}")
        print(f"  Combined Social Score: {metrics['combined_score']}")

    print("\nTesting batch collection:")
    df = collector.get_metrics_batch(test_models)
    print(df.to_string())

    print("\n" + "=" * 80)
    print("Test complete!")
