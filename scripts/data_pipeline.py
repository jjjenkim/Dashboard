#!/usr/bin/env python3
import sys
import os
import json
import argparse
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
# Add current directory to path so we can import modules
sys.path.insert(0, SCRIPT_DIR)

from fis_scraper import FISScraper
from data_processor import DataProcessor

def parse_args():
    parser = argparse.ArgumentParser(description="Team Korea data pipeline")
    parser.add_argument("--force-refresh", action="store_true", help="Bypass fresh cache and fetch from source first")
    parser.add_argument("--cache-ttl-seconds", type=int, default=86400, help="Cache freshness TTL in seconds")
    parser.add_argument("--max-retries", type=int, default=2, help="Retries per URL request")
    parser.add_argument("--request-timeout", type=int, default=10, help="HTTP request timeout in seconds")
    parser.add_argument("--strict-min-success-rate", type=float, default=1.0, help="Minimum acceptable success rate")
    parser.add_argument("--stale-threshold-days", type=int, default=30, help="Mark athletes stale if latest result is older than this")
    parser.add_argument(
        "--health-output",
        default=os.path.join(SCRIPT_DIR, "data", "cache", "logs", "pipeline_health_latest.json"),
        help="Health summary output path",
    )
    return parser.parse_args()

def summarize_freshness(processed_athletes, stale_threshold_days=30):
    all_dates = []
    athlete_latest = []
    stale_athletes = []
    today = datetime.now().date()
    for athlete in processed_athletes:
        dates = [r.get("date") for r in athlete.get("recent_results", []) if r.get("date")]
        if dates:
            latest = max(dates)
            athlete_latest.append(latest)
            all_dates.extend(dates)
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d").date()
                age_days = (today - latest_dt).days
                if age_days > stale_threshold_days:
                    stale_athletes.append({
                        "fis_code": athlete.get("fis_code"),
                        "name": athlete.get("name_en"),
                        "latest_result_date": latest,
                        "age_days": age_days,
                    })
            except Exception:
                pass
    return {
        "max_event_date": max(all_dates) if all_dates else None,
        "min_latest_per_athlete": min(athlete_latest) if athlete_latest else None,
        "athletes_with_results": len(athlete_latest),
        "total_events": len(all_dates),
        "stale_threshold_days": stale_threshold_days,
        "stale_athletes_count": len(stale_athletes),
        "stale_athletes_preview": sorted(stale_athletes, key=lambda x: x["age_days"], reverse=True)[:10],
    }

def main():
    args = parse_args()
    print("🚀 Team Korea Data Pipeline (V6 Agent System)")
    print("=============================================")
    
    # 1. Load URLs
    url_file = os.path.join(SCRIPT_DIR, "data", "raw", "athlete_urls.txt")
    if not os.path.exists(url_file):
        print(f"❌ Error: URL file not found at {url_file}")
        return

    with open(url_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    print(f"📋 Found {len(urls)} athlete URLs.")
    
    # 2. Agent A: Scraping
    scraper = FISScraper(
        cache_ttl_seconds=args.cache_ttl_seconds,
        force_refresh=args.force_refresh,
        max_retries=args.max_retries,
        request_timeout=args.request_timeout,
    )
    raw_data = scraper.scrape_all(urls)
    print(f"✓ Agent A finished: {len(raw_data)} profiles collected.")

    success_rate = (len(raw_data) / len(urls)) if urls else 0.0
    print(f"📈 Success rate: {success_rate:.2%}")
    
    # 3. Agent B: Processing
    processor = DataProcessor()
    processed_athletes = processor.process(raw_data)
    
    # 4. Save to local pipeline output inside v7_복구
    output_path = os.path.join(SCRIPT_DIR, "data", "athletes.json")
    processor.save_to_app(processed_athletes, output_path)

    freshness = summarize_freshness(processed_athletes, stale_threshold_days=args.stale_threshold_days)
    health = {
        "generated_at": datetime.now().isoformat(),
        "force_refresh": args.force_refresh,
        "cache_ttl_seconds": args.cache_ttl_seconds,
        "input_urls": len(urls),
        "scraped_profiles": len(raw_data),
        "success_rate": success_rate,
        "scraper_stats": scraper.stats,
        "freshness": freshness,
        "output_path": output_path,
        "strict_min_success_rate": args.strict_min_success_rate,
        "passed": success_rate >= args.strict_min_success_rate,
    }
    os.makedirs(os.path.dirname(args.health_output), exist_ok=True)
    with open(args.health_output, "w", encoding="utf-8") as f:
        json.dump(health, f, ensure_ascii=False, indent=2)
    print(f"🩺 Health report saved: {args.health_output}")
    
    print("=============================================")
    if success_rate < args.strict_min_success_rate:
        print("❌ Pipeline failed strict success-rate gate.")
        raise SystemExit(2)
    print("✅ Pipeline Complete. V6 Dashboard Data Updated.")

if __name__ == "__main__":
    main()
