import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time
import os
import re
from typing import Optional

class FISScraper:
    """FIS Athlete Data Scraper (Agent A)"""
    
    def __init__(
        self,
        cache_file="scripts/data/cache/scraper_cache.json",
        cache_ttl_seconds=86400,
        force_refresh=False,
        max_retries=2,
        request_timeout=10,
        request_interval_sec=0.5,
    ):
        self.cache_file = cache_file
        self.cache_ttl_seconds = cache_ttl_seconds
        self.force_refresh = force_refresh
        self.max_retries = max_retries
        self.request_timeout = request_timeout
        self.request_interval_sec = request_interval_sec
        self.cache = self._load_cache()
        self.stats = {
            "requested": 0,
            "fetched": 0,
            "cache_hit": 0,
            "stale_cache_fallback": 0,
            "hard_fail": 0,
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def _load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
    
    def _normalize_name(self, name_text):
        if not name_text:
            return None
        # Insert a space between lower->upper transitions (e.g., DonghyunJUNG)
        return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name_text).strip()

    def _parse_birthdate(self, soup):
        field = soup.find('span', class_='profile-info__field', string=lambda s: s and 'Birthdate' in s)
        if not field:
            return None
        value = field.find_next('span', class_='profile-info__value')
        if not value:
            return None
        text = value.get_text(strip=True)
        if not text or not re.match(r'\d{2}-\d{2}-\d{4}', text):
            return None
        day, month, year = text.split('-')
        return f"{year}-{month}-{day}"

    def _parse_results(self, soup):
        rows = soup.select('a.table-row')
        results = []
        for row in rows:
            def text(sel):
                el = row.select_one(sel)
                return el.get_text(' ', strip=True) if el else None

            date_text = text('div.g-xs-4.g-sm-4.g-md-4.g-lg-4')
            place = text('div.g-md.g-lg.justify-left.hidden-sm-down')
            category = text('div.g-md-5.g-lg-5.justify-left.hidden-sm-down')
            discipline = text('div.g-md-3.g-lg-3.justify-left.hidden-sm-down')
            nation = text('span.country__name-short')

            right_cols = row.select('div.g-xs-6.g-sm-6.g-md-6.g-lg-6.justify-right.flex-xs-wrap > div')
            if len(right_cols) >= 3:
                rank_text = right_cols[0].get_text(' ', strip=True)
                fis_points_text = right_cols[1].get_text(' ', strip=True)
                cup_points_text = right_cols[2].get_text(' ', strip=True)
            else:
                rank_text = fis_points_text = cup_points_text = None

            # Normalize date
            iso_date = None
            if date_text and re.match(r'\d{2}-\d{2}-\d{4}', date_text):
                d, m, y = date_text.split('-')
                iso_date = f"{y}-{m}-{d}"

            # Normalize rank and points
            rank = None
            rank_status = None
            if rank_text:
                if rank_text.isdigit():
                    rank = int(rank_text)
                else:
                    rank_status = rank_text.strip().upper()
            fis_points = None
            if fis_points_text:
                try:
                    fis_points = float(fis_points_text)
                except ValueError:
                    fis_points = None
            cup_points = None
            if cup_points_text:
                try:
                    cup_points = float(cup_points_text)
                except ValueError:
                    cup_points = None

            results.append({
                'date': iso_date,
                'place': place,
                'category': category,
                'discipline': discipline,
                'nation': nation,
                'rank': rank,
                'rank_status': rank_status,
                'fis_points': fis_points,
                'cup_points': cup_points
            })
        return results

    def _cache_entry_valid(self, entry: dict) -> bool:
        if not entry:
            return False
        try:
            ts = datetime.fromisoformat(entry.get("timestamp", ""))
        except Exception:
            return False
        cached = entry.get("data", {})
        has_results = isinstance(cached.get("results"), list) and len(cached.get("results")) > 0
        has_birth = bool(cached.get("birth_date"))
        if not (has_results and has_birth):
            return False
        age_sec = (datetime.now() - ts).total_seconds()
        return age_sec < self.cache_ttl_seconds

    def _request_with_retries(self, url: str) -> Optional[requests.Response]:
        last_exc = None
        for attempt in range(1, self.max_retries + 2):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.request_timeout)
                if response.status_code == 200:
                    return response
                print(f"  [Retry {attempt}] Status {response.status_code}")
            except Exception as e:
                last_exc = e
                print(f"  [Retry {attempt}] {e}")
            if attempt <= self.max_retries:
                time.sleep(min(2.0, 0.5 * attempt))
        if last_exc:
            print(f"  [Fail] {last_exc}")
        return None

    def scrape_athlete(self, url):
        self.stats["requested"] += 1
        cache_entry = self.cache.get(url, {})
        cached = cache_entry.get("data", {})

        # Cache-first mode (not used in force-refresh operations)
        if (not self.force_refresh) and self._cache_entry_valid(cache_entry):
            self.stats["cache_hit"] += 1
            print(f"  [Cache] {url.split('competitorid=')[1]}")
            return cached
        
        try:
            print(f"  [Fetching] {url}")
            response = self._request_with_retries(url)
            if response is None:
                if cached:
                    self.stats["stale_cache_fallback"] += 1
                    print(f"  [StaleCacheFallback] {url.split('competitorid=')[1]}")
                    return cached
                self.stats["hard_fail"] += 1
                return None
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Basic Extraction (Simulated/Simplified for Reliability)
            # In a real scenario, this would parse exact CSS selectors.
            # Here we extract key metadata from the URL and page title as a fallback
            
            fis_code = url.split('competitorid=')[1].split('&')[0]
            sector_code = url.split('sectorcode=')[1].split('&')[0]
            
            name = soup.find('h1', class_='athlete-profile__name')
            name_text = self._normalize_name(name.get_text(strip=True) if name else None)
            if not name_text:
                name_text = f"Athlete {fis_code}"

            birth_date = self._parse_birthdate(soup)
            results = self._parse_results(soup)

            data = {
                'fis_url': url,
                'fis_code': fis_code,
                'sport_code': sector_code,
                'name_en': name_text,
                'birth_date': birth_date,
                'gender': None,
                'results': results
            }

            # Parse failure fallback: keep previous usable payload rather than dropping athlete
            if (not data.get("results")) and cached.get("results"):
                self.stats["stale_cache_fallback"] += 1
                print(f"  [ParseFallback] {url.split('competitorid=')[1]}")
                return cached
            
            # Save to cache
            self.cache[url] = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            self._save_cache()
            self.stats["fetched"] += 1
            
            time.sleep(self.request_interval_sec)
            return data
            
        except Exception as e:
            print(f"  [Fail] {e}")
            if cached:
                self.stats["stale_cache_fallback"] += 1
                print(f"  [StaleCacheFallback] {url.split('competitorid=')[1]}")
                return cached
            self.stats["hard_fail"] += 1
            return None

    def scrape_all(self, urls):
        results = []
        failures = []
        print(f"🔍 Agent A: Scraping {len(urls)} athletes...")
        for i, url in enumerate(urls):
            data = self.scrape_athlete(url)
            if data:
                results.append(data)
            else:
                failures.append(url)
        if failures:
            log_dir = os.path.join(os.path.dirname(self.cache_file), "logs")
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, "failed_urls.txt")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(f"failed_count: {len(failures)}\n")
                f.write(f"generated_at: {datetime.now().isoformat()}\n")
                for url in failures:
                    f.write(url + "\n")
            print(f"⚠️ Failed URLs logged: {log_path} ({len(failures)})")
        print(f"📊 Scraper stats: {self.stats}")
        return results
