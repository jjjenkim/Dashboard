import json
from datetime import datetime
import os

class DataProcessor:
    """Data Processing Agent (Agent B)"""
    
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.sport_mapping = {
            "AL": "alpine_skiing",
            "SX": "ski_cross",
            "MO": "moguls",
            "FS": "freeski",
            "SB": "snowboard_park",
            "SBX": "snowboard_cross",
            "PSL": "snowboard_alpine",
            "JP": "ski_jumping",
            "CC": "cross_country"
        }
        
        self.sport_display = {
            "alpine_skiing": "Alpine Skiing",
            "ski_cross": "Ski Cross",
            "moguls": "Moguls",
            "freeski": "Freeski",
            "snowboard_park": "Snowboard Park",
            "snowboard_cross": "Snowboard Cross",
            "snowboard_alpine": "Snowboard Alpine",
            "ski_jumping": "Ski Jumping",
            "cross_country": "Cross Country"
        }
        self.existing = self._load_existing()

    def _load_existing(self):
        path = os.path.join(self.script_dir, "data", "athletes.json")
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {a.get("fis_code"): a for a in data.get("athletes", []) if a.get("fis_code")}
        except Exception:
            return {}

    def process(self, raw_data):
        print("⚙️ Agent B: Processing data...")
        processed = []
        
        for i, athlete in enumerate(raw_data):
            sport_code = athlete.get('sport_code', 'AL')
            sport = self.sport_mapping.get(sport_code, 'alpine_skiing')

            existing = self.existing.get(athlete.get('fis_code'), {})
            
            # Simple Korean Name Mapping (Mock - real world would use a dictionary)
            # Since we don't have the dictionary here, we key off the english name or ID
            # This is a placeholder logic
            name_en = existing.get('name_en') or athlete.get('name_en', 'Unknown')
            name_ko = existing.get('name_ko') or name_en

            birth_date = athlete.get('birth_date') or existing.get('birth_date')
            birth_year = None
            age = None
            if birth_date and isinstance(birth_date, str) and len(birth_date) >= 4:
                try:
                    birth_year = int(birth_date.split('-')[0])
                    age = datetime.now().year - birth_year
                except ValueError:
                    birth_year = None
                    age = None

            # Recent results
            results = athlete.get('results') or []
            # Filter valid results with date
            results = [r for r in results if r.get('date')]
            results.sort(key=lambda r: r.get('date', ''), reverse=True)
            recent_results = []
            numeric_ranks = []
            for r in results:
                rank = r.get('rank')
                rank_status = r.get('rank_status')
                if isinstance(rank, int) and rank > 0:
                    numeric_ranks.append(rank)
                # Keep valid numeric rank or explicit status (DNS/DNF/DSQ)
                if (isinstance(rank, int) and rank > 0) or (rank_status and isinstance(rank_status, str)):
                    recent_results.append({
                        'date': r.get('date'),
                        'event': r.get('discipline') or r.get('category') or 'Result',
                        'rank': rank,
                        'rank_status': rank_status,
                        'points': r.get('fis_points') if r.get('fis_points') is not None else 0.0,
                        'place': r.get('place'),
                        'category': r.get('category'),
                        'discipline': r.get('discipline'),
                        'cup_points': r.get('cup_points')
                    })
                if len(recent_results) >= 40:
                    break

            current_rank = numeric_ranks[0] if numeric_ranks else None
            best_rank = min(numeric_ranks) if numeric_ranks else None
            season_starts = len(results)
            gender = athlete.get('gender') or existing.get('gender') or 'M'
            if isinstance(gender, str):
                gender = gender.upper().strip()[:1]
            if gender not in ('M', 'F'):
                gender = 'M'
            
            processed_athlete = {
                'id': f"KOR{i+1:03d}",
                'name_ko': name_ko, 
                'name_en': name_en,
                'birth_date': birth_date,
                'birth_year': birth_year,
                'age': age,
                'gender': gender,
                'sport': sport,
                'sport_display': existing.get('sport_display') or self.sport_display.get(sport, sport),
                'team': existing.get('team') or 'KOR',
                'fis_code': athlete.get('fis_code'),
                'fis_url': athlete.get('fis_url'),
                'current_rank': current_rank,
                'best_rank': best_rank,
                'season_starts': season_starts,
                'medals': existing.get('medals') or {'gold': 0, 'silver': 0, 'bronze': 0},
                'recent_results': recent_results
            }
            processed.append(processed_athlete)
            
        return processed

    def save_to_app(self, athletes, output_path="src/data/athletes.json"):
        final_data = {
            "metadata": {
                "last_updated": datetime.now().isoformat(),
                "total_athletes": len(athletes)
            },
            "athletes": athletes
        }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Agent B: Data pushed to {output_path} ({len(athletes)} records)")
