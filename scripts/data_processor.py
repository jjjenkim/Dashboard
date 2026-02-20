import json
from datetime import datetime
import os
import subprocess
import re

class DataProcessor:
    """Data Processing Agent (Agent B)"""
    
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.sport_mapping = {
            "AL": "alpine_skiing",
            "SX": "ski_cross",
            "MO": "freestyle_moguls",
            "FS": "freestyle_park",
            "SB": "snowboard_park",
            "SBX": "snowboard_cross",
            "PSL": "snowboard_alpine",
            "JP": "ski_jumping",
            "CC": "cross_country"
        }
        
        self.sport_display = {
            "alpine_skiing": "Alpine Skiing",
            "ski_cross": "Ski Cross",
            "freestyle_moguls": "Moguls",
            "freestyle_park": "Freeski Park",
            "snowboard_park": "Snowboard Park",
            "snowboard_cross": "Snowboard Cross",
            "snowboard_alpine": "Snowboard Alpine",
            "ski_jumping": "Ski Jumping",
            "cross_country": "Cross Country"
        }
        self.existing = self._load_existing()

    def _has_hangul(self, text):
        if not text or not isinstance(text, str):
            return False
        return bool(re.search(r'[\u3131-\u318E\uAC00-\uD7A3]', text))

    def _stage_priority(self, text):
        t = (text or "").strip().lower()
        if "qualif" in t or t == "qua":
            return 0
        if "final" in t:
            return 2
        return 1

    def _rank_score(self, result):
        rank = result.get("rank")
        if isinstance(rank, int) and rank > 0:
            return -rank
        return -9999

    def _load_existing(self):
        merged = {}

        def merge_athlete(dst, src):
            if not dst:
                return dict(src)
            out = dict(dst)
            for k, v in src.items():
                if v is None:
                    continue
                if k == "name_ko":
                    # Prefer Hangul Korean name if available
                    if self._has_hangul(v):
                        out[k] = v
                    elif not out.get(k):
                        out[k] = v
                    continue
                if k == "sport":
                    out[k] = v or out.get(k)
                    continue
                if not out.get(k):
                    out[k] = v
            return out

        # 1) Primary identity source: current real-site bundle (keeps Korean names/sport mapping)
        index_js = os.path.abspath(os.path.join(self.script_dir, "..", "index.js"))
        if os.path.exists(index_js):
            try:
                node_script = r"""
const fs = require('fs');
const p = process.argv[1];
const js = fs.readFileSync(p, 'utf8');
const s = js.indexOf('ma=[');
const e = js.indexOf(',Ko=()=>', s);
if (s < 0 || e < 0) {
  process.stdout.write('{}');
  process.exit(0);
}
const arr = eval('(' + js.slice(s + 3, e) + ')');
const out = {};
for (const a of arr || []) {
  if (!a || !a.fis_code) continue;
  out[String(a.fis_code)] = a;
}
process.stdout.write(JSON.stringify(out));
"""
                raw = subprocess.check_output(["node", "-e", node_script, index_js], text=True)
                parsed = json.loads(raw or "{}")
                for code, athlete in parsed.items():
                    merged[code] = merge_athlete(merged.get(code), athlete)
            except Exception:
                pass

        # 1.5) Backup deployed bundle fallback (recovers Korean names if current index got polluted)
        backup_js = os.path.abspath(os.path.join(self.script_dir, "..", "deployed_index_js_20260212.js"))
        if os.path.exists(backup_js):
            try:
                node_script = r"""
const fs = require('fs');
const p = process.argv[1];
const js = fs.readFileSync(p, 'utf8');
const s = js.indexOf('ma=[');
const e = js.indexOf(',Ko=()=>', s);
if (s < 0 || e < 0) {
  process.stdout.write('{}');
  process.exit(0);
}
const arr = eval('(' + js.slice(s + 3, e) + ')');
const out = {};
for (const a of arr || []) {
  if (!a || !a.fis_code) continue;
  out[String(a.fis_code)] = a;
}
process.stdout.write(JSON.stringify(out));
"""
                raw = subprocess.check_output(["node", "-e", node_script, backup_js], text=True)
                parsed = json.loads(raw or "{}")
                for code, athlete in parsed.items():
                    merged[code] = merge_athlete(merged.get(code), athlete)
            except Exception:
                pass

        # 2) Secondary fallback: previous pipeline output
        path = os.path.join(self.script_dir, "data", "athletes.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for a in data.get("athletes", []):
                    code = a.get("fis_code")
                    if code:
                        code = str(code)
                        merged[code] = merge_athlete(merged.get(code), a)
            except Exception:
                pass

        return merged

    def _infer_sport(self, sport_code, results, existing_sport=None):
        if sport_code != "SB":
            return self.sport_mapping.get(sport_code, existing_sport or "alpine_skiing")

        texts = []
        for r in results or []:
            for key in ("discipline", "category"):
                v = r.get(key)
                if v:
                    texts.append(str(v).lower())
        blob = " | ".join(texts)

        if "snowboard cross" in blob:
            return "snowboard_cross"
        if any(k in blob for k in ["parallel giant", "parallel slalom", "giant slalom", "slalom"]):
            return "snowboard_alpine"
        if any(k in blob for k in ["halfpipe", "slopestyle", "big air"]):
            return "snowboard_park"

        return existing_sport or "snowboard_park"

    def process(self, raw_data):
        print("⚙️ Agent B: Processing data...")
        processed = []
        
        for i, athlete in enumerate(raw_data):
            sport_code = athlete.get('sport_code', 'AL')
            existing = self.existing.get(str(athlete.get('fis_code')), {})
            sport = self._infer_sport(sport_code, athlete.get('results') or [], existing.get('sport'))
            
            # Simple Korean Name Mapping (Mock - real world would use a dictionary)
            # Since we don't have the dictionary here, we key off the english name or ID
            # This is a placeholder logic
            name_en = existing.get('name_en') or athlete.get('name_en', 'Unknown')
            existing_name_ko = existing.get('name_ko')
            name_ko = existing_name_ko if self._has_hangul(existing_name_ko) else name_en

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
            results.sort(
                key=lambda r: (
                    r.get('date', ''),
                    self._stage_priority(r.get('category') or r.get('discipline') or ''),
                    self._rank_score(r)
                ),
                reverse=True
            )
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

            current_rank = numeric_ranks[0] if numeric_ranks else None
            best_rank = min(numeric_ranks) if numeric_ranks else None
            season_starts = len(results)
            
            processed_athlete = {
                'id': f"KOR{i+1:03d}",
                'name_ko': name_ko, 
                'name_en': name_en,
                'birth_date': birth_date,
                'birth_year': birth_year,
                'age': age,
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
