#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests


def env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def parse_args():
    p = argparse.ArgumentParser(description="Sync athletes/results to Supabase")
    p.add_argument("--data", required=True, help="path to athletes.json")
    p.add_argument("--health", default="", help="optional health report path")
    p.add_argument("--source", default="v7_pipeline", help="sync source label")
    return p.parse_args()


def load_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_int(v):
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def safe_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def make_result_uid(fis_code: str, r: Dict) -> str:
    key = "|".join(
        [
            str(fis_code or ""),
            str(r.get("date") or ""),
            str(r.get("place") or ""),
            str(r.get("category") or ""),
            str(r.get("discipline") or r.get("event") or ""),
            str(r.get("rank") if r.get("rank") is not None else ""),
            str(r.get("rank_status") or ""),
            str(r.get("points") if r.get("points") is not None else ""),
            str(r.get("cup_points") if r.get("cup_points") is not None else ""),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def build_rows(doc: Dict, sync_run_id: str) -> Tuple[List[Dict], List[Dict], str]:
    athletes = doc.get("athletes", [])
    meta = doc.get("metadata", {})
    source_updated_at = meta.get("last_updated")

    athlete_rows = []
    result_rows = []
    max_date = ""

    for a in athletes:
        fis_code = str(a.get("fis_code") or "").strip()
        if not fis_code:
            continue

        athlete_rows.append(
            {
                "fis_code": fis_code,
                "id": a.get("id"),
                "name_ko": a.get("name_ko"),
                "name_en": a.get("name_en"),
                "birth_date": a.get("birth_date"),
                "birth_year": safe_int(a.get("birth_year")),
                "age": safe_int(a.get("age")),
                "sport": a.get("sport"),
                "sport_display": a.get("sport_display"),
                "team": a.get("team"),
                "fis_url": a.get("fis_url"),
                "current_rank": safe_int(a.get("current_rank")),
                "best_rank": safe_int(a.get("best_rank")),
                "season_starts": safe_int(a.get("season_starts")),
                "medals": a.get("medals") or {"gold": 0, "silver": 0, "bronze": 0},
                "source_updated_at": source_updated_at,
                "synced_at": datetime.now(timezone.utc).isoformat(),
                "sync_run_id": sync_run_id,
            }
        )

        for r in (a.get("recent_results") or []):
            d = r.get("date")
            if d and d > max_date:
                max_date = d
            result_rows.append(
                {
                    "result_uid": make_result_uid(fis_code, r),
                    "fis_code": fis_code,
                    "event_date": d,
                    "place": r.get("place"),
                    "category": r.get("category"),
                    "discipline": r.get("discipline") or r.get("event"),
                    "rank": safe_int(r.get("rank")),
                    "rank_status": r.get("rank_status"),
                    "fis_points": safe_float(r.get("points")),
                    "cup_points": safe_float(r.get("cup_points")),
                    "source_updated_at": source_updated_at,
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                    "sync_run_id": sync_run_id,
                }
            )

    # Deduplicate same logical result rows within one sync batch.
    dedup = {}
    for row in result_rows:
        dedup[row["result_uid"]] = row
    result_rows = list(dedup.values())

    return athlete_rows, result_rows, max_date


def request_headers(service_role_key: str) -> Dict:
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
    }


def post_upsert(base_url: str, table: str, rows: List[Dict], conflict_cols: str, headers: Dict, chunk_size: int = 500):
    if not rows:
        return
    url = f"{base_url}/rest/v1/{table}?on_conflict={conflict_cols}"
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        h = dict(headers)
        h["Prefer"] = "resolution=merge-duplicates,return=minimal"
        r = requests.post(url, headers=h, data=json.dumps(chunk), timeout=30)
        if r.status_code >= 300:
            raise RuntimeError(f"Upsert failed [{table}] {r.status_code}: {r.text[:500]}")


def delete_stale(base_url: str, table: str, sync_run_id: str, headers: Dict):
    url = f"{base_url}/rest/v1/{table}?sync_run_id=neq.{sync_run_id}"
    h = dict(headers)
    h["Prefer"] = "return=minimal"
    r = requests.delete(url, headers=h, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Delete stale failed [{table}] {r.status_code}: {r.text[:500]}")


def insert_sync_log(base_url: str, payload: Dict, headers: Dict):
    url = f"{base_url}/rest/v1/sync_logs"
    h = dict(headers)
    h["Prefer"] = "return=minimal"
    r = requests.post(url, headers=h, data=json.dumps([payload]), timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"sync_logs insert failed {r.status_code}: {r.text[:500]}")


def main():
    args = parse_args()
    supabase_url = env("SUPABASE_URL")
    service_role_key = env("SUPABASE_SERVICE_ROLE_KEY")

    doc = load_json(args.data)
    sync_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    athlete_rows, result_rows, max_event_date = build_rows(doc, sync_run_id)
    headers = request_headers(service_role_key)

    detail = {
        "data_path": args.data,
        "health_path": args.health,
        "source": args.source,
    }

    if args.health and os.path.exists(args.health):
        try:
            detail["health"] = load_json(args.health)
        except Exception:
            detail["health_parse_error"] = True

    try:
        post_upsert(supabase_url, "athletes", athlete_rows, "fis_code", headers)
        post_upsert(supabase_url, "athlete_results", result_rows, "result_uid", headers)
        delete_stale(supabase_url, "athlete_results", sync_run_id, headers)
        delete_stale(supabase_url, "athletes", sync_run_id, headers)

        insert_sync_log(
            supabase_url,
            {
                "sync_run_id": sync_run_id,
                "source": args.source,
                "success": True,
                "athletes_count": len(athlete_rows),
                "results_count": len(result_rows),
                "max_event_date": max_event_date or None,
                "detail": detail,
            },
            headers,
        )

        print(f"supabase_sync_run_id={sync_run_id}")
        print(f"supabase_athletes={len(athlete_rows)}")
        print(f"supabase_results={len(result_rows)}")
        print(f"supabase_max_event_date={max_event_date}")
    except Exception as e:
        err_payload = {
            "sync_run_id": sync_run_id,
            "source": args.source,
            "success": False,
            "athletes_count": len(athlete_rows),
            "results_count": len(result_rows),
            "max_event_date": max_event_date or None,
            "detail": {**detail, "error": str(e)},
        }
        try:
            insert_sync_log(supabase_url, err_payload, headers)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
