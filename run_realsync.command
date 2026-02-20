#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_DIR="$ROOT_DIR/scripts"
LOG_DIR="$SCRIPT_DIR/logs"
STAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/real_sync_${STAMP}.log"
HASH_FILE="$LOG_DIR/real_sync_hash_${STAMP}.txt"
HEALTH_FILE="$LOG_DIR/real_sync_health_${STAMP}.json"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[START] $(date '+%F %T')"
echo "[INFO] ROOT_DIR=$ROOT_DIR"

command -v python3 >/dev/null
command -v node >/dev/null
command -v shasum >/dev/null

echo "[STEP] data pipeline (v7_복구 local scripts)"
python3 "$SCRIPT_DIR/data_pipeline.py" \
  --force-refresh \
  --cache-ttl-seconds 0 \
  --max-retries 2 \
  --request-timeout 10 \
  --strict-min-success-rate 1.0 \
  --health-output "$HEALTH_FILE"

echo "[STEP] patch real-site index.js data block"
node "$SCRIPT_DIR/patch_real_site_data.js" \
  --target "$ROOT_DIR/index.js" \
  --data "$SCRIPT_DIR/data/athletes.json"

echo "[STEP] data consistency audit (43 athletes)"
node "$SCRIPT_DIR/audit_results_consistency.js" \
  --target "$ROOT_DIR/index.js"

if [ -n "${SUPABASE_URL:-}" ] && [ -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]; then
  echo "[STEP] sync Supabase tables"
  python3 "$SCRIPT_DIR/supabase_sync.py" \
    --data "$SCRIPT_DIR/data/athletes.json" \
    --health "$HEALTH_FILE" \
    --source "run_realsync.command"
else
  echo "[STEP] skip Supabase sync (missing SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY)"
fi

echo "[STEP] refresh index.html with hash-busted asset urls"
JS_HASH="$(shasum -a 256 "$ROOT_DIR/index.js" | awk '{print $1}')"
CSS_HASH="$(shasum -a 256 "$ROOT_DIR/index.css" | awk '{print $1}')"
cat > "$ROOT_DIR/index.html" <<HTML
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <link rel="icon" type="image/svg+xml" href="./vite.svg" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>스키스노보드 대표팀 대시보드</title>
    <script type="module" crossorigin src="./index.js?v=${JS_HASH}"></script>
    <link rel="stylesheet" crossorigin href="./index.css?v=${CSS_HASH}">
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
HTML

echo "[STEP] record hashes"
TARGET_JS_HASH="$(shasum -a 256 "$ROOT_DIR/index.js" | awk '{print $1}')"
TARGET_CSS_HASH="$(shasum -a 256 "$ROOT_DIR/index.css" | awk '{print $1}')"
TARGET_HTML_HASH="$(shasum -a 256 "$ROOT_DIR/index.html" | awk '{print $1}')"
TARGET_MAX_DATE="$(node -e 'const fs=require("fs");const s=fs.readFileSync(process.argv[1],"utf8");const m=[...s.matchAll(/"date":"(\d{4}-\d{2}-\d{2})"/g)].map(x=>x[1]);m.sort();process.stdout.write(m[m.length-1]||"")' "$ROOT_DIR/index.js")"

{
  echo "timestamp=$STAMP"
  echo "target_index_js_sha256=$TARGET_JS_HASH"
  echo "target_index_css_sha256=$TARGET_CSS_HASH"
  echo "target_index_html_sha256=$TARGET_HTML_HASH"
  echo "target_max_event_date=$TARGET_MAX_DATE"
  echo "target_index_js_path=$ROOT_DIR/index.js"
  echo "target_index_css_path=$ROOT_DIR/index.css"
  echo "target_index_html_path=$ROOT_DIR/index.html"
} | tee "$HASH_FILE"

echo "[DONE] real-site sync complete"
echo "[LOG] $LOG_FILE"
echo "[HASH] $HASH_FILE"
echo "[HEALTH] $HEALTH_FILE"
