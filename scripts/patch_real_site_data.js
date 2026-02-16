#!/usr/bin/env node
const fs = require("fs");

function usage() {
  console.error(
    "Usage: node patch_real_site_data.js --target <v7_index_js> --data <athletes_json>"
  );
  process.exit(2);
}

function arg(name) {
  const idx = process.argv.indexOf(name);
  if (idx === -1 || idx + 1 >= process.argv.length) return null;
  return process.argv[idx + 1];
}

const targetPath = arg("--target");
const dataPath = arg("--data");
if (!targetPath || !dataPath) usage();

const js = fs.readFileSync(targetPath, "utf8");
const incoming = JSON.parse(fs.readFileSync(dataPath, "utf8")).athletes || [];

const start = js.indexOf("ma=[");
const end = js.indexOf(",Ko=()=>", start);
if (start < 0 || end < 0) {
  throw new Error("Cannot locate ma data block in target bundle");
}

const oldData = eval("(" + js.slice(start + 3, end) + ")");
if (!Array.isArray(oldData)) {
  throw new Error("Existing ma data block is not an array");
}

const byCode = new Map(incoming.map((a) => [String(a.fis_code), a]));

function toNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function categoryCode(category = "") {
  const c = String(category).toLowerCase();
  if (c.includes("world cup")) return "WC";
  if (c.includes("world championships")) return "WSC";
  if (c.includes("olympic")) return "OWG";
  if (c.includes("asian winter")) return "AWG";
  if (c.includes("far east cup")) return "FEC";
  if (c === "fis") return "FIS";
  return "";
}

function mapResult(r) {
  const rank = toNum(r.rank, 0);
  return {
    date: r.date || null,
    place: r.place || null,
    category: r.category || null,
    category_code: categoryCode(r.category || ""),
    category_name: r.category || null,
    discipline: r.discipline || r.event || null,
    rank,
    result_code: rank > 0 ? null : r.rank_status || "DNF",
    fis_points: toNum(r.points, 0),
    cup_points: toNum(r.cup_points, 0),
  };
}

function resultKey(r) {
  return [r.date || "", r.place || "", r.discipline || ""].join("|");
}

function mergeResults(oldResults, newResults) {
  const map = new Map();
  for (const r of oldResults || []) map.set(resultKey(r), r);
  for (const r of newResults || []) map.set(resultKey(r), r);
  return [...map.values()].sort((a, b) =>
    String(b.date || "").localeCompare(String(a.date || ""))
  );
}

function mapAthlete(oldAthlete, newAthlete) {
  const mappedResults = (newAthlete.recent_results || []).map(mapResult);
  const mergedResults = mergeResults(oldAthlete.recent_results || [], mappedResults);
  const latestPoints =
    mappedResults.find((r) => Number.isFinite(r.fis_points) && r.fis_points > 0)
      ?.fis_points ?? oldAthlete.fis_points ?? 0;

  return {
    ...oldAthlete,
    fis_code: String(newAthlete.fis_code || oldAthlete.fis_code),
    fis_url: newAthlete.fis_url || oldAthlete.fis_url,
    birth_date: newAthlete.birth_date || oldAthlete.birth_date,
    birth_year: toNum(newAthlete.birth_year, oldAthlete.birth_year || null),
    age: toNum(newAthlete.age, oldAthlete.age || null),
    current_rank: toNum(newAthlete.current_rank, oldAthlete.current_rank || 0),
    fis_points: latestPoints,
    recent_results: mergedResults,
  };
}

let updated = 0;
const merged = oldData.map((a) => {
  const n = byCode.get(String(a.fis_code));
  if (!n) return a;
  updated += 1;
  return mapAthlete(a, n);
});

const backupPath = `${targetPath}.bak_${Date.now()}`;
fs.copyFileSync(targetPath, backupPath);

const replacement = "ma=" + JSON.stringify(merged);
const patched = js.slice(0, start) + replacement + js.slice(end);
fs.writeFileSync(targetPath, patched, "utf8");

let maxEventDate = "";
let resultCount = 0;
for (const athlete of merged) {
  for (const r of athlete.recent_results || []) {
    if (!r.date) continue;
    resultCount += 1;
    if (!maxEventDate || r.date > maxEventDate) maxEventDate = r.date;
  }
}

console.log(`backup=${backupPath}`);
console.log(`athletes=${merged.length}`);
console.log(`updated_athletes=${updated}`);
console.log(`result_count=${resultCount}`);
console.log(`max_event_date=${maxEventDate}`);
