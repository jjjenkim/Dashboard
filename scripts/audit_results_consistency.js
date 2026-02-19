#!/usr/bin/env node
const fs = require("fs");

function usage() {
  console.error("Usage: node audit_results_consistency.js --target <v7_index_js>");
  process.exit(2);
}

function arg(name) {
  const idx = process.argv.indexOf(name);
  if (idx === -1 || idx + 1 >= process.argv.length) return null;
  return process.argv[idx + 1];
}

function stagePriority(text = "") {
  const t = String(text).trim().toLowerCase();
  if (t.includes("qualif") || t === "qua") return 0;
  if (t.includes("final")) return 2;
  return 1;
}

function hasQualification(text = "") {
  const t = String(text).trim().toLowerCase();
  return t.includes("qualif") || t === "qua";
}

const targetPath = arg("--target");
if (!targetPath) usage();

const js = fs.readFileSync(targetPath, "utf8");
const start = js.indexOf("ma=[");
const end = js.indexOf(",Ko=()=>", start);
if (start < 0 || end < 0) {
  throw new Error("Cannot locate ma data block");
}

const athletes = eval("(" + js.slice(start + 3, end) + ")");
if (!Array.isArray(athletes)) throw new Error("ma block is not array");

const bySport = {};
const orderIssues = [];
const profileModalMismatch = [];

for (const a of athletes) {
  const sport = a.sport || "unknown";
  bySport[sport] = (bySport[sport] || 0) + 1;

  const profile = a.recent_results || [];
  const modal = a.modal_results || profile;
  if (JSON.stringify(profile) !== JSON.stringify(modal)) {
    profileModalMismatch.push({
      fis_code: a.fis_code,
      name: a.name_ko || a.name_en,
    });
  }

  const groups = new Map();
  for (const r of profile) {
    const key = [r.date || "", r.place || "", r.discipline || ""].join("|");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(r);
  }

  for (const [key, rows] of groups.entries()) {
    const hasQual = rows.some((r) => hasQualification(r.category || r.category_name || ""));
    const hasMain = rows.some((r) => !hasQualification(r.category || r.category_name || ""));
    if (!hasQual || !hasMain) continue;

    const first = rows[0];
    if (hasQualification(first.category || first.category_name || "")) {
      orderIssues.push({
        fis_code: a.fis_code,
        name: a.name_ko || a.name_en,
        sport,
        key,
        first_category: first.category || first.category_name || "",
      });
      continue;
    }

    const sorted = [...rows].sort((x, y) => {
      const stage = stagePriority(y.category || y.category_name || "") - stagePriority(x.category || x.category_name || "");
      if (stage !== 0) return stage;
      return (Number(x.rank) || 9999) - (Number(y.rank) || 9999);
    });
    if (JSON.stringify(rows) !== JSON.stringify(sorted)) {
      orderIssues.push({
        fis_code: a.fis_code,
        name: a.name_ko || a.name_en,
        sport,
        key,
        first_category: first.category || first.category_name || "",
      });
    }
  }
}

const summary = {
  athletes: athletes.length,
  sports: bySport,
  order_issue_count: orderIssues.length,
  profile_modal_mismatch_count: profileModalMismatch.length,
  order_issues_preview: orderIssues.slice(0, 20),
  profile_modal_mismatch_preview: profileModalMismatch.slice(0, 20),
};

console.log(JSON.stringify(summary, null, 2));
if (orderIssues.length > 0 || profileModalMismatch.length > 0) process.exit(1);

