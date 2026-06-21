"""Render the machine-readable outputs/ CSVs into human-readable reports/.

A pure presentation layer over the pipeline outputs: it renames snake_case
columns to plain-English phrases and tidies the tier-3 table (drops the raw
0-1 transferability score column and strips the inline numeric subscores from
the auto-generated "Utility/Transferability" prose). Cell values are otherwise
copied verbatim, so reports/ is a readable view of outputs/, never a new
analysis. Re-run any pipeline first, then `python -m litreview.report`.
"""

from __future__ import annotations

import csv
import os
import re
from typing import Optional

from .config import SETTINGS, CANADIAN_PROVINCES

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")

# snake_case output column -> plain-English report header. Anything not listed
# falls back to a Title Case of the raw name (see humanize_header).
COLUMN_LABELS: dict[str, str] = {
    # aggregate_* tables
    "ecosystem": "Ecosystem",
    "service": "Ecosystem Service",
    "count_query": "Search Query",
    "papers_global": "Papers (Global, OpenAlex)",
    "papers_north_america": "Papers (North America, OpenAlex)",
    "papers_canada": "Papers (Canada, OpenAlex)",
    "na_share": "North America Share",
    "ca_share": "Canada Share",
    "value_median_global": "Median Value, Int$/ha/yr (Global)",
    "value_iqr_global": "Value IQR, Int$/ha/yr (Global)",
    "esvd_studies_global": "ESVD Studies (Global)",
    "value_median_north_america": "Median Value, Int$/ha/yr (North America)",
    "value_iqr_north_america": "Value IQR, Int$/ha/yr (North America)",
    "esvd_studies_north_america": "ESVD Studies (North America)",
    "value_median_canada": "Median Value, Int$/ha/yr (Canada)",
    "value_iqr_canada": "Value IQR, Int$/ha/yr (Canada)",
    "esvd_studies_canada": "ESVD Studies (Canada)",
    # esvd_summary_* tables
    "n": "Value Records",
    "n_studies": "Distinct Studies",
    "median": "Median (Int$/ha/yr)",
    "q1": "25th Percentile",
    "q3": "75th Percentile",
    "trimmed_mean_10pct": "Trimmed Mean (10%)",
    "min": "Minimum",
    "max": "Maximum",
    # keywords_* tables
    "term": "Term",
    "count": "Paper Count",
    "share": "Share of Corpus",
    "tfidf": "TF-IDF Score",
    "doc_freq": "Document Frequency",
    "doc_share": "Document Share",
}

# provincial_* matrices use province names as column headers — keep them
# verbatim (so "Newfoundland and Labrador" isn't title-cased to "... And ...").
COLUMN_LABELS.update({prov: prov for prov in CANADIAN_PROVINCES})

# tier-3 already ships English headers, so it is passed through unchanged except
# for this column, which is dropped from the human report (the raw 0-1 score is
# internal provenance; see TableRow).
TIER3_DROP_COLUMNS = {"Transferability Score"}
TIER3_PROSE_COLUMN = "Utility/Transferability"

# Strips the inline subscore that follows a criterion word in the auto
# rationale, e.g. "location 1.00 (mentions ...)" -> "location (mentions ...)"
# and "peer-reviewed 1.0" -> "peer-reviewed".
_INLINE_SCORE_RE = re.compile(
    r"\b(location|ecosystem|service|peer-reviewed|method)\s+\d+(?:\.\d+)?"
)


def humanize_header(col: str) -> str:
    return COLUMN_LABELS.get(col, col.replace("_", " ").strip().title())


def clean_tier3_prose(text: str) -> str:
    """Remove the inline 1.0/1.00 subscores from the auto rationale text."""
    text = _INLINE_SCORE_RE.sub(r"\1", text)
    return re.sub(r"\s{2,}", " ", text).strip()


def _read_csv(path: str) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        return fields, list(reader)


def _process_tier3(fields: list[str], rows: list[dict]) -> tuple[list[str], list[dict]]:
    out_fields = [c for c in fields if c not in TIER3_DROP_COLUMNS]
    out_rows = []
    for r in rows:
        d = {c: r.get(c, "") for c in out_fields}
        if TIER3_PROSE_COLUMN in d:
            d[TIER3_PROSE_COLUMN] = clean_tier3_prose(d[TIER3_PROSE_COLUMN] or "")
        out_rows.append(d)
    return out_fields, out_rows


def _process_generic(fields: list[str], rows: list[dict]) -> tuple[list[str], list[dict]]:
    headers = [humanize_header(c) for c in fields]
    out_rows = [{humanize_header(c): r.get(c, "") for c in fields} for r in rows]
    return headers, out_rows


def process_file(src_path: str, dest_path: str, *, tier3_style: bool = False) -> str:
    """Render one CSV. `tier3_style` applies the per-paper-table cleanup
    (drop the raw score column, strip inline subscores) instead of header
    humanization; it's set for everything under the filtered/ subdir."""
    fields, rows = _read_csv(src_path)
    if not fields:  # empty file — copy through as-is
        open(dest_path, "w", encoding="utf-8-sig").close()
        return dest_path
    if tier3_style or os.path.basename(src_path).startswith(("tier3", "topic_table")):
        out_fields, out_rows = _process_tier3(fields, rows)
    else:
        out_fields, out_rows = _process_generic(fields, rows)
    with open(dest_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()
        w.writerows(out_rows)
    return dest_path


def run(
    *,
    outputs_dir: Optional[str] = None,
    reports_dir: Optional[str] = None,
    do_print: bool = True,
) -> list[str]:
    """Render every CSV under outputs/ into reports/, mirroring subdirs."""
    outputs_dir = outputs_dir or SETTINGS.output_dir
    reports_dir = reports_dir or REPORTS_DIR
    written: list[str] = []
    for root, _dirs, files in os.walk(outputs_dir):
        for name in sorted(files):
            if not name.endswith(".csv"):
                continue
            src = os.path.join(root, name)
            rel = os.path.relpath(src, outputs_dir)
            dest = os.path.join(reports_dir, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            # Per-paper tables live under filtered/ and get the tier-3 cleanup.
            tier3_style = rel.split(os.sep)[0] == "filtered"
            process_file(src, dest, tier3_style=tier3_style)
            written.append(dest)
            if do_print:
                print(f"  {rel}")
    if do_print:
        print(f"\nWrote {len(written)} report(s) to {reports_dir}/")
    return written


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(
        description="Render outputs/*.csv into human-readable reports/*.csv "
                    "(English headers; tier-3 score cleanup).")
    p.add_argument("--outputs", default=None, help="source dir (default: outputs)")
    p.add_argument("--reports", default=None, help="dest dir (default: reports)")
    args = p.parse_args()
    run(outputs_dir=args.outputs, reports_dir=args.reports)


if __name__ == "__main__":
    main()
