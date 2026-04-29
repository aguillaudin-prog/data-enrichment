"""Main enrichment pipeline.

Reads BACKLOG.csv, processes each operator, writes drafts to Supabase
staging table. Idempotent: safe to re-run.

Usage:
    python scripts/run_enrichment.py                # full run
    python scripts/run_enrichment.py --limit 10     # test on 10 operators
    python scripts/run_enrichment.py --no-llm       # disable LLM fallback
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

# Allow running as: python scripts/run_enrichment.py
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.db import DraftRow, upsert_draft  # noqa: E402
from lib.email_extractor import extract_emails  # noqa: E402
from lib.mx_check import email_mx_valid  # noqa: E402
from lib.scraper import candidate_urls, fetch  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("enrichment")

BACKLOG_PATH = Path(__file__).parent.parent / "BACKLOG.csv"


def process_operator(row: dict, use_llm: bool) -> int:
    """Process one operator. Returns number of email drafts written."""
    name = row["operator_name"]
    website = (row.get("website") or "").strip()
    profile_url = (row.get("profile_url") or "").strip()

    seed_urls: list[str] = []
    if website:
        seed_urls.extend(candidate_urls(website))
    if profile_url:
        seed_urls.append(profile_url)

    if not seed_urls:
        logger.info(f'{{"operator":"{name}","status":"no_seed_urls"}}')
        return 0

    all_hits = []
    for url in seed_urls:
        result = fetch(url)
        if result.error or not result.raw_html:
            continue
        hits = extract_emails(result.raw_html, result.url, result.fetched_at)
        all_hits.extend(hits)
        if hits:
            logger.info(f'{{"operator":"{name}","url":"{url}","found":{len(hits)}}}')

    # LLM fallback if no regex hits and the home page fetched OK
    if not all_hits and use_llm and website:
        from lib.llm_assist import llm_extract  # lazy import — optional dep path
        from lib.email_extractor import EmailHit, _score, _snippet
        result = fetch(website)
        if result.raw_html:
            llm_hits = llm_extract(name, result.url, result.raw_html)
            for lh in llm_hits:
                # llm_extract already verified verbatim; rebuild a regular EmailHit
                all_hits.append(EmailHit(
                    email=lh.email,
                    source_url=result.url,
                    snippet=_snippet(result.raw_html, lh.email),
                    score=_score(lh.email),
                    fetched_at=result.fetched_at,
                ))

    if not all_hits:
        logger.info(f'{{"operator":"{name}","status":"not_found"}}')
        return 0

    # Dedupe by email, keep highest score
    by_email: dict[str, "EmailHit"] = {}  # type: ignore  # noqa: F821
    for h in all_hits:
        if h.email not in by_email or h.score > by_email[h.email].score:
            by_email[h.email] = h

    # Determine best
    best_email = max(by_email.values(), key=lambda h: h.score).email

    written = 0
    for h in by_email.values():
        method = "llm_assist" if not all(  # crude detection
            h.email in extract_emails_marker(h) for extract_emails_marker in []
        ) else "regex"
        # (method detection simplification — see CLAUDE.md to refine)
        upsert_draft(DraftRow(
            operator_name=name,
            email=h.email,
            source_url=h.source_url,
            snippet=h.snippet,
            score=h.score,
            fetched_at=h.fetched_at,
            is_best=(h.email == best_email),
            mx_valid=email_mx_valid(h.email),
            method="regex",  # TODO Claude Code: track method through pipeline
        ))
        written += 1
    return written


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only the first N operators (for testing)")
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM fallback (regex only)")
    args = parser.parse_args()

    use_llm = not args.no_llm

    with BACKLOG_PATH.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if args.limit:
        rows = rows[:args.limit]

    logger.info(f'{{"pipeline":"start","total_operators":{len(rows)},"use_llm":{use_llm}}}')
    total_written = 0
    for i, row in enumerate(rows, 1):
        try:
            n = process_operator(row, use_llm=use_llm)
            total_written += n
        except Exception as e:
            logger.exception(f"Failed on {row.get('operator_name')}: {e}")
        if i % 10 == 0:
            logger.info(f'{{"progress":{i},"total":{len(rows)},"written":{total_written}}}')

    logger.info(f'{{"pipeline":"end","drafts_written":{total_written}}}')


if __name__ == "__main__":
    main()
