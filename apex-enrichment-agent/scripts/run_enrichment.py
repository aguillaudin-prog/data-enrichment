"""Main enrichment pipeline.

Reads BACKLOG.csv, processes each operator, writes one row per email
candidate to drafts.csv. No database — the human reviewer validates by
filling the validation_status column in a spreadsheet.

Two modes per operator (decided automatically from existing_email):

  - discovery: operator has no email. Scrape candidate URLs and emit any
    email found verbatim in the raw HTML.
  - verify:    operator already has an email. Re-scrape and confirm it
    still appears on the official site (mode='existing_confirmed') or
    flag it as missing (mode='existing_missing'). Discovery still runs
    in parallel so we can spot a better cargo@ alternative.

Usage:
    python scripts/run_enrichment.py                # full run
    python scripts/run_enrichment.py --limit 10     # test on 10 operators
    python scripts/run_enrichment.py --no-llm       # disable LLM fallback
    python scripts/run_enrichment.py --retry        # re-process non-pending
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

from lib.email_extractor import (  # noqa: E402
    EmailHit,
    _score,
    _snippet,
    extract_emails,
    verify_in_source,
)
from lib.mx_check import email_mx_valid  # noqa: E402
from lib.scraper import candidate_urls, fetch  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","lvl":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("enrichment")

ROOT = Path(__file__).parent.parent
BACKLOG_PATH = ROOT / "BACKLOG.csv"
DRAFTS_PATH = ROOT / "drafts.csv"

DRAFT_FIELDS = [
    "operator_name",
    "email",
    "score",
    "is_best",
    "mx_valid",
    "method",          # 'regex' | 'llm_assist' | 'previous_data'
    "mode",            # 'discovered' | 'existing_confirmed' | 'existing_missing' | 'no_source'
    "source_url",
    "snippet",
    "fetched_at",
    "existing_email",  # the email that was in BACKLOG.existing_email (for context)
    "validation_status",  # filled by human: 'approved' | 'rejected' | ''
    "validation_notes",
]


def _build_seeds(website: str, profile_url: str) -> list[str]:
    seeds: list[str] = []
    if website:
        seeds.extend(candidate_urls(website))
    if profile_url:
        seeds.append(profile_url)
    return seeds


def _row(
    operator_name: str,
    email: str,
    score: int,
    is_best: bool,
    mx_valid: bool,
    method: str,
    mode: str,
    source_url: str,
    snippet: str,
    fetched_at: str,
    existing_email: str,
    prev: dict | None = None,
) -> dict:
    """Build a draft row, preserving any prior human-entered validation."""
    prev = prev or {}
    return {
        "operator_name": operator_name,
        "email": email,
        "score": score,
        "is_best": "true" if is_best else "false",
        "mx_valid": "true" if mx_valid else "false",
        "method": method,
        "mode": mode,
        "source_url": source_url,
        "snippet": snippet,
        "fetched_at": fetched_at,
        "existing_email": existing_email,
        "validation_status": prev.get("validation_status", ""),
        "validation_notes": prev.get("validation_notes", ""),
    }


def process_operator(
    row: dict,
    use_llm: bool,
    prior_drafts: dict[tuple[str, str, str], dict],
) -> list[dict]:
    """Return the list of draft rows for one operator (zero or more)."""
    name = row["operator_name"]
    website = (row.get("website") or "").strip()
    profile_url = (row.get("profile_url") or "").strip()
    existing = (row.get("existing_email") or "").strip().lower()

    seeds = _build_seeds(website, profile_url)
    if not seeds:
        # No way to verify or discover anything from the web.
        if existing:
            logger.info(f'{{"operator":"{name}","status":"no_source_existing"}}')
            return [_row(
                operator_name=name, email=existing, score=_score(existing),
                is_best=True, mx_valid=email_mx_valid(existing),
                method="previous_data", mode="no_source",
                source_url="", snippet="(no website / profile_url to verify against)",
                fetched_at="", existing_email=existing,
                prev=prior_drafts.get((name, existing, "")),
            )]
        logger.info(f'{{"operator":"{name}","status":"no_source_no_email"}}')
        return []

    discovered: list[EmailHit] = []
    existing_seen: tuple[str, str, str] | None = None  # (url, fetched_at, snippet)

    for url in seeds:
        result = fetch(url)
        if result.error or not result.raw_html:
            continue
        hits = extract_emails(result.raw_html, result.url, result.fetched_at)
        discovered.extend(hits)
        if hits:
            logger.info(f'{{"operator":"{name}","url":"{url}","found":{len(hits)}}}')
        if existing and existing_seen is None and verify_in_source(existing, result.raw_html):
            existing_seen = (
                result.url,
                result.fetched_at,
                _snippet(result.raw_html, existing),
            )

    # LLM fallback only if regex found absolutely nothing and we're in pure discovery.
    if not discovered and use_llm and website and not existing:
        from lib.llm_assist import llm_extract  # lazy import — optional dep path
        result = fetch(website)
        if result.raw_html:
            llm_hits = llm_extract(name, result.url, result.raw_html)
            for lh in llm_hits:
                discovered.append(EmailHit(
                    email=lh.email,
                    source_url=result.url,
                    snippet=_snippet(result.raw_html, lh.email),
                    score=_score(lh.email),
                    fetched_at=result.fetched_at,
                    method="llm_assist",
                ))

    # Dedupe discovered by email, keep highest score
    by_email: dict[str, EmailHit] = {}
    for h in discovered:
        if h.email not in by_email or h.score > by_email[h.email].score:
            by_email[h.email] = h

    # Determine the best email overall.
    candidates: list[EmailHit] = list(by_email.values())
    if existing and existing not in by_email and existing_seen is not None:
        url0, ts0, snip0 = existing_seen
        candidates.append(EmailHit(
            email=existing, source_url=url0, snippet=snip0,
            score=_score(existing), fetched_at=ts0, method="regex",
        ))
    best = max(candidates, key=lambda h: h.score).email if candidates else None

    out: list[dict] = []

    for h in by_email.values():
        mode = "existing_confirmed" if (existing and h.email == existing) else "discovered"
        out.append(_row(
            operator_name=name, email=h.email, score=h.score,
            is_best=(h.email == best),
            mx_valid=email_mx_valid(h.email),
            method=h.method, mode=mode,
            source_url=h.source_url, snippet=h.snippet,
            fetched_at=h.fetched_at, existing_email=existing,
            prev=prior_drafts.get((name, h.email, h.source_url)),
        ))

    # Existing email not rediscovered by regex — emit a row for it
    # so the reviewer always sees its status.
    if existing and existing not in by_email:
        if existing_seen is not None:
            url0, ts0, snip0 = existing_seen
            out.append(_row(
                operator_name=name, email=existing, score=_score(existing),
                is_best=(existing == best),
                mx_valid=email_mx_valid(existing),
                method="regex", mode="existing_confirmed",
                source_url=url0, snippet=snip0, fetched_at=ts0,
                existing_email=existing,
                prev=prior_drafts.get((name, existing, url0)),
            ))
        else:
            out.append(_row(
                operator_name=name, email=existing, score=_score(existing),
                is_best=False,
                mx_valid=email_mx_valid(existing),
                method="previous_data", mode="existing_missing",
                source_url="",
                snippet="Existing email NOT found on operator's site — review manually",
                fetched_at="",
                existing_email=existing,
                prev=prior_drafts.get((name, existing, "")),
            ))

    if not out:
        logger.info(f'{{"operator":"{name}","status":"not_found"}}')
    return out


def _load_prior_drafts() -> dict[tuple[str, str, str], dict]:
    """Index existing drafts by (operator, email, source_url) so reruns
    preserve any human-entered validation_status / validation_notes."""
    if not DRAFTS_PATH.exists():
        return {}
    with DRAFTS_PATH.open(encoding="utf-8", newline="") as f:
        return {
            (r["operator_name"], r["email"], r["source_url"]): r
            for r in csv.DictReader(f)
        }


def _save_drafts(rows: list[dict]) -> None:
    tmp = DRAFTS_PATH.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DRAFT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(DRAFTS_PATH)


def _save_backlog(rows: list[dict], fieldnames: list[str]) -> None:
    tmp = BACKLOG_PATH.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(BACKLOG_PATH)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only the first N operators (for testing)")
    parser.add_argument("--no-llm", action="store_true",
                        help="Disable LLM fallback (regex only)")
    parser.add_argument("--retry", action="store_true",
                        help="Re-process operators whose status is not 'pending'")
    args = parser.parse_args()

    use_llm = not args.no_llm

    with BACKLOG_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        backlog_fields = list(reader.fieldnames or [])
        backlog = list(reader)

    if args.retry:
        to_process = list(backlog)
    else:
        to_process = [r for r in backlog if (r.get("enrichment_status") or "pending") == "pending"]

    if args.limit:
        to_process = to_process[:args.limit]

    prior = _load_prior_drafts()

    # Keep prior rows for operators we are NOT re-processing this run, so we
    # don't drop validations the human already made on operators outside scope.
    skipped_ops = {r["operator_name"] for r in backlog} - {r["operator_name"] for r in to_process}
    out_rows: list[dict] = [
        row for row in prior.values() if row["operator_name"] in skipped_ops
    ]

    logger.info(
        f'{{"pipeline":"start","total_operators":{len(to_process)},'
        f'"skipped":{len(backlog) - len(to_process)},"use_llm":{use_llm}}}'
    )

    for i, row in enumerate(to_process, 1):
        try:
            new_rows = process_operator(row, use_llm=use_llm, prior_drafts=prior)
            out_rows.extend(new_rows)
            row["enrichment_status"] = "enriched" if new_rows else "not_found"
        except Exception as e:
            logger.exception(f"Failed on {row.get('operator_name')}: {e}")
            row["enrichment_status"] = "error"

        if i % 10 == 0:
            _save_drafts(out_rows)
            _save_backlog(backlog, backlog_fields)
            logger.info(f'{{"progress":{i},"total":{len(to_process)},"drafts":{len(out_rows)}}}')

    _save_drafts(out_rows)
    _save_backlog(backlog, backlog_fields)
    logger.info(f'{{"pipeline":"end","drafts":{len(out_rows)}}}')


if __name__ == "__main__":
    main()
