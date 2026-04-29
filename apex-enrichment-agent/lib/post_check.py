"""Post-check: re-fetch the source URL and verify the email is still there.

If the email has disappeared from the source, mark the draft
post_check_failed=true. The promotion UI in APEX should refuse to
promote drafts with post_check_failed=true unless explicitly forced.
"""
from __future__ import annotations

import logging

from lib.db import list_pending_post_check, mark_post_check_failed
from lib.email_extractor import verify_in_source
from lib.scraper import fetch

logger = logging.getLogger(__name__)


def run_post_check() -> dict:
    """Iterate over pending drafts and re-verify."""
    drafts = list_pending_post_check(older_than_hours=24)
    stats = {"checked": 0, "still_present": 0, "failed": 0}

    for draft in drafts:
        stats["checked"] += 1
        result = fetch(draft["source_url"])
        if result.error:
            logger.warning(f"Re-fetch failed for {draft['source_url']}: {result.error}")
            mark_post_check_failed(
                draft["operator_name"], draft["email"], draft["source_url"]
            )
            stats["failed"] += 1
            continue

        if verify_in_source(draft["email"], result.raw_html):
            stats["still_present"] += 1
        else:
            logger.info(f"Post-check FAILED: {draft['email']} no longer in {draft['source_url']}")
            mark_post_check_failed(
                draft["operator_name"], draft["email"], draft["source_url"]
            )
            stats["failed"] += 1

    return stats
