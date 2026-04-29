"""Supabase client — STAGING TABLE ONLY.

This module is deliberately restricted: it can only insert/update on
operator_enrichment_drafts. Any attempt to touch other tables should
fail at code review (and ideally at Supabase RLS level).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from supabase import Client, create_client

logger = logging.getLogger(__name__)

STAGING_TABLE = "operator_enrichment_drafts"

_client: Optional[Client] = None


def _client_lazy() -> Client:
    global _client
    if _client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        _client = create_client(url, key)
    return _client


@dataclass
class DraftRow:
    operator_name: str
    email: str
    source_url: str
    snippet: str
    score: int
    fetched_at: str
    is_best: bool
    mx_valid: bool
    method: str  # 'regex' | 'llm_assist' | 'post_check'


def upsert_draft(row: DraftRow) -> None:
    """Insert or update a draft. Idempotent on (operator_name, email, source_url)."""
    client = _client_lazy()
    payload = {
        "operator_name": row.operator_name,
        "email": row.email,
        "source_url": row.source_url,
        "snippet": row.snippet,
        "score": row.score,
        "fetched_at": row.fetched_at,
        "is_best": row.is_best,
        "mx_valid": row.mx_valid,
        "method": row.method,
        "post_check_failed": False,
    }
    client.table(STAGING_TABLE).upsert(
        payload,
        on_conflict="operator_name,email,source_url",
    ).execute()
    logger.info(f"Upserted draft: {row.operator_name} / {row.email} (score={row.score})")


def mark_post_check_failed(operator_name: str, email: str, source_url: str) -> None:
    client = _client_lazy()
    client.table(STAGING_TABLE).update({"post_check_failed": True}).match({
        "operator_name": operator_name,
        "email": email,
        "source_url": source_url,
    }).execute()


def list_pending_post_check(older_than_hours: int = 24) -> list[dict]:
    """Return drafts that haven't been post-checked yet."""
    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=older_than_hours)).isoformat()
    client = _client_lazy()
    resp = (
        client.table(STAGING_TABLE)
        .select("*")
        .lt("fetched_at", cutoff)
        .is_("post_check_failed", "null")
        .execute()
    )
    return resp.data or []
