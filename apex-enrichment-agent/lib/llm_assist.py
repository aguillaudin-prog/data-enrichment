"""LLM assistance for ambiguous cases — with mandatory verbatim citation.

Claude Sonnet is called ONLY when:
  - The HTML is unstructured (no obvious contact section)
  - Multiple emails are present and we need disambiguation
  - The contact info is in a non-obvious location

The model is required to return emails it has *quoted verbatim* from the
HTML. The pipeline then re-verifies each returned email against the raw
HTML via email_extractor.verify_in_source(). If verification fails,
the email is rejected — no exception.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

from anthropic import Anthropic

from lib.email_extractor import verify_in_source

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-5"  # use the latest Sonnet — adjust as needed
MAX_HTML_CHARS = 100_000     # truncate very long pages

_client: Anthropic | None = None


def _client_lazy() -> Anthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        _client = Anthropic(api_key=api_key)
    return _client


PROMPT = """You are extracting cargo charter contact emails from raw website HTML.

CRITICAL RULES:
1. Only return emails that LITERALLY appear in the HTML below. Do not guess, infer, or construct emails.
2. Do not return emails that are example placeholders (test@, info@example.com, etc.).
3. Prioritize emails relevant to cargo / charter / freight / operations / sales.

Return your answer as JSON only, in this exact schema:
{{
  "emails": [
    {{"email": "...", "context": "<verbatim 30-100 chars surrounding the email in the HTML>"}}
  ]
}}

If no email is found, return {{"emails": []}}.

Operator: {operator_name}
Source URL: {source_url}

Raw HTML:
---
{html}
---

JSON only, no preamble."""


@dataclass
class LLMHit:
    email: str
    context: str  # verbatim quote from HTML, per the prompt


def llm_extract(operator_name: str, source_url: str, raw_html: str) -> list[LLMHit]:
    """Call Claude Sonnet to extract emails from ambiguous HTML.

    EVERY returned email is re-verified against raw_html. Hallucinations
    are rejected at this layer — they cannot reach the database.
    """
    if not raw_html.strip():
        return []

    truncated = raw_html[:MAX_HTML_CHARS]
    client = _client_lazy()

    msg = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": PROMPT.format(
                operator_name=operator_name,
                source_url=source_url,
                html=truncated,
            ),
        }],
    )

    text = "".join(b.text for b in msg.content if hasattr(b, "text"))
    text = text.strip()
    # Strip code fences if model added them despite instructions
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning(f"LLM returned non-JSON for {operator_name}: {e}")
        return []

    hits: list[LLMHit] = []
    for item in data.get("emails", []):
        email = item.get("email", "").strip().lower()
        context = item.get("context", "")
        if not email:
            continue
        # ANTI-HALLUCINATION GATE: must be in raw HTML
        if not verify_in_source(email, raw_html):
            logger.warning(f"LLM hallucination rejected: {email} not in source for {operator_name}")
            continue
        hits.append(LLMHit(email=email, context=context))

    return hits
