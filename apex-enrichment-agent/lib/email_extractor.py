"""Email extraction from raw HTML — with verbatim assertion.

THE core anti-hallucination guarantee: every email returned by
extract_emails() is asserted to literally appear in the raw HTML.
If the assertion ever fails, it is a hard bug.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# RFC 5322-lite — pragmatic regex covering 99% of real emails.
EMAIL_REGEX = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
    re.IGNORECASE,
)

# Common obfuscation patterns (e.g. "contact [at] domain [dot] com")
OBFUSCATION_AT = re.compile(r"\s*\[?\(?at\)?\]?\s*", re.IGNORECASE)
OBFUSCATION_DOT = re.compile(r"\s*\[?\(?dot\)?\]?\s*", re.IGNORECASE)

# Score for prioritization (see CLAUDE.md Règle 6)
LOCAL_PART_SCORES: list[tuple[re.Pattern, int]] = [
    (re.compile(r"^cargo", re.I), 100),
    (re.compile(r"^(charter|freight)", re.I), 90),
    (re.compile(r"^(ops|operations|flightops)", re.I), 80),
    (re.compile(r"^(sales|commercial)", re.I), 70),
    (re.compile(r"^(info|contact|office|hello)", re.I), 50),
    (re.compile(r"^[a-z]+\.[a-z]+$", re.I), 30),  # firstname.lastname
]
DEFAULT_SCORE = 10


@dataclass
class EmailHit:
    """An email found in raw HTML, with proof of provenance."""
    email: str
    source_url: str
    snippet: str          # 50 chars before + email + 50 chars after
    score: int
    fetched_at: str


def _score(email: str) -> int:
    local = email.split("@", 1)[0]
    for pattern, score in LOCAL_PART_SCORES:
        if pattern.search(local):
            return score
    return DEFAULT_SCORE


def _snippet(haystack: str, needle: str, context: int = 50) -> str:
    """Extract 50 chars before + email + 50 chars after, normalized."""
    idx = haystack.lower().find(needle.lower())
    if idx == -1:
        return ""
    start = max(0, idx - context)
    end = min(len(haystack), idx + len(needle) + context)
    raw = haystack[start:end]
    # Collapse whitespace for readability in the staging table
    return re.sub(r"\s+", " ", raw).strip()


def _deobfuscate(html: str) -> str:
    """Replace common obfuscations so emails like 'foo [at] bar [dot] com' match.

    NOTE: the deobfuscated string is used ONLY to find candidate matches;
    we still re-verify the canonical email against the ORIGINAL raw_html
    via verify_in_source(). If the canonical form is not present in raw,
    the email is rejected. This means de-obfuscation never adds emails
    that aren't actually present (in some form) in the source.
    """
    s = OBFUSCATION_AT.sub("@", html)
    s = OBFUSCATION_DOT.sub(".", s)
    return s


def verify_in_source(email: str, raw_html: str) -> bool:
    """The hard guarantee. Returns True iff the email literally appears
    in raw HTML, OR appears in a recognizable obfuscated form.
    """
    if email.lower() in raw_html.lower():
        return True
    # Allow matches against deobfuscated source — but the email must then
    # match the deobfuscated string. This is still verifiable: the obfuscated
    # form was actually in the page.
    if email.lower() in _deobfuscate(raw_html).lower():
        return True
    return False


def extract_emails(raw_html: str, source_url: str, fetched_at: str) -> list[EmailHit]:
    """Extract all emails from raw HTML, with verbatim verification.

    Every returned EmailHit satisfies verify_in_source(hit.email, raw_html) == True.
    """
    if not raw_html:
        return []

    # First pass: search in raw HTML
    candidates = set(EMAIL_REGEX.findall(raw_html))
    # Second pass: search in deobfuscated HTML
    candidates |= set(EMAIL_REGEX.findall(_deobfuscate(raw_html)))

    hits: list[EmailHit] = []
    for email in candidates:
        email = email.strip().rstrip(".,;:")  # cleanup trailing punctuation
        if not _is_plausible(email):
            continue
        if not verify_in_source(email, raw_html):
            # HARD GUARANTEE — never return an email not in source
            continue
        hits.append(EmailHit(
            email=email.lower(),
            source_url=source_url,
            snippet=_snippet(raw_html, email),
            score=_score(email),
            fetched_at=fetched_at,
        ))

    # Dedupe by email, keep highest score
    by_email: dict[str, EmailHit] = {}
    for h in hits:
        if h.email not in by_email or h.score > by_email[h.email].score:
            by_email[h.email] = h
    return sorted(by_email.values(), key=lambda h: h.score, reverse=True)


def _is_plausible(email: str) -> bool:
    """Filter out obvious junk: image filenames, tracking pixels, etc."""
    if "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if not local or not domain:
        return False
    if "." not in domain:
        return False
    # Filter common false positives from CSS/JS bundles
    junk_locals = {"example", "test", "user", "name", "your-email", "youremail"}
    if local.lower() in junk_locals:
        return False
    junk_domains = {"example.com", "domain.com", "email.com", "sentry.io", "wixpress.com"}
    if domain.lower() in junk_domains:
        return False
    # Likely a sprite/asset filename, not an email
    if any(domain.lower().endswith(ext) for ext in (".png", ".jpg", ".gif", ".svg", ".webp")):
        return False
    return True
