"""HTTP scraper with rate limiting, robots.txt respect, and caching.

Anti-hallucination: this module returns RAW HTML only. No parsing, no
interpretation. All extraction logic lives in email_extractor.py and
must verify content against the raw HTML returned here.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

USER_AGENT_CONTACT = os.getenv("USER_AGENT_CONTACT", "noreply@example.com")
USER_AGENT = f"APEX-Enrichment-Agent/0.1 (contact: {USER_AGENT_CONTACT})"
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
RATE_LIMIT = float(os.getenv("RATE_LIMIT_PER_DOMAIN_SECONDS", "1.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Pages candidates to look for contact info, in priority order
CANDIDATE_PATHS = [
    "/contact",
    "/contact-us",
    "/contacts",
    "/cargo",
    "/charter",
    "/freight",
    "/about/contact",
    "/about-us/contact",
    "/about/team",
    "/team",
    "/legal",
    "/imprint",
    "/mentions-legales",
    "/",
]

# Per-domain last-fetch timestamps for rate limiting
_last_fetch: dict[str, float] = {}
_robots_cache: dict[str, RobotFileParser] = {}


@dataclass
class FetchResult:
    """Result of fetching a URL.

    raw_html is the verbatim response body. All downstream extraction
    must verify against this string.
    """
    url: str
    status_code: int
    raw_html: str
    fetched_at: str  # ISO timestamp
    error: Optional[str] = None


def _domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def _respect_rate_limit(domain: str) -> None:
    last = _last_fetch.get(domain, 0.0)
    elapsed = time.time() - last
    if elapsed < RATE_LIMIT:
        time.sleep(RATE_LIMIT - elapsed)
    _last_fetch[domain] = time.time()


def _robots_allowed(url: str) -> bool:
    """Check robots.txt for the given URL. Caches per domain."""
    domain = _domain(url)
    if domain not in _robots_cache:
        rp = RobotFileParser()
        robots_url = f"{urlparse(url).scheme}://{domain}/robots.txt"
        try:
            rp.set_url(robots_url)
            rp.read()
        except Exception as e:
            logger.warning(f"Could not fetch robots.txt for {domain}: {e}")
            # If robots.txt unreachable, default to allowed (conservative would be False;
            # but many small operator sites have no robots.txt and we want to enrich them).
            rp = RobotFileParser()
            rp.parse([])
        _robots_cache[domain] = rp
    return _robots_cache[domain].can_fetch(USER_AGENT, url)


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=1, min=1, max=10))
def _http_get(url: str) -> requests.Response:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en;q=0.9,fr;q=0.8",
    }
    return requests.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)


def fetch(url: str) -> FetchResult:
    """Fetch a URL with rate limiting and robots.txt respect.

    Always returns a FetchResult; errors are captured in the .error field.
    """
    from datetime import datetime, timezone
    fetched_at = datetime.now(timezone.utc).isoformat()
    domain = _domain(url)

    if not _robots_allowed(url):
        logger.info(f"Blocked by robots.txt: {url}")
        return FetchResult(url=url, status_code=0, raw_html="",
                           fetched_at=fetched_at, error="blocked_by_robots")

    _respect_rate_limit(domain)

    try:
        resp = _http_get(url)
        return FetchResult(
            url=resp.url,  # final URL after redirects
            status_code=resp.status_code,
            raw_html=resp.text if resp.status_code == 200 else "",
            fetched_at=fetched_at,
            error=None if resp.status_code == 200 else f"http_{resp.status_code}",
        )
    except Exception as e:
        logger.warning(f"Fetch failed for {url}: {e}")
        return FetchResult(url=url, status_code=0, raw_html="",
                           fetched_at=fetched_at, error=str(e))


def candidate_urls(base_url: str) -> list[str]:
    """Generate the priority-ordered list of URLs to try for one operator."""
    base = base_url.rstrip("/")
    return [urljoin(base + "/", path.lstrip("/")) for path in CANDIDATE_PATHS]
