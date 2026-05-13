"""OpenAIP API client — airspaces, airports, navaids.

Reads OPENAIP_API_KEY from the environment. Caches responses per query in
seeds/openaip_*.json so repeat runs don't hit the API.

API reference: https://docs.openaip.net (Core API v1).
Endpoints used here:
  - /api/airspaces : returns airspaces (CTR, TMA, P/R/D, MOA, FIR…)
  - /api/airports  : returns airports (use OurAirports primarily; OpenAIP as cross-check)
  - /api/navaids   : VOR/NDB/DME

Notes:
  - Rate limit ≈ 100 req/min on the free tier.
  - `country` filter takes ISO 3166-1 alpha-2 codes (e.g. 'BJ', 'NG').
  - Pagination via `page` and `limit` (max 1000 per page).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Iterable

import requests

API_BASE = "https://api.core.openaip.net/api"
SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"
SEEDS_DIR.mkdir(parents=True, exist_ok=True)


class OpenAIPError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.environ.get("OPENAIP_API_KEY", "").strip()
    if not key:
        raise OpenAIPError(
            "OPENAIP_API_KEY missing. Copy .env.example to .env and fill OPENAIP_API_KEY."
        )
    return key


def _get(endpoint: str, params: dict) -> dict:
    headers = {"x-openaip-api-key": _api_key(), "Accept": "application/json"}
    url = f"{API_BASE}/{endpoint}"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 429:
        time.sleep(2)
        r = requests.get(url, headers=headers, params=params, timeout=30)
    if not r.ok:
        raise OpenAIPError(f"OpenAIP {r.status_code} on {endpoint}: {r.text[:200]}")
    return r.json()


def _paginate(endpoint: str, params: dict, page_size: int = 1000) -> Iterable[dict]:
    page = 1
    while True:
        data = _get(endpoint, {**params, "page": page, "limit": page_size})
        items = data.get("items") or data.get("results") or []
        for it in items:
            yield it
        total_pages = data.get("totalPages") or 1
        if page >= total_pages or not items:
            break
        page += 1


def fetch_airspaces(country_iso2: str, use_cache: bool = True) -> list[dict]:
    """Fetch all airspaces for a country. Caches under seeds/openaip_airspaces_{ISO}.json."""
    cache = SEEDS_DIR / f"openaip_airspaces_{country_iso2.upper()}.json"
    if use_cache and cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    items = list(_paginate("airspaces", {"country": country_iso2.upper()}))
    cache.write_text(json.dumps(items), encoding="utf-8")
    return items


def fetch_navaids(country_iso2: str, use_cache: bool = True) -> list[dict]:
    cache = SEEDS_DIR / f"openaip_navaids_{country_iso2.upper()}.json"
    if use_cache and cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    items = list(_paginate("navaids", {"country": country_iso2.upper()}))
    cache.write_text(json.dumps(items), encoding="utf-8")
    return items


def fetch_airports(country_iso2: str, use_cache: bool = True) -> list[dict]:
    cache = SEEDS_DIR / f"openaip_airports_{country_iso2.upper()}.json"
    if use_cache and cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    items = list(_paginate("airports", {"country": country_iso2.upper()}))
    cache.write_text(json.dumps(items), encoding="utf-8")
    return items


if __name__ == "__main__":
    # Manual test: fetch a small country
    import sys
    iso = sys.argv[1] if len(sys.argv) > 1 else "BJ"
    print(f"Fetching airspaces for {iso}…")
    spaces = fetch_airspaces(iso)
    print(f"  {len(spaces)} airspaces cached → seeds/openaip_airspaces_{iso}.json")
    if spaces:
        s = spaces[0]
        print(f"  first sample: type={s.get('type')} name={s.get('name')}")
