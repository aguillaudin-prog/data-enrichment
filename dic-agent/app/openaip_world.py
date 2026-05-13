"""Fetch OpenAIP airspaces for every country in the local DB.

Iterates over `country.iso_a2`, calls fetch_airspaces() per country, respects
the rate limit (~100 req/min on the free tier), caches everything locally in
seeds/openaip_airspaces_<ISO>.json (gitignored).

Run: python -m app.openaip_world
Resume-safe: countries already cached are skipped, so you can stop with Ctrl+C
and resume later.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from app import db, openaip_client


SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


def main() -> int:
    if not os.environ.get("OPENAIP_API_KEY"):
        print("OPENAIP_API_KEY missing. Fill it in .env first.")
        return 1
    rows = db.list_countries()
    isos = sorted({r["iso_a2"] for r in rows if r["iso_a2"]})
    print(f"{len(isos)} countries to fetch.")
    ok = skipped = failed = 0
    failed_isos: list[str] = []
    for i, iso in enumerate(isos, 1):
        cache = SEEDS_DIR / f"openaip_airspaces_{iso}.json"
        if cache.exists() and cache.stat().st_size > 0:
            skipped += 1
            continue
        try:
            spaces = openaip_client.fetch_airspaces(iso)
            ok += 1
            print(f"  [{i:3d}/{len(isos)}] {iso}: {len(spaces)} airspaces")
        except Exception as e:
            failed += 1
            failed_isos.append(iso)
            print(f"  [{i:3d}/{len(isos)}] {iso}: ERROR — {e}")
        # Conservative pacing: 1 req every 0.7 s ≈ 85 req/min (under the 100 limit).
        time.sleep(0.7)
    print(
        f"\nDone. fetched={ok} skipped={skipped} failed={failed}"
    )
    if failed_isos:
        print(f"Failed ISOs: {' '.join(failed_isos)}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
