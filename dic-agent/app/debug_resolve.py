"""Diagnostic helper — print what the route engine sees for the Cotonou→Ilorin leg.

Run: python -m app.debug_resolve
"""
from __future__ import annotations

import datetime as dt

from app import db
from app.route_engine import _resolve_token, compute_leg, _build_country_index


def main() -> int:
    print("=" * 60)
    print("Waypoints with ident in (POLTO, LAG, TYE, IBA, ILR):")
    print("=" * 60)
    for ident in ["POLTO", "LAG", "TYE", "IBA", "ILR"]:
        rows = db.find_waypoints_all(ident)
        if not rows:
            print(f"  {ident:8s}: NOT FOUND IN DB")
            continue
        for r in rows:
            print(f"  {ident:8s}: region={r['region']:>5s} lat={r['lat']:.4f} lon={r['lon']:.4f}  user_added={r['user_added']}")

    print()
    print("=" * 60)
    print("Resolver test (DBBB as near_pt, max=300 NM):")
    print("=" * 60)
    dbbb = db.find_airport("DBBB")
    if not dbbb:
        print("  DBBB airport NOT FOUND IN DB — seed_db incomplete?")
        return 1
    near = (dbbb["lat"], dbbb["lon"])
    print(f"  DBBB at lat={near[0]:.4f} lon={near[1]:.4f}")
    for ident in ["TYE", "POLTO", "LAG", "IBA", "ILR"]:
        rp = _resolve_token(ident, near_pt=near, max_nm_from_near=300)
        if rp.missing:
            print(f"  {ident:8s}: UNKNOWN ❌")
        else:
            print(f"  {ident:8s}: ({rp.lat:.4f}, {rp.lon:.4f}) source={rp.source} country={rp.country_iso} ✓")

    print()
    print("=" * 60)
    print("Full compute_leg DBBB → DNIL :")
    print("=" * 60)
    idx = _build_country_index()
    r = compute_leg(
        eobt=dt.datetime(2026, 5, 13, 6, 0, tzinfo=dt.timezone.utc),
        origin_icao="DBBB", destination_icao="DNIL",
        route_text="TYE POLTO LAG L433 IBA ILR",
        fl=90, tas_kt=140, country_index=idx,
    )
    print(f"  Distance: {r.total_distance_nm:.0f} NM")
    print(f"  Temps:    {r.total_time_min:.0f} min")
    print(f"  Pays:     {len(r.segments)}  → {' / '.join(s.state_name for s in r.segments)}")
    if r.warnings:
        for w in r.warnings:
            print(f"  ⚠ {w}")
    else:
        print("  no warnings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
