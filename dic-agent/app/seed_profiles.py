"""Seed default profiles (aircraft, POC) and Benin route templates.

This wraps seed_pilots + adds the default Amazone profile, the West Africa
IFR fixes used by the bundled DIC templates (POLTO, KELIG, etc. — these are
RNAV named fixes that OurAirports' navaid file doesn't carry), and bulk-loads
the Benin route templates extracted from past DICs (sanitised — operational
route data only, no personal info).

Run: python -m app.seed_profiles
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from app import db, seed_pilots

SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"

DEFAULT_AIRCRAFT = [
    {
        "registration": "TY-BAB",
        "type_icao": "DHC6",
        "callsign": "TY-BAB",
        "operator": "AMAZONE AIRLINES / DYNAMI AVIATION OPS",
    },
]

DEFAULT_POCS = [
    {
        "rank": "OF1",
        "name": "MERLIN",
        "phone": "+ 225 07 15 013 761",
        "email_personal": "cos-det14.j10@intradef.gouv.fr",
        "email_functional": "",
        "fax": "",
    },
]


def seed_aircraft() -> None:
    for a in DEFAULT_AIRCRAFT:
        db.save_aircraft(
            registration=a["registration"],
            type_icao=a["type_icao"],
            callsign=a["callsign"],
            operator=a["operator"],
        )
    print(f"Seeded {len(DEFAULT_AIRCRAFT)} aircraft profiles.")


def seed_pocs() -> None:
    for p in DEFAULT_POCS:
        db.save_poc(
            rank=p["rank"], name=p["name"], phone=p["phone"],
            email_personal=p["email_personal"], email_functional=p["email_functional"],
            fax=p["fax"],
        )
    print(f"Seeded {len(DEFAULT_POCS)} POC profiles.")


def seed_extra_waypoints() -> None:
    """Add the IFR named fixes used by the bundled DIC templates.

    These are 5-letter RNAV fixes (POLTO, KELIG, MESES, etc.) that don't
    appear in OurAirports' navaid feed. Marked user_added so the proximity
    resolver prefers them when ambiguous.
    """
    csv_path = SEEDS_DIR / "minimal_waypoints.csv"
    if not csv_path.exists():
        print("  minimal_waypoints.csv missing — skipped")
        return
    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({
                "ident": r["ident"].strip().upper(),
                "region": r["region"].strip().upper(),
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "kind": r.get("kind") or None,
                "user_added": 1,
            })
    n = db.upsert_waypoints(rows)
    print(f"Seeded {n} West Africa fixes (POLTO, KELIG, MESES, …).")


def seed_route_templates() -> None:
    """Load every seeds/route_templates_*.json into the route_template table."""
    files = sorted(SEEDS_DIR.glob("route_templates_*.json"))
    if not files:
        print("  No route_templates_*.json files found.")
        return
    total = 0
    for f in files:
        items = json.loads(f.read_text(encoding="utf-8"))
        for tpl in items:
            with db.connect() as c:
                c.execute(
                    """
                    INSERT INTO route_template (name, category, legs_json)
                    VALUES (?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                        category = excluded.category,
                        legs_json = excluded.legs_json
                    """,
                    (tpl["name"], tpl.get("category"), json.dumps(tpl["legs"], ensure_ascii=False)),
                )
            total += 1
        print(f"  loaded {f.name}: {len(items)} templates")
    print(f"Seeded {total} route templates total.")


def main() -> int:
    db.init_schema()
    seed_pilots.main()
    seed_aircraft()
    seed_pocs()
    seed_extra_waypoints()
    seed_route_templates()
    print("\nAll profile seeds done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
