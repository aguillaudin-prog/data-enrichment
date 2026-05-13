"""Offline seed for tests / sandbox.

Loads the minimal CSVs / GeoJSON shipped under seeds/ — enough to exercise the
5 provided West-Africa DIC examples without internet access.

For real-world use, run `python -m app.seed_db` instead (downloads OurAirports
and Natural Earth).
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from app import db

SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"


def seed_minimal_airports() -> None:
    rows = []
    with (SEEDS_DIR / "minimal_airports.csv").open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(
                {
                    "icao": r["icao"].strip().upper(),
                    "iata": (r.get("iata") or None) or None,
                    "name": r["name"],
                    "country_iso": r["country_iso"].upper(),
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "elevation_ft": int(r["elevation_ft"]) if r.get("elevation_ft") else None,
                    "is_military": int(r.get("is_military") or 0),
                    "user_added": 0,
                }
            )
    print(f"airports: {db.upsert_airports(rows)}")


def seed_minimal_waypoints() -> None:
    rows = []
    with (SEEDS_DIR / "minimal_waypoints.csv").open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(
                {
                    "ident": r["ident"].strip().upper(),
                    "region": r["region"].strip().upper(),
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "kind": r.get("kind") or None,
                    "user_added": 0,
                }
            )
    print(f"waypoints: {db.upsert_waypoints(rows)}")


def seed_minimal_countries() -> None:
    data = json.loads((SEEDS_DIR / "minimal_countries.geojson").read_text(encoding="utf-8"))
    rows = []
    for feat in data["features"]:
        props = feat["properties"]
        rows.append(
            {
                "iso_a2": props["ISO_A2"],
                "iso_a3": props.get("ISO_A3"),
                "name_en": props["NAME_EN"],
                "name_fr": props["NAME_FR"],
                "geom_geojson": json.dumps(feat["geometry"]),
            }
        )
    print(f"countries: {db.upsert_countries(rows)}")


def seed_minimal_aircraft_types() -> None:
    from app.seed_db import seed_aircraft_types
    seed_aircraft_types()


def main() -> int:
    db.init_schema()
    seed_minimal_aircraft_types()
    seed_minimal_airports()
    seed_minimal_waypoints()
    seed_minimal_countries()
    print("Offline seed done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
