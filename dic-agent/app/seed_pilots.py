"""Seed default crew members.

Run: python -m app.seed_pilots
"""
from __future__ import annotations

from app import db

DEFAULT_PILOTS = [
    {"name": "Kornelius Wicaksono", "role": "CDB", "rank": "CPT"},
    {"name": "Aditya Tri Hertiawan", "role": "CDB", "rank": "CPT"},
    {"name": "Saba Muhammad", "role": "FO", "rank": "FO"},
    {"name": "Wanda Respati", "role": "FO", "rank": "FO"},
]


def main() -> int:
    db.init_schema()
    for p in DEFAULT_PILOTS:
        db.save_pilot(name=p["name"], role=p["role"], rank=p["rank"])
    print(f"Seeded {len(DEFAULT_PILOTS)} pilots:")
    for p in db.list_pilots():
        print(f"  [{p['role']}] {p['rank']} {p['name']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
