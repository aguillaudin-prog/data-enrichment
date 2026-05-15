"""Seed default crew members.

Run: python -m app.seed_pilots
"""
from __future__ import annotations

from app import db

AMAZONE = "AMAZONE AIRLINES / DYNAMI AVIATION OPS"
REVOLUTION_AIR = "REVOLUTION'AIR"

DEFAULT_PILOTS = [
    {"name": "Kornelius Wicaksono", "role": "CDB", "rank": "CPT", "operator": AMAZONE},
    {"name": "Aditya Tri Hertiawan", "role": "CDB", "rank": "CPT", "operator": AMAZONE},
    {"name": "Saba Muhammad", "role": "FO", "rank": "FO", "operator": AMAZONE},
    {"name": "Wanda Respati", "role": "FO", "rank": "FO", "operator": AMAZONE},
    {"name": "Lazare Jean-Michel", "role": "CDB", "rank": "CPT", "operator": REVOLUTION_AIR},
]


def main() -> int:
    db.init_schema()
    for p in DEFAULT_PILOTS:
        db.save_pilot(name=p["name"], role=p["role"], rank=p["rank"], allowed_operator=p["operator"])
    print(f"Seeded {len(DEFAULT_PILOTS)} pilots:")
    for p in db.list_pilots():
        op = (p["allowed_operator"] or "(any)") if "allowed_operator" in p.keys() else "(any)"
        print(f"  [{p['role']}] {p['rank']} {p['name']}  → {op}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
