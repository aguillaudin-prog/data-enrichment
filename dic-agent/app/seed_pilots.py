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


def _dedupe_pilots_case_insensitive() -> int:
    """Remove case-duplicate pilot rows (e.g. 'LAZARE Jean-Michel' alongside
    'Lazare Jean-Michel'). The pilot table's UNIQUE(name, role) is case-
    sensitive in SQLite, so a previous typo can leave both around. Keep the
    row whose name matches `DEFAULT_PILOTS` exactly when applicable, else the
    most recently saved one."""
    canonical = {p["name"].upper(): p["name"] for p in DEFAULT_PILOTS}
    removed = 0
    with db.connect() as c:
        rows = c.execute(
            "SELECT id, name, role FROM pilot ORDER BY id"
        ).fetchall()
        seen: dict[tuple[str, str], int] = {}
        to_delete: list[int] = []
        for r in rows:
            key = (r["name"].upper(), r["role"])
            preferred_name = canonical.get(key[0])
            if key in seen:
                # already have a row for this canonical (name, role) — keep the
                # one whose case matches DEFAULT_PILOTS, drop the other.
                prev_id = seen[key]
                # If the current row matches the canonical case, replace.
                if preferred_name and r["name"] == preferred_name:
                    to_delete.append(prev_id)
                    seen[key] = r["id"]
                else:
                    to_delete.append(r["id"])
            else:
                seen[key] = r["id"]
        for pid in to_delete:
            c.execute("DELETE FROM pilot WHERE id = ?", (pid,))
            removed += 1
    return removed


def main() -> int:
    db.init_schema()
    n = _dedupe_pilots_case_insensitive()
    if n:
        print(f"Removed {n} case-duplicate pilot row(s).")
    for p in DEFAULT_PILOTS:
        db.save_pilot(name=p["name"], role=p["role"], rank=p["rank"], allowed_operator=p["operator"])
    print(f"Seeded {len(DEFAULT_PILOTS)} pilots:")
    for p in db.list_pilots():
        op = (p["allowed_operator"] or "(any)") if "allowed_operator" in p.keys() else "(any)"
        print(f"  [{p['role']}] {p['rank']} {p['name']}  → {op}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
