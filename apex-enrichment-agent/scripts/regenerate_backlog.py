"""Regenerate BACKLOG.csv from the raw operators_list.csv.

Filters cargo-capable operators (any aircraft Is_Cargo=Yes), aggregates one
row per operator (first non-empty value across aircraft rows), and emits the
columns the enrichment pipeline expects.

Run once when the upstream operators_list.csv is updated:

    python scripts/regenerate_backlog.py
"""
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
SOURCE = ROOT / "operators_list.csv"
TARGET = ROOT / "BACKLOG.csv"

BACKLOG_COLUMNS = [
    "operator_name",
    "country",
    "city",
    "cargo_aircraft_count",
    "fleet_count",
    "priority",
    "enrichment_status",
    "website",
    "existing_email",
    "phone",
    "address",
    "profile_url",
    "source",
    "base_icao",
    "aircraft_types",
]


def _first_nonempty(values: list[str]) -> str:
    for v in values:
        v = (v or "").strip()
        if v:
            return v
    return ""


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"Source not found: {SOURCE}")

    with SOURCE.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    by_op: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        name = (r.get("Operator_Name") or "").strip()
        if not name:
            continue
        by_op[name].append(r)

    out_rows: list[dict] = []
    for name, group in by_op.items():
        cargo_count = sum(1 for r in group if (r.get("Is_Cargo") or "").strip() == "Yes")
        if cargo_count == 0:
            continue  # not cargo-capable

        country = _first_nonempty([r.get("Country", "") for r in group])
        city = _first_nonempty([r.get("City", "") for r in group])
        website = _first_nonempty([r.get("Website", "") for r in group])
        phone = _first_nonempty([r.get("Phone", "") for r in group])
        address = _first_nonempty([r.get("Address", "") for r in group])
        profile_url = _first_nonempty([r.get("Profile_URL", "") for r in group])
        source = _first_nonempty([r.get("Source", "") for r in group])
        base_icao = _first_nonempty([r.get("Base_ICAO", "") for r in group])
        existing_email = _first_nonempty([r.get("Charter_Email", "") for r in group]).lower()

        aircraft_types = sorted({
            (r.get("Aircraft_Type") or "").strip()
            for r in group
            if (r.get("Is_Cargo") or "").strip() == "Yes" and (r.get("Aircraft_Type") or "").strip()
        })

        # priority heuristic: higher cargo fleet share => higher priority
        ratio = cargo_count / max(1, len(group))
        priority = "high" if ratio >= 0.5 else ("medium" if ratio >= 0.2 else "low")

        out_rows.append({
            "operator_name": name,
            "country": country,
            "city": city,
            "cargo_aircraft_count": str(cargo_count),
            "fleet_count": str(len(group)),
            "priority": priority,
            "enrichment_status": "pending",
            "website": website,
            "existing_email": existing_email,
            "phone": phone,
            "address": address,
            "profile_url": profile_url,
            "source": source,
            "base_icao": base_icao,
            "aircraft_types": "|".join(aircraft_types),
        })

    out_rows.sort(key=lambda r: r["operator_name"])

    with TARGET.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BACKLOG_COLUMNS)
        writer.writeheader()
        writer.writerows(out_rows)

    n_with = sum(1 for r in out_rows if r["existing_email"])
    n_without = len(out_rows) - n_with
    print(f"Wrote {len(out_rows)} cargo-capable operators to {TARGET.name}")
    print(f"  with existing email: {n_with} (verify mode)")
    print(f"  without email:       {n_without} (discovery mode)")


if __name__ == "__main__":
    main()
