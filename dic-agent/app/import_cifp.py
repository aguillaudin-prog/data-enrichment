"""Import X-Plane CIFP (Coded Instrument Flight Procedures) into local DB.

CIFP folder layout (typical X-Plane install):
  X-Plane 12/Custom Data/CIFP/<ICAO>.dat
  X-Plane 12/Resources/default data/CIFP/<ICAO>.dat

One file per airport. Each file lists every published SID, STAR and approach
for that airport, as ARINC 424-derived records.

Record line format (comma-separated, type-prefixed):

  SID:<seq>,<route_type>,<sid_name>,<transition>,<runway>,<fix_ident>,<fix_region>,…
  STAR:<seq>,<route_type>,<star_name>,<transition>,<runway>,<fix_ident>,<fix_region>,…
  APPCH:<seq>,<route_type>,<appr_name>,<transition>,<runway>,<fix_ident>,<fix_region>,…

Multiple lines per procedure (one per sequenced fix). We group them by
(type, name, runway, transition) and store the ordered list of fix idents.

For DIC purposes, listing procedures by airport is the immediate win: an OPS
officer can pick a SID for the origin and a STAR for the destination,
matching what real flight planning systems output.

Usage:
  python -m app.import_cifp --dir "C:/X-Plane 12/Custom Data/CIFP"
  python -m app.import_cifp --dir "C:/X-Plane 12/Resources/default data/CIFP"
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from app import db


PROC_TYPES = ("SID", "STAR", "APPCH")
HEADER_RE = re.compile(r"^(SID|STAR|APPCH):", re.IGNORECASE)


def _parse_cifp_record(line: str) -> dict | None:
    """Parse one CIFP record line into a structured dict.

    Returns None for non-procedure lines (RWY/PRDAT/comments/empty).

    The parser is intentionally lenient — CIFP record layouts vary slightly
    across X-Plane versions and ARINC dialects, but the early fields are
    stable: type, sequence, route_type, name, transition, runway, fix_ident.
    """
    line = line.strip()
    m = HEADER_RE.match(line)
    if not m:
        return None
    proc_type = m.group(1).upper()
    body = line[len(m.group(0)):]  # everything after 'SID:'
    fields = [f.strip() for f in body.split(",")]
    if len(fields) < 6:
        return None
    # Field layout (best-effort):
    #   0: sequence number ('010', '020'…)
    #   1: route type code (B, F, R, S, …)
    #   2: procedure name
    #   3: transition identifier (or blank)
    #   4: runway identifier (e.g. 'RW16R') or blank
    #   5: fix identifier
    #   6: fix region (2-letter)
    name = fields[2].upper()
    transition = fields[3].upper() or None
    runway = fields[4].upper().lstrip("RW") or None
    fix_ident = fields[5].upper()
    if not name or not fix_ident:
        return None
    return {
        "proc_type": proc_type, "name": name,
        "transition": transition, "runway": runway,
        "fix_ident": fix_ident,
    }


def parse_cifp_file(path: Path) -> list[dict]:
    """Parse one CIFP/<ICAO>.dat file. Returns list of upsert-ready rows.

    One row per (procedure_type, procedure_name). Runways that serve the
    procedure are concatenated as a CSV; waypoints are listed in the order
    they appear in the file, with consecutive duplicates collapsed.
    """
    airport_icao = path.stem.upper()
    waypoints_by: dict[tuple, list[str]] = defaultdict(list)
    runways_by: dict[tuple, set[str]] = defaultdict(set)
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            rec = _parse_cifp_record(raw)
            if rec is None:
                continue
            key = (rec["proc_type"], rec["name"])
            if waypoints_by[key] and waypoints_by[key][-1] == rec["fix_ident"]:
                pass  # skip consecutive duplicate fix idents
            else:
                waypoints_by[key].append(rec["fix_ident"])
            if rec["runway"]:
                runways_by[key].add(rec["runway"])
    rows: list[dict] = []
    for key, waypoints in waypoints_by.items():
        proc_type, name = key
        rows.append({
            "airport_icao": airport_icao,
            "proc_type": proc_type,
            "proc_name": name,
            "runways_csv": ",".join(sorted(runways_by[key])) or None,
            "waypoints_json": json.dumps(waypoints, ensure_ascii=False),
        })
    return rows


def import_folder(folder: Path) -> tuple[int, int]:
    """Scan every .dat file in `folder` and import procedures.

    Returns (airports_processed, total_procedures_imported).
    """
    files = sorted(folder.glob("*.dat"))
    total = 0
    airports = 0
    batch: list[dict] = []
    for fp in files:
        rows = parse_cifp_file(fp)
        if not rows:
            continue
        batch.extend(rows)
        airports += 1
        if len(batch) >= 2000:
            total += db.upsert_procedures(batch)
            batch = []
    if batch:
        total += db.upsert_procedures(batch)
    return airports, total


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, required=True,
                    help="CIFP folder (e.g. 'C:/X-Plane 12/Resources/default data/CIFP')")
    ap.add_argument("--clean-first", action="store_true",
                    help="Drop all rows from procedure before importing.")
    args = ap.parse_args(argv)

    if not args.dir.exists() or not args.dir.is_dir():
        print(f"Folder not found: {args.dir}")
        return 1

    db.init_schema()
    if args.clean_first:
        with db.connect() as c:
            n = c.execute("DELETE FROM procedure").rowcount
        print(f"Cleaned {n} existing procedure rows.")

    print(f"Scanning {args.dir} for *.dat …")
    airports, total = import_folder(args.dir)
    print(f"Imported {total} procedures across {airports} airports.")
    print(f"Total procedures in DB now: {db.count_procedures()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
