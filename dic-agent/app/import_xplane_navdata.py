"""Import X-Plane navdata into the local SQLite DB.

X-Plane (and FlightGear) bundle three CIFP-derived .dat files that together
form a usable IFR routing graph:

  earth_fix.dat  — ~280 000 named RNAV fixes worldwide (POLTO, KELIG, EBUSO…)
  earth_nav.dat  —  ~30 000 NAVAIDs (VOR/NDB/DME) — overlaps with OurAirports
  earth_awy.dat  —  ~80 000 airway segments (UA601, UN857, G851, L433…)

This script parses all three and writes them to the `waypoint` and
`airway_segment` tables. Run it AFTER `python -m app.seed_db` (which seeds
the airports + countries baseline).

Where to get the files:
  - X-Plane installation: copy `Resources/default data/earth_*.dat`
  - or `Custom Data/earth_*.dat` if you've updated to a recent AIRAC cycle.
  - Public community mirrors exist (search 'X-Plane earth_awy.dat'); recency
    varies. AIRAC cycle is printed in the file header.

Usage:
  python -m app.import_xplane_navdata --dir /path/to/folder/containing/earth_dat_files
  python -m app.import_xplane_navdata --fixes earth_fix.dat --airways earth_awy.dat

File format reference (X-Plane 1100 / 1101 / 1200):
  earth_fix.dat:  lat  lon  ident  terminal_area_id  icao_region  type  [spoken_name]
  earth_nav.dat:  row_code  lat  lon  elev  freq  range  …  ident  region  name
                  (row_code: 2=NDB, 3=VOR, 12/13=DME)
  earth_awy.dat:  from_id from_subregion from_type to_id to_subregion to_type
                  direction base_fl top_fl awy_name(s separated by -)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from app import db


def _open_dat(path: Path):
    return path.open("r", encoding="utf-8", errors="ignore")


def _is_data_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith("#"):
        return False
    if stripped == "I" or stripped == "A":
        return False
    if stripped == "99":
        return False
    # version header line: starts with 4-digit version then word "Version"
    if stripped.split() and stripped.split()[0].isdigit() and len(stripped.split()[0]) == 4:
        return False
    return True


def import_fixes(path: Path) -> int:
    """Parse earth_fix.dat. Upserts each entry into the waypoint table."""
    rows: list[dict] = []
    with _open_dat(path) as f:
        for raw in f:
            if not _is_data_line(raw):
                continue
            parts = raw.split()
            # Minimum required: lat lon ident [region]
            if len(parts) < 3:
                continue
            try:
                lat = float(parts[0])
                lon = float(parts[1])
            except ValueError:
                continue
            ident = parts[2].strip().upper()
            # region : 4th field in v1100+, ENRT or terminal-area id in v1101+.
            # Accept the 5th field as a 2-letter region if present; otherwise blank.
            region = ""
            if len(parts) >= 5:
                cand = parts[4].strip().upper()
                if len(cand) == 2 and cand.isalpha():
                    region = cand
            rows.append({
                "ident": ident, "region": region, "lat": lat, "lon": lon,
                "kind": "FIX", "user_added": 0,
            })
            if len(rows) >= 5000:
                db.upsert_waypoints(rows); rows = []
    n = db.upsert_waypoints(rows) if rows else 0
    return n


def import_navaids(path: Path) -> int:
    """Parse earth_nav.dat. Upserts VORs/NDBs/DMEs into the waypoint table."""
    rows: list[dict] = []
    with _open_dat(path) as f:
        for raw in f:
            if not _is_data_line(raw):
                continue
            parts = raw.split()
            if len(parts) < 9:
                continue
            try:
                row_code = int(parts[0])
            except ValueError:
                continue
            if row_code not in (2, 3, 12, 13):
                continue  # 2=NDB, 3=VOR, 12=DME, 13=Stand-alone DME
            try:
                lat = float(parts[1])
                lon = float(parts[2])
            except ValueError:
                continue
            # ident is somewhere around column 7 depending on row_code; field count varies.
            # Conservative: find a token of 1-5 uppercase letters that looks like an ident.
            ident = None
            region = ""
            for i, tok in enumerate(parts[3:], start=3):
                if tok.isalpha() and 2 <= len(tok) <= 5 and tok.isupper():
                    ident = tok
                    # the very next token is sometimes the airport area id, then the
                    # region (2-letter). Take the first 2-letter all-alpha token after ident.
                    for tok2 in parts[i + 1:]:
                        if len(tok2) == 2 and tok2.isalpha() and tok2.isupper():
                            region = tok2
                            break
                    break
            if not ident:
                continue
            kind_map = {2: "NDB", 3: "VOR", 12: "DME", 13: "DME"}
            rows.append({
                "ident": ident, "region": region, "lat": lat, "lon": lon,
                "kind": kind_map[row_code], "user_added": 0,
            })
            if len(rows) >= 5000:
                db.upsert_waypoints(rows); rows = []
    n = db.upsert_waypoints(rows) if rows else 0
    return n


def import_airways(path: Path) -> int:
    """Parse earth_awy.dat. Upserts every airway segment.

    Multi-airway segments (e.g. 'UA601-UN857') yield one row per airway name.
    """
    rows: list[dict] = []
    with _open_dat(path) as f:
        for raw in f:
            if not _is_data_line(raw):
                continue
            parts = raw.split()
            if len(parts) < 10:
                continue
            from_id = parts[0].strip().upper()
            from_reg = parts[1].strip().upper() if len(parts[1]) == 2 else ""
            # parts[2] = from_type (kind code)
            to_id = parts[3].strip().upper()
            to_reg = parts[4].strip().upper() if len(parts[4]) == 2 else ""
            # parts[5] = to_type
            try:
                direction = int(parts[6])
            except ValueError:
                direction = 1
            try:
                fl_min = int(parts[7])
            except ValueError:
                fl_min = None
            try:
                fl_max = int(parts[8])
            except ValueError:
                fl_max = None
            airway_names = " ".join(parts[9:]).strip()
            # Multi-airway syntax: 'UA601-UN857-Q42' → one row per name
            for awy in airway_names.split("-"):
                awy = awy.strip().upper()
                if not awy:
                    continue
                rows.append({
                    "from_ident": from_id, "from_region": from_reg,
                    "to_ident": to_id, "to_region": to_reg,
                    "direction": direction, "fl_min": fl_min, "fl_max": fl_max,
                    "airway_name": awy,
                })
            if len(rows) >= 5000:
                db.upsert_airway_segments(rows); rows = []
    n = db.upsert_airway_segments(rows) if rows else 0
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, help="Folder containing earth_fix.dat / earth_nav.dat / earth_awy.dat")
    ap.add_argument("--fixes", type=Path)
    ap.add_argument("--navaids", type=Path)
    ap.add_argument("--airways", type=Path)
    args = ap.parse_args(argv)

    fixes_path = args.fixes or (args.dir / "earth_fix.dat" if args.dir else None)
    navaids_path = args.navaids or (args.dir / "earth_nav.dat" if args.dir else None)
    airways_path = args.airways or (args.dir / "earth_awy.dat" if args.dir else None)

    if not any([fixes_path, navaids_path, airways_path]):
        print("Pass --dir <folder> OR individual --fixes/--navaids/--airways paths.")
        return 1

    db.init_schema()

    if fixes_path and fixes_path.exists():
        print(f"→ Importing fixes from {fixes_path}…")
        n = import_fixes(fixes_path)
        print(f"  fixes upserted: {n}")
    elif fixes_path:
        print(f"  (skip fixes — file not found: {fixes_path})")

    if navaids_path and navaids_path.exists():
        print(f"→ Importing navaids from {navaids_path}…")
        n = import_navaids(navaids_path)
        print(f"  navaids upserted: {n}")
    elif navaids_path:
        print(f"  (skip navaids — file not found: {navaids_path})")

    if airways_path and airways_path.exists():
        print(f"→ Importing airways from {airways_path}…")
        n = import_airways(airways_path)
        print(f"  airway segments upserted: {n}")
    elif airways_path:
        print(f"  (skip airways — file not found: {airways_path})")

    print(f"\nTotal airway segments in DB now: {db.count_airway_segments()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
