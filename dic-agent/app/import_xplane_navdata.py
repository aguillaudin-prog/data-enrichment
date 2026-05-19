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
    total = 0
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
                total += db.upsert_waypoints(rows); rows = []
    if rows:
        total += db.upsert_waypoints(rows)
    return total


def import_navaids(path: Path) -> int:
    """Parse earth_nav.dat. Upserts VORs/NDBs/DMEs into the waypoint table."""
    rows: list[dict] = []
    total = 0
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
                total += db.upsert_waypoints(rows); rows = []
    if rows:
        total += db.upsert_waypoints(rows)
    return total


def _is_int(s: str) -> bool:
    try:
        int(s)
        return True
    except ValueError:
        return False


def _parse_awy_line(parts: list[str]) -> dict | None:
    """Robust earth_awy.dat parser.

    The format has varied across X-Plane versions (1100, 1101, 1200…). To stay
    independent of the exact field count, we anchor from BOTH ends:

      <from_id> <from_terminal?> <from_subregion?> <from_type>  …  <to_id> <to_terminal?> <to_subregion?> <to_type>  <direction> <base_fl> <top_fl> <name>

    Right side is the fixed tail. The last field is the airway name (or a
    'name1-name2' multi-airway string). top_fl and base_fl are integers
    immediately before, then direction (1 or 2). to_type is the integer
    before direction. from_id is parts[0]; from_type is the FIRST integer in
    parts[1:]; to_id is the token RIGHT AFTER from_type.
    """
    if len(parts) < 8:
        return None
    # 4 tail tokens: direction, base_fl, top_fl, name
    name_token = parts[-1].strip().upper()
    if not name_token:
        return None
    try:
        top_fl = int(parts[-2])
    except ValueError:
        return None
    try:
        base_fl = int(parts[-3])
    except ValueError:
        return None
    direction_tok = parts[-4]
    try:
        direction = int(direction_tok)
    except ValueError:
        # Some formats use a letter (e.g. 'J', 'V'). Default to bidirectional.
        direction = 1

    from_id = parts[0].strip().upper()
    # Locate from_type (first integer in parts[1:end-4])
    middle = parts[1:-4]
    from_type_idx_in_middle: int | None = None
    for i, tok in enumerate(middle):
        if _is_int(tok):
            from_type_idx_in_middle = i
            break
    if from_type_idx_in_middle is None:
        return None
    # Region(s) are between from_id and from_type
    from_region_tokens = middle[:from_type_idx_in_middle]
    from_region = ""
    for t in from_region_tokens:
        if len(t) == 2 and t.isalpha():
            from_region = t.upper()
            break
    # to_id is the next non-empty token after from_type
    after_from_type = middle[from_type_idx_in_middle + 1:]
    if not after_from_type:
        return None
    to_id = after_from_type[0].strip().upper()
    # to_type is the first integer after to_id
    to_region = ""
    for t in after_from_type[1:]:
        if _is_int(t):
            break
        if len(t) == 2 and t.isalpha():
            to_region = t.upper()
            break

    return {
        "from_id": from_id, "from_region": from_region,
        "to_id": to_id, "to_region": to_region,
        "direction": direction, "fl_min": base_fl, "fl_max": top_fl,
        "name": name_token,
    }


def import_airways(path: Path) -> int:
    """Parse earth_awy.dat. Upserts every airway segment.

    Multi-airway segments (e.g. 'UA601-UN857') yield one row per airway name.
    """
    rows: list[dict] = []
    total = 0
    with _open_dat(path) as f:
        for raw in f:
            if not _is_data_line(raw):
                continue
            parts = raw.split()
            parsed = _parse_awy_line(parts)
            if parsed is None:
                continue
            # Multi-airway syntax: 'UA601-UN857-Q42' → one row per name
            for awy in parsed["name"].split("-"):
                awy = awy.strip().upper()
                if not awy:
                    continue
                # Sanity check: airway names look like 1-2 letters + digits.
                # Reject pure-numeric values that leak from malformed lines.
                if awy.isdigit():
                    continue
                rows.append({
                    "from_ident": parsed["from_id"], "from_region": parsed["from_region"],
                    "to_ident": parsed["to_id"], "to_region": parsed["to_region"],
                    "direction": parsed["direction"],
                    "fl_min": parsed["fl_min"], "fl_max": parsed["fl_max"],
                    "airway_name": awy,
                })
            if len(rows) >= 5000:
                total += db.upsert_airway_segments(rows); rows = []
    if rows:
        total += db.upsert_airway_segments(rows)
    return total


def import_holdings(path: Path) -> int:
    """Parse earth_hold.dat (X-Plane CIFP-derived). Format ARINC 424 :

      <fix_ident> <region> <area> <inbound_course> <leg_time_min>
      <leg_dist_nm> <max_speed_kt> <lower_alt_ft> <upper_alt_ft> <turn_dir>

    Exemple :
      ABTAL EG ENRT 058.50 1.00 0.00 220 0 36000 R

    Skip les lignes header (version + cycle date sur 2 premières lignes)
    et toute ligne avec moins de 9 colonnes.
    """
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line_no, raw in enumerate(f):
            line = raw.strip()
            if not line or line.startswith("I ") or line == "99":
                continue
            # Skip header lines
            if line_no < 3 and (line.startswith("1100") or line.startswith("1140")
                                or "Version" in line or "Cycle" in line):
                continue
            parts = line.split()
            if len(parts) < 9:
                continue
            try:
                row = {
                    "fix_ident": parts[0].upper(),
                    "fix_region": parts[1].upper(),
                    "inbound_course": float(parts[3]),
                    "leg_time_min": float(parts[4]),
                    "leg_dist_nm": float(parts[5]),
                    "max_speed_kt": int(float(parts[6])),
                    "lower_alt_ft": int(float(parts[7])),
                    "upper_alt_ft": int(float(parts[8])),
                    "turn_direction": parts[9].upper() if len(parts) > 9 else None,
                }
            except (ValueError, IndexError):
                continue
            rows.append(row)
    return db.upsert_holdings(rows) if rows else 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, help="Folder containing earth_fix.dat / earth_nav.dat / earth_awy.dat")
    ap.add_argument("--fixes", type=Path)
    ap.add_argument("--navaids", type=Path)
    ap.add_argument("--airways", type=Path)
    ap.add_argument("--holds", type=Path, help="Path to earth_hold.dat")
    ap.add_argument(
        "--clean-first", action="store_true",
        help="Drop all rows from airway_segment before importing. Use when a previous "
             "buggy import left corrupted airway names (e.g. pure-digit names).",
    )
    args = ap.parse_args(argv)

    fixes_path = args.fixes or (args.dir / "earth_fix.dat" if args.dir else None)
    navaids_path = args.navaids or (args.dir / "earth_nav.dat" if args.dir else None)
    airways_path = args.airways or (args.dir / "earth_awy.dat" if args.dir else None)
    holds_path = args.holds or (args.dir / "earth_hold.dat" if args.dir else None)

    if not any([fixes_path, navaids_path, airways_path, holds_path]):
        print("Pass --dir <folder> OR individual --fixes/--navaids/--airways paths.")
        return 1

    db.init_schema()

    if args.clean_first:
        with db.connect() as c:
            n = c.execute("DELETE FROM airway_segment").rowcount
        print(f"Cleaned {n} existing airway_segment rows.")

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

    if holds_path and holds_path.exists():
        print(f"→ Importing holdings from {holds_path}…")
        n = import_holdings(holds_path)
        print(f"  holdings upserted: {n}")
    elif holds_path:
        print(f"  (skip holdings — file not found: {holds_path})")

    print(f"\nTotal airway segments in DB now: {db.count_airway_segments()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
