"""Initial seed: airports, navaids, aircraft types, country borders.

Sources (all free, public):
- OurAirports — https://ourairports.com/data/
- Natural Earth Admin-0 50m — https://www.naturalearthdata.com/

Run: python -m app.seed_db
"""
from __future__ import annotations

import csv
import io
import json
import sys
import zipfile
from pathlib import Path

import requests

from app import db

SEEDS_DIR = Path(__file__).resolve().parent.parent / "seeds"
SEEDS_DIR.mkdir(parents=True, exist_ok=True)

OURAIRPORTS_AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
OURAIRPORTS_NAVAIDS_URL = "https://davidmegginson.github.io/ourairports-data/navaids.csv"
OURAIRPORTS_RUNWAYS_URL = "https://davidmegginson.github.io/ourairports-data/runways.csv"
NATURAL_EARTH_COUNTRIES_URLS = [
    # Primary: official Natural Earth vector repo on GitHub.
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_0_countries.geojson",
    # Fallback: naciscdn (often 403 from corporate networks).
    "https://naciscdn.org/naturalearth/50m/cultural/ne_50m_admin_0_countries.geojson",
]


def _download(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  cached {dest.name}")
        return dest
    print(f"  downloading {url} → {dest.name}")
    r = requests.get(url, timeout=120, headers={"User-Agent": "dic-agent/1.0"})
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def _download_first(urls: list[str], dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  cached {dest.name}")
        return dest
    last_err = None
    for url in urls:
        try:
            return _download(url, dest)
        except Exception as e:
            print(f"  failed {url}: {e}")
            last_err = e
    raise RuntimeError(f"All sources failed for {dest.name}: {last_err}")


def seed_airports() -> None:
    csv_path = _download(OURAIRPORTS_AIRPORTS_URL, SEEDS_DIR / "airports.csv")
    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            icao = (r.get("ident") or "").strip().upper()
            if not icao or len(icao) < 3:
                continue
            try:
                lat = float(r["latitude_deg"])
                lon = float(r["longitude_deg"])
            except (KeyError, ValueError):
                continue
            ap_type = (r.get("type") or "").lower()
            if ap_type == "closed":
                continue
            elev = r.get("elevation_ft")
            try:
                elev_i = int(float(elev)) if elev else None
            except ValueError:
                elev_i = None
            rows.append(
                {
                    "icao": icao,
                    "iata": (r.get("iata_code") or None) or None,
                    "name": r.get("name") or icao,
                    "municipality": (r.get("municipality") or "").strip() or None,
                    "country_iso": (r.get("iso_country") or "").upper() or None,
                    "lat": lat,
                    "lon": lon,
                    "elevation_ft": elev_i,
                    "is_military": 1 if "military" in ap_type else 0,
                    "user_added": 0,
                }
            )
    n = db.upsert_airports(rows)
    print(f"  airports: {n}")


def seed_waypoints() -> None:
    csv_path = _download(OURAIRPORTS_NAVAIDS_URL, SEEDS_DIR / "navaids.csv")
    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ident = (r.get("ident") or "").strip().upper()
            if not ident:
                continue
            try:
                lat = float(r["latitude_deg"])
                lon = float(r["longitude_deg"])
            except (KeyError, ValueError):
                continue
            rows.append(
                {
                    "ident": ident,
                    "region": (r.get("iso_country") or "").upper() or "",
                    "lat": lat,
                    "lon": lon,
                    "kind": (r.get("type") or "").upper() or None,
                    "user_added": 0,
                }
            )
    n = db.upsert_waypoints(rows)
    print(f"  waypoints: {n}")


def seed_runways() -> None:
    """Import runway lengths from OurAirports.

    Each row in runways.csv describes one runway with two ends (le_ident,
    he_ident). length_ft applies to both. We insert one row per end so the
    `runways_csv` field of CIFP procedures (which references specific ends
    like '06L') can be matched by identifier.
    """
    csv_path = _download(OURAIRPORTS_RUNWAYS_URL, SEEDS_DIR / "runways.csv")
    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            icao = (r.get("airport_ident") or "").strip().upper()
            if not icao:
                continue
            try:
                length = int(float(r["length_ft"])) if r.get("length_ft") else None
            except (KeyError, ValueError):
                length = None
            surface = (r.get("surface") or "").strip().upper() or None
            closed = 1 if (r.get("closed") or "0").strip() in ("1", "yes", "true") else 0
            for end_key in ("le_ident", "he_ident"):
                ident = (r.get(end_key) or "").strip().upper()
                if not ident:
                    continue
                rows.append({
                    "airport_icao": icao,
                    "ident": ident,
                    "length_ft": length,
                    "surface": surface,
                    "closed": closed,
                })
    n = db.upsert_runways(rows)
    print(f"  runways: {n}")


def seed_aircraft_types() -> None:
    csv_path = SEEDS_DIR / "aircraft_types.csv"
    if not csv_path.exists():
        print("  aircraft_types.csv missing — skipped")
        return
    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            def _int(k):
                v = r.get(k)
                if not v:
                    return None
                try:
                    return int(v)
                except ValueError:
                    return None

            rows.append(
                {
                    "icao_designator": r["icao_designator"].strip().upper(),
                    "full_name": r.get("full_name") or None,
                    "manufacturer": r.get("manufacturer") or None,
                    "cruise_tas_kt": _int("cruise_tas_kt"),
                    "service_ceiling_ft": _int("service_ceiling_ft"),
                    "range_nm": _int("range_nm"),
                    "wake_category": r.get("wake_category") or None,
                }
            )
    n = db.upsert_aircraft_types(rows)
    print(f"  aircraft types: {n}")


def seed_countries() -> None:
    geo_path = _download_first(NATURAL_EARTH_COUNTRIES_URLS, SEEDS_DIR / "ne_50m_admin_0_countries.geojson")
    data = json.loads(geo_path.read_text(encoding="utf-8"))
    rows: list[dict] = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        iso2 = (props.get("ISO_A2") or props.get("ISO_A2_EH") or "").upper()
        iso3 = (props.get("ISO_A3") or props.get("ISO_A3_EH") or "").upper()
        if not iso2 or iso2 == "-99":
            iso2 = iso3[:2] if iso3 else ""
        if not iso2:
            continue
        rows.append(
            {
                "iso_a2": iso2,
                "iso_a3": iso3 or None,
                "name_en": props.get("NAME_EN") or props.get("NAME") or iso2,
                "name_fr": props.get("NAME_FR") or props.get("NAME") or iso2,
                "geom_geojson": json.dumps(feat["geometry"]),
            }
        )
    n = db.upsert_countries(rows)
    print(f"  countries: {n}")


def main() -> int:
    print("→ Init schema…")
    db.init_schema()
    print("→ Aircraft types…")
    seed_aircraft_types()
    print("→ Airports…")
    seed_airports()
    print("→ Waypoints…")
    seed_waypoints()
    print("→ Runways…")
    seed_runways()
    print("→ Countries…")
    seed_countries()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
