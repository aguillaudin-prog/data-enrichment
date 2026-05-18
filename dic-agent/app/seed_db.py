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


def seed_canonical_routes() -> int:
    """Catalogue de routes officielles opérateur (Amazone Airlines / DHC6-400),
    pré-calculées au TY-BAB OEW=3813kg, ISA+20, calm. Source : doc opérateur
    'AMAZONE AIRLINES — Temps de Vol & Charges offertes DHC6-400'.

    Idempotent : rejouable, ON CONFLICT update les colonnes perf.
    Retourne le nombre de routes seedées.
    """
    csv_path = SEEDS_DIR / "amazone_routes.csv"
    if not csv_path.exists():
        print("  amazone_routes.csv missing — skipped")
        return 0
    import csv as _csv
    n = 0
    with csv_path.open(encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            variant = row.get("variant", "").strip()
            origin = row.get("origin", "").strip().upper()
            destination = row.get("destination", "").strip().upper()
            if not (origin and destination):
                continue
            route_text = row.get("route_text", "").strip()
            # Nom unique : "AMAZONE 13a DBBB→DNAA" — variant identifie la
            # sous-route (1.A maritime vs 1.B overflight etc.)
            name = f"AMAZONE {variant} {origin}→{destination}"
            legs_payload = [{
                "order": 1, "origin": origin, "destination": destination,
                "route_text": route_text, "fl": 90, "tas": 140,
            }]
            try:
                payload_kg = int(row["payload_kg"]) if row.get("payload_kg") else None
            except ValueError:
                payload_kg = None
            try:
                ft_min = int(row["flight_time_min"]) if row.get("flight_time_min") else None
            except ValueError:
                ft_min = None
            try:
                dist_nm = float(row["distance_nm"]) if row.get("distance_nm") else None
            except ValueError:
                dist_nm = None
            db.upsert_canonical_route({
                "name": name,
                "category": "AMAZONE — Bénin / DHC6 official",
                "legs_json": json.dumps(legs_payload, ensure_ascii=False),
                "origin_icao": origin,
                "destination_icao": destination,
                "distance_nm": dist_nm,
                "payload_kg": payload_kg,
                "flight_time_min": ft_min,
                "alternate": (row.get("alternate") or "").strip() or None,
                "aircraft_type": "DHC6",
                "variant": variant or None,
                "operator": "AMAZONE AIRLINES / DYNAMI AVIATION OPS",
            })
            n += 1
    print(f"  canonical routes: {n}")
    return n


def seed_amazone_waypoints() -> int:
    """3 fixes maritimes custom Amazone (EBUSO, ENKIT, ARABA) utilisés sur
    les routes 1.A et 11.A (évitement Ghana & Togo en trajet maritime).
    Coordonnées extraites du doc opérateur — non publiés OurAirports.
    Idempotent (PRIMARY KEY ident+region)."""
    csv_path = SEEDS_DIR / "amazone_waypoints.csv"
    if not csv_path.exists():
        print("  amazone_waypoints.csv missing — skipped")
        return 0
    import csv as _csv
    rows = []
    with csv_path.open(encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "ident": r["ident"].strip().upper(),
                    "region": r["region"].strip().upper() or None,
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "kind": r.get("kind", "WPT") or "WPT",
                    "user_added": int(r.get("user_added", 1) or 1),
                })
            except (KeyError, ValueError):
                continue
    n = db.upsert_waypoints(rows) if rows else 0
    print(f"  amazone waypoints: {n}")
    return n


def seed_dhc6_perf_refinements() -> None:
    """Affine la fiche aircraft_type du DHC6 avec les valeurs Amazone
    précises (OEW 3813 kg / 8406 lbs, ISA+20 still air conditions).
    On préserve les autres champs, on enrichit juste le full_name pour
    que l'OPS voie d'où viennent les chiffres dans les calculs."""
    with db.connect() as c:
        cur = c.execute("SELECT * FROM aircraft_type WHERE icao_designator = 'DHC6'")
        row = cur.fetchone()
        if not row:
            return
        new_name = "De Havilland Canada DHC-6-400 Twin Otter (TY-BAB, OEW 3813 kg)"
        if (row["full_name"] or "") != new_name:
            c.execute(
                "UPDATE aircraft_type SET full_name = ? WHERE icao_designator = 'DHC6'",
                (new_name,),
            )
            print(f"  DHC6 perf refined → {new_name}")


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
    print("→ Amazone maritime waypoints…")
    seed_amazone_waypoints()
    print("→ Canonical routes (Amazone)…")
    seed_canonical_routes()
    print("→ DHC6 perf refinements…")
    seed_dhc6_perf_refinements()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
