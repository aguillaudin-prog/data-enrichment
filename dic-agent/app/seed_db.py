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


def seed_diplomatic_lead_times() -> int:
    """Délais de préavis pour DIC par pays (jours). Référence pour la
    pré-DIC checklist : alerte si EOBT - now < lead_time_days."""
    csv_path = SEEDS_DIR / "diplomatic_lead_times.csv"
    if not csv_path.exists():
        print("  diplomatic_lead_times.csv missing — skipped")
        return 0
    rows = []
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "country_iso": r["country_iso"].strip().upper(),
                    "lead_time_days": int(r["lead_time_days"]),
                    "notes": (r.get("notes") or "").strip() or None,
                })
            except (KeyError, ValueError):
                continue
    n = db.upsert_lead_times(rows) if rows else 0
    print(f"  diplomatic lead times: {n}")
    return n


def seed_amazone_missions() -> int:
    """Missions multi-leg pré-cablées du catalogue Amazone, surfaceées
    dans le picker Mission de la page Legs (dossier Bénin).

    Une mission = un round-trip ou un tour multi-legs cohérent, avec
    EOBT pré-positionnés à 06:00Z J0 puis +2h par leg. L'OPS click sur
    la mission → les N legs sont pré-remplis (origins, destinations,
    routes, alternates, FL, TAS).

    Distinct des entries `seed_canonical_routes()` qui sont des routes
    unidirectionnelles consommées par l'auto-apply. Ces missions sont
    consommées par _apply_template via le picker.
    """
    # Catalogue complet Amazone PDF — 15 routes numérotées (1 à 15),
    # 23 missions au total avec les variants (1.A/1.B/1.C, 11.A/11.B, etc.).
    # Organisées par dossier en fonction du hub :
    #   - Bénin       : DBBB ou TOUROU comme bout (routes 1, 2, 11, 12, 13, 14, 15)
    #   - Côte d'Ivoire : DIAP-only (routes 3, 8, 9)
    #   - Sénégal-Guinée : GUCY ↔ GOBD (route 4) + GOBD ↔ GQNO (route 7)
    #   - Cameroun    : FKYS ↔ FOOL / FKKN (routes 5, 6)
    #   - Mauritanie  : GQNO ↔ GQND (route 10)
    missions = [
        # ────── Bénin (DBBB hub) ──────────────────────────────────────────
        ("Amazone", "[BJ] 1.A — DBBB ↔ DIAP maritime", [
            {"order": 1, "origin": "DBBB", "destination": "DIAP",
             "route_text": "TYE - EBUSO - ENKIT - ARABA - DCT - AD",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 2, "origin": "DIAP", "destination": "DBBB",
             "route_text": "AD - ARABA - ENKIT - EBUSO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 1.B — DBBB ↔ DIAP overflight", [
            {"order": 1, "origin": "DBBB", "destination": "DIAP",
             "route_text": "TYE - LM - ACC - TI - ONESI - AD",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 2, "origin": "DIAP", "destination": "DBBB",
             "route_text": "AD - ONESI - TI - ACC - LM - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 1.C — DBBB ↔ DIAP techstop DGAA", [
            {"order": 1, "origin": "DBBB", "destination": "DGAA",
             "route_text": "TYE - LM - ACC",
             "fl": 90, "tas": 140, "alternate": "DGTK"},
            {"order": 2, "origin": "DGAA", "destination": "DIAP",
             "route_text": "ACC - TI - ONESI - AD",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 3, "origin": "DIAP", "destination": "DGAA",
             "route_text": "AD - ONESI - TI - ACC",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
            {"order": 4, "origin": "DGAA", "destination": "DBBB",
             "route_text": "ACC - LM - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 2.A — DBBB ↔ FKYS direct", [
            {"order": 1, "origin": "DBBB", "destination": "FKYS",
             "route_text": "TYE - POLTO - LAG - R984 - POT - R984 - DLA - EDEBA - NLY",
             "fl": 90, "tas": 140, "alternate": "FKKD"},
            {"order": 2, "origin": "FKYS", "destination": "DBBB",
             "route_text": "NLY - EDEBA - DLA - R984 - POT - R984 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 2.B — DBBB ↔ FKYS techstop DNPO", [
            {"order": 1, "origin": "DBBB", "destination": "DNPO",
             "route_text": "TYE - POLTO - LAG - R984 - POT",
             "fl": 90, "tas": 140, "alternate": "DNCA"},
            {"order": 2, "origin": "DNPO", "destination": "FKYS",
             "route_text": "POT - R984 - DLA - EDEBA - NLY",
             "fl": 90, "tas": 140, "alternate": "FKKD"},
            {"order": 3, "origin": "FKYS", "destination": "DNPO",
             "route_text": "NLY - EDEBA - DLA - R984 - POT",
             "fl": 100, "tas": 140, "alternate": "DNEN"},
            {"order": 4, "origin": "DNPO", "destination": "DBBB",
             "route_text": "POT - R984 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 11.A — TOUROU ↔ DIAP maritime", [
            {"order": 1, "origin": "TOUROU", "destination": "DIAP",
             "route_text": "TOUROU - TYE - EBUSO - ENKIT - ARABA - DCT - AD",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 2, "origin": "DIAP", "destination": "TOUROU",
             "route_text": "AD - ARABA - ENKIT - EBUSO - TYE - TOUROU",
             "fl": 100, "tas": 140, "alternate": "DBBB"},
        ]),
        ("Amazone", "[BJ] 11.B — TOUROU ↔ DIAP techstop DGAA", [
            {"order": 1, "origin": "TOUROU", "destination": "DGAA",
             "route_text": "TOUROU - TYE - LM - ACC",
             "fl": 90, "tas": 140, "alternate": "DGTK"},
            {"order": 2, "origin": "DGAA", "destination": "DIAP",
             "route_text": "ACC - TI - ONESI - AD",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 3, "origin": "DIAP", "destination": "DGAA",
             "route_text": "AD - ONESI - TI - ACC",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
            {"order": 4, "origin": "DGAA", "destination": "TOUROU",
             "route_text": "ACC - LM - TYE - TOUROU",
             "fl": 100, "tas": 140, "alternate": "DBBB"},
        ]),
        ("Amazone", "[BJ] 12 — TOUROU ↔ DNKJ (KAINJI NAFB)", [
            {"order": 1, "origin": "TOUROU", "destination": "DNKJ",
             "route_text": "TOUROU - DCT - KIGRA - DNKJ",
             "fl": 90, "tas": 140, "alternate": "DNIL"},
            {"order": 2, "origin": "DNKJ", "destination": "TOUROU",
             "route_text": "DNKJ - KIGRA - DCT - TOUROU",
             "fl": 100, "tas": 140, "alternate": "DBBB"},
        ]),
        ("Amazone", "[BJ] 13 — DBBB ↔ DNAA", [
            {"order": 1, "origin": "DBBB", "destination": "DNAA",
             "route_text": "TYE - POLTO - LAG - R778 - KELIG - W951 - MESES - ABC",
             "fl": 90, "tas": 140, "alternate": "DNKA"},
            {"order": 2, "origin": "DNAA", "destination": "DBBB",
             "route_text": "ABC - VONUK - H340 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 14 — DBBB ↔ DNMN", [
            {"order": 1, "origin": "DBBB", "destination": "DNMN",
             "route_text": "TYE - POLTO - LAG - L433 - IBA - R778 - TEGDA - MNA",
             "fl": 90, "tas": 140, "alternate": "DNAA"},
            {"order": 2, "origin": "DNMN", "destination": "DBBB",
             "route_text": "MNA - MAGIA - V377 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 15 — DBBB ↔ DNIL", [
            {"order": 1, "origin": "DBBB", "destination": "DNIL",
             "route_text": "TYE - POLTO - LAG - L433 - IBA - ILR",
             "fl": 90, "tas": 140, "alternate": "DNIB"},
            {"order": 2, "origin": "DNIL", "destination": "DBBB",
             "route_text": "ILR - USGUN - V377 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        ("Amazone", "[BJ] 16 — DBBB ↔ DNKJ (KAINJI NAFB)", [
            {"order": 1, "origin": "DBBB", "destination": "DNKJ",
             "route_text": "TYE - POLTO - LAG - B731 - KIGRA - DCT DNKJ",
             "fl": 90, "tas": 140, "alternate": "DNIL"},
            {"order": 2, "origin": "DNKJ", "destination": "DBBB",
             "route_text": "DNKJ - DCT KIGRA - B731 - LAG - POLTO - TYE",
             "fl": 100, "tas": 140, "alternate": "DXXX"},
        ]),
        # ────── Côte d'Ivoire (DIAP hub) ──────────────────────────────────
        ("Amazone", "[CI] 3.A — DIAP ↔ GUCY évitement", [
            {"order": 1, "origin": "DIAP", "destination": "GUCY",
             "route_text": "AD - MAN - NZ - MA - J579 - KOLIP - A612 - ILGOT - CK",
             "fl": 90, "tas": 140, "alternate": "GFLL"},
            {"order": 2, "origin": "GUCY", "destination": "DIAP",
             "route_text": "CK - A612 - ILGOT - KOLIP - J579 - MA - NZ - MAN - AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        ("Amazone", "[CI] 3.B — DIAP ↔ GUCY overflight", [
            {"order": 1, "origin": "DIAP", "destination": "GUCY",
             "route_text": "AD - V207 - LGI - BIREL - CK",
             "fl": 90, "tas": 140, "alternate": "GFLL"},
            {"order": 2, "origin": "GUCY", "destination": "DIAP",
             "route_text": "CK - BIREL - LGI - V207 - AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        ("Amazone", "[CI] 3.C — DIAP ↔ GUCY techstop GLRB", [
            {"order": 1, "origin": "DIAP", "destination": "GLRB",
             "route_text": "AD - B600 - ROB",
             "fl": 90, "tas": 140, "alternate": "GLMR"},
            {"order": 2, "origin": "GLRB", "destination": "GUCY",
             "route_text": "ROB - B614 - LGI - BIREL - CK",
             "fl": 90, "tas": 140, "alternate": "GFLL"},
            {"order": 3, "origin": "GUCY", "destination": "GLRB",
             "route_text": "CK - BIREL - LGI - B614 - ROB",
             "fl": 100, "tas": 140, "alternate": "GLMR"},
            {"order": 4, "origin": "GLRB", "destination": "DIAP",
             "route_text": "ROB - B600 - AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        ("Amazone", "[CI] 8 — DIAP ↔ DIBK", [
            {"order": 1, "origin": "DIAP", "destination": "DIBK",
             "route_text": "AD - DEGAS - BKY",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 2, "origin": "DIBK", "destination": "DIAP",
             "route_text": "BKY - DEGAS - AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        ("Amazone", "[CI] 9 — DIAP ↔ DIKO", [
            {"order": 1, "origin": "DIAP", "destination": "DIKO",
             "route_text": "AD - DEGAS - BKY - KRG",
             "fl": 90, "tas": 140, "alternate": "DIYO"},
            {"order": 2, "origin": "DIKO", "destination": "DIAP",
             "route_text": "KRG - BKY - DEGAS - AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        ("Amazone", "[CI] 17 — DIAP ↔ DNKJ — évitement GH/TG", [
            {"order": 1, "origin": "DIAP", "destination": "DNKJ",
             "route_text": "AD - ARABA - ENKIT - EBUSO - TYE - POLTO - LAG - B731 - KIGRA - DNKJ",
             "fl": 90, "tas": 140, "alternate": "DNIL"},
            {"order": 2, "origin": "DNKJ", "destination": "DIAP",
             "route_text": "DNKJ - KIGRA - B731 - LAG - POLTO - TYE - EBUSO - ENKIT - ARABA - DCT AD",
             "fl": 100, "tas": 140, "alternate": "DIYO"},
        ]),
        # ────── Sénégal-Guinée (GUCY↔GOBD + GOBD↔GQNO) ────────────────────
        ("Amazone", "[SN/GN] 4.A — GUCY ↔ GOBD évitement", [
            {"order": 1, "origin": "GUCY", "destination": "GOBD",
             "route_text": "CK - H330 - SB - TD - A601 - DS",
             "fl": 90, "tas": 140, "alternate": "GOOY"},
            {"order": 2, "origin": "GOBD", "destination": "GUCY",
             "route_text": "DS - A601 - TD - SB - H330 - CK",
             "fl": 100, "tas": 140, "alternate": "GFLL"},
        ]),
        ("Amazone", "[SN/GN] 4.B — GUCY ↔ GOBD overflight", [
            {"order": 1, "origin": "GUCY", "destination": "GOBD",
             "route_text": "CK - AXIRO - KIRTI - GULAV - ABBIS - POTOX - BJ - B600 - ANITI - DS",
             "fl": 90, "tas": 140, "alternate": "GOOY"},
            {"order": 2, "origin": "GOBD", "destination": "GUCY",
             "route_text": "DS - ANITI - B600 - BJ - POTOX - ABBIS - GULAV - KIRTI - AXIRO - CK",
             "fl": 100, "tas": 140, "alternate": "GFLL"},
        ]),
        ("Amazone", "[SN/GN] 4.C — GUCY ↔ GOBD techstop GOGS", [
            {"order": 1, "origin": "GUCY", "destination": "GOGS",
             "route_text": "CK - AXIRO - KIRTI - GULAV - ABBIS - POTOX",
             "fl": 90, "tas": 140, "alternate": "GOGG"},
            {"order": 2, "origin": "GOGS", "destination": "GOBD",
             "route_text": "GOGS - BJ - B600 - ANITI - DS",
             "fl": 90, "tas": 140, "alternate": "GOOY"},
            {"order": 3, "origin": "GOBD", "destination": "GOGS",
             "route_text": "DS - ANITI - B600 - BJ - GOGS",
             "fl": 100, "tas": 140, "alternate": "GOGG"},
            {"order": 4, "origin": "GOGS", "destination": "GUCY",
             "route_text": "GOGS - POTOX - ABBIS - GULAV - KIRTI - AXIRO - CK",
             "fl": 100, "tas": 140, "alternate": "GFLL"},
        ]),
        ("Amazone", "[SN/GN] 7 — GOBD ↔ GQNO", [
            {"order": 1, "origin": "GOBD", "destination": "GQNO",
             "route_text": "DS - ANITI - R975 - NH",
             "fl": 90, "tas": 140, "alternate": "GQPP"},
            {"order": 2, "origin": "GQNO", "destination": "GOBD",
             "route_text": "NH - R975 - ANITI - DS",
             "fl": 100, "tas": 140, "alternate": "GOOY"},
        ]),
        # ────── Cameroun (FKYS hub) ───────────────────────────────────────
        ("Amazone", "[CM] 5 — FKYS ↔ FOOL", [
            {"order": 1, "origin": "FKYS", "destination": "FOOL",
             "route_text": "NLY - H455 - LV",
             "fl": 90, "tas": 140, "alternate": "FOOG"},
            {"order": 2, "origin": "FOOL", "destination": "FKYS",
             "route_text": "LV - H455 - NLY",
             "fl": 100, "tas": 140, "alternate": "FKKD"},
        ]),
        ("Amazone", "[CM] 6 — FKYS ↔ FKKN", [
            {"order": 1, "origin": "FKYS", "destination": "FKKN",
             "route_text": "NLY - H455 - BIRIX - TJN",
             "fl": 90, "tas": 140, "alternate": "FKKR"},
            {"order": 2, "origin": "FKKN", "destination": "FKYS",
             "route_text": "TJN - BIRIX - H455 - NLY",
             "fl": 100, "tas": 140, "alternate": "FKKD"},
        ]),
        # ────── Mauritanie ────────────────────────────────────────────────
        ("Amazone", "[MR] 10 — GQNO ↔ GQND", [
            {"order": 1, "origin": "GQNO", "destination": "GQND",
             "route_text": "NH - TKA",
             "fl": 90, "tas": 140, "alternate": "GQPA"},
            {"order": 2, "origin": "GQND", "destination": "GQNO",
             "route_text": "TKA - NH",
             "fl": 100, "tas": 140, "alternate": "GOSS"},
        ]),
    ]
    # Nettoie les anciennes missions seedées avec un autre schéma
    # (anciens noms type "BÉNIN / DBBB ↔ DNAA (Abuja)"). On supprime
    # toutes les missions officielles puis re-insert avec le nouveau
    # nommage "Dossier / N — origin ↔ destination (variante)".
    try:
        with db.connect() as c:
            c.execute(
                "DELETE FROM route_template "
                "WHERE official = 1 AND variant = 'mission'"
            )
    except Exception:
        pass
    n = 0
    for dossier, name, legs in missions:
        first_origin = legs[0]["origin"]
        last_destination = legs[-1]["destination"]
        full_name = f"{dossier} / {name}"
        db.upsert_canonical_route({
            "name": full_name,
            "category": dossier,
            "legs_json": json.dumps(legs, ensure_ascii=False),
            "origin_icao": first_origin,
            "destination_icao": last_destination,
            "distance_nm": None,
            "payload_kg": None,
            "flight_time_min": None,
            "alternate": None,
            "aircraft_type": "DHC6",
            "variant": "mission",
            "operator": "AMAZONE AIRLINES / DYNAMI AVIATION OPS",
        })
        n += 1
    print(f"  amazone missions: {n}")
    return n


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


def seed_amazone_airports() -> int:
    """Terrains spécifiques opérateur Amazone non publiés OurAirports
    (typiquement bases militaires comme Kainji NAFB / DNKJ). Idempotent.

    CSV : icao,iata,name,municipality,country_iso,lat,lon,elevation_ft,is_military
    """
    csv_path = SEEDS_DIR / "amazone_airports.csv"
    if not csv_path.exists():
        print("  amazone_airports.csv missing — skipped")
        return 0
    rows = []
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "icao": r["icao"].strip().upper(),
                    "iata": (r.get("iata") or "").strip() or None,
                    "name": r["name"].strip(),
                    "municipality": (r.get("municipality") or "").strip() or None,
                    "country_iso": (r.get("country_iso") or "").strip().upper() or None,
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "elevation_ft": int(r["elevation_ft"]) if r.get("elevation_ft") else None,
                    "is_military": int(r.get("is_military") or 0),
                    "user_added": 1,
                })
            except (KeyError, ValueError):
                continue
    n = db.upsert_airports(rows) if rows else 0
    print(f"  amazone airports: {n}")
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
    précises (OEW 3813 kg / 8406 lbs pour TY-BAB, MTOW 5670 kg DHC6-400,
    ISA+20 still air conditions). Utilisé pour fuel/W&B futurs."""
    db.init_schema()
    with db.connect() as c:
        cur = c.execute("SELECT * FROM aircraft_type WHERE icao_designator = 'DHC6'")
        row = cur.fetchone()
        if not row:
            return
        new_name = "De Havilland Canada DHC-6-400 Twin Otter (TY-BAB, OEW 3813 kg)"
        c.execute(
            "UPDATE aircraft_type SET full_name = ?, oew_kg = ?, mtow_kg = ? "
            "WHERE icao_designator = 'DHC6'",
            (new_name, 3813, 5670),
        )
        print(f"  DHC6 perf refined → OEW 3813 kg / MTOW 5670 kg")


def seed_il76_comjet_perf() -> None:
    """Affine la fiche aircraft_type de l'IL76 avec les valeurs réelles
    de COMJET (IL-76TD) : cruise 430 kt N0430, plafond utilisable FL390
    (préf. FL350), OEW 98 950 kg, MTOW 190 000 kg, MZFW 138 000 kg,
    MLW 151 500 kg, wake H. Source : profil RocketRoute / autorouter."""
    db.init_schema()
    with db.connect() as c:
        cur = c.execute("SELECT * FROM aircraft_type WHERE icao_designator = 'IL76'")
        row = cur.fetchone()
        if not row:
            # Ligne IL76 absente : on l'insère depuis zéro avec les valeurs
            # COMJET pour ne pas dépendre du CSV seed.
            c.execute(
                "INSERT INTO aircraft_type (icao_designator, full_name, manufacturer, "
                "cruise_tas_kt, service_ceiling_ft, range_nm, wake_category, "
                "oew_kg, mtow_kg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("IL76", "Ilyushin Il-76TD (COMJET, MTOW 190 t)", "Ilyushin",
                 430, 39000, 2700, "H", 98950, 190000),
            )
            print("  IL76 inserted with COMJET perf (cruise 430, OEW 98950, MTOW 190000)")
            return
        c.execute(
            "UPDATE aircraft_type SET full_name = ?, cruise_tas_kt = ?, "
            "service_ceiling_ft = ?, wake_category = ?, oew_kg = ?, mtow_kg = ? "
            "WHERE icao_designator = 'IL76'",
            ("Ilyushin Il-76TD (COMJET, MTOW 190 t)",
             430, 39000, "H", 98950, 190000),
        )
        print("  IL76 perf refined → COMJET (cruise 430, OEW 98950, MTOW 190000)")


def seed_comjet_aircraft() -> None:
    """Insère/MAJ l'appareil COMJET (IL-76TD) dans la table aircraft.
    Idempotent via UNIQUE(registration). Bypass db.save_aircraft pour
    éviter sa clause RETURNING (SQLite 3.35+) qui peut planter sur
    certains runtimes Cloud avec un SQLite plus ancien."""
    db.init_schema()
    with db.connect() as c:
        c.execute(
            """
            INSERT INTO aircraft (registration, type_icao, callsign, operator)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(registration) DO UPDATE SET
                type_icao=excluded.type_icao,
                callsign=excluded.callsign,
                operator=excluded.operator
            """,
            ("COMJET", "IL76", "COMJET", "COMJET"),
        )
    print("  COMJET (IL76) aircraft saved")


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
    print("→ Amazone airfields (DNKJ Kainji NAFB)…")
    seed_amazone_airports()
    print("→ Amazone maritime waypoints…")
    seed_amazone_waypoints()
    print("→ Canonical routes (Amazone)…")
    seed_canonical_routes()
    print("→ Amazone missions (multi-leg)…")
    seed_amazone_missions()
    print("→ DHC6 perf refinements…")
    seed_dhc6_perf_refinements()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
