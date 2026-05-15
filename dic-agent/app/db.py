"""SQLite schema + helpers for the DIC agent."""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "dic.sqlite"

SCHEMA = """
CREATE TABLE IF NOT EXISTS airport (
    icao TEXT PRIMARY KEY,
    iata TEXT,
    name TEXT NOT NULL,
    municipality TEXT,
    country_iso TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    elevation_ft INTEGER,
    is_military INTEGER DEFAULT 0,
    user_added INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_airport_iata ON airport(iata);
CREATE INDEX IF NOT EXISTS idx_airport_country ON airport(country_iso);

CREATE TABLE IF NOT EXISTS waypoint (
    ident TEXT NOT NULL,
    region TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    kind TEXT,
    user_added INTEGER DEFAULT 0,
    PRIMARY KEY (ident, region)
);
CREATE INDEX IF NOT EXISTS idx_waypoint_ident ON waypoint(ident);

CREATE TABLE IF NOT EXISTS aircraft_type (
    icao_designator TEXT PRIMARY KEY,
    full_name TEXT,
    manufacturer TEXT,
    cruise_tas_kt INTEGER,
    service_ceiling_ft INTEGER,
    range_nm INTEGER,
    wake_category TEXT
);

CREATE TABLE IF NOT EXISTS aircraft (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    registration TEXT UNIQUE NOT NULL,
    type_icao TEXT REFERENCES aircraft_type(icao_designator),
    callsign TEXT,
    operator TEXT
);

CREATE TABLE IF NOT EXISTS crew (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    members_json TEXT NOT NULL,
    n_crew INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pilot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('CDB', 'FO')),
    rank TEXT,
    allowed_operator TEXT,
    active INTEGER DEFAULT 1,
    UNIQUE(name, role)
);

CREATE TABLE IF NOT EXISTS poc (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rank TEXT,
    name TEXT NOT NULL,
    phone TEXT,
    email_personal TEXT,
    email_functional TEXT,
    fax TEXT,
    UNIQUE(name, email_personal)
);

CREATE TABLE IF NOT EXISTS route_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    legs_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mission (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reference TEXT,
    amendment TEXT,
    mission_number TEXT,
    template_format TEXT CHECK (template_format IN ('FRA', 'ICAO')) NOT NULL,
    aircraft_id INTEGER REFERENCES aircraft(id),
    crew_id INTEGER REFERENCES crew(id),
    poc_id INTEGER REFERENCES poc(id),
    requesting_state TEXT DEFAULT 'FRANCE',
    purpose TEXT,
    radio_frequencies TEXT,
    alternates TEXT,
    n_passengers TEXT,
    vip_title TEXT,
    dg_details TEXT,
    indicators_json TEXT,
    legs_json TEXT,
    overrides_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS country (
    iso_a2 TEXT PRIMARY KEY,
    iso_a3 TEXT,
    name_en TEXT,
    name_fr TEXT,
    geom_geojson TEXT
);

CREATE TABLE IF NOT EXISTS procedure (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    airport_icao TEXT NOT NULL,
    proc_type TEXT NOT NULL CHECK (proc_type IN ('SID', 'STAR', 'APPCH')),
    proc_name TEXT NOT NULL,
    runways_csv TEXT,
    waypoints_json TEXT NOT NULL,
    UNIQUE (airport_icao, proc_type, proc_name)
);
CREATE INDEX IF NOT EXISTS idx_proc_airport_type ON procedure(airport_icao, proc_type);
CREATE INDEX IF NOT EXISTS idx_proc_name ON procedure(proc_name);

CREATE TABLE IF NOT EXISTS runway (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    airport_icao TEXT NOT NULL,
    ident TEXT NOT NULL,
    length_ft INTEGER,
    surface TEXT,
    closed INTEGER DEFAULT 0,
    UNIQUE (airport_icao, ident)
);
CREATE INDEX IF NOT EXISTS idx_runway_airport ON runway(airport_icao);

CREATE TABLE IF NOT EXISTS airway_segment (
    from_ident TEXT NOT NULL,
    from_region TEXT,
    to_ident TEXT NOT NULL,
    to_region TEXT,
    direction INTEGER NOT NULL DEFAULT 1,  -- 1 = both directions, 2 = from→to only
    fl_min INTEGER,
    fl_max INTEGER,
    airway_name TEXT NOT NULL,
    PRIMARY KEY (from_ident, from_region, to_ident, to_region, airway_name)
);
CREATE INDEX IF NOT EXISTS idx_awy_from ON airway_segment(from_ident);
CREATE INDEX IF NOT EXISTS idx_awy_to ON airway_segment(to_ident);
CREATE INDEX IF NOT EXISTS idx_awy_name ON airway_segment(airway_name);
"""


def _ensure_parent() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def connect():
    _ensure_parent()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _migrate(conn) -> None:
    """Idempotent additive migrations for older DBs."""
    cur = conn.execute("PRAGMA table_info(pilot)")
    pilot_cols = {row[1] for row in cur.fetchall()}
    if pilot_cols and "allowed_operator" not in pilot_cols:
        conn.execute("ALTER TABLE pilot ADD COLUMN allowed_operator TEXT")
    cur = conn.execute("PRAGMA table_info(route_template)")
    tpl_cols = {row[1] for row in cur.fetchall()}
    if tpl_cols and "category" not in tpl_cols:
        conn.execute("ALTER TABLE route_template ADD COLUMN category TEXT")
    cur = conn.execute("PRAGMA table_info(aircraft_type)")
    ac_cols = {row[1] for row in cur.fetchall()}
    if ac_cols:
        if "min_runway_ft" not in ac_cols:
            conn.execute("ALTER TABLE aircraft_type ADD COLUMN min_runway_ft INTEGER")
        if "approach_cat" not in ac_cols:
            conn.execute("ALTER TABLE aircraft_type ADD COLUMN approach_cat TEXT")
        if "climb_gradient_pct" not in ac_cols:
            conn.execute("ALTER TABLE aircraft_type ADD COLUMN climb_gradient_pct REAL")
    cur = conn.execute("PRAGMA table_info(airport)")
    apt_cols = {row[1] for row in cur.fetchall()}
    if apt_cols and "municipality" not in apt_cols:
        conn.execute("ALTER TABLE airport ADD COLUMN municipality TEXT")


def init_schema() -> None:
    with connect() as c:
        c.executescript(SCHEMA)
        _migrate(c)


def upsert_airports(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO airport (icao, iata, name, municipality, country_iso, lat, lon,
                         elevation_ft, is_military, user_added)
    VALUES (:icao, :iata, :name, :municipality, :country_iso, :lat, :lon,
            :elevation_ft, :is_military, :user_added)
    ON CONFLICT(icao) DO UPDATE SET
        iata=excluded.iata, name=excluded.name, municipality=excluded.municipality,
        country_iso=excluded.country_iso,
        lat=excluded.lat, lon=excluded.lon, elevation_ft=excluded.elevation_ft,
        is_military=excluded.is_military
    """
    n = 0
    with connect() as c:
        for r in rows:
            row = {"municipality": None, **r}
            c.execute(sql, row)
            n += 1
    return n


def upsert_waypoints(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO waypoint (ident, region, lat, lon, kind, user_added)
    VALUES (:ident, :region, :lat, :lon, :kind, :user_added)
    ON CONFLICT(ident, region) DO UPDATE SET
        lat=excluded.lat, lon=excluded.lon, kind=excluded.kind
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


def upsert_aircraft_types(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO aircraft_type (icao_designator, full_name, manufacturer, cruise_tas_kt,
        service_ceiling_ft, range_nm, wake_category,
        min_runway_ft, approach_cat, climb_gradient_pct)
    VALUES (:icao_designator, :full_name, :manufacturer, :cruise_tas_kt,
        :service_ceiling_ft, :range_nm, :wake_category,
        :min_runway_ft, :approach_cat, :climb_gradient_pct)
    ON CONFLICT(icao_designator) DO UPDATE SET
        full_name=excluded.full_name, manufacturer=excluded.manufacturer,
        cruise_tas_kt=excluded.cruise_tas_kt, service_ceiling_ft=excluded.service_ceiling_ft,
        range_nm=excluded.range_nm, wake_category=excluded.wake_category,
        min_runway_ft=COALESCE(excluded.min_runway_ft, aircraft_type.min_runway_ft),
        approach_cat=COALESCE(excluded.approach_cat, aircraft_type.approach_cat),
        climb_gradient_pct=COALESCE(excluded.climb_gradient_pct, aircraft_type.climb_gradient_pct)
    """
    n = 0
    with connect() as c:
        for r in rows:
            row = {
                "min_runway_ft": None, "approach_cat": None, "climb_gradient_pct": None,
                **r,
            }
            c.execute(sql, row)
            n += 1
    return n


def find_aircraft_type(icao_designator: str) -> sqlite3.Row | None:
    if not icao_designator:
        return None
    with connect() as c:
        return c.execute(
            "SELECT * FROM aircraft_type WHERE icao_designator = ?",
            (icao_designator.strip().upper(),),
        ).fetchone()


def upsert_runways(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO runway (airport_icao, ident, length_ft, surface, closed)
    VALUES (:airport_icao, :ident, :length_ft, :surface, :closed)
    ON CONFLICT(airport_icao, ident) DO UPDATE SET
        length_ft=excluded.length_ft, surface=excluded.surface, closed=excluded.closed
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


def list_airport_runways(icao: str) -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute(
            "SELECT * FROM runway WHERE airport_icao = ? AND closed = 0 "
            "ORDER BY length_ft DESC NULLS LAST",
            (icao.strip().upper(),),
        ).fetchall()


def runway_length_ft(icao: str, ident: str) -> int | None:
    """Length of a specific runway end (e.g. '06L'). Returns None if unknown."""
    if not icao or not ident:
        return None
    with connect() as c:
        row = c.execute(
            "SELECT length_ft FROM runway WHERE airport_icao = ? AND ident = ?",
            (icao.strip().upper(), ident.strip().upper()),
        ).fetchone()
        return row["length_ft"] if row and row["length_ft"] is not None else None


def count_runways() -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM runway").fetchone()[0]


def upsert_countries(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO country (iso_a2, iso_a3, name_en, name_fr, geom_geojson)
    VALUES (:iso_a2, :iso_a3, :name_en, :name_fr, :geom_geojson)
    ON CONFLICT(iso_a2) DO UPDATE SET
        iso_a3=excluded.iso_a3, name_en=excluded.name_en, name_fr=excluded.name_fr,
        geom_geojson=excluded.geom_geojson
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


def find_airport(token: str) -> sqlite3.Row | None:
    token = token.strip().upper()
    with connect() as c:
        row = c.execute("SELECT * FROM airport WHERE icao = ?", (token,)).fetchone()
        if row:
            return row
        return c.execute("SELECT * FROM airport WHERE iata = ?", (token,)).fetchone()


def find_airports_by_prefix(prefix: str, limit: int = 15) -> list[sqlite3.Row]:
    """Airports whose ICAO starts with `prefix`, ordered by ICAO. Used for
    type-ahead autocomplete in the leg editor. Returns at most `limit` rows
    so a 1-letter prefix doesn't dump the entire DB into the UI."""
    prefix = prefix.strip().upper()
    if not prefix:
        return []
    with connect() as c:
        return c.execute(
            "SELECT icao, name, country_iso FROM airport "
            "WHERE icao LIKE ? ORDER BY icao LIMIT ?",
            (prefix + "%", limit),
        ).fetchall()


def default_alternate_for(destination_icao: str) -> str | None:
    """Look up the typical alternate airport for `destination_icao`, derived
    from existing route_template legs in the DB. Returns the most-frequent
    alternate ICAO seen for that destination across all stored templates,
    or None if the destination has never been used.

    Implementation: scan every route_template.legs_json entry, count the
    (destination, alternate) pairs, return the alternate with the highest
    count for the given destination. Cheap because the templates table is
    small (dozens of rows max).
    """
    destination_icao = (destination_icao or "").strip().upper()
    if not destination_icao:
        return None
    counts: dict[str, int] = {}
    with connect() as c:
        rows = c.execute("SELECT legs_json FROM route_template").fetchall()
    import json as _json
    for r in rows:
        try:
            legs = _json.loads(r["legs_json"])
        except Exception:
            continue
        for leg in legs:
            if (leg.get("destination") or "").strip().upper() != destination_icao:
                continue
            alt = (leg.get("alternate") or "").strip().upper()
            if alt:
                counts[alt] = counts.get(alt, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def find_airports_by_name_substring(query: str, limit: int = 15) -> list[sqlite3.Row]:
    """Airports whose name contains `query` (case-insensitive). Fallback for
    type-ahead when the user types a city/airport name instead of an ICAO
    prefix. Ordered by ICAO so output is deterministic."""
    query = query.strip()
    if not query:
        return []
    with connect() as c:
        return c.execute(
            "SELECT icao, name, country_iso FROM airport "
            "WHERE name LIKE ? COLLATE NOCASE ORDER BY icao LIMIT ?",
            (f"%{query}%", limit),
        ).fetchall()


def find_waypoint(ident: str, region_hint: str | None = None) -> sqlite3.Row | None:
    ident = ident.strip().upper()
    with connect() as c:
        if region_hint:
            row = c.execute(
                "SELECT * FROM waypoint WHERE ident = ? AND region = ?",
                (ident, region_hint),
            ).fetchone()
            if row:
                return row
        return c.execute(
            "SELECT * FROM waypoint WHERE ident = ? ORDER BY user_added DESC LIMIT 1",
            (ident,),
        ).fetchone()


def find_waypoints_all(ident: str) -> list[sqlite3.Row]:
    """Return every waypoint matching `ident` (any region)."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM waypoint WHERE ident = ? ORDER BY user_added DESC", (ident.strip().upper(),)
        ).fetchall()


def list_countries() -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT iso_a2, iso_a3, name_en, name_fr, geom_geojson FROM country").fetchall()


def find_country_name(iso_a2: str) -> str | None:
    """Return the English name for an ISO2 country code (BJ → 'Benin')."""
    if not iso_a2:
        return None
    with connect() as c:
        row = c.execute(
            "SELECT name_en FROM country WHERE iso_a2 = ?", (iso_a2.strip().upper(),)
        ).fetchone()
        return row["name_en"] if row else None


def list_aircraft(operator: str | None = None) -> list[sqlite3.Row]:
    with connect() as c:
        if operator:
            return c.execute(
                "SELECT * FROM aircraft WHERE operator = ? ORDER BY registration",
                (operator,),
            ).fetchall()
        return c.execute("SELECT * FROM aircraft ORDER BY registration").fetchall()


def list_operators() -> list[str]:
    """Distinct, sorted list of operator names found in the aircraft + pilot tables."""
    with connect() as c:
        rows = c.execute(
            """
            SELECT operator AS name FROM aircraft WHERE operator IS NOT NULL AND operator != ''
            UNION
            SELECT allowed_operator AS name FROM pilot WHERE allowed_operator IS NOT NULL AND allowed_operator != ''
            ORDER BY name
            """
        ).fetchall()
        return [r["name"] for r in rows]


def list_aircraft_types(prefix: str = "") -> list[sqlite3.Row]:
    with connect() as c:
        if prefix:
            return c.execute(
                "SELECT * FROM aircraft_type WHERE icao_designator LIKE ? OR full_name LIKE ? ORDER BY icao_designator LIMIT 50",
                (f"{prefix}%", f"%{prefix}%"),
            ).fetchall()
        return c.execute("SELECT * FROM aircraft_type ORDER BY icao_designator LIMIT 200").fetchall()


def list_crews() -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM crew ORDER BY name").fetchall()


def list_pocs() -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM poc ORDER BY name").fetchall()


def list_route_templates() -> list[sqlite3.Row]:
    with connect() as c:
        return c.execute("SELECT * FROM route_template ORDER BY name").fetchall()


def save_aircraft(registration: str, type_icao: str | None, callsign: str | None, operator: str | None) -> int:
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO aircraft (registration, type_icao, callsign, operator)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(registration) DO UPDATE SET
                type_icao=excluded.type_icao, callsign=excluded.callsign, operator=excluded.operator
            RETURNING id
            """,
            (registration.strip().upper(), type_icao, callsign, operator),
        )
        return cur.fetchone()[0]


def list_pilots(role: str | None = None, operator: str | None = None) -> list[sqlite3.Row]:
    with connect() as c:
        sql = "SELECT * FROM pilot WHERE active = 1"
        params: list = []
        if role:
            sql += " AND role = ?"
            params.append(role)
        if operator:
            sql += " AND (allowed_operator IS NULL OR allowed_operator = '' OR allowed_operator = ?)"
            params.append(operator)
        sql += " ORDER BY role, name"
        return c.execute(sql, params).fetchall()


def save_pilot(name: str, role: str, rank: str | None = None, allowed_operator: str | None = None) -> int:
    if role not in ("CDB", "FO"):
        raise ValueError("role must be 'CDB' or 'FO'")
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO pilot (name, role, rank, allowed_operator, active)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(name, role) DO UPDATE SET
                rank=excluded.rank, allowed_operator=excluded.allowed_operator, active=1
            RETURNING id
            """,
            (name.strip(), role, rank, allowed_operator),
        )
        return cur.fetchone()[0]


def deactivate_pilot(pilot_id: int) -> None:
    with connect() as c:
        c.execute("UPDATE pilot SET active = 0 WHERE id = ?", (pilot_id,))


def save_crew(name: str, members: list[dict], n_crew: int) -> int:
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO crew (name, members_json, n_crew)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET members_json=excluded.members_json, n_crew=excluded.n_crew
            RETURNING id
            """,
            (name, json.dumps(members), n_crew),
        )
        return cur.fetchone()[0]


def save_poc(rank: str, name: str, phone: str, email_personal: str, email_functional: str, fax: str) -> int:
    with connect() as c:
        cur = c.execute(
            """
            INSERT INTO poc (rank, name, phone, email_personal, email_functional, fax)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(name, email_personal) DO UPDATE SET
                rank=excluded.rank, phone=excluded.phone,
                email_functional=excluded.email_functional, fax=excluded.fax
            RETURNING id
            """,
            (rank, name, phone, email_personal, email_functional, fax),
        )
        return cur.fetchone()[0]


def save_user_waypoint(ident: str, lat: float, lon: float, region: str | None = None, kind: str = "USER") -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO waypoint (ident, region, lat, lon, kind, user_added)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(ident, region) DO UPDATE SET lat=excluded.lat, lon=excluded.lon
            """,
            (ident.strip().upper(), region or "", lat, lon, kind),
        )


def upsert_procedures(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO procedure (airport_icao, proc_type, proc_name, runways_csv, waypoints_json)
    VALUES (:airport_icao, :proc_type, :proc_name, :runways_csv, :waypoints_json)
    ON CONFLICT(airport_icao, proc_type, proc_name) DO UPDATE SET
        runways_csv = excluded.runways_csv,
        waypoints_json = excluded.waypoints_json
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


def list_procedures(airport_icao: str, proc_type: str | None = None) -> list[sqlite3.Row]:
    with connect() as c:
        if proc_type:
            return c.execute(
                "SELECT * FROM procedure WHERE airport_icao = ? AND proc_type = ? ORDER BY proc_name",
                (airport_icao.strip().upper(), proc_type),
            ).fetchall()
        return c.execute(
            "SELECT * FROM procedure WHERE airport_icao = ? ORDER BY proc_type, proc_name",
            (airport_icao.strip().upper(),),
        ).fetchall()


def count_procedures() -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM procedure").fetchone()[0]


def upsert_airway_segments(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO airway_segment (from_ident, from_region, to_ident, to_region, direction, fl_min, fl_max, airway_name)
    VALUES (:from_ident, :from_region, :to_ident, :to_region, :direction, :fl_min, :fl_max, :airway_name)
    ON CONFLICT(from_ident, from_region, to_ident, to_region, airway_name) DO UPDATE SET
        direction=excluded.direction, fl_min=excluded.fl_min, fl_max=excluded.fl_max
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


def airway_segments_for(ident: str) -> list[sqlite3.Row]:
    """Return all airway segments incident to a given waypoint ident."""
    with connect() as c:
        return c.execute(
            "SELECT * FROM airway_segment WHERE from_ident = ? OR to_ident = ?",
            (ident.strip().upper(), ident.strip().upper()),
        ).fetchall()


def count_airway_segments() -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM airway_segment").fetchone()[0]


def airway_segments_in_bbox(lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> list[sqlite3.Row]:
    """Return segments where both endpoints fall in the lat/lon bounding box."""
    with connect() as c:
        return c.execute(
            """
            SELECT s.*, w1.lat AS from_lat, w1.lon AS from_lon, w2.lat AS to_lat, w2.lon AS to_lon
            FROM airway_segment s
            JOIN waypoint w1 ON w1.ident = s.from_ident
                AND (w1.region = COALESCE(s.from_region, '') OR s.from_region IS NULL OR s.from_region = '')
            JOIN waypoint w2 ON w2.ident = s.to_ident
                AND (w2.region = COALESCE(s.to_region, '') OR s.to_region IS NULL OR s.to_region = '')
            WHERE w1.lat BETWEEN ? AND ? AND w1.lon BETWEEN ? AND ?
              AND w2.lat BETWEEN ? AND ? AND w2.lon BETWEEN ? AND ?
            """,
            (lat_min, lat_max, lon_min, lon_max, lat_min, lat_max, lon_min, lon_max),
        ).fetchall()


def save_user_airport(icao: str, name: str, country_iso: str, lat: float, lon: float, is_military: bool = True) -> None:
    with connect() as c:
        c.execute(
            """
            INSERT INTO airport (icao, iata, name, country_iso, lat, lon, elevation_ft, is_military, user_added)
            VALUES (?, NULL, ?, ?, ?, ?, NULL, ?, 1)
            ON CONFLICT(icao) DO UPDATE SET
                name=excluded.name, country_iso=excluded.country_iso,
                lat=excluded.lat, lon=excluded.lon, is_military=excluded.is_military
            """,
            (icao.strip().upper(), name, country_iso, lat, lon, 1 if is_military else 0),
        )
