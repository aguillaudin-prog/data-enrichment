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


def init_schema() -> None:
    with connect() as c:
        c.executescript(SCHEMA)
        _migrate(c)


def upsert_airports(rows: Iterable[dict]) -> int:
    sql = """
    INSERT INTO airport (icao, iata, name, country_iso, lat, lon, elevation_ft, is_military, user_added)
    VALUES (:icao, :iata, :name, :country_iso, :lat, :lon, :elevation_ft, :is_military, :user_added)
    ON CONFLICT(icao) DO UPDATE SET
        iata=excluded.iata, name=excluded.name, country_iso=excluded.country_iso,
        lat=excluded.lat, lon=excluded.lon, elevation_ft=excluded.elevation_ft,
        is_military=excluded.is_military
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
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
    INSERT INTO aircraft_type (icao_designator, full_name, manufacturer, cruise_tas_kt, service_ceiling_ft, range_nm, wake_category)
    VALUES (:icao_designator, :full_name, :manufacturer, :cruise_tas_kt, :service_ceiling_ft, :range_nm, :wake_category)
    ON CONFLICT(icao_designator) DO UPDATE SET
        full_name=excluded.full_name, manufacturer=excluded.manufacturer,
        cruise_tas_kt=excluded.cruise_tas_kt, service_ceiling_ft=excluded.service_ceiling_ft,
        range_nm=excluded.range_nm, wake_category=excluded.wake_category
    """
    n = 0
    with connect() as c:
        for r in rows:
            c.execute(sql, r)
            n += 1
    return n


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
