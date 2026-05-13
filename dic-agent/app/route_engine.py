"""Route engine.

Inputs per leg:
  - eobt: datetime UTC (Estimated Off-Block Time)
  - origin / destination: airport ICAO (or coord)
  - route_text: free-form ICAO route, e.g. 'TYE POLTO LAG L433 IBA R778 TEGDA MNA'
  - fl, tas

Outputs:
  - resolved_points: ordered list of (label, lat, lon, source, missing)
  - segments_by_state: list of {'state_iso', 'state_name', 'entry_point', 'entry_time',
                                'exit_point', 'exit_time', 'route_in_country', 'fl', 'tas'}
  - warnings: human-readable list of issues
"""
from __future__ import annotations

import datetime as dt
import json
import math
import re
from dataclasses import dataclass, field
from typing import Iterable

from shapely.geometry import LineString, Point, shape
from shapely.ops import unary_union

from app import db

EARTH_NM = 3440.065  # nautical miles per radian

AIRWAY_RE = re.compile(r"^[A-Z]{1,2}\d{1,4}$")  # G851, UG851, L433, V377…

# Coord pattern, either embedded in a route_text or as a single token after gluing.
COORD_PATTERN = (
    r"([NS])\s*(\d{1,2})\s*[°\s]\s*(\d{1,2})?\s*[\'\s]?\s*(\d{1,2}(?:\.\d+)?)?\s*[\"]?"
    r"\s*/\s*"
    r"([EW])\s*(\d{1,3})\s*[°\s]\s*(\d{1,2})?\s*[\'\s]?\s*(\d{1,2}(?:\.\d+)?)?\s*[\"]?"
)
COORD_RE = re.compile(rf"^{COORD_PATTERN}$", re.IGNORECASE | re.VERBOSE)
COORD_FINDER = re.compile(COORD_PATTERN, re.IGNORECASE)


@dataclass
class ResolvedPoint:
    label: str
    lat: float | None
    lon: float | None
    source: str  # 'airport' | 'waypoint' | 'coord' | 'airway' | 'unknown'
    missing: bool = False
    country_iso: str | None = None


@dataclass
class StateSegment:
    state_iso: str
    state_name: str
    entry_label: str
    entry_time: dt.datetime | None
    exit_label: str
    exit_time: dt.datetime | None
    route_in_country: str
    fl: int | None
    tas: int | None


@dataclass
class LegResolution:
    points: list[ResolvedPoint] = field(default_factory=list)
    segments: list[StateSegment] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    total_distance_nm: float = 0.0
    total_time_min: float = 0.0


def _parse_coord(token: str) -> tuple[float, float] | None:
    m = COORD_RE.match(token.strip().upper())
    if not m:
        return None
    ns, d_lat, m_lat, s_lat, ew, d_lon, m_lon, s_lon = m.groups()
    lat = int(d_lat) + (int(m_lat) / 60 if m_lat else 0) + (float(s_lat) / 3600 if s_lat else 0)
    lon = int(d_lon) + (int(m_lon) / 60 if m_lon else 0) + (float(s_lon) / 3600 if s_lon else 0)
    if ns.upper() == "S":
        lat = -lat
    if ew.upper() == "W":
        lon = -lon
    return lat, lon


def _great_circle_nm(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    lat1, lon1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lon2 = math.radians(p2[0]), math.radians(p2[1])
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2) ** 2
    return 2 * EARTH_NM * math.asin(math.sqrt(a))


def _interp_great_circle(p1: tuple[float, float], p2: tuple[float, float], f: float) -> tuple[float, float]:
    lat1, lon1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lon2 = math.radians(p2[0]), math.radians(p2[1])
    d = _great_circle_nm(p1, p2) / EARTH_NM
    if d == 0:
        return p1
    a = math.sin((1 - f) * d) / math.sin(d)
    b = math.sin(f * d) / math.sin(d)
    x = a * math.cos(lat1) * math.cos(lon1) + b * math.cos(lat2) * math.cos(lon2)
    y = a * math.cos(lat1) * math.sin(lon1) + b * math.cos(lat2) * math.sin(lon2)
    z = a * math.sin(lat1) + b * math.sin(lat2)
    lat = math.atan2(z, math.sqrt(x * x + y * y))
    lon = math.atan2(y, x)
    return math.degrees(lat), math.degrees(lon)


def _resolve_token(
    token: str,
    region_hint: str | None = None,
    near_pt: tuple[float, float] | None = None,
    max_nm_from_near: float = 1500.0,
) -> ResolvedPoint:
    """Resolve a single route token to a geocoded point.

    `near_pt` (lat, lon) is used when a token matches multiple waypoints
    worldwide: we pick the closest one within `max_nm_from_near`. Without
    this filter, an ambiguous ident like 'AD' resolves to some random point
    on the globe.
    """
    token = token.strip().upper()
    if not token:
        return ResolvedPoint(label=token, lat=None, lon=None, source="unknown", missing=True)

    if token in ("DCT", "DIRECT"):
        return ResolvedPoint(label=token, lat=None, lon=None, source="airway")

    coord = _parse_coord(token)
    if coord:
        return ResolvedPoint(label=token, lat=coord[0], lon=coord[1], source="coord")

    if AIRWAY_RE.match(token):
        return ResolvedPoint(label=token, lat=None, lon=None, source="airway")

    # Skip 1- and 2-letter tokens — they are almost always noise from import
    # (single letters left over from airway splits like 'G 851', shorthand
    # like 'AD'). The proximity filter would catch most of them anyway, but
    # rejecting them up front avoids polluting the trace.
    if len(token) < 3:
        return ResolvedPoint(label=token, lat=None, lon=None, source="unknown", missing=True)

    ap = db.find_airport(token)
    if ap:
        return ResolvedPoint(
            label=token, lat=ap["lat"], lon=ap["lon"], source="airport",
            country_iso=ap["country_iso"],
        )

    candidates = db.find_waypoints_all(token)
    if not candidates:
        return ResolvedPoint(label=token, lat=None, lon=None, source="unknown", missing=True)

    # Prefer region hint if it picks a unique match.
    if region_hint:
        regional = [c for c in candidates if c["region"] == region_hint]
        if regional:
            candidates = regional

    if near_pt is not None:
        # Pick the candidate closest to the previous point, within max_nm_from_near.
        scored = sorted(candidates, key=lambda c: _great_circle_nm(near_pt, (c["lat"], c["lon"])))
        best = scored[0]
        d = _great_circle_nm(near_pt, (best["lat"], best["lon"]))
        if d <= max_nm_from_near:
            return ResolvedPoint(
                label=token, lat=best["lat"], lon=best["lon"], source="waypoint",
                country_iso=best["region"] or None,
            )
        return ResolvedPoint(label=token, lat=None, lon=None, source="unknown", missing=True)

    # No proximity hint yet — fall back to first user-added (else first by region).
    best = candidates[0]
    return ResolvedPoint(
        label=token, lat=best["lat"], lon=best["lon"], source="waypoint",
        country_iso=best["region"] or None,
    )


def tokenize_route(route_text: str) -> list[str]:
    """Tokenise a route string.

    Strategy: first extract any lat/lon coordinate substrings (which contain
    spaces and slashes), replace them with single placeholders, split on
    whitespace/dashes/commas, then re-inject the coordinate tokens.
    """
    if not route_text:
        return []
    coords: list[str] = []

    def _stash(m: re.Match) -> str:
        coords.append(m.group(0).strip())
        return f" __COORD_{len(coords) - 1}__ "

    cleaned = COORD_FINDER.sub(_stash, route_text)
    cleaned = cleaned.replace("-", " ").replace(",", " ")
    out: list[str] = []
    for raw in cleaned.split():
        raw = raw.strip()
        if not raw:
            continue
        if raw.startswith("__COORD_") and raw.endswith("__"):
            idx = int(raw[len("__COORD_") : -2])
            out.append(coords[idx].upper())
        else:
            out.append(raw.upper())
    return out


def resolve_route(
    origin_icao: str,
    destination_icao: str,
    route_text: str,
    region_hint: str | None = None,
) -> list[ResolvedPoint]:
    points: list[ResolvedPoint] = []
    origin_pt = _resolve_token(origin_icao, region_hint)
    points.append(origin_pt)

    # Resolve destination first (without proximity) to bracket the corridor.
    dest_pt = _resolve_token(destination_icao, region_hint)

    # near_pt = the last geo-located point we know. Start with origin.
    last_geo: tuple[float, float] | None = (
        (origin_pt.lat, origin_pt.lon)
        if origin_pt.lat is not None and origin_pt.lon is not None
        else None
    )

    for tok in tokenize_route(route_text):
        rp = _resolve_token(tok, region_hint, near_pt=last_geo)
        if rp.source == "airway":
            continue
        points.append(rp)
        if rp.lat is not None and rp.lon is not None:
            last_geo = (rp.lat, rp.lon)

    points.append(dest_pt)
    out: list[ResolvedPoint] = []
    for p in points:
        if out and out[-1].label == p.label and out[-1].source == p.source:
            continue
        out.append(p)
    return out


def _build_country_index() -> list[tuple[str, str, "shape"]]:
    rows = db.list_countries()
    return [(r["iso_a2"], r["name_en"], shape(json.loads(r["geom_geojson"]))) for r in rows]


def _country_at(lat: float, lon: float, idx) -> tuple[str | None, str | None]:
    p = Point(lon, lat)
    for iso, name, geom in idx:
        if geom.contains(p):
            return iso, name
    return None, None


def _sample_country_along(
    p1: tuple[float, float], p2: tuple[float, float], idx, samples: int = 80
) -> list[tuple[float, tuple[float, float], str | None, str | None]]:
    out = []
    for k in range(samples + 1):
        f = k / samples
        lat, lon = _interp_great_circle(p1, p2, f)
        iso, name = _country_at(lat, lon, idx)
        out.append((f, (lat, lon), iso, name))
    return out


def compute_leg(
    eobt: dt.datetime,
    origin_icao: str,
    destination_icao: str,
    route_text: str,
    fl: int | None,
    tas_kt: int | None,
    country_index: list | None = None,
) -> LegResolution:
    res = LegResolution()
    points = resolve_route(origin_icao, destination_icao, route_text)
    res.points = points

    for p in points:
        if p.missing:
            res.warnings.append(f"Point inconnu : '{p.label}' — coller la coordonnée ou l'ajouter en base.")

    coord_points = [p for p in points if p.lat is not None and p.lon is not None]
    if len(coord_points) < 2:
        res.warnings.append("Pas assez de points géolocalisés pour calculer la trace.")
        return res
    if tas_kt is None or tas_kt <= 0:
        res.warnings.append("TAS manquant ou nul — temps de vol non calculé.")
        return res

    cum_nm = [0.0]
    for i in range(1, len(coord_points)):
        cum_nm.append(cum_nm[-1] + _great_circle_nm(
            (coord_points[i - 1].lat, coord_points[i - 1].lon),
            (coord_points[i].lat, coord_points[i].lon),
        ))
    res.total_distance_nm = cum_nm[-1]
    res.total_time_min = (cum_nm[-1] / tas_kt) * 60.0

    if country_index is None:
        country_index = _build_country_index()

    crossings: list[tuple[str, str | None, str | None, dt.datetime, tuple[float, float]]] = []
    cur_iso = None
    cur_name = None
    cur_pt = (coord_points[0].lat, coord_points[0].lon)
    cur_iso, cur_name = _country_at(cur_pt[0], cur_pt[1], country_index)
    crossings.append(("origin", cur_iso, cur_name, eobt, cur_pt))

    for i in range(1, len(coord_points)):
        p1 = (coord_points[i - 1].lat, coord_points[i - 1].lon)
        p2 = (coord_points[i].lat, coord_points[i].lon)
        seg_nm = cum_nm[i] - cum_nm[i - 1]
        if seg_nm < 0.01:
            continue
        samples = _sample_country_along(p1, p2, country_index, samples=60)
        for j in range(1, len(samples)):
            _, _, iso_prev, name_prev = samples[j - 1]
            f_curr, pt_curr, iso_curr, name_curr = samples[j]
            if iso_curr != iso_prev:
                f_event = (samples[j - 1][0] + samples[j][0]) / 2
                dist_nm_at = cum_nm[i - 1] + seg_nm * f_event
                t_event = eobt + dt.timedelta(minutes=(dist_nm_at / tas_kt) * 60.0)
                crossings.append(("border", iso_curr, name_curr, t_event, pt_curr))

    t_end = eobt + dt.timedelta(minutes=res.total_time_min)
    end_pt = (coord_points[-1].lat, coord_points[-1].lon)
    end_iso, end_name = _country_at(end_pt[0], end_pt[1], country_index)
    crossings.append(("destination", end_iso, end_name, t_end, end_pt))

    segments: list[StateSegment] = []
    start_idx = 0
    while start_idx < len(crossings) - 1:
        kind_s, iso_s, name_s, t_s, pt_s = crossings[start_idx]
        end_idx = start_idx + 1
        while end_idx < len(crossings) - 1 and crossings[end_idx][1] == iso_s:
            end_idx += 1
        kind_e, iso_e, name_e, t_e, pt_e = crossings[end_idx]

        entry_label = _label_for_crossing(kind_s, points, coord_points, start_idx == 0)
        exit_label = _label_for_crossing(kind_e, points, coord_points, end_idx == len(crossings) - 1)
        route_in_country = _route_string_in_country(points, iso_s, country_index)
        segments.append(
            StateSegment(
                state_iso=iso_s or "??",
                state_name=name_s or "Unknown",
                entry_label=entry_label,
                entry_time=t_s,
                exit_label=exit_label,
                exit_time=t_e,
                route_in_country=route_in_country,
                fl=fl,
                tas=tas_kt,
            )
        )
        start_idx = end_idx
    res.segments = segments
    return res


def _label_for_crossing(kind: str, points, coord_points, is_first_or_last: bool) -> str:
    if kind == "origin":
        return points[0].label
    if kind == "destination":
        return points[-1].label
    return "BORDER"


def _route_string_in_country(points: list[ResolvedPoint], iso: str | None, idx) -> str:
    if not iso:
        return ""
    labels: list[str] = []
    for p in points:
        if p.lat is None:
            continue
        p_iso, _ = _country_at(p.lat, p.lon, idx)
        if p_iso == iso:
            labels.append(p.label)
    return " - ".join(labels)


def format_zulu(t: dt.datetime, style: str = "FRA") -> str:
    if style == "ICAO":
        return t.strftime("%d %b %y %H%MZ").upper()
    return t.strftime("%d/%m/%Y, %H.%M")
