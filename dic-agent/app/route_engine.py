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
# SID / STAR procedure names: 3-5 letters (exit fix) + 1-2 digits (rev)
# + 1 letter (runway variant). Examples: TRETS8N, BIRGO3D, DORDI6C,
# RLP9E, MENKU1G, MUS5G. Distinct from airways (different shape).
SID_STAR_RE = re.compile(r"^[A-Z]{3,5}\d{1,2}[A-Z]$")

# Coord pattern, either embedded in a route_text or as a single token after gluing.
COORD_PATTERN = (
    r"([NS])\s*(\d{1,2})\s*[°\s]\s*(\d{1,2})?\s*[\'\s]?\s*(\d{1,2}(?:\.\d+)?)?\s*[\"]?"
    r"\s*/\s*"
    r"([EW])\s*(\d{1,3})\s*[°\s]\s*(\d{1,2})?\s*[\'\s]?\s*(\d{1,2}(?:\.\d+)?)?\s*[\"]?"
)
COORD_RE = re.compile(rf"^{COORD_PATTERN}$", re.IGNORECASE | re.VERBOSE)
COORD_FINDER = re.compile(COORD_PATTERN, re.IGNORECASE)

# ICAO compact coordinate format used in flight plans / autorouter output :
#   DDMM[SS]<N|S>DDDMM[SS]<E|W>
# Examples : 4334N00650E (no seconds), 433407N0065031E (with seconds).
ICAO_COORD_RE = re.compile(
    r"^(\d{2})(\d{2})(\d{2})?([NS])"
    r"(\d{3})(\d{2})(\d{2})?([EW])$",
    re.IGNORECASE,
)


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
    t = token.strip().upper()
    m = COORD_RE.match(t)
    if m:
        ns, d_lat, m_lat, s_lat, ew, d_lon, m_lon, s_lon = m.groups()
        lat = int(d_lat) + (int(m_lat) / 60 if m_lat else 0) + (float(s_lat) / 3600 if s_lat else 0)
        lon = int(d_lon) + (int(m_lon) / 60 if m_lon else 0) + (float(s_lon) / 3600 if s_lon else 0)
        if ns.upper() == "S":
            lat = -lat
        if ew.upper() == "W":
            lon = -lon
        return lat, lon
    m = ICAO_COORD_RE.match(t)
    if m:
        d_lat, mi_lat, s_lat, ns, d_lon, mi_lon, s_lon, ew = m.groups()
        lat = int(d_lat) + int(mi_lat) / 60 + (int(s_lat) / 3600 if s_lat else 0)
        lon = int(d_lon) + int(mi_lon) / 60 + (int(s_lon) / 3600 if s_lon else 0)
        if ns == "S":
            lat = -lat
        if ew == "W":
            lon = -lon
        return lat, lon
    return None


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

    # SID / STAR / APPCH procedure : DB authoritative first (le catalog
    # CIFP a 94k+ procédures), puis fallback heuristique sur pattern.
    # Évite les warnings "Point inconnu" injustifiés pour TRETS8N,
    # BIRGO3D, DORDI6C, MENKU1G, RLP9E, etc. + évite les faux positifs
    # sur des waypoints qui ressemblent par hasard à des SID names.
    try:
        if db.find_procedure_by_name(token):
            return ResolvedPoint(label=token, lat=None, lon=None, source="procedure")
    except Exception:
        pass
    if SID_STAR_RE.match(token):
        return ResolvedPoint(label=token, lat=None, lon=None, source="procedure")

    # Skip 1- and 2-letter tokens — they are almost always noise from import
    # (single letters left over from airway splits like 'G 851', shorthand
    # like 'AD'). The proximity filter would catch most of them anyway, but
    # rejecting them up front avoids polluting the trace.
    if len(token) < 3:
        return ResolvedPoint(label=token, lat=None, lon=None, source="unknown", missing=True)

    ap = db.find_airport(token)
    if ap is not None:
        # Apply proximity even to airports. Otherwise a 3-letter IATA token
        # like 'TYE' (which collides with Tyonek, Alaska) short-circuits the
        # waypoint search and pulls the route 7000 NM off-course.
        if near_pt is None or _great_circle_nm(near_pt, (ap["lat"], ap["lon"])) <= max_nm_from_near:
            return ResolvedPoint(
                label=token, lat=ap["lat"], lon=ap["lon"], source="airport",
                country_iso=ap["country_iso"],
            )
        # else: fall through to the waypoint search — maybe the 'real' token
        # is a NAVAID with the same ident, located closer.

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

    ICAO Item 15 supports "<WAYPOINT>/SLLLL" syntax for speed/level
    changes at a fix (e.g. "ELEXI/N0138F080" = pass ELEXI at 138 kt FL080).
    We strip the "/SLLLL" suffix to resolve the base waypoint; the speed/
    level annotation is dropped (caller's responsibility to surface it
    if needed).
    """
    if not route_text:
        return []
    coords: list[str] = []

    def _stash(m: re.Match) -> str:
        coords.append(m.group(0).strip())
        return f" __COORD_{len(coords) - 1}__ "

    cleaned = COORD_FINDER.sub(_stash, route_text)
    cleaned = cleaned.replace("-", " ").replace(",", " ")
    # Pattern ICAO speed/level change : /SLLLL où S∈{N,M,K} + 4 chiffres
    # + L∈{F,S,A,M} + 3 chiffres. Ex: /N0138F080, /M082F360, /K0900A050.
    # On retire l'annotation pour garder le waypoint nu.
    item15_annot_re = re.compile(r"/[NMK]\d{4}[FSAM]\d{3}\b", re.IGNORECASE)
    out: list[str] = []
    for raw in cleaned.split():
        raw = raw.strip()
        if not raw:
            continue
        if raw.startswith("__COORD_") and raw.endswith("__"):
            idx = int(raw[len("__COORD_") : -2])
            out.append(coords[idx].upper())
        else:
            # Strip "/SLLLL" speed/level annotation si présente
            raw = item15_annot_re.sub("", raw)
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

    # Distance-aware proximity threshold. For a 200-NM leg a candidate waypoint
    # must be within ~300 NM of the previous point. For a 2000-NM leg we allow
    # up to ~2600 NM. Eliminates the 'TYE matches a point in Portugal' bug.
    if origin_pt.lat is not None and dest_pt.lat is not None:
        leg_nm = _great_circle_nm(
            (origin_pt.lat, origin_pt.lon), (dest_pt.lat, dest_pt.lon)
        )
        max_nm = max(300.0, leg_nm * 1.3)
    else:
        max_nm = 800.0

    last_geo: tuple[float, float] | None = (
        (origin_pt.lat, origin_pt.lon)
        if origin_pt.lat is not None and origin_pt.lon is not None
        else None
    )

    for tok in tokenize_route(route_text):
        rp = _resolve_token(tok, region_hint, near_pt=last_geo, max_nm_from_near=max_nm)
        # Tokens sans coord (airway nom ou SID/STAR procedure name) ne
        # comptent pas dans le chemin géographique. On les ignore pour
        # le calcul mais l'affichage les conserve via tokenize_route.
        if rp.source in ("airway", "procedure"):
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
    """ISO2 → display name → polygon, sourced from Natural Earth.
    Prefers the French name (CÔTE D'IVOIRE, GUINÉE) when available, since
    reference DICs target French defence attachés. Falls back to English."""
    rows = db.list_countries()
    return [(r["iso_a2"], r["name_fr"] or r["name_en"], shape(json.loads(r["geom_geojson"]))) for r in rows]


def _country_at(lat: float, lon: float, idx) -> tuple[str | None, str | None]:
    p = Point(lon, lat)
    for iso, name, geom in idx:
        if geom.contains(p):
            return iso, name
    return None, None


def _country_for_point(rp, idx) -> tuple[str | None, str | None]:
    """Prefer the ResolvedPoint's own country_iso (set by db.find_airport /
    find_waypoints_all) over a polygon lookup. Natural Earth polygons can
    have edge gaps (notably along complex coastlines), and the airport
    DB's ISO is authoritative for published airports.

    Falls back to polygon lookup when the ResolvedPoint has no country_iso
    (e.g. inline coordinates emitted by autorouter)."""
    iso = getattr(rp, "country_iso", None)
    if iso:
        for ci_iso, ci_name, _ in idx:
            if ci_iso == iso:
                return iso, ci_name
        # ISO known but not in the Natural Earth index — surface it anyway,
        # using the ISO as display name (better than Unknown).
        return iso, iso
    return _country_at(rp.lat, rp.lon, idx)


def _fill_oceanic_samples(
    samples: list[tuple[float, tuple[float, float], str | None, str | None]],
) -> list[tuple[float, tuple[float, float], str | None, str | None]]:
    """Replace 'no country' samples (over water) with the nearest land
    neighbour along the route, in either direction.

    Natural Earth polygons cover land only. When the great-circle dips
    over water (gulf, sea, ocean), `_country_at` returns (None, None).
    For diplomatic-clearance purposes that's useless ('Unknown' country
    in the DIC). Operationally, oceanic FIRs are extensions of coastal
    countries' airspace, so attributing offshore samples to the closest
    coastal country is a reasonable proxy.

    Strategy: for each unknown sample, find the nearest known sample
    forward and backward in the list. Whichever is closer (in number of
    sample-steps along the route) wins. For a Ghana → ocean → Côte
    d'Ivoire crossing, oceanic samples split at the midpoint.
    """
    n = len(samples)
    if n == 0:
        return samples
    # Index of the nearest known sample going FORWARD from each position
    next_known: list[int | None] = [None] * n
    nxt = None
    for i in range(n - 1, -1, -1):
        if samples[i][2] is not None:
            nxt = i
        next_known[i] = nxt
    prev_known: list[int | None] = [None] * n
    prv = None
    for i in range(n):
        if samples[i][2] is not None:
            prv = i
        prev_known[i] = prv
    out: list[tuple[float, tuple[float, float], str | None, str | None]] = []
    for i, (f, pt, iso, name) in enumerate(samples):
        if iso is not None:
            out.append((f, pt, iso, name))
            continue
        forward_idx = next_known[i]
        backward_idx = prev_known[i]
        forward_dist = (forward_idx - i) if forward_idx is not None else float("inf")
        backward_dist = (i - backward_idx) if backward_idx is not None else float("inf")
        if backward_dist <= forward_dist and backward_idx is not None:
            chosen = samples[backward_idx]
        elif forward_idx is not None:
            chosen = samples[forward_idx]
        else:
            out.append((f, pt, iso, name))
            continue
        out.append((f, pt, chosen[2], chosen[3]))
    return out


def _sample_country_along(
    p1: tuple[float, float], p2: tuple[float, float], idx, samples: int = 80
) -> list[tuple[float, tuple[float, float], str | None, str | None]]:
    out = []
    for k in range(samples + 1):
        f = k / samples
        lat, lon = _interp_great_circle(p1, p2, f)
        iso, name = _country_at(lat, lon, idx)
        out.append((f, (lat, lon), iso, name))
    return _fill_oceanic_samples(out)


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
    cur_iso, cur_name = _country_for_point(coord_points[0], country_index)
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
    end_iso, end_name = _country_for_point(coord_points[-1], country_index)
    crossings.append(("destination", end_iso, end_name, t_end, end_pt))

    segments: list[StateSegment] = []
    start_idx = 0
    while start_idx < len(crossings) - 1:
        kind_s, iso_s, name_s, t_s, pt_s = crossings[start_idx]
        end_idx = start_idx + 1
        while end_idx < len(crossings) - 1 and crossings[end_idx][1] == iso_s:
            end_idx += 1
        kind_e, iso_e, name_e, t_e, pt_e = crossings[end_idx]

        entry_label = _label_for_crossing(
            kind_s, points, coord_points, start_idx == 0,
            crossing_point=pt_s,
        )
        exit_label = _label_for_crossing(
            kind_e, points, coord_points, end_idx == len(crossings) - 1,
            crossing_point=pt_e,
        )
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

    # Defensive: ensure the destination country is always represented as a
    # segment. The main loop labels the last segment with whatever country
    # was last detected by the sampler — when the sampler misses a thin
    # coastal country between an ocean stretch and the destination (e.g. a
    # great-circle DBBB→DIAP returning Ghana as the last segment exiting
    # at DIAP, when DIAP is in Côte d'Ivoire), the destination's country is
    # silently dropped. Here we split the last segment in two so the
    # destination's country surfaces in the DIC.
    if segments and end_iso and segments[-1].state_iso != end_iso:
        last = segments[-1]
        # Heuristic split: 70/30 of the last segment in favour of the
        # previously detected country. Better than 50/50 because the route
        # was probably in that country for most of the segment before
        # entering the destination's airspace.
        delta = last.exit_time - last.entry_time
        split_time = last.entry_time + delta * 0.7
        last.exit_time = split_time
        last.exit_label = f"EXIT {(last.state_name or '').upper()}"
        segments.append(
            StateSegment(
                state_iso=end_iso,
                state_name=end_name or end_iso,
                entry_label=f"ENTRY {(end_name or '').upper()}",
                entry_time=split_time,
                exit_label=_label_for_crossing("destination", points, coord_points, True),
                exit_time=t_end,
                route_in_country=_route_string_in_country(points, end_iso, country_index) or "DCT",
                fl=fl,
                tas=tas_kt,
            )
        )

    res.segments = segments
    return res


def _label_for_crossing(
    kind: str, points, coord_points, is_first_or_last: bool,
    crossing_point: tuple[float, float] | None = None,
) -> str:
    """Label de l'entrée/sortie d'un pays.

    - Origin / destination : on prend le label de l'aéroport (1er ou
      dernier point de la route).
    - Border crossing intermédiaire : avant on retournait "BORDER" en dur,
      ce qui posait problème aux autorités destinataires (cf. retour
      collègue : ils attendent POLTO pour la frontière BJ/NG, etc.).
      Maintenant on prend le **waypoint nommé géographiquement le plus
      proche** du point de crossing — applique génériquement à toute
      frontière, quelle qu'elle soit.
    """
    if kind == "origin":
        return points[0].label
    if kind == "destination":
        return points[-1].label
    if crossing_point and coord_points:
        lat0, lon0 = crossing_point
        nearest = min(
            coord_points,
            key=lambda p: (p.lat - lat0) ** 2 + (p.lon - lon0) ** 2,
        )
        # Sanity check : si le waypoint nommé le plus proche est à plus
        # de 50 NM du crossing géographique, on retombe sur "BORDER" —
        # le waypoint est probablement du mauvais côté ou trop loin pour
        # représenter réellement la frontière (route DCT sans fix proche,
        # détour qui s'éloigne du grand-cercle, etc.).
        dist_nm = _great_circle_nm((nearest.lat, nearest.lon), (lat0, lon0))
        if dist_nm <= 50:
            return nearest.label
    return "BORDER"


def _route_string_in_country(points: list[ResolvedPoint], iso: str | None, idx) -> str:
    """Waypoints + airways traversant le pays `iso`, dans l'ordre du plan
    de vol, joints en 'A - B - R778 - C'.

    Avant : on filtrait tous les tokens sans coordonnées (= les airways
    R778, W951, L433, etc.) → la cellule (41) du DIC ne montrait que les
    waypoints, perdant la lisibilité ATC. Retour collègue : il faut
    *garder* les noms d'airways entre les waypoints pour que la route
    soit auto-vérifiable.

    Règle : un airway est inclus si SES DEUX waypoints voisins (le plus
    proche avant et le plus proche après dans la liste) sont dans le même
    pays `iso`. Sinon on l'omet (airway qui traverse la frontière n'a pas
    sa place dans la cellule d'un pays unique).

    Fallback 'DCT' inchangé pour les pays traversés sans waypoint
    (great circle qui clippe juste un coin de territoire).
    """
    if not iso:
        return ""
    # Classifier chaque point : (point, pays_iso_ou_None)
    classified: list[tuple[ResolvedPoint, str | None]] = []
    for p in points:
        if p.lat is None:
            classified.append((p, None))  # airway : à résoudre via voisins
        else:
            p_iso, _ = _country_at(p.lat, p.lon, idx)
            classified.append((p, p_iso))

    out: list[str] = []
    n = len(classified)
    for i, (p, c) in enumerate(classified):
        if c == iso:
            out.append(p.label)
            continue
        if p.lat is None:
            # Airway : check le pays du waypoint précédent ET suivant
            prev_c = next(
                (classified[j][1] for j in range(i - 1, -1, -1)
                 if classified[j][1] is not None),
                None,
            )
            next_c = next(
                (classified[j][1] for j in range(i + 1, n)
                 if classified[j][1] is not None),
                None,
            )
            if prev_c == iso and next_c == iso:
                out.append(p.label)
    return " - ".join(out) if out else "DCT"


def format_zulu(t: dt.datetime, style: str = "FRA") -> str:
    # Reference DIC samples use 'DD/MM/YYYY HHhMMZ' (e.g. '04/05/2026 03H00Z').
    # The legacy 'DD/MM/YYYY, HH.MM' format is gone — single style now.
    return t.strftime("%d/%m/%Y %HH%MZ")
