"""Route suggestion via A* on a graph of NAVAIDs/fixes in a corridor.

Approach (free-data only):
  - Nodes : every waypoint within `corridor_nm` of the great-circle origin→dest line.
  - Edges : each node connected to its K nearest neighbours (Euclidean ≈ great circle
            for typical leg lengths). Edge cost = great-circle distance in NM.
  - Heuristic : great-circle distance from current node to destination.

OpenAIP airspace penalties are added on top (function `inflate_with_airspaces`) when
the user has fetched OpenAIP data for the leg's region. They multiply edges that
cross prohibited/restricted/danger areas at the requested FL.

This is NOT real IFR routing — there is no airway connectivity. The output is a
plausible draft that the user reviews, edits, then validates.
"""
from __future__ import annotations

import heapq
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from shapely.geometry import LineString, Point, shape

from app import db
from app.route_engine import _great_circle_nm, _interp_great_circle

EARTH_NM = 3440.065


@dataclass
class SuggestedRoute:
    origin: str
    destination: str
    waypoints: list[str]
    distance_nm: float
    nodes_explored: int
    warnings: list[str]

    @property
    def route_text(self) -> str:
        # exclude origin/destination from the route string (FPL convention)
        return " ".join(self.waypoints[1:-1]) if len(self.waypoints) > 2 else "DCT"


def _airport_point(icao: str) -> tuple[str, float, float] | None:
    ap = db.find_airport(icao)
    if not ap:
        return None
    return (ap["icao"], ap["lat"], ap["lon"])


def _candidate_waypoints(
    origin_pt: tuple[float, float],
    dest_pt: tuple[float, float],
    corridor_nm: float = 100.0,
    max_candidates: int = 4000,
) -> list[dict]:
    """Return waypoints whose perpendicular distance to the great-circle line
    (approximated linearly) is within `corridor_nm`. Cheap and sufficient for
    legs up to ~2000 NM. For longer flights, we still cap at `max_candidates`
    nearest-to-line points."""
    lat1, lon1 = origin_pt
    lat2, lon2 = dest_pt
    leg_nm = _great_circle_nm(origin_pt, dest_pt)

    # Bounding box around the great circle, with a margin of corridor_nm (≈ 1.6° lat).
    lat_min = min(lat1, lat2) - corridor_nm / 60
    lat_max = max(lat1, lat2) + corridor_nm / 60
    # lon margin scaled by latitude
    cos_lat = math.cos(math.radians((lat_min + lat_max) / 2))
    lon_margin = corridor_nm / 60 / max(cos_lat, 0.1)
    lon_min = min(lon1, lon2) - lon_margin
    lon_max = max(lon1, lon2) + lon_margin

    with db.connect() as c:
        rows = c.execute(
            """
            SELECT ident, region, lat, lon, kind FROM waypoint
            WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
            """,
            (lat_min, lat_max, lon_min, lon_max),
        ).fetchall()

    # Compute perpendicular distance ≈ cross-track distance.
    def _xtrack_nm(p):
        # Great-circle cross-track distance
        lat3, lon3 = p[2], p[3]
        d13 = _great_circle_nm(origin_pt, (lat3, lon3))
        brng13 = _bearing(origin_pt, (lat3, lon3))
        brng12 = _bearing(origin_pt, dest_pt)
        xt = math.asin(math.sin(d13 / EARTH_NM) * math.sin(math.radians(brng13 - brng12))) * EARTH_NM
        return abs(xt)

    scored = []
    for r in rows:
        try:
            xt = _xtrack_nm((r["ident"], r["region"], r["lat"], r["lon"]))
        except (ValueError, ZeroDivisionError):
            continue
        if xt <= corridor_nm:
            scored.append((xt, r))
    scored.sort(key=lambda x: x[0])
    return [dict(r) for _, r in scored[:max_candidates]]


def _bearing(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    """Initial bearing from p1 to p2 in degrees."""
    lat1, lon1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lon2 = math.radians(p2[0]), math.radians(p2[1])
    dlon = lon2 - lon1
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _build_graph(
    origin: tuple[str, float, float],
    destination: tuple[str, float, float],
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
) -> tuple[dict[str, tuple[float, float]], dict[str, list[tuple[str, float]]]]:
    """Return (nodes, adjacency). Nodes keyed by label (ICAO or waypoint ident).

    Edges: each node connected to its k nearest within `corridor_nm/2`. Origin
    and destination are always connected to their k nearest. A direct DCT edge
    origin→destination is always added as a fallback.
    """
    o_label, o_lat, o_lon = origin
    d_label, d_lat, d_lon = destination
    cand = _candidate_waypoints((o_lat, o_lon), (d_lat, d_lon), corridor_nm=corridor_nm)

    nodes: dict[str, tuple[float, float]] = {o_label: (o_lat, o_lon), d_label: (d_lat, d_lon)}
    for c in cand:
        label = c["ident"]
        # Deduplicate (same ident across regions): keep the one closest to track
        if label not in nodes:
            nodes[label] = (c["lat"], c["lon"])

    labels = list(nodes.keys())
    coords = [nodes[l] for l in labels]
    adj: dict[str, list[tuple[str, float]]] = {l: [] for l in labels}

    for i, li in enumerate(labels):
        pi = coords[i]
        # find k nearest others
        dists: list[tuple[float, int]] = []
        for j, lj in enumerate(labels):
            if i == j:
                continue
            d = _great_circle_nm(pi, coords[j])
            dists.append((d, j))
        dists.sort()
        for d, j in dists[:k_neighbours]:
            adj[li].append((labels[j], d))

    # Always include direct origin → destination (DCT) with high cost so A* prefers airways
    direct_d = _great_circle_nm((o_lat, o_lon), (d_lat, d_lon))
    adj[o_label].append((d_label, direct_d * 1.3))

    return nodes, adj


def _astar(
    nodes: dict[str, tuple[float, float]],
    adj: dict[str, list[tuple[str, float]]],
    start: str,
    goal: str,
    edge_cost_fn=None,
) -> tuple[list[str], float, int]:
    h_to_goal = lambda lbl: _great_circle_nm(nodes[lbl], nodes[goal])
    open_heap: list[tuple[float, str]] = []
    heapq.heappush(open_heap, (h_to_goal(start), start))
    came_from: dict[str, str] = {}
    g_score: dict[str, float] = {start: 0.0}
    explored = 0
    while open_heap:
        _, current = heapq.heappop(open_heap)
        explored += 1
        if current == goal:
            path = [current]
            while current in came_from:
                current = came_from[current]
                path.append(current)
            return list(reversed(path)), g_score[goal], explored
        for nbr, base_cost in adj.get(current, []):
            cost = edge_cost_fn(current, nbr, base_cost) if edge_cost_fn else base_cost
            tentative = g_score[current] + cost
            if tentative < g_score.get(nbr, float("inf")):
                came_from[nbr] = current
                g_score[nbr] = tentative
                f = tentative + h_to_goal(nbr)
                heapq.heappush(open_heap, (f, nbr))
    return [], float("inf"), explored


def suggest_route(
    origin_icao: str,
    destination_icao: str,
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
    airspace_penalties: list[dict] | None = None,
    fl: int | None = None,
) -> SuggestedRoute:
    """Suggest a route between two airports.

    airspace_penalties: optional list of dicts of shape:
        {'geom_geojson': '...', 'multiplier': 10.0, 'fl_min': 0, 'fl_max': 999}
    Any edge whose great-circle segment crosses the geometry at the given FL
    band sees its cost multiplied.
    """
    warnings: list[str] = []
    origin = _airport_point(origin_icao)
    if not origin:
        return SuggestedRoute(origin_icao, destination_icao, [], 0.0, 0, [f"Origin '{origin_icao}' not in airport DB."])
    destination = _airport_point(destination_icao)
    if not destination:
        return SuggestedRoute(origin_icao, destination_icao, [], 0.0, 0, [f"Destination '{destination_icao}' not in airport DB."])

    nodes, adj = _build_graph(origin, destination, corridor_nm=corridor_nm, k_neighbours=k_neighbours)
    if len(nodes) < 3:
        warnings.append(f"Très peu de waypoints dans le corridor (±{corridor_nm:.0f} NM). Résultat = DCT.")

    # Build airspace shapes once
    geoms: list[tuple] = []  # (shape, multiplier, fl_min, fl_max)
    if airspace_penalties:
        for sp in airspace_penalties:
            try:
                g = shape(json.loads(sp["geom_geojson"]) if isinstance(sp["geom_geojson"], str) else sp["geom_geojson"])
                geoms.append((g, float(sp.get("multiplier", 5.0)), sp.get("fl_min", 0), sp.get("fl_max", 999)))
            except Exception:
                continue

    def _edge_cost(u: str, v: str, base: float) -> float:
        if not geoms:
            return base
        line = LineString([
            (nodes[u][1], nodes[u][0]),
            (nodes[v][1], nodes[v][0]),
        ])
        mult = 1.0
        for g, m, flmin, flmax in geoms:
            if fl is not None and not (flmin <= fl <= flmax):
                continue
            if line.intersects(g):
                mult *= m
        return base * mult

    path, dist, explored = _astar(nodes, adj, origin_icao, destination_icao, edge_cost_fn=_edge_cost)
    if not path:
        warnings.append("A* sans solution — fallback DCT.")
        path = [origin_icao, destination_icao]
        dist = _great_circle_nm((origin[1], origin[2]), (destination[1], destination[2]))

    return SuggestedRoute(
        origin=origin_icao,
        destination=destination_icao,
        waypoints=path,
        distance_nm=dist,
        nodes_explored=explored,
        warnings=warnings,
    )


def airspace_penalty_rows_for_country(country_iso2: str) -> list[dict]:
    """Load cached OpenAIP airspaces for a country and return rows usable by
    `suggest_route`'s `airspace_penalties` argument.

    Penalty multipliers by airspace type:
      P (prohibited)  → 100
      R (restricted)  →  10
      D (danger)      →   3
      MOA/TRA/TSA     →   5
      others          → ignored
    """
    cache = Path(__file__).resolve().parent.parent / "seeds" / f"openaip_airspaces_{country_iso2.upper()}.json"
    if not cache.exists():
        return []
    spaces = json.loads(cache.read_text(encoding="utf-8"))
    out: list[dict] = []
    type_mult = {"P": 100.0, "R": 10.0, "D": 3.0, "MOA": 5.0, "TRA": 5.0, "TSA": 5.0}
    for s in spaces:
        t = (s.get("type") or "").upper()
        mult = type_mult.get(t)
        if not mult:
            continue
        geom = s.get("geometry")
        if not geom:
            continue
        # FL band from OpenAIP `upperLimit` / `lowerLimit` (when present)
        fl_min = 0
        fl_max = 999
        try:
            up = s.get("upperLimit", {})
            if up.get("unit") == 6:  # FL
                fl_max = int(up.get("value") or fl_max)
            lo = s.get("lowerLimit", {})
            if lo.get("unit") == 6:
                fl_min = int(lo.get("value") or fl_min)
        except Exception:
            pass
        out.append({"geom_geojson": geom, "multiplier": mult, "fl_min": fl_min, "fl_max": fl_max,
                    "name": s.get("name"), "type": t})
    return out
