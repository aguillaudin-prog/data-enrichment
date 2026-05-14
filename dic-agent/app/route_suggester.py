"""Route suggestion via A*.

Two strategies, picked automatically:

1. **Airway-aware** (when `airway_segment` table is populated, typically after
   `python -m app.import_xplane_navdata`). The graph is built from real
   airway segments; A* picks the shortest sequence of named airways
   between origin and destination. Output looks like
   `EBUSO UA601 ARABA UA602 ABC` — direct-deposit-able in an FPL.

2. **Corridor DCT fallback** (no airway data). The graph is built from
   NAVAIDs along the great-circle corridor, connected by k-nearest
   neighbours. Output is `WPT1 DCT WPT2 DCT WPT3` — unambiguous for IFR
   parsers, but only a starting point for an OPS officer.

OpenAIP airspace penalties (P/R/D zones at the requested FL) are applied
on top of either strategy, as long as the airspaces were cached via
`python -m app.openaip_world` or `python -m app.openaip_client <ISO>`.
"""
from __future__ import annotations

import heapq
import json
import math
from dataclasses import dataclass, field
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
    waypoints: list[str]                     # full path: origin, ..., destination
    edge_labels: list[str] = field(default_factory=list)  # len = len(waypoints) - 1
    distance_nm: float = 0.0
    nodes_explored: int = 0
    strategy: str = "dct"                    # 'airway' or 'dct'
    warnings: list[str] = field(default_factory=list)

    @property
    def route_text(self) -> str:
        """Render FPL field 15 ROUTE in compact ICAO Doc 4444 format.

        - Drops origin/destination ICAOs (they live in field 13/16).
        - Collapses consecutive segments on the same airway to just
          `<entry> <airway> <exit>`. Intermediate fixes on the same airway
          are implicit, as required by IFPS.
        - Strips stray leading/trailing 'DCT' tokens (implicit from the
          aerodrome).

        Examples:
          [DBBB, EBUSO, ARABA, ABC, DNAA] + edges [DCT, UA601, UA602, DCT]
            → 'EBUSO UA601 ARABA UA602 ABC'
          [LFMD, ADUDU, LERMA, EPOLO, MAMES, BISBA, BGR, LEIB] + edges
            [DCT, N86, N86, N86, M984, M984, DCT]
            → 'ADUDU N86 MAMES M984 BGR'
        """
        inner = self.waypoints[1:-1]
        if not inner:
            return "DCT"
        joins = self.edge_labels[1:-1] if len(self.edge_labels) >= 2 else []
        parts: list[str] = [inner[0]]
        i = 0
        while i < len(joins):
            edge = joins[i] or "DCT"
            if edge != "DCT":
                j = i
                while j + 1 < len(joins) and joins[j + 1] == edge:
                    j += 1
                parts.append(edge)
                parts.append(inner[j + 1])
                i = j + 1
            else:
                parts.append("DCT")
                parts.append(inner[i + 1])
                i += 1
        while len(parts) > 1 and parts[0] == "DCT":
            parts.pop(0)
        while len(parts) > 1 and parts[-1] == "DCT":
            parts.pop()
        return " ".join(parts) if parts else "DCT"


def _airport_point(icao: str) -> tuple[str, float, float] | None:
    ap = db.find_airport(icao)
    if not ap:
        return None
    return (ap["icao"], ap["lat"], ap["lon"])


# ─── Strategy 1 : airway-aware A* ──────────────────────────────────────────────

def _build_airway_graph(
    origin: tuple[float, float],
    destination: tuple[float, float],
    bbox_margin_nm: float = 200.0,
    fl: int | None = None,
) -> tuple[dict[str, tuple[float, float]], dict[str, list[tuple[str, float, str]]]]:
    """Pull every airway segment in the lat/lon bounding box of the great
    circle and build a graph: node = waypoint ident, edge = airway segment.

    Returns (nodes, adj). adj[u] = [(v, distance_nm, airway_name), …].
    """
    lat_min = min(origin[0], destination[0]) - bbox_margin_nm / 60
    lat_max = max(origin[0], destination[0]) + bbox_margin_nm / 60
    cos_lat = math.cos(math.radians((lat_min + lat_max) / 2))
    lon_margin = bbox_margin_nm / 60 / max(cos_lat, 0.1)
    lon_min = min(origin[1], destination[1]) - lon_margin
    lon_max = max(origin[1], destination[1]) + lon_margin

    segs = db.airway_segments_in_bbox(lat_min, lat_max, lon_min, lon_max)

    nodes: dict[str, tuple[float, float]] = {}
    adj: dict[str, list[tuple[str, float, str]]] = {}
    for s in segs:
        # Respect altitude band if the leg's FL is known.
        if fl is not None and s["fl_min"] is not None and s["fl_max"] is not None:
            if not (s["fl_min"] <= fl <= s["fl_max"]):
                continue
        u, v = s["from_ident"], s["to_ident"]
        nodes.setdefault(u, (s["from_lat"], s["from_lon"]))
        nodes.setdefault(v, (s["to_lat"], s["to_lon"]))
        d = _great_circle_nm(nodes[u], nodes[v])
        awy = s["airway_name"]
        adj.setdefault(u, []).append((v, d, awy))
        if s["direction"] == 1:
            adj.setdefault(v, []).append((u, d, awy))
    return nodes, adj


def _connect_airport_to_graph(
    label: str,
    pt: tuple[float, float],
    nodes: dict[str, tuple[float, float]],
    adj: dict[str, list[tuple[str, float, str]]],
    k: int = 5,
    max_link_nm: float = 200.0,
) -> None:
    """Add the airport as a node, link it via DCT to its k nearest neighbours
    in the airway graph."""
    if not nodes:
        nodes[label] = pt
        adj[label] = []
        return
    nodes[label] = pt
    dists: list[tuple[float, str]] = []
    for n, p in nodes.items():
        if n == label:
            continue
        d = _great_circle_nm(pt, p)
        if d <= max_link_nm:
            dists.append((d, n))
    dists.sort()
    adj[label] = []
    for d, n in dists[:k]:
        adj[label].append((n, d * 1.2, "DCT"))  # +20% to prefer real airways
        adj.setdefault(n, []).append((label, d * 1.2, "DCT"))


def _astar_labeled(
    nodes: dict[str, tuple[float, float]],
    adj: dict[str, list[tuple[str, float, str]]],
    start: str,
    goal: str,
    edge_cost_fn=None,
) -> tuple[list[str], list[str], float, int]:
    """A* that also returns the airway label used on each step."""
    h = lambda lbl: _great_circle_nm(nodes[lbl], nodes[goal])
    open_heap: list[tuple[float, str]] = []
    heapq.heappush(open_heap, (h(start), start))
    came_from: dict[str, tuple[str, str]] = {}
    g: dict[str, float] = {start: 0.0}
    explored = 0
    while open_heap:
        _, cur = heapq.heappop(open_heap)
        explored += 1
        if cur == goal:
            path = [cur]
            edges: list[str] = []
            while cur in came_from:
                prev, label = came_from[cur]
                path.append(prev)
                edges.append(label)
                cur = prev
            return list(reversed(path)), list(reversed(edges)), g[goal], explored
        for nbr, base_cost, awy in adj.get(cur, []):
            cost = edge_cost_fn(cur, nbr, base_cost) if edge_cost_fn else base_cost
            tentative = g[cur] + cost
            if tentative < g.get(nbr, float("inf")):
                came_from[nbr] = (cur, awy)
                g[nbr] = tentative
                heapq.heappush(open_heap, (tentative + h(nbr), nbr))
    return [], [], float("inf"), explored


# ─── Strategy 2 : corridor DCT fallback (unchanged shape, kept as fallback) ─────

AIRWAY_RE = None  # not needed here, route_engine has it


def _bearing(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    lat1, lon1 = math.radians(p1[0]), math.radians(p1[1])
    lat2, lon2 = math.radians(p2[0]), math.radians(p2[1])
    dlon = lon2 - lon1
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def _candidate_waypoints_in_corridor(
    origin_pt: tuple[float, float], dest_pt: tuple[float, float],
    corridor_nm: float = 100.0, max_candidates: int = 4000,
) -> list[dict]:
    lat1, lon1 = origin_pt
    lat2, lon2 = dest_pt
    lat_min = min(lat1, lat2) - corridor_nm / 60
    lat_max = max(lat1, lat2) + corridor_nm / 60
    cos_lat = math.cos(math.radians((lat_min + lat_max) / 2))
    lon_margin = corridor_nm / 60 / max(cos_lat, 0.1)
    lon_min = min(lon1, lon2) - lon_margin
    lon_max = max(lon1, lon2) + lon_margin
    with db.connect() as c:
        rows = c.execute(
            "SELECT ident, region, lat, lon, kind FROM waypoint "
            "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
            (lat_min, lat_max, lon_min, lon_max),
        ).fetchall()

    def _xt(p):
        lat3, lon3 = p["lat"], p["lon"]
        d13 = _great_circle_nm(origin_pt, (lat3, lon3))
        b13 = _bearing(origin_pt, (lat3, lon3))
        b12 = _bearing(origin_pt, dest_pt)
        try:
            return abs(math.asin(math.sin(d13 / EARTH_NM) * math.sin(math.radians(b13 - b12))) * EARTH_NM)
        except ValueError:
            return float("inf")

    scored = [(_xt(r), r) for r in rows]
    scored = [(d, r) for d, r in scored if d <= corridor_nm and len(r["ident"]) >= 3]
    scored.sort(key=lambda x: x[0])
    return [dict(r) for _, r in scored[:max_candidates]]


def _build_corridor_graph(origin, destination, corridor_nm: float, k: int):
    o_label, o_lat, o_lon = origin
    d_label, d_lat, d_lon = destination
    cand = _candidate_waypoints_in_corridor((o_lat, o_lon), (d_lat, d_lon), corridor_nm=corridor_nm)
    nodes = {o_label: (o_lat, o_lon), d_label: (d_lat, d_lon)}
    for c in cand:
        nodes.setdefault(c["ident"], (c["lat"], c["lon"]))
    labels = list(nodes.keys())
    coords = [nodes[l] for l in labels]
    adj: dict[str, list[tuple[str, float, str]]] = {l: [] for l in labels}
    for i, li in enumerate(labels):
        dists = sorted([(_great_circle_nm(coords[i], coords[j]), j) for j in range(len(labels)) if i != j])
        for d, j in dists[:k]:
            adj[li].append((labels[j], d, "DCT"))
    adj[o_label].append((d_label, _great_circle_nm((o_lat, o_lon), (d_lat, d_lon)) * 1.3, "DCT"))
    return nodes, adj


# ─── Public entry point ────────────────────────────────────────────────────────

def suggest_route(
    origin_icao: str,
    destination_icao: str,
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
    airspace_penalties: list[dict] | None = None,
    fl: int | None = None,
) -> SuggestedRoute:
    warnings: list[str] = []
    origin = _airport_point(origin_icao)
    if not origin:
        return SuggestedRoute(origin_icao, destination_icao, [], [], 0.0, 0, "dct",
                              [f"Origin '{origin_icao}' not in airport DB."])
    destination = _airport_point(destination_icao)
    if not destination:
        return SuggestedRoute(origin_icao, destination_icao, [], [], 0.0, 0, "dct",
                              [f"Destination '{destination_icao}' not in airport DB."])
    o_pt = (origin[1], origin[2])
    d_pt = (destination[1], destination[2])

    geoms: list[tuple] = []
    if airspace_penalties:
        for sp in airspace_penalties:
            try:
                g = shape(json.loads(sp["geom_geojson"]) if isinstance(sp["geom_geojson"], str) else sp["geom_geojson"])
                geoms.append((g, float(sp.get("multiplier", 5.0)), sp.get("fl_min", 0), sp.get("fl_max", 999)))
            except Exception:
                continue

    def _edge_cost(u: str, v: str, base: float, nodes: dict) -> float:
        if not geoms:
            return base
        line = LineString([(nodes[u][1], nodes[u][0]), (nodes[v][1], nodes[v][0])])
        mult = 1.0
        for g, m, flmin, flmax in geoms:
            if fl is not None and not (flmin <= fl <= flmax):
                continue
            if line.intersects(g):
                mult *= m
        return base * mult

    has_airways = db.count_airway_segments() > 0
    if has_airways:
        nodes, adj = _build_airway_graph(o_pt, d_pt, bbox_margin_nm=max(150, corridor_nm * 1.5), fl=fl)
        _connect_airport_to_graph(origin_icao, o_pt, nodes, adj, k=k_neighbours)
        _connect_airport_to_graph(destination_icao, d_pt, nodes, adj, k=k_neighbours)
        path, edges, dist, explored = _astar_labeled(
            nodes, adj, origin_icao, destination_icao,
            edge_cost_fn=lambda u, v, b: _edge_cost(u, v, b, nodes),
        )
        strategy = "airway"
    else:
        warnings.append("Pas de base d'airways IFR (lance `python -m app.import_xplane_navdata`). Fallback DCT.")
        nodes, adj = _build_corridor_graph(origin, destination, corridor_nm, k_neighbours)
        path, edges, dist, explored = _astar_labeled(
            nodes, adj, origin_icao, destination_icao,
            edge_cost_fn=lambda u, v, b: _edge_cost(u, v, b, nodes),
        )
        strategy = "dct"

    if not path:
        warnings.append("A* sans solution — fallback DCT direct.")
        path = [origin_icao, destination_icao]
        edges = ["DCT"]
        dist = _great_circle_nm(o_pt, d_pt)

    # Cleanup: drop intermediate waypoints collocated with endpoints, with
    # 1-2 letter idents (airway-name collision), or too close to the previous
    # kept point. Rebuild edges consistently from the original A* labels:
    # an edge linking two NON-adjacent kept positions is downgraded to DCT.
    if len(path) > 2:
        keep_inner: list[int] = []
        for i in range(1, len(path) - 1):
            label = path[i]
            p = nodes.get(label)
            if p is None:
                continue
            if len(label) < 3:
                continue
            if _great_circle_nm(p, o_pt) < 15.0 or _great_circle_nm(p, d_pt) < 15.0:
                continue
            last_kept = keep_inner[-1] if keep_inner else 0  # 0 = origin
            last_p = nodes.get(path[last_kept])
            if last_p and _great_circle_nm(p, last_p) < 10.0:
                continue
            keep_inner.append(i)

        positions = [0] + keep_inner + [len(path) - 1]
        new_path = [path[i] for i in positions]
        new_edges: list[str] = []
        for k in range(len(positions) - 1):
            a, b = positions[k], positions[k + 1]
            if b == a + 1:
                new_edges.append(edges[a] if a < len(edges) else "DCT")
            else:
                new_edges.append("DCT")  # skipped intermediates → unknown link
        path, edges = new_path, new_edges

    return SuggestedRoute(
        origin=origin_icao, destination=destination_icao,
        waypoints=path, edge_labels=edges,
        distance_nm=dist, nodes_explored=explored,
        strategy=strategy, warnings=warnings,
    )


def airspace_penalty_rows_for_country(country_iso2: str) -> list[dict]:
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
        fl_min = 0
        fl_max = 999
        try:
            up = s.get("upperLimit", {})
            if up.get("unit") == 6:
                fl_max = int(up.get("value") or fl_max)
            lo = s.get("lowerLimit", {})
            if lo.get("unit") == 6:
                fl_min = int(lo.get("value") or fl_min)
        except Exception:
            pass
        out.append({"geom_geojson": geom, "multiplier": mult, "fl_min": fl_min, "fl_max": fl_max,
                    "name": s.get("name"), "type": t})
    return out
