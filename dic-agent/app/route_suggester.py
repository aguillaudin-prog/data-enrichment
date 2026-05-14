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

def _suggest_between_labeled_points(
    o_label: str, o_pt: tuple[float, float],
    d_label: str, d_pt: tuple[float, float],
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
    airspace_penalties: list[dict] | None = None,
    fl: int | None = None,
) -> SuggestedRoute:
    """A* + cleanup between two labeled (lat, lon) points.

    Shared backend for `suggest_route` (airport-to-airport) and
    `suggest_with_procedures` (SID-exit-to-STAR-entry re-route).
    """
    warnings: list[str] = []

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
        _connect_airport_to_graph(o_label, o_pt, nodes, adj, k=k_neighbours)
        _connect_airport_to_graph(d_label, d_pt, nodes, adj, k=k_neighbours)
        path, edges, dist, explored = _astar_labeled(
            nodes, adj, o_label, d_label,
            edge_cost_fn=lambda u, v, b: _edge_cost(u, v, b, nodes),
        )
        strategy = "airway"
    else:
        warnings.append("Pas de base d'airways IFR (lance `python -m app.import_xplane_navdata`). Fallback DCT.")
        nodes, adj = _build_corridor_graph(
            (o_label, o_pt[0], o_pt[1]), (d_label, d_pt[0], d_pt[1]),
            corridor_nm, k_neighbours,
        )
        path, edges, dist, explored = _astar_labeled(
            nodes, adj, o_label, d_label,
            edge_cost_fn=lambda u, v, b: _edge_cost(u, v, b, nodes),
        )
        strategy = "dct"

    if not path:
        warnings.append("A* sans solution — fallback DCT direct.")
        path = [o_label, d_label]
        edges = ["DCT"]
        dist = _great_circle_nm(o_pt, d_pt)

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
            last_kept = keep_inner[-1] if keep_inner else 0
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
                new_edges.append("DCT")
        path, edges = new_path, new_edges

    return SuggestedRoute(
        origin=o_label, destination=d_label,
        waypoints=path, edge_labels=edges,
        distance_nm=dist, nodes_explored=explored,
        strategy=strategy, warnings=warnings,
    )


def suggest_route(
    origin_icao: str,
    destination_icao: str,
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
    airspace_penalties: list[dict] | None = None,
    fl: int | None = None,
) -> SuggestedRoute:
    origin = _airport_point(origin_icao)
    if not origin:
        return SuggestedRoute(origin_icao, destination_icao, [], [], 0.0, 0, "dct",
                              [f"Origin '{origin_icao}' not in airport DB."])
    destination = _airport_point(destination_icao)
    if not destination:
        return SuggestedRoute(origin_icao, destination_icao, [], [], 0.0, 0, "dct",
                              [f"Destination '{destination_icao}' not in airport DB."])
    return _suggest_between_labeled_points(
        origin_icao, (origin[1], origin[2]),
        destination_icao, (destination[1], destination[2]),
        corridor_nm=corridor_nm, k_neighbours=k_neighbours,
        airspace_penalties=airspace_penalties, fl=fl,
    )


def suggest_with_procedures(
    origin_icao: str,
    destination_icao: str,
    fl: int | None = None,
    min_runway_ft: int | None = None,
    corridor_nm: float = 100.0,
    k_neighbours: int = 8,
    airspace_penalties: list[dict] | None = None,
) -> tuple[SuggestedRoute, dict | None, dict | None]:
    """Full IFR route with auto-picked SID/STAR that chain correctly.

    The naive approach (run A* airport-to-airport, then auto-pick SIDs/STARs
    that *contain* the first/last enroute fix) produces routes like
    ``MAMES6N DIVKO …`` where RocketRoute rejects the SID/transition combo:
    MAMES6N's published exit is MAMES, not DIVKO.

    This function does it correctly:

      1. Run airport-to-airport A* to get an approximate enroute direction
         and identify the first/last natural enroute fix.
      2. Pick the best SID at origin and STAR at destination.
      3. **Re-run A* from SID exit fix to STAR entry fix** (or to airport
         if no SID/STAR was picked). This produces an enroute that joins
         the SID's published exit and the STAR's published entry, which is
         the format RocketRoute / IFPS expect.

    Returns (SuggestedRoute, sid_dict|None, star_dict|None). The
    SuggestedRoute's route_text now drops the SID exit and STAR entry
    fixes (implicit from the SID/STAR names), so callers should assemble
    the final FPL route as ``<SID_name> <route_text> <STAR_name>``.
    """
    sug = suggest_route(
        origin_icao, destination_icao, corridor_nm=corridor_nm,
        k_neighbours=k_neighbours, airspace_penalties=airspace_penalties, fl=fl,
    )
    if not sug.waypoints or sug.distance_nm == 0:
        return sug, None, None

    enroute_tokens = [t for t in sug.route_text.split() if t and t != "DCT"]
    first_fix = enroute_tokens[0] if enroute_tokens else None
    last_fix = enroute_tokens[-1] if enroute_tokens else None
    sid = pick_procedure(origin_icao, first_fix, "SID", min_runway_ft=min_runway_ft)
    star = pick_procedure(destination_icao, last_fix, "STAR", min_runway_ft=min_runway_ft)

    o = _airport_point(origin_icao)
    d = _airport_point(destination_icao)
    new_o_label = origin_icao
    new_o_pt = (o[1], o[2])
    new_d_label = destination_icao
    new_d_pt = (d[1], d[2])

    rerouted = False
    if sid:
        wp = db.find_waypoint(sid["connecting_fix"])
        if wp:
            new_o_label = sid["connecting_fix"]
            new_o_pt = (wp["lat"], wp["lon"])
            rerouted = True
    if star:
        wp = db.find_waypoint(star["connecting_fix"])
        if wp:
            new_d_label = star["connecting_fix"]
            new_d_pt = (wp["lat"], wp["lon"])
            rerouted = True

    if rerouted:
        sug2 = _suggest_between_labeled_points(
            new_o_label, new_o_pt, new_d_label, new_d_pt,
            corridor_nm=corridor_nm, k_neighbours=k_neighbours,
            airspace_penalties=airspace_penalties, fl=fl,
        )
        if sug2.waypoints and sug2.distance_nm > 0:
            sug = sug2

    return sug, sid, star


def _expand_truncated_proc_name(proc_name: str, waypoints: list[str]) -> str:
    """Reverse the ARINC 424 6-char truncation of a procedure name.

    ARINC 424 / CIFP cap procedure names at 6 characters. When the
    underlying fix is 5+ letters (e.g. MAMES → 'MAMES6N' = 7 chars),
    CIFP truncates to 6 ('MAME6N'). Real-world AIP / Eurocontrol / OPS
    tools (RocketRoute, Lido, Foreflight) use the *full* name from the
    AIP, so submitting the truncated form fails IFPS lookup.

    Heuristic: if the proc_name is exactly 6 chars of pattern
    ``<4 letters><digit><letter>``, look at the procedure's own
    waypoints for a fix starting with those 4 letters and longer than
    4 — that's the truncated fix. Splice it back in.

    Conservative: pure 4-letter fixes (BADO, RUBI, MUS, AGN…) where
    the name is already canonical are left untouched.
    """
    if len(proc_name) != 6:
        return proc_name
    if not (proc_name[:4].isalpha() and proc_name[4].isdigit() and proc_name[5].isalpha()):
        return proc_name
    prefix = proc_name[:4]
    suffix = proc_name[4:]
    for wp in waypoints:
        wp_u = (wp or "").upper()
        if wp_u.startswith(prefix) and 4 < len(wp_u) <= 5:
            return wp_u + suffix
    return proc_name


def _runways_compatible(
    airport_icao: str, runways_csv: str | None, min_runway_ft: int | None,
) -> tuple[list[str], list[str]]:
    """Split a procedure's runway list into (compatible, too_short) wrt the
    aircraft's min_runway_ft. Runways unknown in the DB are kept as compatible
    (conservative — better surface the SID than hide it on missing data).
    """
    runways = [r.strip().upper() for r in (runways_csv or "").split(",") if r.strip()]
    if not runways or not min_runway_ft:
        return runways, []
    ok: list[str] = []
    short: list[str] = []
    for rwy in runways:
        length = db.runway_length_ft(airport_icao, rwy)
        if length is None or length >= min_runway_ft:
            ok.append(rwy)
        else:
            short.append(rwy)
    return ok, short


def pick_procedure(
    airport_icao: str,
    connecting_fix: str | None,
    proc_type: str,
    min_runway_ft: int | None = None,
) -> dict | None:
    """Pick the best SID or STAR for an airport given the connecting fix and
    (optionally) the aircraft's minimum runway length.

    Scoring (higher = better):
      +1000   procedure exit (SID) / entry (STAR) fix == `connecting_fix`
      + 500   `connecting_fix` appears anywhere in the procedure path
      +0..100 fallback: 100 minus great-circle NM from connect-fix to
              `connecting_fix` (if both resolvable, capped at 80 NM)
      +0.5*N  bonus per usable runway (broader = more flexible to wind)

    Procedures whose runways are ALL too short for the aircraft are rejected.
    The returned dict's `runways_csv` is narrowed to the compatible subset.
    """
    if not airport_icao or not connecting_fix:
        return None
    procs = db.list_procedures(airport_icao, proc_type)
    if not procs:
        return None
    fix_u = connecting_fix.upper()
    target_wp = db.find_waypoint(fix_u)
    target_pt = (target_wp["lat"], target_wp["lon"]) if target_wp else None
    candidates: list[tuple[float, dict, str]] = []
    for p in procs:
        try:
            wpts = json.loads(p["waypoints_json"])
        except Exception:
            continue
        if not wpts:
            continue
        ok_rwys, _short = _runways_compatible(airport_icao, p["runways_csv"], min_runway_ft)
        if min_runway_ft and p["runways_csv"] and not ok_rwys:
            continue
        connect = wpts[-1] if proc_type == "SID" else wpts[0]
        connect_u = connect.upper()
        if connect_u == fix_u:
            score = 1000.0
        elif fix_u in {w.upper() for w in wpts}:
            score = 500.0
        elif target_pt:
            wp = db.find_waypoint(connect_u)
            if not wp:
                continue
            d = _great_circle_nm(target_pt, (wp["lat"], wp["lon"]))
            if d > 80.0:
                continue
            score = 100.0 - d
        else:
            continue
        score += len(ok_rwys) * 0.5
        row = dict(p)
        if ok_rwys:
            row["runways_csv"] = ",".join(ok_rwys)
        candidates.append((score, row, connect))
    if not candidates:
        return None
    candidates.sort(key=lambda x: -x[0])
    score, best, connect = candidates[0]
    best["waypoints"] = json.loads(best["waypoints_json"])
    best["connecting_fix"] = connect
    best["score"] = score
    best["proc_name"] = _expand_truncated_proc_name(best["proc_name"], best["waypoints"])
    return best


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
