"""Vertical profile chart for a leg.

Plotly figure showing :
- Terrain elevation along the route (brown filled area)
- Flight profile (blue line: climb → cruise → descent)
- MORA band (red translucent: terrain + 1000ft buffer)
- Waypoint annotations

Uses terrain_client (Open-Topodata SRTM 30m, already cached).
"""
from __future__ import annotations

import math
from typing import Any

import plotly.graph_objects as go


_M_PER_FT = 0.3048


def _great_circle_nm(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    R_NM = 3440.065
    phi1, phi2 = math.radians(p1[0]), math.radians(p2[0])
    dphi = math.radians(p2[0] - p1[0])
    dlam = math.radians(p2[1] - p1[1])
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R_NM * math.asin(math.sqrt(a))


def _interpolate(p1: tuple[float, float], p2: tuple[float, float], n: int) -> list[tuple[float, float]]:
    if n < 2:
        return [p1, p2]
    return [
        (p1[0] + (p2[0] - p1[0]) * i / (n - 1),
         p1[1] + (p2[1] - p1[1]) * i / (n - 1))
        for i in range(n)
    ]


def _flight_profile_altitudes(
    distance_nm: float, cruise_ft: int, climb_grad_pct: float,
    descent_grad_pct: float = 5.0,
    n_points: int = 60,
) -> tuple[list[float], list[float]]:
    """Construit le profil de vol idéal climb → cruise → descent.

    - climb_grad_pct : pente de montée (ex: 5.0 = 5%)
    - descent_grad_pct : pente descente standard 5% (~3°)
    - cruise_ft : altitude de croisière demandée

    Returns (distances_nm_cumulees, altitudes_ft).

    Si la distance est trop courte pour atteindre cruise_ft (climb_nm +
    descent_nm > distance_nm), on plafonne au top achievable.
    """
    if distance_nm <= 0 or cruise_ft <= 0:
        return [0, distance_nm], [0, 0]
    climb_grad_pct = max(climb_grad_pct, 1.0)
    descent_grad_pct = max(descent_grad_pct, 1.0)
    climb_nm = cruise_ft / (60.76 * climb_grad_pct)
    descent_nm = cruise_ft / (60.76 * descent_grad_pct)
    if climb_nm + descent_nm > distance_nm:
        # Pas assez de distance pour cruise plat : profile triangle
        max_alt = distance_nm * 60.76 * climb_grad_pct * descent_grad_pct / (climb_grad_pct + descent_grad_pct)
        peak_at = max_alt / (60.76 * climb_grad_pct)
        return [0, peak_at, distance_nm], [0, max_alt, 0]
    cruise_start = climb_nm
    cruise_end = distance_nm - descent_nm
    # Profile en 4 points : 0 → top-of-climb → top-of-descent → arrival
    distances = [0, cruise_start, cruise_end, distance_nm]
    altitudes = [0, cruise_ft, cruise_ft, 0]
    return distances, altitudes


def build_vertical_profile(
    resolution: Any,
    cruise_fl: int,
    climb_grad_pct: float = 5.0,
    leg_label: str = "",
    fetch_terrain: bool = True,
) -> go.Figure | None:
    """Construit la figure plotly verticale pour ce leg.

    Args:
        resolution : LegResolution avec resolution.points (avec coords)
        cruise_fl : FL de croisière demandé (FL90 = 9000 ft)
        climb_grad_pct : pente de montée appareil (de aircraft_type)
        leg_label : "Leg 1 — LFMV → LFMD" pour le titre
        fetch_terrain : si True, sample terrain via Open-Topodata.
            Désactive pour test sans réseau.

    Returns plotly Figure ou None si pas assez de waypoints / pas
    de coords.
    """
    coord_pts = [
        (p.lat, p.lon, p.label) for p in (resolution.points or [])
        if p.lat is not None and p.lon is not None
    ]
    if len(coord_pts) < 2:
        return None

    # Cumul distance NM le long des waypoints réels
    cum_d = [0.0]
    for i in range(1, len(coord_pts)):
        cum_d.append(cum_d[-1] + _great_circle_nm(
            (coord_pts[i - 1][0], coord_pts[i - 1][1]),
            (coord_pts[i][0], coord_pts[i][1]),
        ))
    total_nm = cum_d[-1]
    if total_nm <= 0:
        return None

    cruise_ft = cruise_fl * 100

    # 1. Terrain : sample N points sur la route entière, fetch via API
    terrain_x, terrain_y = [], []
    terrain_max_ft = 0
    if fetch_terrain:
        try:
            from app import terrain_client
            samples: list[tuple[float, float]] = []
            sample_distances: list[float] = []
            samples_per_seg = 8
            for i in range(len(coord_pts) - 1):
                seg = _interpolate(
                    (coord_pts[i][0], coord_pts[i][1]),
                    (coord_pts[i + 1][0], coord_pts[i + 1][1]),
                    samples_per_seg,
                )
                for j, pt in enumerate(seg):
                    if i > 0 and j == 0:
                        continue  # skip duplicate endpoint
                    samples.append(pt)
                    # interp distance
                    frac = j / (samples_per_seg - 1) if samples_per_seg > 1 else 0
                    sample_distances.append(cum_d[i] + (cum_d[i + 1] - cum_d[i]) * frac)
            # Limit to API max
            if len(samples) > 100:
                step = max(1, len(samples) // 100)
                samples = samples[::step][:100]
                sample_distances = sample_distances[::step][:100]
            elevs_m = terrain_client.fetch_elevations_meters(samples)
            for d, e in zip(sample_distances, elevs_m):
                if e is not None:
                    terrain_x.append(d)
                    terrain_y.append(e / _M_PER_FT)
            if terrain_y:
                terrain_max_ft = max(terrain_y)
        except Exception:
            pass

    # 2. Flight profile climb / cruise / descent
    flight_x, flight_y = _flight_profile_altitudes(
        total_nm, cruise_ft, climb_grad_pct,
    )

    # 3. Build figure
    fig = go.Figure()
    if terrain_x:
        fig.add_trace(go.Scatter(
            x=terrain_x, y=terrain_y, mode="lines", name="Terrain",
            line=dict(color="#8B4513", width=1),
            fill="tozeroy", fillcolor="rgba(139, 69, 19, 0.25)",
            hovertemplate="<b>%{y:.0f} ft</b> à %{x:.0f} NM<extra></extra>",
        ))
        # MORA band : terrain + 1000 ft buffer, jusqu'au cruise_ft
        mora_y = [t + 1000 for t in terrain_y]
        fig.add_trace(go.Scatter(
            x=terrain_x, y=mora_y, mode="lines", name="MORA (terrain + 1000ft)",
            line=dict(color="rgba(220, 60, 60, 0.5)", width=1, dash="dot"),
            hovertemplate="MORA <b>%{y:.0f} ft</b><extra></extra>",
        ))
    # Flight profile (climb + cruise + descent)
    fig.add_trace(go.Scatter(
        x=flight_x, y=flight_y, mode="lines+markers", name=f"Vol FL{cruise_fl:03d}",
        line=dict(color="#2563eb", width=3),
        marker=dict(size=8, color="#2563eb"),
        hovertemplate="<b>%{y:.0f} ft</b> à %{x:.0f} NM<extra></extra>",
    ))

    # Annotations waypoints (verticales discrètes)
    for d, (_lat, _lon, label) in zip(cum_d, coord_pts):
        fig.add_vline(
            x=d, line=dict(color="rgba(80, 80, 80, 0.15)", width=1, dash="dot"),
            annotation_text=label, annotation_position="top",
            annotation_font=dict(size=10, color="#555"),
        )

    fig.update_layout(
        title=dict(
            text=leg_label or "Profil vertical",
            font=dict(size=14, color="#1e3a8a"),
        ),
        xaxis=dict(title="Distance (NM)", showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(
            title="Altitude (ft)", showgrid=True, gridcolor="rgba(0,0,0,0.05)",
            range=[0, max(cruise_ft * 1.15, terrain_max_ft * 1.3 + 1500, 5000)],
        ),
        height=320,
        margin=dict(l=50, r=20, t=50, b=40),
        plot_bgcolor="white",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hovermode="x unified",
    )
    return fig
