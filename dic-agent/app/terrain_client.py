"""Terrain elevation client (Open-Topodata SRTM 30m).

Free REST API, no auth, 1 req/sec / 1000 locations per req.
Used to compute MORA (Minimum Off-Route Altitude) along a route :
sample N points within a corridor, fetch terrain elevation, MORA =
max(elev) + 1000 ft buffer, rounded up to nearest 100 ft.

Reference: https://www.opentopodata.org

Silent-fail design: if the API is down or unreachable, MORA returns
None and the UI degrades gracefully (no terrain check shown).
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests


_BASE_URL = "https://api.opentopodata.org/v1/srtm30m"
_M_PER_FT = 0.3048
_CACHE_TTL_S = 86400  # 24 h — le terrain bouge pas
_CACHE: dict[str, tuple[float, float | None]] = {}

_LAST_STATUS: dict = {
    "ok": None,
    "last_check": None,
    "error": None,
}


def _round_key(lat: float, lon: float) -> str:
    """Clé cache arrondie à 0.001° (~110m) pour un peu de mutualisation."""
    return f"{lat:.3f},{lon:.3f}"


def _great_circle_nm(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    R_NM = 3440.065
    phi1, phi2 = math.radians(p1[0]), math.radians(p2[0])
    dphi = math.radians(p2[0] - p1[0])
    dlam = math.radians(p2[1] - p1[1])
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R_NM * math.asin(math.sqrt(a))


def _interpolate_points(
    p1: tuple[float, float], p2: tuple[float, float], n_samples: int,
) -> list[tuple[float, float]]:
    """Échantillonne n points uniformément sur la ligne géodésique p1→p2."""
    if n_samples < 2:
        return [p1, p2]
    pts = []
    for i in range(n_samples):
        t = i / (n_samples - 1)
        lat = p1[0] + (p2[0] - p1[0]) * t
        lon = p1[1] + (p2[1] - p1[1]) * t
        pts.append((lat, lon))
    return pts


def fetch_elevations_meters(locations: list[tuple[float, float]]) -> list[float | None]:
    """POST batch jusqu'à 100 locations à Open-Topodata. Retourne la
    liste des élévations en mètres (None pour les fails individuels).
    Silent-fail global → liste de None si l'API tombe."""
    global _LAST_STATUS
    if not locations:
        return []
    # Check cache
    cached: dict[int, float | None] = {}
    fetch_indices: list[int] = []
    fetch_locs: list[tuple[float, float]] = []
    for i, (lat, lon) in enumerate(locations):
        key = _round_key(lat, lon)
        if key in _CACHE:
            ts, val = _CACHE[key]
            if time.time() - ts < _CACHE_TTL_S:
                cached[i] = val
                continue
        fetch_indices.append(i)
        fetch_locs.append((lat, lon))

    if fetch_locs:
        loc_str = "|".join(f"{lat:.5f},{lon:.5f}" for lat, lon in fetch_locs)
        try:
            resp = requests.get(_BASE_URL, params={"locations": loc_str}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                results = data.get("results") or []
                for j, r in enumerate(results):
                    if j >= len(fetch_indices):
                        break
                    orig_idx = fetch_indices[j]
                    elev = r.get("elevation")
                    cached[orig_idx] = float(elev) if elev is not None else None
                    key = _round_key(*fetch_locs[j])
                    _CACHE[key] = (time.time(), cached[orig_idx])
                _LAST_STATUS = {
                    "ok": True, "last_check": datetime.now(timezone.utc),
                    "error": None,
                }
            else:
                _LAST_STATUS = {
                    "ok": False, "last_check": datetime.now(timezone.utc),
                    "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
                }
                # Marque les fetch_indices comme None
                for idx in fetch_indices:
                    cached[idx] = None
        except requests.exceptions.RequestException as e:
            _LAST_STATUS = {
                "ok": False, "last_check": datetime.now(timezone.utc),
                "error": f"network: {e}",
            }
            for idx in fetch_indices:
                cached[idx] = None

    return [cached.get(i) for i in range(len(locations))]


def compute_mora_for_leg(
    waypoints: list[tuple[float, float]],
    samples_per_segment: int = 8,
    buffer_ft: int = 1000,
) -> dict:
    """Calcule MORA (Minimum Off-Route Altitude) pour une suite de
    waypoints. Échantillonne `samples_per_segment` points sur chaque
    segment, fetch terrain via Open-Topodata, retourne :

      {
        "mora_ft": int (rounded up to nearest 100),
        "max_terrain_ft": int,
        "n_samples": int,
        "available": bool,
        "error": str | None,
      }

    Si l'API est down → available=False, le caller fall-back sur "—".
    """
    if len(waypoints) < 2:
        return {"available": False, "mora_ft": None, "max_terrain_ft": None,
                "n_samples": 0, "error": "less than 2 waypoints"}
    # Build sample points along all segments
    all_samples: list[tuple[float, float]] = [waypoints[0]]
    for i in range(len(waypoints) - 1):
        seg = _interpolate_points(
            waypoints[i], waypoints[i + 1], samples_per_segment,
        )
        all_samples.extend(seg[1:])  # skip endpoint to avoid dups
    # Limite : Open-Topodata accepte max 100 locations/req. Si plus,
    # on subsample (un point sur N).
    if len(all_samples) > 100:
        step = max(1, len(all_samples) // 100)
        all_samples = all_samples[::step][:100]
    elevs_m = fetch_elevations_meters(all_samples)
    valid = [e for e in elevs_m if e is not None]
    if not valid:
        return {
            "available": False, "mora_ft": None, "max_terrain_ft": None,
            "n_samples": len(all_samples),
            "error": _LAST_STATUS.get("error") or "no elevation data",
        }
    max_m = max(valid)
    max_ft = int(max_m / _M_PER_FT)
    mora_raw = max_ft + buffer_ft
    # Round up to nearest 100 ft
    mora_ft = ((mora_raw + 99) // 100) * 100
    return {
        "available": True,
        "mora_ft": mora_ft,
        "max_terrain_ft": max_ft,
        "n_samples": len(all_samples),
        "error": None,
    }


def get_last_status() -> dict:
    return dict(_LAST_STATUS)


def health_check() -> dict:
    """Probe Open-Topodata avec 1 point (0,0 — océan Atlantique).
    Met à jour _LAST_STATUS pour rafraîchir la caption Admin."""
    global _LAST_STATUS
    t0 = time.time()
    try:
        resp = requests.get(
            _BASE_URL, params={"locations": "0,0"}, timeout=8,
        )
        latency = int((time.time() - t0) * 1000)
        ok = resp.status_code == 200
        err = None if ok else f"HTTP {resp.status_code}: {resp.text[:200]}"
        _LAST_STATUS = {
            "ok": ok, "last_check": datetime.now(timezone.utc), "error": err,
        }
        return {"ok": ok, "latency_ms": latency, "error": err}
    except requests.exceptions.RequestException as e:
        _LAST_STATUS = {
            "ok": False, "last_check": datetime.now(timezone.utc), "error": str(e),
        }
        return {"ok": False, "latency_ms": int((time.time() - t0) * 1000), "error": str(e)}
