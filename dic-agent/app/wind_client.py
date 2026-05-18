"""Open-Meteo client for wind aloft along a route.

Free REST API (no auth, no rate limit for moderate use). Pressure-level
winds via GFS model. We map FL → pressure level approximately:

  FL90  ≈ 700 hPa
  FL150 ≈ 575 hPa  (closest available: 500 hPa)
  FL180 ≈ 500 hPa
  FL250 ≈ 380 hPa  (closest available: 400 hPa)
  FL300 ≈ 300 hPa
  FL360 ≈ 220 hPa  (closest available: 250 hPa)
  FL390 ≈ 200 hPa

We use the closest pressure level Open-Meteo offers.

Usage (silent fail policy — if API down, return zero wind, UI keeps
the still-air time and doesn't show wind caption):

    wc = WindClient()
    avg = wc.average_wind_along_route(
        waypoints=[(lat, lon), ...],
        eobt_utc=datetime(...),
        fl=90,
    )
    # avg = {'wind_speed_kt': 12.5, 'wind_dir_deg': 045, 'available': True}

For computing headwind from average wind given a route bearing:
    hw = headwind_component(avg['wind_speed_kt'], avg['wind_dir_deg'], course_deg)
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests


_BASE_URL = "https://api.open-meteo.com/v1/gfs"

# Pressure levels Open-Meteo offers + their ISA altitude approx in ft.
# We pick the closest one to the requested FL.
_PRESSURE_LEVELS = [
    (1000, 364),   (925, 2500),  (850, 5000),  (700, 9882),
    (500, 18289),  (400, 23574),  (300, 30065), (250, 33999),
    (200, 38662),
]


_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = 1800  # 30 min — wind doesn't change rapidly within that window

_LAST_STATUS: dict = {
    "ok": None,
    "last_check": None,
    "error": None,
}


def _pressure_for_fl(fl: int) -> int:
    """Closest pressure level (hPa) Open-Meteo serves for the given FL."""
    if fl <= 0:
        return 850
    target_ft = fl * 100
    return min(_PRESSURE_LEVELS, key=lambda x: abs(x[1] - target_ft))[0]


def _cache_get(key: str) -> dict | None:
    if key not in _CACHE:
        return None
    ts, data = _CACHE[key]
    if time.time() - ts > _CACHE_TTL_S:
        del _CACHE[key]
        return None
    return data


def _cache_set(key: str, data: dict) -> None:
    _CACHE[key] = (time.time(), data)


def fetch_wind_at_point(
    lat: float, lon: float, when_utc: datetime, fl: int,
) -> dict:
    """Vent (speed kt + direction deg) au point lat/lon à `when_utc` au FL.

    Retourne {wind_speed_kt, wind_dir_deg, available, error?}. Si l'API
    échoue ou si on est hors fenêtre de prévision (J+16), available=False
    et caller doit fall back au still-air.
    """
    global _LAST_STATUS
    pl = _pressure_for_fl(fl)
    hour_iso = when_utc.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")
    cache_key = f"{lat:.2f},{lon:.2f},{hour_iso},{pl}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params = {
        "latitude": round(lat, 3),
        "longitude": round(lon, 3),
        "hourly": f"wind_speed_{pl}hPa,wind_direction_{pl}hPa",
        "wind_speed_unit": "kn",  # nœuds, pas m/s
        "timezone": "UTC",
        "start_hour": hour_iso,
        "end_hour": hour_iso,
        # NB : `forecast_days` est mutuellement exclusif avec
        # start_hour/end_hour côté Open-Meteo. On laisse l'API gérer
        # l'horizon de prévision par défaut (~7 jours, jusqu'à 16 jours
        # selon disponibilité). Si l'EOBT est plus loin que ça, on
        # tombera dans le branche "no data at hour" et le caller
        # fall-back proprement sur still-air.
    }
    try:
        resp = requests.get(_BASE_URL, params=params, timeout=8)
    except requests.exceptions.RequestException as e:
        _LAST_STATUS = {"ok": False, "last_check": datetime.now(timezone.utc), "error": str(e)}
        return {"wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False, "error": f"network: {e}"}
    if resp.status_code != 200:
        _LAST_STATUS = {
            "ok": False, "last_check": datetime.now(timezone.utc),
            "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
        }
        return {
            "wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False,
            "error": f"HTTP {resp.status_code}",
        }
    try:
        data = resp.json()
    except ValueError:
        _LAST_STATUS = {"ok": False, "last_check": datetime.now(timezone.utc), "error": "non-JSON response"}
        return {"wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False, "error": "bad JSON"}

    hourly = data.get("hourly") or {}
    speeds = hourly.get(f"wind_speed_{pl}hPa") or []
    dirs = hourly.get(f"wind_direction_{pl}hPa") or []
    if not speeds or not dirs or speeds[0] is None or dirs[0] is None:
        return {"wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False, "error": "no data at hour"}

    result = {
        "wind_speed_kt": float(speeds[0]),
        "wind_dir_deg": float(dirs[0]),
        "available": True,
        "pressure_level": pl,
    }
    _LAST_STATUS = {"ok": True, "last_check": datetime.now(timezone.utc), "error": None}
    _cache_set(cache_key, result)
    return result


def average_wind_along_route(
    waypoints: list[tuple[float, float]],
    eobt_utc: datetime,
    fl: int,
) -> dict:
    """Moyenne du vent sur les waypoints d'un leg.

    On échantillonne le 1er, le dernier et le midpoint (3 mesures suffisent
    pour un leg ouest-africain typique 200-1500 NM). Si moins de 2 points,
    on prend juste le 1er. Si l'API est down on retourne available=False.
    """
    if not waypoints:
        return {"wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False}
    pts = []
    pts.append(waypoints[0])
    if len(waypoints) >= 3:
        pts.append(waypoints[len(waypoints) // 2])
    if len(waypoints) >= 2:
        pts.append(waypoints[-1])

    samples = []
    for lat, lon in pts:
        s = fetch_wind_at_point(lat, lon, eobt_utc, fl)
        if s["available"]:
            samples.append(s)
    if not samples:
        return {"wind_speed_kt": 0.0, "wind_dir_deg": 0.0, "available": False}

    # Moyenne vectorielle (composantes U/V) pour ne pas se planter sur
    # les passages 359°→0°.
    u_sum = sum(s["wind_speed_kt"] * math.sin(math.radians(s["wind_dir_deg"])) for s in samples)
    v_sum = sum(s["wind_speed_kt"] * math.cos(math.radians(s["wind_dir_deg"])) for s in samples)
    n = len(samples)
    u_avg, v_avg = u_sum / n, v_sum / n
    speed = math.hypot(u_avg, v_avg)
    direction = (math.degrees(math.atan2(u_avg, v_avg)) + 360) % 360
    return {
        "wind_speed_kt": speed,
        "wind_dir_deg": direction,
        "available": True,
        "n_samples": n,
    }


def headwind_component(wind_speed_kt: float, wind_dir_deg: float, course_deg: float) -> float:
    """Composante headwind (positive = headwind, négative = tailwind).

    Formule classique : headwind = wind_speed * cos(wind_dir - course).
    Le vent souffle DE la direction wind_dir vers la course = headwind +.
    """
    angle_rad = math.radians(wind_dir_deg - course_deg)
    return wind_speed_kt * math.cos(angle_rad)


def initial_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Cap initial du grand-cercle de p1 vers p2, en degrés vrais [0, 360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlam = math.radians(lon2 - lon1)
    y = math.sin(dlam) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlam)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def compute_wind_adjusted_time(
    waypoints: list[tuple[float, float]],
    distance_nm: float,
    tas_kt: float,
    eobt_utc: datetime,
    fl: int,
) -> dict:
    """Calcule le temps de vol corrigé du vent pour un leg.

    Retourne dict avec :
      - still_air_min : temps still-air (distance / tas)
      - wind_adjusted_min : temps avec headwind moyen
      - headwind_kt : composante headwind moyenne sur la route
      - wind_speed_kt, wind_dir_deg : vent moyen
      - delta_pct : (adjusted - still_air) / still_air * 100
      - available : True si données vent OK, False sinon
    """
    still_air_min = (distance_nm / max(tas_kt, 1)) * 60
    if len(waypoints) < 2 or tas_kt <= 0:
        return {
            "still_air_min": still_air_min, "wind_adjusted_min": still_air_min,
            "headwind_kt": 0.0, "wind_speed_kt": 0.0, "wind_dir_deg": 0.0,
            "delta_pct": 0.0, "available": False,
        }
    avg_wind = average_wind_along_route(waypoints, eobt_utc, fl)
    if not avg_wind["available"]:
        return {
            "still_air_min": still_air_min, "wind_adjusted_min": still_air_min,
            "headwind_kt": 0.0, "wind_speed_kt": 0.0, "wind_dir_deg": 0.0,
            "delta_pct": 0.0, "available": False,
        }
    course = initial_bearing(waypoints[0][0], waypoints[0][1], waypoints[-1][0], waypoints[-1][1])
    hw = headwind_component(avg_wind["wind_speed_kt"], avg_wind["wind_dir_deg"], course)
    ground_speed = max(tas_kt - hw, 30)  # garde-fou : pas de GS négatif
    adjusted_min = (distance_nm / ground_speed) * 60
    return {
        "still_air_min": still_air_min,
        "wind_adjusted_min": adjusted_min,
        "headwind_kt": hw,
        "wind_speed_kt": avg_wind["wind_speed_kt"],
        "wind_dir_deg": avg_wind["wind_dir_deg"],
        "delta_pct": (adjusted_min - still_air_min) / still_air_min * 100 if still_air_min else 0.0,
        "available": True,
    }


def get_last_status() -> dict:
    """Statut du dernier appel API (pour la page Admin)."""
    return dict(_LAST_STATUS)


def health_check() -> dict:
    """Probe Open-Meteo avec une requête minimale. Retourne {ok, latency_ms, error}.

    Met aussi à jour `_LAST_STATUS` pour que le caption "dernier appel"
    de la page Admin se rafraîchisse au test manuel sans attendre un
    fetch_wind_at_point réel via une mission.
    """
    global _LAST_STATUS
    t0 = time.time()
    try:
        resp = requests.get(
            _BASE_URL,
            params={
                "latitude": 0, "longitude": 0,
                "hourly": "wind_speed_500hPa",
                "wind_speed_unit": "kn", "forecast_days": 1,
            },
            timeout=5,
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
