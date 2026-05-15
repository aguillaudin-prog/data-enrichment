"""HTTP client for the autorouter.aero REST API.

OAuth 2.0 client_credentials authentication (the user's autorouter email +
password are passed as client_id/client_secret to get a bearer token,
which the wiki documents as the supported flow for direct account access).

Routes are async: POST /router returns a routeid immediately, then the
client must longpoll /router/<id>/longpoll until a `solution` command with
`routesuccess: true` is delivered. We handle that loop here with a hard
timeout so a stuck router can't hang Streamlit.

Non-ICAO departures/destinations (e.g. user-added FOB labels like TOUROU,
KAINJI NAFB) are passed as inline objects {name, arplatdeg, arplondeg,
elevation} per the autorouter docs, instead of an ICAO string.

Configuration is read from st.secrets under [autorouter]:

  [autorouter]
  base_url = "https://api.autorouter.aero/v1.0"
  client_id = "user@example.org"   # autorouter login (email)
  client_secret = "..."             # autorouter password
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

from app import db


_BASE_URL_DEFAULT = "https://api.autorouter.aero/v1.0"
_TOKEN_URL_DEFAULT = "https://api.autorouter.aero/v1.0/oauth2/token"


@dataclass
class AutorouterConfig:
    base_url: str = _BASE_URL_DEFAULT
    token_url: str = _TOKEN_URL_DEFAULT
    client_id: str = ""
    client_secret: str = ""

    @classmethod
    def from_secrets(cls, secrets: Any) -> "AutorouterConfig":
        try:
            section = secrets["autorouter"] if "autorouter" in secrets else {}
        except Exception:
            section = {}
        return cls(
            base_url=(section.get("base_url") if hasattr(section, "get") else None) or _BASE_URL_DEFAULT,
            token_url=(section.get("token_url") if hasattr(section, "get") else None) or _TOKEN_URL_DEFAULT,
            client_id=(section.get("client_id") if hasattr(section, "get") else "") or "",
            client_secret=(section.get("client_secret") if hasattr(section, "get") else "") or "",
        )

    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret)


@dataclass
class _TokenCache:
    access_token: str = ""
    expires_at: float = 0.0


_CACHE = _TokenCache()


@dataclass
class AutorouterRoute:
    """Normalised view of the chosen autorouter `solution`."""
    fpl: str = ""                            # ICAO field-15 flight plan string
    route_text: str = ""                     # enroute portion (without dep/dest)
    waypoints: list[str] = field(default_factory=list)
    distance_nm: float = 0.0
    fuel_units: float = 0.0
    time_seconds: int = 0
    raw_solution: dict = field(default_factory=dict)
    log_messages: list[str] = field(default_factory=list)


class AutorouterError(Exception):
    pass


# ─── Auth ──────────────────────────────────────────────────────────────────────

def _get_token(cfg: AutorouterConfig) -> str:
    if not cfg.is_configured():
        raise AutorouterError(
            "Autorouter non configuré. Ajoute client_id et client_secret "
            "sous [autorouter] dans Streamlit Cloud → Settings → Secrets."
        )
    now = time.time()
    if _CACHE.access_token and _CACHE.expires_at - now > 30:
        return _CACHE.access_token
    resp = requests.post(
        cfg.token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
        },
        timeout=20,
    )
    if resp.status_code != 200:
        try:
            err = resp.json()
            msg = err.get("error_description") or err.get("error") or resp.text
        except Exception:
            msg = resp.text
        raise AutorouterError(f"OAuth token request failed (HTTP {resp.status_code}): {msg[:300]}")
    data = resp.json()
    _CACHE.access_token = data.get("access_token") or ""
    if not _CACHE.access_token:
        raise AutorouterError(f"Token response missing access_token: {data}")
    expires_in = int(data.get("expires_in", 3600))
    _CACHE.expires_at = now + expires_in
    return _CACHE.access_token


def _auth_headers(cfg: AutorouterConfig) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_token(cfg)}",
        "Accept": "application/json",
    }


def ping_version(cfg: AutorouterConfig) -> dict:
    """Health-check the API. Calls /system/version (no auth required)."""
    resp = requests.get(f"{cfg.base_url}/system/version", timeout=15)
    if resp.status_code != 200:
        raise AutorouterError(f"version probe failed: HTTP {resp.status_code} {resp.text[:300]}")
    return resp.json()


# ─── Routes ────────────────────────────────────────────────────────────────────

def _airport_payload(icao_or_label: str) -> Any:
    """Return either an ICAO string (for published airports) or an inline
    object {name, arplatdeg, arplondeg, elevation} for user-added FOB labels.

    autorouter accepts both forms (the docs' 'Non ICAO airports' section).
    """
    icao = (icao_or_label or "").strip().upper()
    if not icao:
        return None
    ap = db.find_airport(icao)
    if not ap:
        return icao
    try:
        is_user_added = bool(ap["user_added"])
    except (KeyError, IndexError):
        is_user_added = False
    if not is_user_added:
        return icao
    name = (ap["name"] or icao).strip()
    elev = 0
    try:
        elev = int(ap["elevation_ft"] or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        elev = 0
    return {
        "name": name,
        "arplatdeg": float(ap["lat"]),
        "arplondeg": float(ap["lon"]),
        "elevation": elev,
    }


def _build_route_request(
    departure: str, destination: str,
    aircraft_type: str | None = None,
    cruise_level: int | None = None,
    eobt_iso: str | None = None,
    alternate1: str | None = None,
    alternate2: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "departure": _airport_payload(departure),
        "destination": _airport_payload(destination),
        # Don't force IFR-only — the user's missions are FRA defence flights
        # that can mix IFR/VFR. autorouter will choose the cleanest option.
        "vfrdowngrade": True,
    }
    if eobt_iso:
        payload["departuretime"] = eobt_iso
    if cruise_level is not None:
        payload["minlevel"] = max(10, int(cruise_level) - 20)
        payload["maxlevel"] = int(cruise_level) + 20
    if aircraft_type:
        # Pass an inline aircraft definition with just the ICAO type — the
        # full aircraft profile (mass, perf) is on autorouter's side.
        # Using aircraftid=0 selects their built-in standard aircraft (P28R),
        # which works as a fallback if the inline definition is rejected.
        payload["aircraftid"] = 0
    else:
        payload["aircraftid"] = 0
    if alternate1:
        alt1 = _airport_payload(alternate1)
        if isinstance(alt1, str):
            payload["alternate1"] = alt1
    if alternate2:
        alt2 = _airport_payload(alternate2)
        if isinstance(alt2, str):
            payload["alternate2"] = alt2
    return payload


def suggest_route(
    cfg: AutorouterConfig,
    departure: str, destination: str,
    aircraft_type: str | None = None,
    cruise_level: int | None = None,
    eobt_iso: str | None = None,
    alternate1: str | None = None,
    alternate2: str | None = None,
    poll_timeout_s: int = 90,
    poll_interval_s: float = 1.5,
) -> AutorouterRoute:
    """End-to-end route request:
      1. POST /router → routeid
      2. PUT /router/<id>/longpoll until a `solution` arrives (or stopping)
      3. PUT /router/<id>/close
    Returns the first valid solution. Raises AutorouterError on failure or
    timeout (the longpoll loop is bounded by `poll_timeout_s`).
    """
    payload = _build_route_request(
        departure, destination, aircraft_type=aircraft_type,
        cruise_level=cruise_level, eobt_iso=eobt_iso,
        alternate1=alternate1, alternate2=alternate2,
    )
    headers = _auth_headers(cfg)
    resp = requests.post(f"{cfg.base_url}/router", json=payload, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise AutorouterError(
            f"POST /router failed: HTTP {resp.status_code} {resp.text[:500]}"
        )
    data = resp.json()
    route_id = data.get("routeid") or data.get("id") or data.get("route_id")
    if not route_id:
        raise AutorouterError(f"POST /router returned no routeid: {data}")

    solution: dict | None = None
    logs: list[str] = []
    last_fpl: dict | None = None
    deadline = time.time() + poll_timeout_s
    try:
        while time.time() < deadline:
            poll_resp = requests.put(
                f"{cfg.base_url}/router/{route_id}/longpoll",
                headers=headers, timeout=30,
            )
            if poll_resp.status_code != 200:
                raise AutorouterError(
                    f"longpoll failed: HTTP {poll_resp.status_code} {poll_resp.text[:300]}"
                )
            messages = poll_resp.json() or []
            if not isinstance(messages, list):
                messages = [messages]
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                if msg.get("error"):
                    raise AutorouterError(f"router error: {msg.get('error')}")
                cmd = msg.get("cmdname")
                if cmd == "solution":
                    solution = msg
                elif cmd == "fpl":
                    last_fpl = msg
                elif cmd == "log":
                    item = msg.get("item") or ""
                    text = msg.get("text") or ""
                    if text:
                        logs.append(f"{item}: {text}" if item else text)
                elif cmd == "autoroute":
                    status = msg.get("status")
                    if status == "stopping":
                        if not msg.get("routesuccess"):
                            err_flags = [k for k in (
                                "enrouteerror", "internalerror", "iterationerror",
                                "siderror", "starerror", "validatorerror",
                            ) if msg.get(k)]
                            raise AutorouterError(
                                "router stopped without a valid route. "
                                f"flags: {err_flags or 'none'}. "
                                f"Last logs: {logs[-3:] if logs else '—'}"
                            )
                        # routesuccess=true but no `solution` command yet:
                        # wait one more poll cycle.
                    elif status == "terminate":
                        raise AutorouterError("router terminated (likely timeout server-side)")
            if solution is not None:
                break
            time.sleep(poll_interval_s)
        if solution is None:
            raise AutorouterError(
                f"timeout après {poll_timeout_s}s sans solution. "
                f"Logs: {logs[-3:] if logs else '—'}"
            )
        return _normalise_solution(solution, fpl_fallback=last_fpl, logs=logs)
    finally:
        # Best-effort close — ignore errors here, the session will time
        # out server-side anyway.
        try:
            requests.put(
                f"{cfg.base_url}/router/{route_id}/close",
                headers=headers, timeout=10,
            )
        except Exception:
            pass


def _normalise_solution(
    solution: dict, fpl_fallback: dict | None, logs: list[str],
) -> AutorouterRoute:
    """Map an autorouter `solution`/`fpl` command to AutorouterRoute."""
    fpl_str = solution.get("fpl") or (fpl_fallback or {}).get("fpl") or ""
    fplan = solution.get("fplan") or (fpl_fallback or {}).get("fplan") or []
    waypoints: list[str] = []
    for wp in fplan:
        name = (wp.get("name") or wp.get("icao") or "").strip()
        if name:
            waypoints.append(name)
    route_text = ""
    if len(waypoints) > 2:
        # Drop departure + destination; the middle is the enroute portion
        route_text = " ".join(waypoints[1:-1])
    distance = float(solution.get("routedist") or (fpl_fallback or {}).get("routedist") or 0.0)
    fuel = float(solution.get("routefuel") or (fpl_fallback or {}).get("routefuel") or 0.0)
    time_s = int(solution.get("routetime") or (fpl_fallback or {}).get("routetime") or 0)
    return AutorouterRoute(
        fpl=fpl_str,
        route_text=route_text,
        waypoints=waypoints,
        distance_nm=distance,
        fuel_units=fuel,
        time_seconds=time_s,
        raw_solution=solution,
        log_messages=logs,
    )
