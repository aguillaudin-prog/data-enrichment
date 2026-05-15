"""HTTP client for the autorouter.aero REST API.

Wraps the routes endpoint of `https://api.autorouter.aero/v1.0/` with the
OAuth 2.0 authentication their docs describe. The exact OAuth flow + the
`/router` payload schema are not fully published — what we have is the
high-level API doc + the user's account. So this module starts with a
client-credentials skeleton and will need adjustment once the user runs a
first call and we see what the server expects/returns.

Configuration is read from st.secrets (in Streamlit Cloud → Settings →
Secrets, or `.streamlit/secrets.toml` locally) under the [autorouter]
section:

  [autorouter]
  base_url = "https://api.autorouter.aero/v1.0"
  token_url = "https://api.autorouter.aero/oauth2/token"   # to confirm
  client_id = "…"
  client_secret = "…"
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests


@dataclass
class AutorouterConfig:
    base_url: str = "https://api.autorouter.aero/v1.0"
    token_url: str = "https://api.autorouter.aero/oauth2/token"
    client_id: str = ""
    client_secret: str = ""

    @classmethod
    def from_secrets(cls, secrets: dict) -> "AutorouterConfig":
        section = secrets.get("autorouter", {}) if secrets else {}
        return cls(
            base_url=section.get("base_url", cls.base_url),
            token_url=section.get("token_url", cls.token_url),
            client_id=section.get("client_id", ""),
            client_secret=section.get("client_secret", ""),
        )

    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret)


@dataclass
class _TokenCache:
    access_token: str = ""
    expires_at: float = 0.0   # epoch seconds


_CACHE = _TokenCache()


@dataclass
class AutorouterRoute:
    """Normalised representation of an autorouter route response, mapped to
    the same shape our local A* suggester returns so the UI layer can
    consume either interchangeably."""
    waypoints: list[str] = field(default_factory=list)
    edge_labels: list[str] = field(default_factory=list)
    distance_nm: float = 0.0
    route_text: str = ""
    raw: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


class AutorouterError(Exception):
    pass


def _get_token(cfg: AutorouterConfig) -> str:
    """Returns a bearer token, refreshing if cached one is near expiry.

    Defaults to the OAuth 2.0 'client_credentials' grant (the typical B2B
    flow). If autorouter uses a different grant (authorization_code, ROPC),
    we'll see a 4xx from the token endpoint and adjust."""
    if not cfg.is_configured():
        raise AutorouterError(
            "autorouter not configured. Add client_id/client_secret under "
            "[autorouter] in Streamlit secrets."
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
        raise AutorouterError(
            f"OAuth token request failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    data = resp.json()
    _CACHE.access_token = data.get("access_token", "")
    if not _CACHE.access_token:
        raise AutorouterError(f"Token response missing access_token: {data}")
    expires_in = int(data.get("expires_in", 3600))
    _CACHE.expires_at = now + expires_in
    return _CACHE.access_token


def ping_version(cfg: AutorouterConfig) -> dict:
    """Health check — calls the well-documented /system/version endpoint.
    Doesn't require authentication (returns auth=false when unauthenticated).
    Returns the JSON dict, or raises AutorouterError on failure."""
    resp = requests.get(f"{cfg.base_url}/system/version", timeout=15)
    if resp.status_code != 200:
        raise AutorouterError(
            f"version probe failed: HTTP {resp.status_code} {resp.text[:300]}"
        )
    return resp.json()


def suggest_route(
    cfg: AutorouterConfig,
    departure_icao: str,
    destination_icao: str,
    aircraft_type: str | None = None,
    cruise_level: int | None = None,
    eobt_iso: str | None = None,
) -> AutorouterRoute:
    """POST to /router to obtain a validated IFR route.

    The exact request payload schema for /router is not published in the
    API wiki entry we have. The fields below are educated guesses based on
    EUROCONTROL IFPS conventions and similar B2B routing APIs. If the
    server returns 400/422, the user can inspect the response and we adapt
    the keys.
    """
    token = _get_token(cfg)
    payload: dict[str, Any] = {
        "departure": departure_icao,
        "destination": destination_icao,
    }
    if aircraft_type:
        payload["aircraftType"] = aircraft_type
    if cruise_level is not None:
        payload["cruiseLevel"] = cruise_level
    if eobt_iso:
        payload["eobt"] = eobt_iso
    resp = requests.post(
        f"{cfg.base_url}/router",
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise AutorouterError(
            f"router call failed: HTTP {resp.status_code} {resp.text[:500]}"
        )
    data = resp.json()
    return _normalise_route(data)


def _normalise_route(data: dict) -> AutorouterRoute:
    """Adapt the autorouter response to AutorouterRoute. The actual keys
    will be confirmed against a real response — this is a first guess that
    will likely need tweaking after the first live call."""
    waypoints: list[str] = []
    edge_labels: list[str] = []
    # Common patterns: 'route' as a single string, or 'segments' list
    route_text = data.get("route") or data.get("routeText") or ""
    segments = data.get("segments") or data.get("path") or []
    for s in segments:
        if isinstance(s, dict):
            waypoints.append(s.get("ident") or s.get("name") or "")
            edge_labels.append(s.get("airway") or s.get("via") or "DCT")
        elif isinstance(s, str):
            waypoints.append(s)
    distance = float(data.get("distanceNm") or data.get("distance") or 0.0)
    warnings = data.get("warnings") or data.get("messages") or []
    if not isinstance(warnings, list):
        warnings = [str(warnings)]
    return AutorouterRoute(
        waypoints=waypoints,
        edge_labels=edge_labels,
        distance_nm=distance,
        route_text=route_text or " ".join(waypoints),
        raw=data,
        warnings=[str(w) for w in warnings],
    )
