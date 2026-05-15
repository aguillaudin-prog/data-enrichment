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

import json as _json
import time
from concurrent.futures import ThreadPoolExecutor
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


def _parse_route_id(resp: requests.Response) -> str:
    """Extract a route_id from POST /router's response.

    The autorouter wiki examples show IDs as plain strings (e.g.
    'LFSB-LYTV-59156af893577'). Different deployments wrap them in
    different ways: bare text, JSON-encoded string, or an object with a
    routeid/id field. Cover all three so the integration is robust to
    minor server-side changes.
    """
    body = (resp.text or "").strip()
    if not body:
        return ""
    # JSON object with the id field
    try:
        data = resp.json()
        if isinstance(data, dict):
            return (data.get("routeid") or data.get("id") or data.get("route_id") or "").strip()
        if isinstance(data, str):
            return data.strip()
    except (ValueError, requests.exceptions.JSONDecodeError):
        pass
    # Plain text id — strip surrounding quotes if any
    return body.strip().strip('"').strip("'")


def _parse_messages(resp: requests.Response) -> list[dict]:
    """Parse a longpoll response that should be a JSON array of command
    objects. Tolerates non-JSON responses by returning an empty list
    rather than raising, so the polling loop can carry on."""
    try:
        data = resp.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        return []
    if isinstance(data, list):
        return [m for m in data if isinstance(m, dict)]
    if isinstance(data, dict):
        return [data]
    return []


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
    allow_vfr_downgrade: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "departure": _airport_payload(departure),
        "destination": _airport_payload(destination),
    }
    # vfrdowngrade fait que Eurocontrol IFPS crache WARN313 même sur des
    # routes 100 % IFR — désactivé par défaut, activé seulement quand on
    # sait qu'on a un aérodrome non-publié (FOB / militaire).
    if allow_vfr_downgrade:
        payload["vfrdowngrade"] = True
    if eobt_iso:
        payload["departuretime"] = eobt_iso
    if cruise_level is not None:
        # Fenêtre large pour laisser autorouter choisir un FL routable
        # selon les airways. Trop serré → internalerror.
        payload["minlevel"] = max(10, int(cruise_level) - 60)
        payload["maxlevel"] = int(cruise_level) + 60
    # aircraftid=0 → P28R built-in (Arrow). Acceptable pour la plupart
    # des trajets ; les contraintes perf restent gérées côté DIC.
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


def _is_user_added(icao: str) -> bool:
    ap = db.find_airport((icao or "").strip().upper())
    if not ap:
        return False
    try:
        return bool(ap["user_added"])
    except (KeyError, IndexError):
        return False


def suggest_route(
    cfg: AutorouterConfig,
    departure: str, destination: str,
    aircraft_type: str | None = None,
    cruise_level: int | None = None,
    eobt_iso: str | None = None,
    alternate1: str | None = None,
    alternate2: str | None = None,
    poll_timeout_s: int = 240,
    poll_interval_s: float = 1.5,
    per_request_timeout_s: int = 90,
) -> AutorouterRoute:
    """End-to-end route request:
      1. POST /router → routeid
      2. PUT /router/<id>/longpoll until a `solution` arrives (or stopping)
      3. PUT /router/<id>/close
    Returns the first valid solution. Raises AutorouterError on failure or
    timeout (the longpoll loop is bounded by `poll_timeout_s`).

    The longpoll endpoint is server-blocking: it holds the connection
    open until messages arrive or its internal timeout triggers. We use
    per_request_timeout_s=90 so a single poll has room to receive a
    full router-status update. Network-level ReadTimeout/ConnectionError
    during longpoll are retried up to 3 times — the route is still
    running server-side, we just reconnect.
    """
    # vfrdowngrade only when we know one end is a non-published FOB —
    # otherwise it pollutes IFPS validation on standard IFR routes.
    needs_vfr = _is_user_added(departure) or _is_user_added(destination)
    payload = _build_route_request(
        departure, destination, aircraft_type=aircraft_type,
        cruise_level=cruise_level, eobt_iso=eobt_iso,
        alternate1=alternate1, alternate2=alternate2,
        allow_vfr_downgrade=needs_vfr,
    )
    headers = _auth_headers(cfg)
    resp = requests.post(
        f"{cfg.base_url}/router", json=payload, headers=headers,
        timeout=per_request_timeout_s,
    )
    if resp.status_code != 200:
        raise AutorouterError(
            f"POST /router failed: HTTP {resp.status_code} {resp.text[:500]}"
        )
    # The wiki examples show route IDs as plain strings like
    # 'LFSB-LYTV-59156af893577'. The server returns the route_id either as
    # raw text or as a JSON-encoded string — handle both.
    route_id = _parse_route_id(resp)
    if not route_id:
        raise AutorouterError(
            f"POST /router returned no route_id (body: {resp.text[:300]!r})"
        )

    solution: dict | None = None
    logs: list[str] = []
    last_fpl: dict | None = None
    deadline = time.time() + poll_timeout_s
    max_transient_errors = 3
    transient_errors = 0
    try:
        while time.time() < deadline:
            try:
                poll_resp = requests.put(
                    f"{cfg.base_url}/router/{route_id}/longpoll",
                    headers=headers, timeout=per_request_timeout_s,
                )
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as e:
                # The router is still running server-side; reconnect and
                # keep polling until poll_timeout_s is exhausted.
                transient_errors += 1
                if transient_errors > max_transient_errors:
                    raise AutorouterError(
                        f"Trop d'erreurs réseau pendant le polling ({e})"
                    )
                time.sleep(poll_interval_s)
                continue
            transient_errors = 0
            if poll_resp.status_code != 200:
                raise AutorouterError(
                    f"longpoll failed: HTTP {poll_resp.status_code} {poll_resp.text[:300]}"
                )
            messages = _parse_messages(poll_resp)
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
                            log_blob = " ".join(logs).upper()
                            # Only call it "non-IFR" when we actually asked for
                            # vfrdowngrade (i.e. user-added FOB on one end).
                            # On standard IFR airports, internalerror usually
                            # means the default aircraft profile can't make it
                            # or the FL/route constraints don't match airways.
                            non_ifr = (
                                needs_vfr
                                and "internalerror" in err_flags
                                and "ENTIRELY IFR/GAT" in log_blob
                            )
                            if non_ifr:
                                raise AutorouterError(
                                    "Autorouter rejette les routes non-IFR pures. "
                                    "Cas typique : aérodrome militaire / non-publié "
                                    "(TOUROU, KAINJI, FOB). "
                                    "Utilise la suggestion locale (✨)."
                                )
                            # Generic but actionable: surface what we know.
                            tail = logs[-3:] if logs else []
                            extras = ""
                            if "internalerror" in err_flags and not tail:
                                extras = (
                                    " — Souvent : appareil de référence "
                                    "(P28R par défaut) trop limité pour la distance/FL, "
                                    "ou pas d'airways compatibles à ce FL. "
                                    "Essaie un FL différent ou la suggestion locale."
                                )
                            raise AutorouterError(
                                f"Autorouter n'a pas trouvé de route valide "
                                f"(flags: {err_flags or 'none'}).{extras} "
                                + (f"Logs: {tail}" if tail else "")
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


# ─── Weather & NOTAMs ──────────────────────────────────────────────────────────

@dataclass
class MetarTaf:
    icao: str = ""
    metar: str = ""
    taf: str = ""
    error: str = ""


def fetch_metartaf(cfg: AutorouterConfig, icao: str) -> MetarTaf:
    """GET /met/metartaf/<icao> — returns latest METAR + TAF or empty fields.

    Per the wiki, individual fields are null when unavailable. We squash to
    empty strings so the UI can show '—' uniformly.
    """
    icao = (icao or "").strip().upper()
    if not icao:
        return MetarTaf(error="missing ICAO")
    if not cfg.is_configured():
        return MetarTaf(icao=icao, error="autorouter not configured")
    try:
        resp = requests.get(
            f"{cfg.base_url}/met/metartaf/{icao}",
            headers=_auth_headers(cfg), timeout=20,
        )
    except requests.exceptions.RequestException as e:
        return MetarTaf(icao=icao, error=f"network: {e}")
    if resp.status_code == 404:
        return MetarTaf(icao=icao, error="no METAR/TAF for this station")
    if resp.status_code != 200:
        return MetarTaf(icao=icao, error=f"HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError:
        return MetarTaf(icao=icao, error="invalid JSON response")
    return MetarTaf(
        icao=icao,
        metar=(data.get("metar") or "").strip(),
        taf=(data.get("taf") or "").strip(),
    )


def fetch_metartaf_batch(cfg: AutorouterConfig, icaos: list[str]) -> dict[str, MetarTaf]:
    """Parallel METAR/TAF fetch (one HTTP call per ICAO, 5 workers).

    Skips user-added FOB airports (no METAR/TAF published). The autorouter
    endpoint is per-station so we parallelise to keep total latency under
    ~3 s for 6-8 airports.
    """
    targets = []
    skipped: dict[str, MetarTaf] = {}
    for ic in icaos:
        ic_u = (ic or "").strip().upper()
        if not ic_u or ic_u in skipped or any(t == ic_u for t in targets):
            continue
        ap = db.find_airport(ic_u)
        if ap:
            try:
                if ap["user_added"]:
                    skipped[ic_u] = MetarTaf(icao=ic_u, error="aérodrome non-publié (pas de METAR/TAF)")
                    continue
            except (KeyError, IndexError):
                pass
        targets.append(ic_u)

    results: dict[str, MetarTaf] = dict(skipped)
    if not targets:
        return results
    with ThreadPoolExecutor(max_workers=min(5, len(targets))) as ex:
        for mt in ex.map(lambda ic: fetch_metartaf(cfg, ic), targets):
            results[mt.icao] = mt
    return results


@dataclass
class NotamRow:
    """Decoded subset of an autorouter NOTAM row. The full row is kept in
    `raw` for any consumer that wants the Garmin-format coords etc."""
    id: int = 0
    series: str = ""
    number: int = 0
    year: int = 0
    itema: list[str] = field(default_factory=list)
    iteme: str = ""           # the human-readable body
    itemd: str | None = None  # schedule, may be null
    itemf: str | None = None  # lower limit text
    itemg: str | None = None  # upper limit text
    fir: str = ""
    startvalidity: int = 0
    endvalidity: int = 0
    traffic: str = ""
    purpose: str = ""
    scope: str = ""
    type: str = ""
    raw: dict = field(default_factory=dict)

    @property
    def notam_id(self) -> str:
        return f"{self.series}{self.number:04d}/{self.year:02d}" if self.number else f"#{self.id}"


def _decode_notam_row(row: dict) -> NotamRow:
    return NotamRow(
        id=int(row.get("id") or 0),
        series=(row.get("series") or "").strip(),
        number=int(row.get("number") or 0),
        year=int(row.get("year") or 0),
        itema=list(row.get("itema") or []),
        iteme=(row.get("iteme") or "").strip(),
        itemd=row.get("itemd"),
        itemf=row.get("itemf"),
        itemg=row.get("itemg"),
        fir=(row.get("fir") or "").strip(),
        startvalidity=int(row.get("startvalidity") or 0),
        endvalidity=int(row.get("endvalidity") or 0),
        traffic=(row.get("traffic") or "").strip(),
        purpose=(row.get("purpose") or "").strip(),
        scope=(row.get("scope") or "").strip(),
        type=(row.get("type") or "").strip(),
        raw=row,
    )


def fetch_notams(
    cfg: AutorouterConfig,
    icaos: list[str],
    *,
    startvalidity: int | None = None,
    endvalidity: int | None = None,
    limit: int = 100,
) -> list[NotamRow]:
    """GET /notam?itemas=[...]&… — fetches NOTAMs valid for the given
    ICAO list inside the [startvalidity, endvalidity] window (Unix seconds).

    The autorouter NOTAM DB is European/Eurocontrol-EAD only. For West
    Africa (DI/DR/DN/DX) the response will be empty — we surface that
    explicitly to the user instead of silently dropping it.
    """
    items = [(ic or "").strip().upper() for ic in icaos if (ic or "").strip()]
    items = sorted(set(items))
    if not items:
        return []
    if not cfg.is_configured():
        return []
    params: dict[str, Any] = {
        "itemas": _json.dumps(items),
        "offset": 0,
        "limit": max(1, min(int(limit), 100)),
    }
    if startvalidity is not None:
        params["startvalidity"] = int(startvalidity)
    if endvalidity is not None:
        params["endvalidity"] = int(endvalidity)
    try:
        resp = requests.get(
            f"{cfg.base_url}/notam",
            params=params, headers=_auth_headers(cfg), timeout=30,
        )
    except requests.exceptions.RequestException:
        return []
    if resp.status_code != 200:
        return []
    try:
        data = resp.json()
    except ValueError:
        return []
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    return [_decode_notam_row(r) for r in rows if isinstance(r, dict)]


def format_notam(n: NotamRow) -> str:
    """Render a NOTAM as the standard ICAO Q-line + A/B/C/E format."""
    import datetime as _dt
    def _fmt_ts(ts: int) -> str:
        try:
            return _dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
        except (OverflowError, OSError, ValueError):
            return "—"
    lines = [f"{n.notam_id} NOTAM"]
    if n.itema:
        lines.append(f"A) {' '.join(n.itema)}")
    lines.append(f"B) {_fmt_ts(n.startvalidity)}  C) {_fmt_ts(n.endvalidity)}")
    if n.itemd:
        lines.append(f"D) {n.itemd}")
    if n.iteme:
        lines.append(f"E) {n.iteme}")
    if n.itemf:
        lines.append(f"F) {n.itemf}")
    if n.itemg:
        lines.append(f"G) {n.itemg}")
    return "\n".join(lines)
