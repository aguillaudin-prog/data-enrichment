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
    # Persisted so the caller can later request a briefing pack against
    # the same /flightplan/<routeid>/briefing endpoint.
    route_id: str = ""


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


def list_aircraft_templates(cfg: AutorouterConfig) -> list[dict]:
    """GET /aircraft/templates — les profils appareils créés par l'utilisateur
    dans son compte autorouter. On les utilise pour matcher par type ICAO
    (au lieu du fallback P28R par défaut) : un DHC6/DA62/L410 a une perf
    radicalement différente, et autorouter peut refuser une route si le
    profil ne tient pas la distance/FL."""
    try:
        resp = requests.get(
            f"{cfg.base_url}/aircraft/templates",
            headers=_auth_headers(cfg), timeout=20,
        )
    except requests.exceptions.RequestException as e:
        raise AutorouterError(f"network: {e}")
    if resp.status_code != 200:
        raise AutorouterError(
            f"GET /aircraft/templates HTTP {resp.status_code}: {resp.text[:300]}"
        )
    try:
        data = resp.json()
    except ValueError:
        return []
    if isinstance(data, list):
        return [t for t in data if isinstance(t, dict)]
    if isinstance(data, dict) and "templates" in data:
        return [t for t in data["templates"] if isinstance(t, dict)]
    return []


def list_aircraft(cfg: AutorouterConfig) -> list[dict]:
    """GET /aircraft — la liste des appareils créés par l'utilisateur sur
    son compte autorouter. Distinct de /aircraft/templates (catalogue
    manufacturer/model). Les IDs retournés ici sont utilisables comme
    `aircraftid` dans le payload POST /router."""
    try:
        resp = requests.get(
            f"{cfg.base_url}/aircraft",
            headers=_auth_headers(cfg), timeout=20,
        )
    except requests.exceptions.RequestException as e:
        raise AutorouterError(f"network: {e}")
    if resp.status_code != 200:
        raise AutorouterError(
            f"GET /aircraft HTTP {resp.status_code}: {resp.text[:300]}"
        )
    try:
        data = resp.json()
    except ValueError:
        return []
    if isinstance(data, list):
        return [a for a in data if isinstance(a, dict)]
    if isinstance(data, dict) and "aircraft" in data:
        return [a for a in data["aircraft"] if isinstance(a, dict)]
    return []


# Cache module-level (vit la durée du process Streamlit) : on POST /aircraft
# une seule fois par type ICAO, on réutilise l'ID ensuite. Évite de remplir
# le compte autorouter de doublons.
_AIRCRAFT_ID_CACHE: dict[str, int] = {}
# Diagnostic de la dernière action ensure_aircraft_for_type — surfaceé dans
# les messages d'erreur /router pour qu'on sache exactement ce qui a été
# fait avec l'aircraft setup (créé / réutilisé / échec API).
_LAST_AIRCRAFT_DIAG: str = ""


def ensure_aircraft_for_type(
    cfg: AutorouterConfig,
    icao_type: str,
    callsign: str = "",
) -> int | None:
    """Garantit qu'un appareil autorouter existe pour le type ICAO donné
    avec un équipement avionique IFR complet. Retourne son ID utilisable
    comme `aircraftid` dans /router.

    Workflow :
      1. Lookup cache module-level → return cached ID si déjà résolu.
      2. GET /aircraft → si un appareil existant matche le type ICAO,
         on prend le sien (l'utilisateur peut avoir personnalisé la perf).
      3. POST /aircraft avec équipement IFR forcé (SDFGRY) + transpondeur
         Mode S + perf de notre table aircraft_type ou fallback table.

    Retourne None si la création échoue (réseau, auth, schema), le caller
    fall-back alors sur aircraftid=0 (built-in P28R) — moins bon que le
    template dédié mais évite le crash.
    """
    global _LAST_AIRCRAFT_DIAG
    icao = _canonical_icao_type(icao_type)
    if not icao:
        _LAST_AIRCRAFT_DIAG = "skip (no ICAO type)"
        return None
    if icao in _AIRCRAFT_ID_CACHE:
        _LAST_AIRCRAFT_DIAG = f"cached {icao} id={_AIRCRAFT_ID_CACHE[icao]}"
        return _AIRCRAFT_ID_CACHE[icao]
    # 1. Cherche un appareil existant matchant
    try:
        existing = list_aircraft(cfg)
    except AutorouterError as e:
        existing = []
        _LAST_AIRCRAFT_DIAG = f"GET /aircraft failed: {e}"
    for ac in existing:
        ac_type = (ac.get("icaotypename") or ac.get("icaotype") or "").upper()
        if ac_type and _canonical_icao_type(ac_type) == icao:
            try:
                aid = int(ac.get("id"))
            except (TypeError, ValueError):
                continue
            _AIRCRAFT_ID_CACHE[icao] = aid
            _LAST_AIRCRAFT_DIAG = f"found existing {icao} id={aid}"
            return aid
    # 2. Lookup catalogue autorouter pour récupérer les IDs manufacturer +
    # icaotype requis par POST /aircraft (sans eux : HTTP 500 "missing
    # manufacturer"). Le catalogue est l'inventaire complet manufacturer /
    # model / icaotype maintenu par autorouter.
    try:
        catalog = list_aircraft_templates(cfg)
    except AutorouterError as e:
        _LAST_AIRCRAFT_DIAG = f"GET /aircraft/templates failed: {e}"
        return None
    catalog_entry: dict | None = None
    for t in catalog:
        for key in ("icao", "icaoid", "type", "icaotype", "designator"):
            v = (t.get(key) or "").upper().strip() if isinstance(t.get(key), str) else ""
            if v and v == icao:
                catalog_entry = t
                break
        if catalog_entry:
            break
    if not catalog_entry:
        # Dump quelques entrées pour qu'on voie la vraie structure
        # retournée par autorouter (les noms de clé peuvent différer).
        sample_keys = sorted(catalog[0].keys()) if catalog else []
        # Sortie compacte de tous les "ICAO-likes" pour voir ce qui est dispo.
        all_icaos = []
        for t in catalog:
            for k in ("icao", "icaoid", "type", "icaotype", "designator",
                      "name", "model", "modelname"):
                v = t.get(k)
                if isinstance(v, str) and v.strip():
                    all_icaos.append(f"{k}={v}")
                    break
        _LAST_AIRCRAFT_DIAG = (
            f"no catalog match for {icao} (catalog={len(catalog)} entries, "
            f"keys={sample_keys}, sample={all_icaos[:20]})"
        )
        return None
    # IDs côté catalogue. Les noms de clé varient — on essaie en cascade.
    manufacturer_id = (
        catalog_entry.get("manufacturerid")
        or catalog_entry.get("manufacturer_id")
        or catalog_entry.get("manufacturer")
    )
    icao_type_id = (
        catalog_entry.get("id")
        or catalog_entry.get("icaotypeid")
        or catalog_entry.get("icao_type_id")
    )
    model_name = (
        catalog_entry.get("model")
        or catalog_entry.get("modelname")
        or icao
    )
    # 3. Crée l'appareil via POST /aircraft avec tous les champs requis
    body = _build_inline_aircraft(icao) or {}
    body["callsign"] = (callsign or f"ZZZ{icao}")[:7]
    body["manufacturer"] = manufacturer_id
    body["icaotype"] = icao_type_id
    body["modelname"] = model_name
    body["year"] = 2020  # arbitraire mais requis pour les définitions persistantes
    try:
        resp = requests.post(
            f"{cfg.base_url}/aircraft",
            json=body, headers=_auth_headers(cfg), timeout=30,
        )
    except requests.exceptions.RequestException as e:
        _LAST_AIRCRAFT_DIAG = f"POST /aircraft network: {e}"
        return None
    if resp.status_code != 200:
        _LAST_AIRCRAFT_DIAG = (
            f"POST /aircraft HTTP {resp.status_code}: {resp.text[:200]} "
            f"(body keys: {sorted(body.keys())})"
        )
        return None
    try:
        data = resp.json()
    except ValueError:
        _LAST_AIRCRAFT_DIAG = f"POST /aircraft non-JSON: {resp.text[:200]}"
        return None
    aid = None
    if isinstance(data, int):
        aid = data
    elif isinstance(data, dict):
        aid = data.get("id") or data.get("aircraftid")
    elif isinstance(data, str) and data.strip().isdigit():
        aid = int(data.strip())
    try:
        aid_int = int(aid) if aid is not None else None
    except (TypeError, ValueError):
        _LAST_AIRCRAFT_DIAG = f"POST /aircraft unexpected body: {data!r}"
        return None
    if aid_int is not None:
        _AIRCRAFT_ID_CACHE[icao] = aid_int
        _LAST_AIRCRAFT_DIAG = f"created {icao} id={aid_int}"
    return aid_int


def find_template_id_for_type(cfg: AutorouterConfig, icao_type: str) -> int | None:
    """Look up the aircraft template ID best matching `icao_type`.

    Matching is case-insensitive and tolerates the DHC6-400 vs DHC6 split
    (compares first 4 letters of the type code). Returns None if no match
    so the caller falls back to autorouter's built-in P28R (aircraftid=0).
    """
    if not icao_type:
        return None
    needle = icao_type.strip().upper()
    try:
        templates = list_aircraft_templates(cfg)
    except AutorouterError:
        return None
    if not templates:
        return None
    # Strong match: exact ICAO designator
    for t in templates:
        for key in ("icao", "icaoid", "type", "icaotype", "designator"):
            v = (t.get(key) or "").upper().strip()
            if v and v == needle:
                tid = t.get("id") or t.get("aircraftid") or t.get("templateid")
                if tid is not None:
                    return int(tid)
    # Weak match: same first 4 letters (DHC6-400 ↔ DHC6)
    needle4 = needle[:4]
    for t in templates:
        for key in ("icao", "icaoid", "type", "icaotype", "designator"):
            v = (t.get(key) or "").upper().strip()
            if v and v[:4] == needle4:
                tid = t.get("id") or t.get("aircraftid") or t.get("templateid")
                if tid is not None:
                    return int(tid)
    return None


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


def _canonical_icao_type(raw: str) -> str:
    """Normalise un type appareil saisi par l'utilisateur en code ICAO
    4 lettres standard. Les types DIC sont souvent saisis avec leur suffixe
    variant (DHC6-400, B1900D, L410UVP-E20) — ces formes ne sont pas
    reconnues par autorouter ni par notre table aircraft_type.

    Exemples :
      DHC6-400 → DHC6
      B1900D   → B190
      L410UVP  → L410
      DA62     → DA62 (déjà 4 lettres)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""
    # On garde la base avant tout tiret / suffixe variant
    base = s.split("-")[0]
    # Les designators ICAO font 2-4 lettres+chiffres. Si la base est plus
    # longue, on garde les 4 premiers caractères.
    return base[:4]


# Fallback perf par ICAO designator pour les appareils qu'on opère,
# au cas où la table aircraft_type serait incomplète. Valeurs nominales
# manufacturer / Wikipedia, pas critique → autorouter ajuste de toute façon
# selon les airways accessibles.
_FALLBACK_AIRCRAFT_PERF: dict[str, dict] = {
    "DA62":  {"cruisetas": 175, "defaultmaxfl": 200, "wakecategory": "L"},
    "DHC6":  {"cruisetas": 160, "defaultmaxfl": 250, "wakecategory": "L"},
    "L410":  {"cruisetas": 195, "defaultmaxfl": 200, "wakecategory": "L"},
    "B190":  {"cruisetas": 280, "defaultmaxfl": 250, "wakecategory": "M"},
}


def _build_inline_aircraft(aircraft_type: str | None) -> dict | None:
    """Construit une définition appareil JSON inline pour bypass le besoin
    d'un template configuré côté autorouter.aero. L'API accepte un objet
    complet comme valeur de `aircraftid` (au lieu d'un entier ID).

    On force l'équipement avionique IFR/RNAV5 standard (SDFGRY) et le
    transpondeur Mode S — c'est ce que Eurocontrol exige pour considérer
    le vol comme "ENTIRELY IFR/GAT" (sinon WARN313 et rejet de la route).

    Retourne None si on n'a aucune info type → laisse le caller fall back
    sur aircraftid=0 (built-in P28R).
    """
    icao = _canonical_icao_type(aircraft_type)
    if not icao:
        return None
    # Priorité 1 : perf depuis notre base. Priorité 2 : fallback table.
    # Priorité 3 : defaults turboprop léger générique.
    perf = db.find_aircraft_type(icao)
    fb = _FALLBACK_AIRCRAFT_PERF.get(icao, {})
    cruise_tas = fb.get("cruisetas", 140)
    ceiling_fl = fb.get("defaultmaxfl", 150)
    wake = fb.get("wakecategory", "L")
    if perf:
        try:
            cruise_tas = int(perf["cruise_tas_kt"]) if perf["cruise_tas_kt"] else cruise_tas
        except (KeyError, IndexError, TypeError):
            pass
        try:
            if perf["service_ceiling_ft"]:
                ceiling_fl = int(perf["service_ceiling_ft"]) // 100
        except (KeyError, IndexError, TypeError):
            pass
        try:
            if perf["wake_category"]:
                wake = (perf["wake_category"] or "L")[0].upper()
        except (KeyError, IndexError, TypeError):
            pass
    return {
        "icaotypename": icao,
        # Équipement standard IFR + GNSS + PBN + 8.33 kHz. Ne pas ajouter W
        # (RVSM) pour les turboprops légers — la plupart ne sont pas
        # certifiés et ça déclencherait un autre warning IFPS.
        "equipment": "SDFGRY",
        "transponder": "S",
        "wakecategory": wake,
        "defaultmaxfl": ceiling_fl,
        "cruisetas": cruise_tas,
        # PBN/B2D2 = RNAV5 GNSS + DME/DME — config standard pour le routage
        # sur airways européennes basses-moyennes altitudes.
        "code": "PBN/B2D2",
    }


def _build_route_request(
    departure: str, destination: str,
    aircraft_type: str | None = None,
    cruise_level: int | None = None,
    eobt_iso: str | None = None,
    alternate1: str | None = None,
    alternate2: str | None = None,
    allow_vfr_downgrade: bool = False,
    aircraft_template_id: int | None = None,
    fl_window: int = 60,
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
        payload["minlevel"] = max(10, int(cruise_level) - fl_window)
        payload["maxlevel"] = int(cruise_level) + fl_window
    # aircraftid : préfère le template utilisateur configuré sur autorouter.
    # Fallback sur 0 = built-in P28R. L'inline JSON dans ce champ n'est PAS
    # supporté par /router (HTTP 500 "Array to string conversion" côté PHP),
    # contrairement à ce que le wiki suggère pour /flightplan. Pour bypass
    # le besoin de template manuel, on POST /aircraft/templates ailleurs
    # (voir ensure_template_for_type) et on récupère un ID utilisable ici.
    payload["aircraftid"] = aircraft_template_id if aircraft_template_id else 0
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


def _suggest_route_once(
    cfg: AutorouterConfig,
    departure: str, destination: str,
    *, aircraft_type: str | None,
    cruise_level: int | None,
    eobt_iso: str | None,
    alternate1: str | None,
    alternate2: str | None,
    aircraft_template_id: int | None,
    fl_window: int,
    allow_vfr_downgrade: bool,
    poll_timeout_s: int,
    poll_interval_s: float,
    per_request_timeout_s: int,
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
    needs_vfr = allow_vfr_downgrade
    payload = _build_route_request(
        departure, destination, aircraft_type=aircraft_type,
        cruise_level=cruise_level, eobt_iso=eobt_iso,
        alternate1=alternate1, alternate2=alternate2,
        allow_vfr_downgrade=needs_vfr,
        aircraft_template_id=aircraft_template_id,
        fl_window=fl_window,
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
                            # On signale "non-IFR" UNIQUEMENT quand un des
                            # aéroports est réellement user-added (FOB).
                            # Avant on se basait sur le flag needs_vfr du
                            # 2e essai (toujours True), ce qui produisait le
                            # message à tort sur des routes 100 % civiles
                            # type LFRV → LFMV qui échouent pour d'autres
                            # raisons (perf appareil de référence, airways
                            # incompatibles à ce FL, etc.).
                            actually_fob = (
                                _is_user_added(departure)
                                or _is_user_added(destination)
                            )
                            non_ifr = (
                                actually_fob
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
                            # Message neutre quand autorouter ne trouve juste
                            # pas de route. On joint flags + 3 dernières
                            # lignes de log pour diagnostiquer (sinon on
                            # navigue à l'aveugle sur les vraies causes).
                            flags_str = ", ".join(err_flags) or "—"
                            tail = " | ".join(logs[-3:]) if logs else "—"
                            ac_diag = (
                                f"aircraftid={aircraft_template_id}"
                                if aircraft_template_id
                                else "aircraftid=0 (P28R built-in)"
                            )
                            setup_diag = _LAST_AIRCRAFT_DIAG or "—"
                            raise AutorouterError(
                                f"Autorouter n'a pas trouvé de route. "
                                f"{ac_diag}. Setup : {setup_diag}. "
                                f"Flags : {flags_str}. Logs : {tail}"
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
        normalised = _normalise_solution(solution, fpl_fallback=last_fpl, logs=logs)
        normalised.route_id = route_id
        return normalised
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
    """Stratégie en 2 essais pour maximiser le succès :

    1. **Strict** : template appareil matchant le type ICAO, fenêtre FL ±60,
       vfrdowngrade off sauf FOB user-added. Donne la meilleure route IFR.
    2. **Relâché** (si le strict échoue avec internalerror) : aircraftid=0
       (P28R), fenêtre FL ±150, vfrdowngrade=True. Catch-all qui produit
       presque toujours quelque chose, quitte à accepter du VFR.

    Le 2e essai n'est lancé que si le 1er échoue avec un signal exploitable
    — pas sur les erreurs réseau, OAuth ou timeout (ces conditions
    persistent).
    """
    needs_vfr_first = _is_user_added(departure) or _is_user_added(destination)
    tpl_id = find_template_id_for_type(cfg, aircraft_type or "") if aircraft_type else None
    # Si aucun template/aircraft existant pour ce type, on en crée un via
    # POST /aircraft avec équipement IFR forcé. Évite le fallback sur
    # aircraftid=0 (P28R built-in sans équipement IFR) qui produit WARN313
    # à tous les coups sur les couples civils IFR.
    if tpl_id is None and aircraft_type and not needs_vfr_first:
        tpl_id = ensure_aircraft_for_type(cfg, aircraft_type)

    try:
        return _suggest_route_once(
            cfg, departure, destination,
            aircraft_type=aircraft_type, cruise_level=cruise_level,
            eobt_iso=eobt_iso, alternate1=alternate1, alternate2=alternate2,
            aircraft_template_id=tpl_id, fl_window=60,
            allow_vfr_downgrade=needs_vfr_first,
            poll_timeout_s=poll_timeout_s, poll_interval_s=poll_interval_s,
            per_request_timeout_s=per_request_timeout_s,
        )
    except AutorouterError as e:
        msg = str(e).lower()
        # Retry uniquement sur les échecs "pas de route" qui peuvent céder
        # à des contraintes plus larges.
        retryable = (
            "internalerror" in msg
            or "n'a pas trouvé" in msg
            or "non-ifr" in msg
            or "enrouteerror" in msg
            or "iterationerror" in msg
        )
        if not retryable:
            raise
        # 2e essai : large fenêtre + appareil par défaut + VFR autorisé.
        return _suggest_route_once(
            cfg, departure, destination,
            aircraft_type=aircraft_type, cruise_level=cruise_level,
            eobt_iso=eobt_iso, alternate1=alternate1, alternate2=alternate2,
            aircraft_template_id=None, fl_window=150,
            allow_vfr_downgrade=True,
            poll_timeout_s=poll_timeout_s, poll_interval_s=poll_interval_s,
            per_request_timeout_s=per_request_timeout_s,
        )


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


def fetch_gramet(
    cfg: AutorouterConfig, *,
    waypoints: str, altitude_ft: int, departure_ts: int, totaleet_s: int,
    fmt: str = "pdf",
) -> tuple[bytes, str]:
    """GET /met/gramet — coupe verticale météo le long de la route.

    Returns (file_bytes, mime_type). Raises AutorouterError on failure.
    The `waypoints` form (no FPL) is used so we work even when the route
    text isn't a valid ICAO FPL string.
    """
    if not cfg.is_configured():
        raise AutorouterError("autorouter not configured")
    params = {
        "waypoints": waypoints,
        "altitude": int(altitude_ft),
        "departuretime": int(departure_ts),
        "totaleet": int(totaleet_s),
        "format": fmt if fmt in ("pdf", "png") else "pdf",
    }
    try:
        resp = requests.get(
            f"{cfg.base_url}/met/gramet",
            params=params, headers=_auth_headers(cfg), timeout=60,
        )
    except requests.exceptions.RequestException as e:
        raise AutorouterError(f"network: {e}")
    if resp.status_code != 200:
        raise AutorouterError(f"GRAMET HTTP {resp.status_code}: {resp.text[:300]}")
    mime = "application/pdf" if params["format"] == "pdf" else "image/png"
    return resp.content, mime


# ─── Briefing pack ─────────────────────────────────────────────────────────────

# Ops-oriented preset : tout ce qui sert à l'attaché défense (atcbriefing,
# milbulletin, atcharges) en plus du pack pilote standard. La sortie est un
# PDF unique compilé par autorouter.
BRIEFING_OPS_ITEMS = [
    "navlog", "wb", "distances", "climb", "descent",
    "metartaf", "gramet", "isobaric", "skewt", "sigwx", "mslp", "temsi",
    "atcbriefing", "notam", "milbulletin", "icaofpl", "raim", "atcharges",
]


def request_briefing(
    cfg: AutorouterConfig, route_id: str,
    items: list[str] | None = None,
) -> str:
    """POST /flightplan/<routeid>/briefing (non-blocking download).

    Returns a token to poll for completion. Use poll_briefing(token) and
    fetch_briefing(token) to retrieve the PDF once ready.
    """
    if not route_id:
        raise AutorouterError("route_id manquant — relance une suggestion autorouter d'abord.")
    items = items or BRIEFING_OPS_ITEMS
    try:
        resp = requests.post(
            f"{cfg.base_url}/flightplan/{route_id}/briefing",
            data={"method": "download", "items": _json.dumps(items)},
            headers=_auth_headers(cfg), timeout=30,
        )
    except requests.exceptions.RequestException as e:
        raise AutorouterError(f"network: {e}")
    if resp.status_code != 200:
        raise AutorouterError(
            f"POST /briefing HTTP {resp.status_code}: {resp.text[:300]}"
        )
    # Token returned as plain text or JSON-wrapped string.
    body = (resp.text or "").strip()
    try:
        data = resp.json()
        if isinstance(data, str):
            return data.strip()
        if isinstance(data, dict):
            return (data.get("token") or data.get("id") or "").strip()
    except (ValueError, requests.exceptions.JSONDecodeError):
        pass
    return body.strip('"').strip("'")


def poll_briefing(cfg: AutorouterConfig, route_id: str, token: str) -> bool:
    """Returns True when the briefing pack is ready (HTTP 200), False if 404."""
    try:
        resp = requests.get(
            f"{cfg.base_url}/flightplan/{route_id}/briefing/{token}",
            params={"poll": "1"},
            headers=_auth_headers(cfg), timeout=20,
        )
    except requests.exceptions.RequestException:
        return False
    return resp.status_code == 200


def fetch_briefing(cfg: AutorouterConfig, route_id: str, token: str) -> bytes:
    """Download the PDF once poll_briefing returned True."""
    try:
        resp = requests.get(
            f"{cfg.base_url}/flightplan/{route_id}/briefing/{token}",
            headers=_auth_headers(cfg), timeout=120,
        )
    except requests.exceptions.RequestException as e:
        raise AutorouterError(f"network: {e}")
    if resp.status_code != 200:
        raise AutorouterError(
            f"GET briefing HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return resp.content


def fetch_briefing_pack(
    cfg: AutorouterConfig, route_id: str, *,
    items: list[str] | None = None,
    poll_timeout_s: int = 240,
    poll_interval_s: float = 4.0,
) -> bytes:
    """End-to-end : POST request, poll until ready, download. Synchronous
    wrapper meant for Streamlit usage behind a spinner. autorouter peut
    prendre 1-5 min pour un pack complet — d'où le poll_timeout_s long."""
    token = request_briefing(cfg, route_id, items=items)
    if not token:
        raise AutorouterError("autorouter n'a pas renvoyé de token de briefing.")
    deadline = time.time() + poll_timeout_s
    while time.time() < deadline:
        if poll_briefing(cfg, route_id, token):
            return fetch_briefing(cfg, route_id, token)
        time.sleep(poll_interval_s)
    raise AutorouterError(
        f"Briefing pas prêt après {poll_timeout_s}s. Réessaie plus tard."
    )


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
