"""Streamlit entrypoint — DIC Agent.

Run: streamlit run app/main.py
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

# Ensure the project root is on sys.path so `from app import ...` works
# regardless of the directory `streamlit run` was launched from.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st

from app import db, docx_generator, fpl_exporter, route_engine, route_suggester

st.set_page_config(
    page_title="DIC Agent",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def _ensure_amazone_data_seeded() -> int:
    """Seed des données opérateur Amazone au 1er démarrage Streamlit
    (cache resource = une fois par process). Idempotent — ON CONFLICT
    update sur chaque seed. Silencieux si fichier manque.

    Couvre :
      - 3 waypoints maritimes custom (EBUSO, ENKIT, ARABA)
      - 56 routes catalogue DHC6 (airways + payload + temps de vol)
      - Affinage perf DHC6 (OEW TY-BAB 3813 kg)
    """
    n = 0
    try:
        from app.seed_db import (
            seed_amazone_waypoints, seed_canonical_routes,
            seed_dhc6_perf_refinements,
        )
        n += seed_amazone_waypoints()
        n += seed_canonical_routes()
        seed_dhc6_perf_refinements()
    except Exception:
        pass
    return n


_ensure_amazone_data_seeded()


def _show_logged_in_user() -> None:
    """L'auth est gérée par Streamlit Community Cloud (Settings → Sharing →
    Only specific people, allowlist d'emails Google). Streamlit injecte
    l'email de l'utilisateur connecté dans st.experimental_user — on
    l'affiche en sidebar pour confirmer qui est en session. Pas d'appel
    requis si l'allowlist n'est pas activée (mode dev local)."""
    try:
        email = getattr(st.experimental_user, "email", None) if hasattr(st, "experimental_user") else None
    except Exception:
        email = None
    if email:
        with st.sidebar:
            st.caption(f"👤 Connecté : **{email}**")


_show_logged_in_user()

DB_FILE = Path(__file__).resolve().parent.parent / "data" / "dic.sqlite"
if not DB_FILE.exists():
    st.error(
        "Base SQLite absente. Lance d'abord : `python -m app.seed_db` pour télécharger "
        "aéroports / NAVAID / frontières d'États (~150 Mo, premier run uniquement)."
    )
    st.stop()


@st.cache_data(ttl=600)
def country_index_cached():
    return route_engine._build_country_index()


def _operator_picker(key_prefix: str) -> str | None:
    """Dropdown of known operators (compagnies) — drives downstream filters."""
    operators = db.list_operators()
    options = operators + ["— autre / nouveau —"]

    def _on_operator_change() -> None:
        # Changing the operator invalidates the aircraft & crew selection,
        # plus any half-typed 'new aircraft' form fields. Clear them so the
        # user starts from a clean state with the new operator's fleet.
        for k in (
            f"{key_prefix}_ap_sel", f"{key_prefix}_reg", f"{key_prefix}_type",
            f"{key_prefix}_cs", f"{key_prefix}_cdb_sel", f"{key_prefix}_fo_sel",
            f"{key_prefix}_cdb_text", f"{key_prefix}_fo_text",
        ):
            st.session_state.pop(k, None)

    if not operators:
        sel = "— autre / nouveau —"
        st.info("Aucune compagnie en base. Saisis-en une et un appareil pour démarrer.")
    else:
        sel = st.selectbox(
            "Compagnie", options, key=f"{key_prefix}_op_sel",
            on_change=_on_operator_change,
        )
    if sel == "— autre / nouveau —":
        return st.text_input("Nom de la compagnie (saisie libre)", key=f"{key_prefix}_op_text").strip() or None
    return sel


def _op_slug(operator: str | None) -> str:
    """Slug compact ASCII pour bricoler des keys Streamlit stables.
    Ex : 'AMAZONE AIRLINES / DYNAMI OPS' → 'AMAZONEAIRLINESDYNAMIOPS'.
    Permet de rendre les selectbox keys dépendantes de l'operator courant
    pour que Streamlit recrée un widget vierge à chaque changement de
    compagnie (pop session_state ne suffit pas, le display value est
    indépendamment caché côté React)."""
    return "".join(c for c in (operator or "_none_").upper() if c.isalnum()) or "_none_"


def _aircraft_picker(key_prefix: str, operator: str | None = None) -> dict:
    rows = db.list_aircraft(operator=operator)
    aircraft_options = [
        f"{r['registration']} / {r['type_icao'] or '?'}" for r in rows
    ]
    options = aircraft_options + ["— nouveau —"]
    label = f"Appareil ({operator})" if operator else "Appareil"
    if rows:
        default_idx = 0
    else:
        default_idx = 0  # only '— nouveau —' available
    sel = st.selectbox(label, options, index=default_idx,
                       key=f"{key_prefix}_ap_sel_{_op_slug(operator)}")
    if sel != "— nouveau —":
        idx = options.index(sel)
        r = rows[idx]
        st.caption(
            f"reg `{r['registration']}` • type `{r['type_icao']}` • callsign `{r['callsign']}` • op `{r['operator']}`"
        )
        return {
            "registration": r["registration"],
            "type_icao": r["type_icao"],
            "callsign": r["callsign"],
            "operator": r["operator"],
        }
    c1, c2 = st.columns(2)
    with c1:
        reg = st.text_input("Immatriculation", key=f"{key_prefix}_reg").strip().upper()
        type_icao = st.text_input(
            "Type ICAO (ex. DHC6-400, A400, B738…)", key=f"{key_prefix}_type"
        ).strip().upper()
    with c2:
        callsign = st.text_input("Callsign", key=f"{key_prefix}_cs").strip().upper()
    # operator is inherited from the parent selector
    operator_inherited = operator or ""

    if type_icao:
        types = db.list_aircraft_types(type_icao)
        if types:
            t = types[0]
            st.caption(
                f"Type connu : **{t['icao_designator']}** — {t['full_name']} • "
                f"cruise TAS {t['cruise_tas_kt']} kt • ceiling {t['service_ceiling_ft']} ft"
            )
        else:
            st.warning(f"Type `{type_icao}` inconnu en base — il sera quand même accepté.")

    if reg and st.button("💾 Sauver profil appareil", key=f"{key_prefix}_save_ap"):
        db.save_aircraft(reg, type_icao or None, callsign or None, operator_inherited or None)
        st.success(f"Profil {reg} enregistré.")
        st.rerun()
    return {"registration": reg, "type_icao": type_icao, "callsign": callsign, "operator": operator_inherited}


def _crew_picker(key_prefix: str, operator: str | None = None) -> dict:
    """Two independent dropdowns: CDB and FO. Pilots are mixable, not paired.

    If `operator` is provided, only pilots allowed for that operator are shown.
    """
    cdbs = db.list_pilots(role="CDB", operator=operator)
    fos = db.list_pilots(role="FO", operator=operator)

    if not cdbs and not fos:
        st.warning(
            "Aucun pilote en base. Lance `python -m app.seed_pilots` "
            "ou ajoute-les via l'expander ci-dessous."
        )

    cdb_options = [f"{p['rank'] or ''} {p['name']}".strip() for p in cdbs] + ["— autre —"]
    fo_options = [f"{p['rank'] or ''} {p['name']}".strip() for p in fos] + ["— autre —", "— aucun —"]
    cdb_label_to_name = {f"{p['rank'] or ''} {p['name']}".strip(): p["name"] for p in cdbs}
    fo_label_to_name = {f"{p['rank'] or ''} {p['name']}".strip(): p["name"] for p in fos}

    op_slug = _op_slug(operator)
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel_cdb = st.selectbox("Commandant de bord (CDB)", cdb_options,
                               key=f"{key_prefix}_cdb_sel_{op_slug}")
        if sel_cdb == "— autre —":
            cdb_text = st.text_input("CDB (saisie libre)",
                                     key=f"{key_prefix}_cdb_text_{op_slug}").strip()
        else:
            cdb_text = cdb_label_to_name.get(sel_cdb, sel_cdb)
    with c2:
        sel_fo = st.selectbox("Copilote (FO)", fo_options,
                              key=f"{key_prefix}_fo_sel_{op_slug}")
        if sel_fo == "— autre —":
            fo_text = st.text_input("FO (saisie libre)",
                                    key=f"{key_prefix}_fo_text_{op_slug}").strip()
        elif sel_fo == "— aucun —":
            fo_text = ""
        else:
            fo_text = fo_label_to_name.get(sel_fo, sel_fo)
    with c3:
        n_crew = st.number_input(
            "N. crew",
            min_value=1, max_value=10,
            value=2 if fo_text else 1,
            key=f"{key_prefix}_ncrew",
        )

    pilots_display = cdb_text + (f" and {fo_text}" if fo_text else "")
    st.caption(f"Équipage actuel : **{pilots_display}**")

    with st.expander("➕ Ajouter un pilote en base"):
        ac1, ac2, ac3 = st.columns([2, 1, 1])
        with ac1:
            new_name = st.text_input("Nom prénom", key=f"{key_prefix}_new_pilot_name")
        with ac2:
            new_role = st.selectbox("Rôle", ["CDB", "FO"], key=f"{key_prefix}_new_pilot_role")
        with ac3:
            new_rank = st.text_input("Grade", value="CPT", key=f"{key_prefix}_new_pilot_rank")
        if operator:
            st.caption(f"Le pilote sera associé à la compagnie **{operator}** "
                       f"(visible uniquement quand elle est sélectionnée).")
        else:
            st.caption("⚠ Sélectionne d'abord une compagnie pour associer le pilote.")
        if new_name and st.button("💾 Ajouter", key=f"{key_prefix}_save_pilot"):
            db.save_pilot(new_name, new_role, new_rank or None, allowed_operator=operator)
            st.success(f"Pilote {new_name} ({new_role}) ajouté pour {operator or '(aucun opérateur)'}.")
            st.rerun()

    return {
        "n_crew": int(n_crew),
        "pilots": pilots_display,
        "cdb": cdb_text,
        "fo": fo_text,
    }


def _poc_picker(key_prefix: str, operator: str | None = None) -> dict:
    """POC = Point Of Contact. Pick an existing one from the dropdown,
    or use the '➕ Ajouter un POC' expander below to create a new one
    that becomes available everywhere afterwards.

    Only 3 fields are surfaced (name including rank, email fonctionnel,
    phone) — the legacy email_personal / fax columns stay in the schema
    for backward compatibility but aren't exposed.

    The `operator` param only affects the selectbox key so the widget
    re-renders fresh when the user switches compagnie (visuel cohérent
    avec aircraft/crew pickers, même si POC est en réalité global)."""
    op_slug = _op_slug(operator)
    rows = db.list_pocs()
    if not rows:
        st.info("Aucun POC en base. Crée-en un via l'expander ci-dessous.")
        chosen = None
    else:
        labels = [f"{r['rank'] or ''} {r['name']}".strip() for r in rows]
        sel = st.selectbox("Profil POC", labels, key=f"{key_prefix}_poc_sel_{op_slug}")
        chosen = rows[labels.index(sel)] if sel else None

    with st.expander("➕ Ajouter un POC en base"):
        c1, c2, c3 = st.columns(3)
        with c1:
            new_name = st.text_input(
                "POC (grade + nom, ex. OFFICIER TRANSIT)", key=f"{key_prefix}_poc_new_name",
            )
        with c2:
            new_email = st.text_input(
                "Email fonctionnel", key=f"{key_prefix}_poc_new_email",
            )
        with c3:
            new_phone = st.text_input(
                "Téléphone", key=f"{key_prefix}_poc_new_phone",
            )
        if new_name and st.button("💾 Sauver POC", key=f"{key_prefix}_save_poc"):
            db.save_poc("", new_name, new_phone, "", new_email, "")
            st.success(f"POC '{new_name}' enregistré.")
            st.rerun()

    if chosen is not None:
        return {
            "name": f"{chosen['rank'] or ''} {chosen['name']}".strip(),
            "phone": chosen["phone"] or "",
            "email_personal": chosen["email_personal"] or "",
            "email_functional": chosen["email_functional"] or "",
            "fax": chosen["fax"] or "",
        }
    return {"name": "", "phone": "", "email_personal": "", "email_functional": "", "fax": ""}


def _legs_sid() -> int:
    """Monotonically increasing 'session id' for leg widget keys.

    Every time we load a template or reset to a blank mission, we bump this
    counter. The leg widget keys then include the sid, so previous widget
    state becomes orphaned in session_state and the fresh widgets start
    with their value= defaults. This is the only fully reliable way to
    avoid widget-state superposition across template loads.
    """
    return int(st.session_state.get("_legs_sid", 0))


def _bump_legs_sid() -> None:
    st.session_state["_legs_sid"] = _legs_sid() + 1


def _search_airports(query: str) -> list[tuple[str, str]]:
    """streamlit-searchbox callback. Returns (display_label, value) pairs
    where the value is the ICAO (what we store in leg state). Matches by
    ICAO prefix (most common), then by airport name substring (for users
    who know the city, not the code)."""
    if not query:
        return []
    q = query.strip().upper()
    seen: set[str] = set()
    results: list[tuple[str, str]] = []
    for m in db.find_airports_by_prefix(q, limit=15):
        if m["icao"] in seen:
            continue
        seen.add(m["icao"])
        label = f"{m['icao']}  ·  {m['name']}"
        if m["country_iso"]:
            label += f"  ({m['country_iso']})"
        results.append((label, m["icao"]))
    if len(results) < 15:
        for m in db.find_airports_by_name_substring(q, limit=15 - len(results)):
            if m["icao"] in seen:
                continue
            seen.add(m["icao"])
            # When the match comes from municipality or IATA, surface those
            # in the dropdown label so the user knows why the row showed up.
            muni = None
            iata = None
            try:
                muni = m["municipality"]
            except (KeyError, IndexError):
                muni = None
            try:
                iata = m["iata"]
            except (KeyError, IndexError):
                iata = None
            extras: list[str] = []
            if muni and q in muni.upper() and q not in (m["name"] or "").upper():
                extras.append(muni)
            if iata and q in iata.upper() and q not in (m["name"] or "").upper():
                extras.append(f"IATA {iata}")
            label = f"{m['icao']}  ·  {m['name']}"
            if extras:
                label += "  · _" + " · ".join(extras) + "_"
            if m["country_iso"]:
                label += f"  ({m['country_iso']})"
            results.append((label, m["icao"]))
    return results


@st.fragment
def _airport_searchbox_fragment(field_key: str, label: str, default: str) -> None:
    """Searchbox dans un fragment Streamlit → les reruns du composant
    React restent locaux et n'invalident pas le reste de la page. Sans
    ce wrap, rerun_scope="fragment" lève StreamlitAPIException.

    Le résultat est exposé via st.session_state[f"{field_key}_value"]
    pour que le wrapper _apt_input (hors fragment) le récupère.
    """
    from streamlit_searchbox import st_searchbox

    selected = st_searchbox(
        _search_airports,
        placeholder="ICAO (ex. LFPB) ou nom (ex. Avignon)",
        label=label,
        default=None,
        key=field_key,
        clear_on_submit=False,
        rerun_scope="fragment",
        debounce=250,
        default_use_searchterm=True,
    )
    if isinstance(selected, str) and selected.strip():
        st.session_state[f"{field_key}_value"] = selected.strip().upper()

    # Caption rendue ici (à l'intérieur du fragment) pour qu'elle se
    # mette à jour sur le rerun local sans attendre un rerun global.
    value = st.session_state.get(f"{field_key}_value") or (default or "").strip().upper()
    if value:
        ap = db.find_airport(value)
        if ap:
            country = f" ({ap['country_iso']})" if ap['country_iso'] else ""
            st.caption(f"✓ Sélectionné : **{value}** · {ap['name']}{country}")
        else:
            st.caption(f"✓ Sélectionné : **{value}** (inconnu en base)")


def _apt_input(label: str, default: str, field_key: str) -> str:
    """Champ ICAO unifié avec autocomplete inline (streamlit-searchbox).

    Le bug historique de focus-jump à la 1ère frappe vient du rerun
    global Streamlit qui démonte/remonte le composant React. Solution :
    le searchbox est rendu dans un @st.fragment (voir
    _airport_searchbox_fragment) et configuré avec rerun_scope="fragment"
    → les reruns restent locaux, React garde son focus.
    """
    # Init la valeur de session si pas encore renseignée — laisse le
    # template (default) primer sur "rien" au premier render.
    if f"{field_key}_value" not in st.session_state and default:
        st.session_state[f"{field_key}_value"] = (default or "").strip().upper()
    _airport_searchbox_fragment(field_key, label, default)
    return (st.session_state.get(f"{field_key}_value") or "").strip().upper()


def _run_dual_suggest(
    *, sid_key: str, idx: int, kprefix: str,
    origin: str, destination: str,
    ac_type: str, ac_perf, fl: float,
    eobt, leg: dict,
) -> None:
    """Run local A* + autorouter back-to-back and stash both results into
    session_state under stable keys so the next rerender shows both cards."""
    state_key = f"_dual_sugg_{sid_key}_{idx}"
    result: dict[str, Any] = {"local": None, "ar": None, "local_err": None, "ar_err": None}

    # 1) Local A* — fast (<2 s). Always runs.
    min_rwy = int(ac_perf["min_runway_ft"]) if (ac_perf and ac_perf["min_runway_ft"]) else None
    with st.spinner("Calcul route locale (A*)…"):
        try:
            sug, sid_pick, star_pick = route_suggester.suggest_with_procedures(
                origin, destination, min_runway_ft=min_rwy,
            )
            if sug.waypoints and sug.distance_nm > 0:
                enroute = sug.route_text
                include_procs = st.session_state.get(f"{kprefix}_inc_procs", True)
                full_route = enroute
                extras: list[str] = []
                if sid_pick and include_procs:
                    exit_fix = sid_pick["connecting_fix"]
                    if enroute and not enroute.split()[0].upper() == exit_fix.upper():
                        full_route = f"{sid_pick['proc_name']} {exit_fix} {enroute}".strip()
                    else:
                        full_route = f"{sid_pick['proc_name']} {enroute}".strip()
                    extras.append(
                        f"SID **{sid_pick['proc_name']}** (rwy {sid_pick['runways_csv'] or '-'} → {exit_fix})"
                    )
                elif sid_pick:
                    extras.append(
                        f"SID candidat : **{sid_pick['proc_name']}** → {sid_pick['connecting_fix']}"
                    )
                if star_pick and include_procs:
                    entry_fix = star_pick["connecting_fix"]
                    tokens = full_route.split()
                    if tokens and tokens[-1].upper() != entry_fix.upper():
                        full_route = f"{full_route} {entry_fix} {star_pick['proc_name']}".strip()
                    else:
                        full_route = f"{full_route} {star_pick['proc_name']}".strip()
                    extras.append(
                        f"STAR **{star_pick['proc_name']}** ({entry_fix} → rwy {star_pick['runways_csv'] or '-'})"
                    )
                elif star_pick:
                    extras.append(
                        f"STAR candidate : **{star_pick['proc_name']}** ← {star_pick['connecting_fix']}"
                    )
                result["local"] = {
                    "route_text": full_route,
                    "distance_nm": sug.distance_nm,
                    "nodes": sug.nodes_explored,
                    "extras": extras,
                }
            else:
                result["local_err"] = "Pas de route trouvée (vérifie origine/destination)."
        except Exception as e:
            result["local_err"] = f"A* a échoué : {e}"

    # 2) Autorouter — slower (30-90 s). Skipped silently if not configured.
    from app import autorouter_client
    ar_cfg = autorouter_client.AutorouterConfig.from_secrets(st.secrets)
    if ar_cfg.is_configured():
        with st.spinner("Appel autorouter.aero (peut prendre 30-90 s)…"):
            try:
                ar_route = autorouter_client.suggest_route(
                    ar_cfg,
                    departure=origin, destination=destination,
                    aircraft_type=ac_type or None,
                    cruise_level=int(fl) if fl else None,
                    eobt_iso=eobt.isoformat() if eobt else None,
                    alternate1=leg.get("alternate") or None,
                )
                result["ar"] = {
                    "route_text": ar_route.route_text or ar_route.fpl,
                    "distance_nm": ar_route.distance_nm,
                    "time_min": ar_route.time_seconds // 60,
                    "logs_tail": (ar_route.log_messages or [])[-3:],
                    "route_id": ar_route.route_id,
                }
                # Persist the latest successful autorouter route_id so the
                # briefing PDF endpoint can be called later without re-running
                # a suggestion. Keyed by leg index → simple to consume.
                if ar_route.route_id:
                    st.session_state.setdefault("_ar_routes", {})[idx] = ar_route.route_id
            except autorouter_client.AutorouterError as e:
                result["ar_err"] = str(e)
    else:
        result["ar_err"] = "Pas configuré (voir page ⚙ Admin)."

    st.session_state[state_key] = result


def _render_dual_suggest(*, sid_key: str, idx: int, kprefix: str) -> None:
    """Render the two route options (local + autorouter) side by side
    with an 'Appliquer' button on each."""
    state_key = f"_dual_sugg_{sid_key}_{idx}"
    result = st.session_state.get(state_key)
    if not result:
        return

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**✨ Route locale (A\\*)**")
        if result.get("local"):
            r = result["local"]
            st.code(r["route_text"], language="text")
            st.caption(f"{r['distance_nm']:.0f} NM · {r['nodes']} nœuds explorés")
            if r["extras"]:
                for x in r["extras"]:
                    st.caption("• " + x)
            if st.button("✅ Appliquer", key=f"{kprefix}_apply_local", width="stretch"):
                st.session_state[f"_pending_route_{sid_key}_{idx}"] = r["route_text"]
                st.session_state[f"_pending_suggest_msg_{sid_key}_{idx}"] = (
                    f"Appliquée : route locale `{r['route_text']}` ({r['distance_nm']:.0f} NM)"
                )
                st.session_state.pop(state_key, None)
                st.rerun()
        else:
            st.error(result.get("local_err") or "—")

    with col_b:
        st.markdown("**🌐 Route autorouter.aero**")
        if result.get("ar"):
            r = result["ar"]
            st.code(r["route_text"] or "(vide)", language="text")
            st.caption(f"{r['distance_nm']:.0f} NM · {r['time_min']} min")
            if r["logs_tail"]:
                st.caption("IFPS : " + " · ".join(r["logs_tail"]))
            if st.button("✅ Appliquer", key=f"{kprefix}_apply_ar", width="stretch"):
                st.session_state[f"_pending_route_{sid_key}_{idx}"] = r["route_text"]
                st.session_state[f"_pending_suggest_msg_{sid_key}_{idx}"] = (
                    f"Appliquée : route autorouter `{r['route_text']}` ({r['distance_nm']:.0f} NM)"
                )
                st.session_state.pop(state_key, None)
                st.rerun()
        else:
            st.warning(result.get("ar_err") or "—")

    if st.button("✕ Fermer les suggestions", key=f"{kprefix}_close_sugg"):
        st.session_state.pop(state_key, None)
        st.rerun()


def _render_briefing_section(*, legs: list[dict]) -> None:
    """Briefing météo (METAR/TAF) + NOTAMs pour tous les aérodromes de la
    mission. Affiché en page Preview, derrière un bouton unique pour ne pas
    saturer le réseau à chaque rerun.

    Les résultats sont mis en cache dans st.session_state (clé _briefing_*)
    et invalidés dès qu'un aérodrome change — l'utilisateur peut forcer le
    rafraîchissement via le bouton "🔄 Rafraîchir".
    """
    from app import autorouter_client

    icaos: list[str] = []
    for leg in legs:
        for k in ("origin", "destination", "alternate"):
            v = (leg.get(k) or "").strip().upper()
            if v and v not in icaos:
                icaos.append(v)
    if not icaos:
        return

    ar_cfg = autorouter_client.AutorouterConfig.from_secrets(st.secrets)

    # Fenêtre de validité : du plus tôt EOBT au plus tard EOBT + 24 h.
    times: list[dt.datetime] = []
    for leg in legs:
        eobt = leg.get("eobt")
        if isinstance(eobt, dt.datetime):
            times.append(eobt)
    start_ts = int(min(times).timestamp()) if times else int(dt.datetime.utcnow().timestamp())
    end_ts = int((max(times) + dt.timedelta(hours=24)).timestamp()) if times else start_ts + 86400

    sig = f"{','.join(icaos)}|{start_ts}|{end_ts}"
    cached_sig_key = "_briefing_sig"
    cached_data_key = "_briefing_data"
    have_cache = st.session_state.get(cached_sig_key) == sig

    bc1, bc2 = st.columns([3, 1])
    with bc1:
        st.markdown("**METAR / TAF / NOTAMs**")
        st.caption(
            f"Aérodromes : {', '.join(icaos)}  ·  "
            f"Fenêtre NOTAM : {dt.datetime.utcfromtimestamp(start_ts):%Y-%m-%d %H:%MZ} → "
            f"{dt.datetime.utcfromtimestamp(end_ts):%Y-%m-%d %H:%MZ}"
        )
    with bc2:
        st.write("")
        label = "🔄 Rafraîchir" if have_cache else "🌤️ Charger"
        if st.button(label, key="briefing_fetch", width="stretch"):
            if not ar_cfg.is_configured():
                st.error("Autorouter pas configuré (voir page ⚙ Admin).")
            else:
                with st.spinner("Météo (parallèle) + NOTAMs…"):
                    wx = autorouter_client.fetch_metartaf_batch(ar_cfg, icaos)
                    notams = autorouter_client.fetch_notams(
                        ar_cfg, icaos,
                        startvalidity=start_ts, endvalidity=end_ts,
                    )
                st.session_state[cached_sig_key] = sig
                st.session_state[cached_data_key] = {"wx": wx, "notams": notams}
                st.rerun()

    if not have_cache:
        st.info("Clique sur **🌤️ Charger** pour récupérer METAR/TAF et NOTAMs depuis autorouter.")
        _render_gramet_section(legs=legs, ar_cfg=ar_cfg)
        return

    data = st.session_state.get(cached_data_key) or {}
    wx: dict = data.get("wx") or {}
    notams: list = data.get("notams") or []

    # NOTAMs groupés par premier ICAO de l'itema.
    notams_by_icao: dict[str, list] = {ic: [] for ic in icaos}
    for n in notams:
        for ic in n.itema:
            ic_u = ic.strip().upper()
            if ic_u in notams_by_icao:
                notams_by_icao[ic_u].append(n)

    for ic in icaos:
        ap = db.find_airport(ic)
        title_extras = f" · {ap['name']}" if ap else ""
        n_notams = len(notams_by_icao.get(ic, []))
        label = f"**{ic}**{title_extras}  ·  {n_notams} NOTAM(s)"
        with st.expander(label, expanded=False):
            mt = wx.get(ic)
            if mt is None:
                st.caption("METAR/TAF : non chargé.")
            elif mt.error:
                st.caption(f"METAR/TAF : _{mt.error}_")
            else:
                if mt.metar:
                    st.markdown("**METAR**")
                    st.code(mt.metar, language="text")
                else:
                    st.caption("Pas de METAR.")
                if mt.taf:
                    st.markdown("**TAF**")
                    st.code(mt.taf, language="text")
                else:
                    st.caption("Pas de TAF.")

            ns = notams_by_icao.get(ic, [])
            if ns:
                st.markdown("**NOTAMs**")
                for n in ns:
                    st.code(autorouter_client.format_notam(n), language="text")
            else:
                st.caption(
                    "Aucun NOTAM dans la fenêtre. "
                    "_(Note : autorouter ne couvre que la zone Eurocontrol EAD — "
                    "les aérodromes hors Europe peuvent rester vides.)_"
                )

    _render_gramet_section(legs=legs, ar_cfg=ar_cfg)


def _render_pre_dic_checklist(mission: dict, legs: list[dict]) -> bool:
    """Validation systématique avant export. Retourne True si OK pour générer
    (pas de check rouge), False sinon. Affiche un panneau couleurs.

    Rouge = bloquant (DIC sera rejetée ou incohérente).
    Orange = warning (acceptable mais à vérifier).
    Vert = tout passe.

    Le bouton 'Générer DIC' est désactivé tant qu'il reste du rouge.
    """
    import datetime as _dt
    reds: list[str] = []
    oranges: list[str] = []
    greens: list[str] = []

    # ─── Mission / appareil / équipage ────────────────────────────────────
    if not (mission.get("registration") or "").strip():
        reds.append("Immatriculation appareil manquante.")
    if not (mission.get("aircraft_type_icao") or "").strip():
        reds.append("Type appareil ICAO manquant.")
    if not (mission.get("callsign") or "").strip():
        oranges.append("Callsign manquant — sera repris depuis l'immat.")
    if not (mission.get("operator") or "").strip():
        reds.append("Opérateur / compagnie manquante.")
    if not (mission.get("crew_cdb") or "").strip():
        reds.append("CDB manquant.")
    if not (mission.get("crew_fo") or "").strip():
        oranges.append("FO manquant (vol mono-pilote ?).")
    if not (mission.get("poc_name") or "").strip():
        reds.append("POC (point of contact) manquant.")
    if not (mission.get("poc_phone") or "").strip():
        reds.append("Téléphone POC manquant.")
    if not (mission.get("poc_email_functional") or "").strip():
        oranges.append("Email POC fonctionnel manquant.")

    # ─── Performances appareil ────────────────────────────────────────────
    ac_type = (mission.get("aircraft_type_icao") or "").strip()
    ac_perf = db.find_aircraft_type(ac_type) if ac_type else None
    ceiling_fl = None
    range_nm = None
    if ac_perf:
        try:
            if ac_perf["service_ceiling_ft"]:
                ceiling_fl = int(ac_perf["service_ceiling_ft"]) // 100
        except (KeyError, IndexError, TypeError):
            pass
        try:
            if ac_perf["range_nm"]:
                range_nm = int(ac_perf["range_nm"])
        except (KeyError, IndexError, TypeError):
            pass

    # ─── Legs ─────────────────────────────────────────────────────────────
    valid_legs = [l for l in legs if l.get("origin") and l.get("destination")]
    if not valid_legs:
        reds.append("Aucun leg complet (origine + destination).")
    now = _dt.datetime.now(_dt.timezone.utc)
    idx_cache = country_index_cached()
    for i, leg in enumerate(valid_legs):
        ltag = f"Leg {i + 1}"
        eobt = leg.get("eobt")
        if not isinstance(eobt, _dt.datetime):
            reds.append(f"{ltag} : EOBT manquant.")
            continue
        if eobt < now:
            # Warning, pas bloquant : courant de générer une DIC en
            # archive après-coup ou de re-générer avec EOBT pas re-saisie.
            oranges.append(f"{ltag} : EOBT dans le passé ({eobt:%Y-%m-%d %H:%MZ}).")
        elif (eobt - now) < _dt.timedelta(hours=24):
            oranges.append(
                f"{ltag} : EOBT < 24 h du présent — la plupart des pays "
                f"exigent 5-14 jours de préavis pour une DIC."
            )
        fl = int(leg.get("fl") or 0)
        if fl <= 0:
            reds.append(f"{ltag} : FL non saisi.")
        elif ceiling_fl and fl > ceiling_fl:
            reds.append(
                f"{ltag} : FL{fl:03d} > plafond service appareil "
                f"(FL{ceiling_fl:03d})."
            )
        if not (leg.get("route_text") or "").strip():
            oranges.append(f"{ltag} : route texte vide — DCT par défaut.")
        if not (leg.get("alternate") or "").strip():
            oranges.append(f"{ltag} : pas d'alternate saisi.")
        # Distance vs range
        if range_nm:
            ap_o = db.find_airport(leg["origin"])
            ap_d = db.find_airport(leg["destination"])
            if ap_o and ap_d:
                try:
                    dist = route_engine._great_circle_nm(
                        (ap_o["lat"], ap_o["lon"]),
                        (ap_d["lat"], ap_d["lon"]),
                    )
                    if dist > range_nm * 0.95:
                        reds.append(
                            f"{ltag} : distance {dist:.0f} NM > range "
                            f"appareil {range_nm} NM (marge < 5 %)."
                        )
                    elif dist > range_nm * 0.80:
                        oranges.append(
                            f"{ltag} : distance {dist:.0f} NM proche du "
                            f"range appareil ({range_nm} NM) — vérifier "
                            f"carburant + alternate."
                        )
                except Exception:
                    pass

    # ─── Cohérence cross-leg ──────────────────────────────────────────────
    if len(valid_legs) > 1:
        # Toutes les destinations doivent matcher l'origine du leg suivant
        for i in range(len(valid_legs) - 1):
            if valid_legs[i]["destination"].strip().upper() != valid_legs[i + 1]["origin"].strip().upper():
                oranges.append(
                    f"Discontinuité entre Leg {i + 1} ({valid_legs[i]['destination']}) "
                    f"et Leg {i + 2} ({valid_legs[i + 1]['origin']}) — vol ferry ? Vérifie."
                )

    if not reds and not oranges:
        greens.append("Tout est cohérent.")

    # ─── Rendu UI ─────────────────────────────────────────────────────────
    st.markdown("### ✅ Vérification pré-DIC")
    if reds:
        st.error(
            "**Blocages — DIC ne sera pas générée tant que ce n'est pas corrigé :**\n"
            + "\n".join(f"- {m}" for m in reds)
        )
    if oranges:
        st.warning(
            "**Warnings — DIC peut être générée mais à vérifier :**\n"
            + "\n".join(f"- {m}" for m in oranges)
        )
    if greens:
        st.success("✓ " + greens[0])
    return not reds


def _render_gramet_section(*, legs: list[dict], ar_cfg) -> None:
    """GRAMET (coupe verticale météo) par leg. Téléchargeable en PDF.

    Particulièrement utile en Afrique de l'Ouest où la convection (CB,
    harmattan, mousson) varie vite — la coupe montre les couches nuageuses
    et les vents en altitude le long de la route.
    """
    from app import autorouter_client
    valid_legs = [
        l for l in legs
        if l.get("origin") and l.get("destination") and isinstance(l.get("eobt"), dt.datetime)
    ]
    if not valid_legs:
        return
    st.markdown("#### 📈 GRAMET (coupe verticale météo)")
    st.caption(
        "Téléchargeable en PDF par leg. Synchrone : 5-15 s par appel."
    )
    for i, leg in enumerate(valid_legs):
        origin = leg["origin"].strip().upper()
        destination = leg["destination"].strip().upper()
        route_text = (leg.get("route_text") or "").strip()
        # autorouter resolves waypoints by name; pass dep + enroute + dest
        # so its parser has a complete chain.
        wpts = " ".join([origin] + route_text.split() + [destination]) if route_text else f"{origin} {destination}"
        fl = int(leg.get("fl") or 90)
        tas = int(leg.get("tas") or 150)
        eobt: dt.datetime = leg["eobt"]
        # Estimate flight duration from great-circle / TAS as a fallback —
        # GRAMET only needs an envelope, not a precise EET.
        eet_s = 0
        ap_o = db.find_airport(origin)
        ap_d = db.find_airport(destination)
        if ap_o and ap_d:
            try:
                from app import route_engine
                dist_nm = route_engine._great_circle_nm(
                    (ap_o["lat"], ap_o["lon"]), (ap_d["lat"], ap_d["lon"])
                )
                eet_s = int((dist_nm / max(60, tas)) * 3600)
            except Exception:
                pass
        if eet_s <= 0:
            eet_s = 3600  # 1h fallback
        key = f"gramet_{i}_{origin}_{destination}"
        if st.button(
            f"📈 GRAMET leg {i + 1} : {origin} → {destination} (FL{fl:03d})",
            key=key,
        ):
            if not ar_cfg.is_configured():
                st.error("Autorouter pas configuré (voir page ⚙ Admin).")
            else:
                with st.spinner("Génération GRAMET…"):
                    try:
                        data, mime = autorouter_client.fetch_gramet(
                            ar_cfg,
                            waypoints=wpts,
                            altitude_ft=fl * 100,
                            departure_ts=int(eobt.timestamp()),
                            totaleet_s=eet_s,
                            fmt="pdf",
                        )
                        st.download_button(
                            f"⬇️ GRAMET_{origin}_{destination}.pdf",
                            data=data, mime=mime,
                            file_name=f"GRAMET_{origin}_{destination}.pdf",
                            key=f"{key}_dl",
                        )
                    except autorouter_client.AutorouterError as e:
                        st.error(f"GRAMET : {e}")

    # Briefing pack PDF (ops-oriented) — nécessite un route_id obtenu via
    # une suggestion autorouter récente. Le bouton est désactivé tant qu'on
    # n'a pas tourné le 🤖 Suggérer avec succès côté autorouter sur un leg.
    ar_routes: dict = st.session_state.get("_ar_routes") or {}
    st.markdown("#### 📦 Briefing pack (ops complet, PDF)")
    st.caption(
        "Pack autorouter complet : navlog, W&B, perfs, METAR/TAF, GRAMET, "
        "SIGWX, NOTAM graphique, **ATC briefing + milbulletin + ATC charges** "
        "(orienté ops). Génération asynchrone côté autorouter : 1-5 min."
    )
    if not ar_routes:
        st.info(
            "Pour activer ce bouton : lance d'abord **🤖 Suggérer** sur un "
            "leg en page Legs avec succès côté autorouter. Le `route_id` "
            "récupéré sera réutilisé ici."
        )
    else:
        for leg_idx, route_id in sorted(ar_routes.items()):
            if leg_idx >= len(valid_legs):
                continue
            l = valid_legs[leg_idx] if leg_idx < len(valid_legs) else None
            if not l:
                continue
            label = (
                f"📦 Briefing PDF leg {leg_idx + 1} : "
                f"{l['origin']} → {l['destination']}"
            )
            if st.button(label, key=f"briefing_{leg_idx}_{route_id}"):
                if not ar_cfg.is_configured():
                    st.error("Autorouter pas configuré (voir page ⚙ Admin).")
                else:
                    with st.spinner("Compilation du briefing par autorouter (1-5 min)…"):
                        try:
                            pdf_bytes = autorouter_client.fetch_briefing_pack(
                                ar_cfg, route_id,
                                items=autorouter_client.BRIEFING_OPS_ITEMS,
                            )
                            st.download_button(
                                f"⬇️ Briefing_{l['origin']}_{l['destination']}.pdf",
                                data=pdf_bytes, mime="application/pdf",
                                file_name=f"Briefing_{l['origin']}_{l['destination']}.pdf",
                                key=f"briefing_dl_{leg_idx}",
                            )
                        except autorouter_client.AutorouterError as e:
                            st.error(f"Briefing : {e}")


def _leg_editor(idx: int, leg: dict) -> dict:
    sid = _legs_sid()
    kprefix = f"leg_s{sid}_{idx}"

    # Apply any pending route suggestion BEFORE the route_text widget renders.
    pending_route = st.session_state.pop(f"_pending_route_{sid}_{idx}", None)
    if pending_route is not None:
        st.session_state[f"{kprefix}_route"] = pending_route

    st.markdown(f"### Leg {idx + 1}")
    c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
    with c1:
        origin = _apt_input("Origin (ICAO)", leg.get("origin", ""), f"{kprefix}_orig")
    with c2:
        destination = _apt_input("Destination (ICAO)", leg.get("destination", ""), f"{kprefix}_dest")
    with c3:
        fl = st.number_input("FL", min_value=0, max_value=600, value=int(leg.get("fl", 90)), step=10, key=f"{kprefix}_fl")
    with c4:
        tas = st.number_input("TAS (kt)", min_value=50, max_value=900, value=int(leg.get("tas", 140)), step=10, key=f"{kprefix}_tas")

    ac_type = (st.session_state.get("mission") or {}).get("aircraft_type_icao") or ""
    ac_perf = db.find_aircraft_type(ac_type) if ac_type else None
    if ac_perf and ac_perf["service_ceiling_ft"]:
        ceiling_fl = int(ac_perf["service_ceiling_ft"]) // 100
        if int(fl) > ceiling_fl:
            st.error(
                f"⚠️ FL{int(fl):03d} dépasse le plafond service {ac_type} "
                f"({ac_perf['service_ceiling_ft']} ft = FL{ceiling_fl:03d}). "
                f"Réduis le FL ou change d'appareil."
            )
    if ac_perf and ac_perf["climb_gradient_pct"] and origin and destination and int(fl) > 0:
        ap_o = db.find_airport(origin)
        ap_d = db.find_airport(destination)
        if ap_o and ap_d:
            leg_nm = route_engine._great_circle_nm(
                (ap_o["lat"], ap_o["lon"]), (ap_d["lat"], ap_d["lon"])
            )
            grad = float(ac_perf["climb_gradient_pct"])
            descent_grad = 5.0  # ~3° standard descent profile for light aircraft
            alt_ft = int(fl) * 100
            climb_nm = alt_ft / (60.76 * grad)
            descent_nm = alt_ft / (60.76 * descent_grad)
            needed = climb_nm + descent_nm
            if leg_nm < needed:
                max_alt_ft = leg_nm * 60.76 * grad * descent_grad / (grad + descent_grad)
                max_fl = int(max_alt_ft // 100 // 10) * 10  # round down to nearest 10
                st.warning(
                    f"FL{int(fl):03d} géométriquement irréaliste sur ce leg : "
                    f"montée ≈ {climb_nm:.0f} NM + descente ≈ {descent_nm:.0f} NM "
                    f"= {needed:.0f} NM requis, leg = {leg_nm:.0f} NM. "
                    f"Le DIC reporterait des heures de passage fausses. "
                    f"FL conseillé ≤ FL{max_fl:03d}."
                )

    c5, c6 = st.columns(2)
    with c5:
        d = st.date_input("Date (UTC)", value=leg.get("date", dt.date.today()), key=f"{kprefix}_date")
    with c6:
        t = st.time_input(
            "EOBT (UTC)",
            value=leg.get("eobt_time", dt.time(0, 0)),
            key=f"{kprefix}_eobt",
        )
    eobt = dt.datetime.combine(d, t).replace(tzinfo=dt.timezone.utc)

    # Détection auto d'une route catalogue (opérateur officiel) pour ce
    # couple O/D. Si match, on propose un bouton "📌 Route officielle"
    # qui pré-remplit route_text + alternate + (info) payload, temps de
    # vol, distance. Wrapped en try/except : si le helper db.* manque
    # (déploiement partiel) ou le seed n'a pas tourné, on dégrade en
    # silence sans casser le leg editor.
    if origin and destination and hasattr(db, "find_canonical_routes"):
        try:
            canon_rows = db.find_canonical_routes(origin, destination, ac_type or None)
        except Exception:
            canon_rows = []
        # Pending apply (depuis click d'un bouton du tour précédent)
        pending_canon = st.session_state.pop(f"{kprefix}_pending_canon", None)
        if pending_canon is not None:
            st.session_state[f"{kprefix}_route"] = pending_canon["route_text"]
            st.session_state[f"{kprefix}_alternate"] = pending_canon["alternate"] or ""
        if canon_rows:
            with st.expander(
                f"📌 {len(canon_rows)} route(s) officielle(s) "
                f"{origin}→{destination} ({canon_rows[0]['operator'] or '—'})",
                expanded=True,
            ):
                st.caption(
                    "⚠️ **Routes mandatoires** côté opérateur. "
                    "Perfs calibrées **ISA+20°C, still air, OEW DHC6-400 "
                    "TY-BAB 3813 kg** — pas de marge head/tailwind."
                )
                for r in canon_rows:
                    cols = st.columns([3, 1, 1, 1, 1])
                    with cols[0]:
                        legs_json = json.loads(r["legs_json"])
                        rt = legs_json[0]["route_text"]
                        st.markdown(f"**{r['variant'] or '—'}** — alt `{r['alternate'] or '—'}`")
                        st.caption(f"`{rt}`")
                    with cols[1]:
                        st.metric("Dist", f"{r['distance_nm']:.0f} NM" if r["distance_nm"] else "—")
                    with cols[2]:
                        st.metric("Payload", f"{r['payload_kg']} kg" if r["payload_kg"] else "—")
                    with cols[3]:
                        ft = r["flight_time_min"]
                        st.metric("Time", f"{ft // 60}h{ft % 60:02d}" if ft else "—")
                    with cols[4]:
                        st.write("")
                        if st.button(
                            "📌 Appliquer",
                            key=f"{kprefix}_apply_canon_{r['id']}",
                            help=(
                                f"Alt: {r['alternate'] or '—'} · "
                                f"calibré pour {r['aircraft_type'] or '?'}"
                            ),
                        ):
                            st.session_state[f"{kprefix}_pending_canon"] = {
                                "route_text": json.loads(r["legs_json"])[0]["route_text"],
                                "alternate": r["alternate"],
                            }
                            st.rerun()

    rc1, rc2 = st.columns([4, 1])
    with rc1:
        route_text = st.text_input(
            "Route texte ICAO (ex. `TYE POLTO LAG L433 IBA R778 TEGDA MNA`)",
            value=leg.get("route_text", ""),
            key=f"{kprefix}_route",
        )
    with rc2:
        st.write("")
        st.write("")
        if st.button(
            "🤖 Suggérer",
            key=f"{kprefix}_suggest",
            help="Lance les deux moteurs (A* local + autorouter.aero) et affiche les deux routes. Tu choisis celle à appliquer.",
            width="stretch",
        ):
            if origin and destination:
                _run_dual_suggest(
                    sid_key=sid, idx=idx, kprefix=kprefix,
                    origin=origin, destination=destination,
                    ac_type=ac_type, ac_perf=ac_perf, fl=fl,
                    eobt=eobt, leg=leg,
                )
                st.rerun()

    # Render dual-suggest results (both A* local + autorouter) with
    # an "Appliquer" button on each so the operator picks.
    _render_dual_suggest(sid_key=sid, idx=idx, kprefix=kprefix)

    st.checkbox(
        "Inclure SID/STAR auto dans la route",
        value=leg.get("include_procedures", True),
        key=f"{kprefix}_inc_procs",
        help=(
            "Active : Suggérer ajoute SID + STAR à la route. "
            "Désactive si ton validateur (RocketRoute, IFPS, autorouter) "
            "rejette ces procédures pour ce terrain (couverture commerciale "
            "incomplète sur certains airports) — la route devient enroute "
            "seul, généralement validée."
        ),
    )

    # Auto-fill alternate from historical patterns (most-common alternate
    # seen for this destination across all saved templates). Only applies
    # when the user hasn't typed anything yet.
    suggested_alt = leg.get("alternate", "")
    if not suggested_alt and destination:
        suggested_alt = db.default_alternate_for(destination) or ""
    alternate = st.text_input(
        f"Alternate de {destination or 'destination'} (ICAO)",
        value=suggested_alt,
        key=f"{kprefix}_alternate",
        help=(
            "Aérodrome de déroutement si l'arrivée est impossible. Un par leg. "
            "Pré-rempli automatiquement avec l'alternate le plus fréquent pour "
            "cette destination dans les missions sauvegardées."
        ),
    ).strip().upper()

    msg = st.session_state.pop(f"_pending_suggest_msg_{sid}_{idx}", None)
    if msg:
        st.success(msg)

    # SID / STAR manual override — hidden in an expander by default. The
    # auto-pick in "✨ Suggérer" handles the common case; this is the escape
    # hatch when the auto choice is wrong (e.g. ATIS specifies a different
    # runway). Lists are pre-filtered by the aircraft's min runway length.
    sids = db.list_procedures(origin, "SID") if origin else []
    stars = db.list_procedures(destination, "STAR") if destination else []
    if (sids or stars) and (ac_perf and ac_perf["min_runway_ft"]):
        min_rwy_filter = int(ac_perf["min_runway_ft"])
        def _ok(rec, icao):
            ok, _ = route_suggester._runways_compatible(icao, rec["runways_csv"], min_rwy_filter)
            return bool(ok) if rec["runways_csv"] else True
        sids = [r for r in sids if _ok(r, origin)]
        stars = [r for r in stars if _ok(r, destination)]
    if sids or stars:
        with st.expander("🔧 Override SID/STAR manuel", expanded=False):
            pc1, pc2 = st.columns(2)
            with pc1:
                if sids:
                    opts = ["— aucun —"] + [
                        f"{r['proc_name']}  (rwy {r['runways_csv'] or '-'})  →  "
                        + (json.loads(r['waypoints_json'])[-1] if json.loads(r['waypoints_json']) else '?')
                        for r in sids
                    ]
                    pick = st.selectbox(f"SID au départ {origin}", opts, key=f"{kprefix}_sid")
                    if pick != "— aucun —":
                        chosen_name = pick.split()[0]
                        current_route = route_text or ""
                        if not current_route.upper().startswith(chosen_name):
                            new_route = f"{chosen_name} {current_route}".strip()
                            if new_route != current_route:
                                st.session_state[f"_pending_route_{sid}_{idx}"] = new_route
                                st.rerun()
            with pc2:
                if stars:
                    opts = ["— aucun —"] + [
                        f"{r['proc_name']}  (rwy {r['runways_csv'] or '-'})  ←  "
                        + (json.loads(r['waypoints_json'])[0] if json.loads(r['waypoints_json']) else '?')
                        for r in stars
                    ]
                    pick = st.selectbox(f"STAR à l'arrivée {destination}", opts, key=f"{kprefix}_star")
                    if pick != "— aucun —":
                        chosen_name = pick.split()[0]
                        current_route = route_text or ""
                        if not current_route.upper().endswith(chosen_name):
                            new_route = f"{current_route} {chosen_name}".strip()
                            if new_route != current_route:
                                st.session_state[f"_pending_route_{sid}_{idx}"] = new_route
                                st.rerun()

    return {
        "origin": origin,
        "destination": destination,
        "fl": int(fl),
        "tas": int(tas),
        "date": d,
        "eobt_time": t,
        "eobt": eobt,
        "route_text": route_text,
        "alternate": alternate,
        "include_procedures": st.session_state.get(f"{kprefix}_inc_procs", True),
    }


def _resolve_country_for_airport(icao: str) -> str | None:
    ap = db.find_airport(icao)
    return ap["country_iso"] if ap else None


def _missing_point_form(label: str, key: str) -> None:
    with st.expander(f"Résoudre '{label}'"):
        c1, c2, c3 = st.columns(3)
        with c1:
            lat = st.number_input("Latitude (°)", value=0.0, format="%.6f", key=f"{key}_lat")
        with c2:
            lon = st.number_input("Longitude (°)", value=0.0, format="%.6f", key=f"{key}_lon")
        with c3:
            region = st.text_input("Région ISO (ex. NG, BJ — optionnel)", key=f"{key}_region").upper()
        if st.button(f"Ajouter {label}", key=f"{key}_add"):
            if label.startswith(("N", "S")) and "/" in label:
                st.error("Coordonnée brute déjà parseable — ne pas la rajouter.")
            else:
                db.save_user_waypoint(label, float(lat), float(lon), region or None)
                st.success(f"Waypoint {label} sauvé. Relance le calcul.")
                st.rerun()


# ============ UI ============

st.title("🛩️ DIC Agent — Diplomatic Clearance generator")
st.caption("FRA / ICAO — application locale. Données : OurAirports + Natural Earth.")

if "legs" not in st.session_state:
    st.session_state.legs = [
        {"origin": "", "destination": "", "fl": 90, "tas": 140, "route_text": ""}
    ]
if "page_idx" not in st.session_state:
    st.session_state.page_idx = 0

# Wizard-style navigation. The 3 sections are presented as large buttons in
# the sidebar AND as a "Suivant →" CTA at the bottom of each section, so a
# non-technical user always sees both the current step and how to advance.
# Why not st.tabs: Streamlit's tab bar scrolls out of view on long forms,
# and no reliable CSS makes it sticky across versions.
PAGES = [
    ("1.", "Mission & profils", "Avion, équipage, compagnie"),
    ("2.", "Legs", "Itinéraire, dates, route"),
    ("3.", "Preview & export", "Récapitulatif des legs, validation, puis export .docx + FPL + briefing"),
    ("📋", "Historique", "Missions enregistrées, routes en base"),
    ("⚙", "Admin", "Aérodromes hors ICAO, API autorouter, config"),
]


def _goto_page(idx: int) -> None:
    st.session_state.page_idx = max(0, min(idx, len(PAGES) - 1))
    # Drapeau lu en haut du rendu pour réinitialiser le scroll. Streamlit
    # préserve le scroll entre les reruns, donc on injecte un JS one-shot.
    st.session_state._scroll_top = True


def _scroll_to_top_if_needed() -> None:
    """Inject a one-shot scroll-to-top when a page change just happened.
    Sans ça, passer de Legs → Preview laisse la fenêtre tout en bas, ce
    qui désoriente l'utilisateur."""
    if not st.session_state.pop("_scroll_top", False):
        return
    import streamlit.components.v1 as components
    components.html(
        "<script>"
        "  const doc = window.parent.document;"
        "  doc.documentElement.scrollTo({top: 0, behavior: 'instant'});"
        "  const main = doc.querySelector('section.main');"
        "  if (main) main.scrollTo({top: 0, behavior: 'instant'});"
        "</script>",
        height=0,
    )


_mission_done = bool((st.session_state.get("mission") or {}).get("registration"))
_legs_done = any(
    leg.get("origin") and leg.get("destination") and leg.get("route_text")
    for leg in st.session_state.legs
)

with st.sidebar:
    st.header("🛩️ DIC Agent")
    st.caption("Suis les 3 étapes ci-dessous, dans l'ordre.")
    for i, (num, title, subtitle) in enumerate(PAGES):
        is_current = (st.session_state.page_idx == i)
        if i == 0:
            done = _mission_done
        elif i == 1:
            done = _legs_done
        else:
            done = False
        icon = "✅" if (done and not is_current) else ("▶" if is_current else "○")
        btn_label = f"{icon}  {num} {title}"
        btn_help = subtitle
        clicked = st.button(
            btn_label, key=f"nav_step_{i}", help=btn_help,
            width="stretch",
            type="primary" if is_current else "secondary",
        )
        if clicked:
            _goto_page(i)
            st.rerun()
    st.divider()
    # FRA/ICAO toggle removed: every reference DIC the user has shipped uses
    # the same Annex A layout. The single format that matches those samples
    # is now always used. Kept as a constant so downstream calls (format_zulu)
    # keep working without an inline string.
    template_format = "FRA"

    # Admin sections (aérodromes hors ICAO, autorouter API) ont migré
    # vers la 4e page "⚙ Admin" pour désencombrer la sidebar.

    st.divider()
    st.caption(
        "Astuce : pour un point de coordonnées brutes dans la route, format "
        "`N 9°34'45.56\" / E 3°14'7.09\"` ou `N9 34 45 / E3 14 7`."
    )

page_idx = st.session_state.page_idx

# Top horizontal nav — visible on mobile too (sidebar is collapsed on small
# screens by default). Three big buttons, current step highlighted in primary.
_top_cols = st.columns(len(PAGES))
for i, (num, title, _sub) in enumerate(PAGES):
    with _top_cols[i]:
        is_current = (page_idx == i)
        clicked = st.button(
            f"{num} {title}",
            key=f"topnav_{i}",
            width="stretch",
            type="primary" if is_current else "secondary",
        )
        if clicked:
            _goto_page(i)
            st.rerun()

_scroll_to_top_if_needed()
st.markdown(f"### {PAGES[page_idx][0]} {PAGES[page_idx][1]}")
st.caption(PAGES[page_idx][2])
st.divider()


def _step_nav_footer() -> None:
    """Big Précédent / Suivant CTAs at the bottom of each step.

    Admin (page_idx 3) est hors flux linéaire : on n'y propose pas de
    Précédent → Preview et on ne propose pas Suivant → Admin depuis
    Preview. L'accès Admin se fait uniquement via la sidebar."""
    LINEAR_MAX = 2  # last sequential page (Preview & export)
    st.divider()
    c_prev, c_spacer, c_next = st.columns([2, 3, 2])
    with c_prev:
        if 0 < page_idx <= LINEAR_MAX:
            if st.button(
                f"← Précédent : {PAGES[page_idx - 1][1]}",
                key=f"prev_step_{page_idx}", width="stretch",
            ):
                _goto_page(page_idx - 1)
                st.rerun()
    with c_next:
        if page_idx < LINEAR_MAX:
            if st.button(
                f"Suivant : {PAGES[page_idx + 1][1]}  →",
                key=f"next_step_{page_idx}", width="stretch",
                type="primary",
            ):
                _goto_page(page_idx + 1)
                st.rerun()


if page_idx == 0:
    c1, c2, c3 = st.columns(3)
    with c1:
        reference = st.text_input("Reference number", value="MSG DU " + dt.date.today().strftime("%d/%m/%Y"))
    with c2:
        amendment = st.text_input("Amendment", value="V1")
    with c3:
        mission_number = st.text_input("Mission number", value="")

    st.divider()
    st.subheader("Compagnie")
    selected_operator = _operator_picker("mission")
    # Les sélecteurs Appareil / Crew / POC utilisent un key dynamique qui
    # inclut un slug de l'operator (voir _op_slug). Streamlit recrée donc
    # un widget vierge à chaque changement de compagnie → la 1ère option
    # de la nouvelle liste se sélectionne automatiquement, plus de stale
    # value héritée de l'opérateur précédent.
    st.divider()
    st.subheader("Appareil")
    ap = _aircraft_picker("mission", operator=selected_operator)
    # If user just typed a new operator, override what the aircraft picker returned.
    if selected_operator and not ap.get("operator"):
        ap["operator"] = selected_operator
    st.divider()
    st.subheader("Équipage")
    crew = _crew_picker("mission", operator=selected_operator)
    st.divider()
    st.subheader("POC")
    poc = _poc_picker("mission", operator=selected_operator)

    st.divider()
    # Indicateurs (sensors / armament / EW / VIP / DG) used to live here as
    # a five-checkbox row. Removed entirely: every reference DIC defaults to
    # NO/NIL and the user now puts any exceptional info (VIP onboard, DG,
    # sensitive equipment) directly in the Remarks block (30).

    st.subheader("Vol")
    purpose = st.text_input("Purpose of flight", value="LOGISTIC FLIGHT WITHOUT DANGEROUS GOODS")
    # Alternates moved to per-leg in the Legs tab (one alternate per arrival
    # airport). We aggregate them at DIC-export time.
    radio_freq = st.text_input("Radio frequencies", value="V/U/HF")
    n_passengers = st.text_input("Number of passengers", value="TBN")
    remarks = st.text_area("Remarks", value="")

    st.session_state.mission = {
        "reference": reference,
        "amendment": amendment,
        "mission_number": mission_number,
        "template_format": template_format,
        "requesting_state": "FRANCE",
        "operator": ap.get("operator", ""),
        "aircraft_type_icao": (ap.get("type_icao") or "").strip().upper(),
        "aircraft_count_type": f"1  {ap.get('type_icao','')}",
        "registration": ap.get("registration", ""),
        # Reference DICs use '/' for spare aircraft (no spare on file) and
        # put the 'OR SUBSTITUTE' suffix in the callsign field instead.
        "spare_aircraft": "/",
        "callsign": f"{ap.get('callsign') or ap.get('registration','')} OR SUBSTITUTE".strip(),
        "n_crew": crew.get("n_crew", 2),
        "pilots": crew.get("pilots", ""),
        # Champs séparés CDB/FO consommés par la validation pré-DIC et
        # par les fonctions d'export (pack ZIP résumé, etc.) — `pilots`
        # reste le texte concaténé pour l'item (17) du DIC.
        "crew_cdb": crew.get("cdb", ""),
        "crew_fo": crew.get("fo", ""),
        "purpose": purpose,
        # `alternates` is aggregated from per-leg entries at export time;
        # see export logic that joins legs[*].alternate with " / ".
        "radio_frequencies": radio_freq,
        "n_passengers": n_passengers,
        "remarks": remarks,
        "poc_name": poc.get("name", ""),
        "poc_phone": poc.get("phone", ""),
        "poc_email_personal": poc.get("email_personal", ""),
        "poc_email_functional": poc.get("email_functional", ""),
        "poc_fax": poc.get("fax", ""),
    }


_NEW_MISSION_LABEL = "✨ Nouvelle mission"

# Operator → home dossier mapping. Auto-saved templates use this to decide
# in which folder to file a new mission, regardless of where the route flies.
# Extend as more operators come on board.
OPERATOR_FOLDER = {
    "AMAZONE AIRLINES / DYNAMI AVIATION OPS": "Bénin",
}


def _apply_template(tpl_name: str, filtered_templates: list) -> None:
    """Populate st.session_state.legs from a template. Bumps the legs sid so
    every leg widget key becomes fresh and starts with value= defaults."""
    tpl = next((r for r in filtered_templates if r["name"] == tpl_name), None)
    if tpl is None:
        return
    payload = json.loads(tpl["legs_json"])
    legs_data = payload.get("legs") if isinstance(payload, dict) else payload
    base_date = dt.date.today()
    _bump_legs_sid()
    st.session_state.legs = []
    for li, leg_data in enumerate(legs_data or []):
        hours_from_six = li * 2
        if hours_from_six < 16:
            eobt_day = base_date
            eobt_hour = 6 + hours_from_six
        else:
            rollover = hours_from_six - 16
            day_offset = 1 + rollover // 16
            eobt_day = base_date + dt.timedelta(days=day_offset)
            eobt_hour = 6 + (rollover % 16)
        st.session_state.legs.append({
            "origin": leg_data.get("origin") or "",
            "destination": leg_data.get("destination") or "",
            "fl": leg_data.get("fl") or 90,
            "tas": leg_data.get("tas") or 140,
            "date": eobt_day,
            "eobt_time": dt.time(eobt_hour, 0),
            "route_text": leg_data.get("route_text") or "",
        })


def _reset_to_blank_mission() -> None:
    _bump_legs_sid()
    st.session_state.legs = [
        {"origin": "", "destination": "", "fl": 90, "tas": 140,
         "date": dt.date.today(), "eobt_time": dt.time(0, 0), "route_text": ""}
    ]
    st.session_state.pop("_loaded_tpl_name", None)


    _step_nav_footer()

if page_idx == 1:
    tpl_rows = db.list_route_templates()
    by_cat: dict[str, list] = {}
    for r in tpl_rows:
        cat = r["category"] if "category" in r.keys() and r["category"] else "Autres"
        by_cat.setdefault(cat, []).append(r)
    cats = sorted(by_cat.keys())

    # ────────────────────────────────────────────────────────────────────
    # Pending-application pattern.
    #
    # The Streamlit on_change callbacks below only *flag* what should happen
    # (`_pending_mission`). The actual st.session_state.legs mutation and
    # widget-key clearing run HERE, at the very top of the legs tab, BEFORE
    # any leg widget is created in this rerun. This guarantees that newly
    # rendered widgets read from the fresh st.session_state.legs and don't
    # leak stale values from the previous mission.
    # ────────────────────────────────────────────────────────────────────
    pending = st.session_state.pop("_pending_mission", None)
    if pending is not None:
        if pending == _NEW_MISSION_LABEL:
            _reset_to_blank_mission()
        else:
            cat_sel_now = st.session_state.get("tpl_cat", "— tous —")
            filtered_now = (
                tpl_rows if cat_sel_now == "— tous —" else by_cat.get(cat_sel_now, [])
            )
            _apply_template(pending, filtered_now)
            st.session_state["_loaded_tpl_name"] = pending

    def _on_dossier_change() -> None:
        # Switching dossier resets the Mission picker AND flags a reset so the
        # legs are wiped before the next render.
        st.session_state["tpl_sel"] = _NEW_MISSION_LABEL
        st.session_state["_pending_mission"] = _NEW_MISSION_LABEL

    def _on_mission_change() -> None:
        # Just flag — the apply logic lives at the top of the legs tab.
        st.session_state["_pending_mission"] = st.session_state.get(
            "tpl_sel", _NEW_MISSION_LABEL
        )

    pc1, pc2 = st.columns([1, 3])
    with pc1:
        cat_sel = st.selectbox(
            "Dossier", ["— tous —"] + cats, key="tpl_cat",
            on_change=_on_dossier_change,
            disabled=not cats,
        )
    with pc2:
        # Strict filter: under "— tous —", hide all pre-filled templates.
        # Templates surface only when their explicit dossier is selected.
        # This avoids cluttering the new-user view with operator-specific
        # pre-seeded missions they may not know about.
        filtered = [] if cat_sel == "— tous —" else by_cat.get(cat_sel, [])
        tpl_options = [_NEW_MISSION_LABEL] + [r["name"] for r in filtered]
        sel_tpl = st.selectbox(
            "Mission",
            tpl_options,
            key="tpl_sel",
            on_change=_on_mission_change,
            help="Choisis une mission existante pour pré-remplir les legs, ou « Nouvelle mission » pour repartir vierge.",
        )

    if st.session_state.get("_loaded_tpl_name") and sel_tpl == st.session_state.get("_loaded_tpl_name"):
        st.caption(f"🗂️ Mission active : **{st.session_state['_loaded_tpl_name']}**")

    for i, leg in enumerate(st.session_state.legs):
        st.session_state.legs[i] = _leg_editor(i, leg)

    # Inline +/− leg controls at the bottom — concise, no header noise.
    lc1, lc2, lc3, _ = st.columns([1, 1, 2, 3])
    with lc1:
        if st.button("➕ Leg", help="Ajouter un leg"):
            st.session_state.legs.append(
                {"origin": "", "destination": "", "fl": 90, "tas": 140, "route_text": ""}
            )
            st.rerun()
    with lc2:
        if len(st.session_state.legs) > 1 and st.button("➖ Leg", help="Retirer le dernier leg"):
            st.session_state.legs.pop()
            st.rerun()
    with lc3:
        valid = [l for l in st.session_state.legs if l.get("origin") and l.get("destination")]
        if valid and st.button("💾 Enregistrer la route", help="Sauve cette route en base, accessible depuis la page Historique"):
            mission = st.session_state.get("mission") or {}
            operator = (mission.get("operator") or "").strip()
            origin_iso = _resolve_country_for_airport(valid[0]["origin"])
            country_name = db.find_country_name(origin_iso) if origin_iso else None
            folder = country_name.title() if country_name else (
                OPERATOR_FOLDER.get(operator) or origin_iso or "Divers"
            )
            sanitised = [
                {
                    "order": i + 1, "origin": l["origin"], "destination": l["destination"],
                    "route_text": l.get("route_text", ""), "fl": l.get("fl", 90),
                    "tas": l.get("tas", 140),
                }
                for i, l in enumerate(valid)
            ]
            parts = [sanitised[0]["origin"]] + [l["destination"] for l in sanitised]
            tpl_name = f"{folder} / " + " → ".join(dict.fromkeys(parts))
            with db.connect() as c:
                c.execute(
                    "INSERT INTO route_template (name, category, legs_json) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET "
                    "  category = excluded.category, legs_json = excluded.legs_json",
                    (tpl_name, folder, json.dumps(sanitised, ensure_ascii=False)),
                )
            st.success(f"💾 Route enregistrée : `{tpl_name}` (dossier *{folder}*).")

    _step_nav_footer()

if page_idx == 2:
    # Streamlit garde la position de scroll de la page précédente — sur
    # Preview qui est plus longue que Mission/Legs, ça atterrit en bas.
    # On force le scroll en haut via une injection JS one-shot.
    st.markdown(
        "<script>window.parent.document.querySelector('section.main').scrollTo({top: 0, behavior: 'instant'});</script>",
        unsafe_allow_html=True,
    )
    if not st.session_state.legs or not any(l.get("origin") for l in st.session_state.legs):
        st.info("Saisis au moins un leg avec une origine.")
        st.stop()

    idx = country_index_cached()
    leg_payloads: list[dict] = []
    all_warnings: list[str] = []
    departures = []
    destinations = []
    date_range_min = None
    date_range_max = None

    for i, leg in enumerate(st.session_state.legs):
        if not leg.get("origin") or not leg.get("destination"):
            continue
        st.markdown(f"#### Leg {i + 1} — {leg['origin']} → {leg['destination']}")
        resolution = route_engine.compute_leg(
            eobt=leg["eobt"],
            origin_icao=leg["origin"],
            destination_icao=leg["destination"],
            route_text=leg["route_text"],
            fl=leg["fl"],
            tas_kt=leg["tas"],
            country_index=idx,
        )
        if resolution.warnings:
            for w in resolution.warnings:
                st.warning(w)
                all_warnings.append(w)

        seen_missing: dict[str, int] = {}
        for pidx, p in enumerate(resolution.points):
            if not p.missing:
                continue
            seen_missing[p.label] = seen_missing.get(p.label, 0) + 1
            _missing_point_form(p.label, key=f"miss_{i}_{pidx}_{seen_missing[p.label]}_{p.label}")

        c1, c2, c3 = st.columns(3)
        c1.metric("Distance", f"{resolution.total_distance_nm:.0f} NM")
        c2.metric("Temps de vol", f"{resolution.total_time_min:.0f} min")
        c3.metric("Pays traversés", str(len(resolution.segments)))

        rows_view = []
        for seg in resolution.segments:
            rows_view.append(
                {
                    "State": seg.state_name,
                    "Entry": f"{seg.entry_label} · {route_engine.format_zulu(seg.entry_time, template_format) if seg.entry_time else ''}",
                    "Route": seg.route_in_country,
                    "Exit": f"{seg.exit_label} · {route_engine.format_zulu(seg.exit_time, template_format) if seg.exit_time else ''}",
                    "FL": seg.fl,
                    "TAS": seg.tas,
                }
            )
        st.dataframe(rows_view, width="stretch")

        leg_input = {
            "origin": leg["origin"],
            "destination": leg["destination"],
            "alternate": leg.get("alternate", ""),
            "eobt": leg.get("eobt"),
            "origin_iso": _resolve_country_for_airport(leg["origin"]),
            "destination_iso": _resolve_country_for_airport(leg["destination"]),
            "overrides": {},
        }
        leg_payloads.append(docx_generator.serialize_leg(leg_input, resolution, template_format))
        departures.append(leg["origin"])
        destinations.append(leg["destination"])
        date_range_min = leg["eobt"].date() if date_range_min is None else min(date_range_min, leg["eobt"].date())
        date_range_max = leg["eobt"].date() if date_range_max is None else max(date_range_max, leg["eobt"].date())

    mission = dict(st.session_state.mission)
    mission["departure_airport"] = " / ".join(departures)
    mission["destination_airport"] = " / ".join(destinations)
    # date_of_flight is computed in docx_generator from per-leg EOBTs in the
    # 'MAY 04 TO MAY 05, 2026' format expected by the reference DIC. We don't
    # set it here so the generator's _format_date_of_flight always wins.

    st.divider()
    can_generate = _render_pre_dic_checklist(mission, st.session_state.legs)

    st.divider()
    st.markdown("### 📑 Documents finaux")
    st.caption("À transmettre aux autorités hôtes (DIC) et au plan de vol (FPL).")
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("📄 Générer DIC .docx", type="primary", disabled=not can_generate):
            if all_warnings:
                st.warning("Des warnings subsistent — le doc sera généré mais à vérifier.")
            # Aggregate per-leg alternates into the mission-level field expected
            # by the docx template (one ' / '-separated string).
            mission["alternates"] = " / ".join(
                (leg.get("alternate") or "").strip().upper()
                for leg in st.session_state.legs
                if (leg.get("alternate") or "").strip()
            )
            data = docx_generator.build_dic_document(mission, leg_payloads)
            fn_parts = [
                "DIC",
                (mission.get("registration") or "").replace("/", "-"),
                "_".join(departures + destinations[-1:]),
                (mission.get("amendment") or "V1"),
            ]
            filename = "_".join(p for p in fn_parts if p) + ".docx"
            st.download_button(
                "⬇️ Télécharger DIC",
                data=data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

            # Auto-save the current mission as a route_template so the library
            # grows naturally with use. Folder = country ISO of the very first
            # leg's origin, or 'Divers' if unknown.
            # Dossier de classement : la géographie de la route prime sur
            # l'opérateur. Une route LFMD → LFMV opérée par AMAZONE doit
            # finir dans 'France', pas dans 'Bénin'. OPERATOR_FOLDER ne sert
            # plus que de filet quand l'origine n'a pas d'ISO connu.
            origin_iso = _resolve_country_for_airport(st.session_state.legs[0]["origin"])
            country_name = db.find_country_name(origin_iso) if origin_iso else None
            if country_name:
                # find_country_name renvoie 'CÔTE D'IVOIRE' / 'FRANCE' en
                # majuscules — on titlecase pour des dossiers lisibles
                # ('France', 'Côte D'Ivoire'). 'Bénin' garde son accent
                # via Natural Earth name_fr.
                folder = country_name.title()
            else:
                folder = OPERATOR_FOLDER.get(
                    (mission.get("operator") or "").strip()
                ) or origin_iso or "Divers"
            sanitised_legs = [
                {
                    "order": i + 1,
                    "origin": l["origin"],
                    "destination": l["destination"],
                    "route_text": l["route_text"],
                    "fl": l["fl"],
                    "tas": l["tas"],
                }
                for i, l in enumerate(st.session_state.legs)
                if l.get("origin") and l.get("destination")
            ]
            parts = [sanitised_legs[0]["origin"]] + [l["destination"] for l in sanitised_legs]
            tpl_name = f"{folder} / " + " → ".join(dict.fromkeys(parts))
            try:
                with db.connect() as c:
                    c.execute(
                        """
                        INSERT INTO route_template (name, category, legs_json)
                        VALUES (?, ?, ?)
                        ON CONFLICT(name) DO UPDATE SET
                            category = excluded.category,
                            legs_json = excluded.legs_json
                        """,
                        (tpl_name, folder, json.dumps(sanitised_legs, ensure_ascii=False)),
                    )
                st.success(f"💾 Template auto-sauvé : `{tpl_name}` (dossier *{folder}*).")
            except Exception as e:
                st.warning(f"Auto-save template échoué : {e}")

    with bc2:
        if st.button("✈️ Générer FPL ICAO (1 par leg)"):
            fpls: list[str] = []
            for i, leg in enumerate(st.session_state.legs):
                if not leg.get("origin") or not leg.get("destination"):
                    continue
                eet_min = int(
                    route_engine.compute_leg(
                        eobt=leg["eobt"], origin_icao=leg["origin"],
                        destination_icao=leg["destination"], route_text=leg["route_text"],
                        fl=leg["fl"], tas_kt=leg["tas"], country_index=idx,
                    ).total_time_min
                )
                ap_type = (mission.get("aircraft_count_type") or "").replace("1", "").strip()
                wake = "L"
                if ap_type:
                    types = db.list_aircraft_types(ap_type)
                    if types and types[0]["wake_category"]:
                        wake = types[0]["wake_category"]
                altn_codes = []
                # Per-leg alternates: just use this leg's own one if set.
                leg_alt = (leg.get("alternate") or "").strip().upper()
                if leg_alt:
                    altn_codes.append(leg_alt)
                fpl = fpl_exporter.fpl_for_leg(
                    callsign=(mission.get("callsign") or "").replace("-", "") or "ZZZZZ",
                    aircraft_type=ap_type or "ZZZZ",
                    registration=mission.get("registration") or "",
                    operator=mission.get("operator") or "",
                    wake_category=wake,
                    dep=leg["origin"], dest=leg["destination"],
                    eobt=leg["eobt"], tas_kt=leg["tas"], fl=leg["fl"],
                    route_text=leg["route_text"] or "DCT",
                    eet_min=eet_min,
                    alternates=altn_codes,
                    remarks=mission.get("purpose"),
                    sts="PROTECTED" if mission.get("vip_flag") else None,
                )
                fpls.append(f"# Leg {i + 1}: {leg['origin']} → {leg['destination']}\n{fpl}\n")
            full_text = "\n".join(fpls)
            st.code(full_text, language="text")
            st.download_button(
                "⬇️ Télécharger FPL.txt",
                data=full_text,
                file_name="FPL_" + (mission.get("registration") or "mission").replace("/", "-") + ".txt",
                mime="text/plain",
            )

    # ─── Pack ZIP : FPL + briefing + résumé (sans la DIC qui reste à part) ─
    st.divider()
    st.markdown("### 📦 Pack mission ZIP")
    st.caption(
        "Génère un ZIP avec FPL + résumé mission + briefing météo/NOTAM "
        "(la DIC reste téléchargée séparément). Pratique pour transmettre "
        "tout d'un coup en pièce jointe."
    )
    if st.button("📦 Générer pack ZIP"):
        import io
        import zipfile as _zip
        from app import autorouter_client as _ar

        buf = io.BytesIO()
        with _zip.ZipFile(buf, "w", _zip.ZIP_DEFLATED) as zf:
            # FPL par leg
            fpl_blocks: list[str] = []
            for i, leg in enumerate(st.session_state.legs):
                if not leg.get("origin") or not leg.get("destination"):
                    continue
                try:
                    eet_min = int(
                        route_engine.compute_leg(
                            eobt=leg["eobt"], origin_icao=leg["origin"],
                            destination_icao=leg["destination"],
                            route_text=leg["route_text"],
                            fl=leg["fl"], tas_kt=leg["tas"], country_index=idx,
                        ).total_time_min
                    )
                except Exception:
                    eet_min = 60
                ap_type = (mission.get("aircraft_type_icao") or "").strip()
                wake = "L"
                if ap_type:
                    types = db.list_aircraft_types(ap_type)
                    if types and types[0]["wake_category"]:
                        wake = types[0]["wake_category"]
                fpl_text = fpl_exporter.fpl_for_leg(
                    callsign=(mission.get("callsign") or "").replace("-", "") or "ZZZZZ",
                    aircraft_type=ap_type or "ZZZZ",
                    registration=mission.get("registration") or "",
                    operator=mission.get("operator") or "",
                    wake_category=wake,
                    dep=leg["origin"], dest=leg["destination"],
                    eobt=leg["eobt"], tas_kt=leg["tas"], fl=leg["fl"],
                    route_text=leg["route_text"] or "DCT",
                    eet_min=eet_min,
                    alternates=[(leg.get("alternate") or "").strip().upper()] if leg.get("alternate") else [],
                    remarks=mission.get("purpose"),
                    sts="PROTECTED" if mission.get("vip_flag") else None,
                )
                fpl_blocks.append(f"# Leg {i + 1}: {leg['origin']} → {leg['destination']}\n{fpl_text}\n")
            if fpl_blocks:
                zf.writestr("FPL.txt", "\n".join(fpl_blocks))

            # Résumé mission lisible
            summary_lines = [
                f"Mission DIC — {mission.get('registration', '?')} ({mission.get('aircraft_type_icao', '?')})",
                f"Opérateur : {mission.get('operator', '?')}",
                f"Callsign  : {mission.get('callsign', '?')}",
                f"CDB       : {mission.get('crew_cdb', '?')}",
                f"FO        : {mission.get('crew_fo', '—')}",
                f"POC       : {mission.get('poc_name', '?')} · {mission.get('poc_phone', '?')} · {mission.get('poc_email_functional', '?')}",
                f"Objet     : {mission.get('purpose', '?')}",
                "",
                "Legs :",
            ]
            for i, leg in enumerate(st.session_state.legs):
                if not leg.get("origin"):
                    continue
                eobt = leg.get("eobt")
                eobt_s = eobt.strftime("%Y-%m-%d %H:%MZ") if isinstance(eobt, dt.datetime) else "?"
                summary_lines.append(
                    f"  {i + 1}. {leg['origin']} → {leg['destination']}  ·  "
                    f"FL{int(leg.get('fl', 0)):03d} TAS{int(leg.get('tas', 0))}  ·  "
                    f"EOBT {eobt_s}  ·  ALT {leg.get('alternate', '—')}"
                )
                if leg.get("route_text"):
                    summary_lines.append(f"     route : {leg['route_text']}")
            zf.writestr("Resume_mission.txt", "\n".join(summary_lines))

            # Briefing météo + NOTAM si en cache
            ar_cfg = _ar.AutorouterConfig.from_secrets(st.secrets)
            wx_data = (st.session_state.get("_briefing_data") or {}).get("wx") or {}
            notams = (st.session_state.get("_briefing_data") or {}).get("notams") or []
            if wx_data or notams:
                lines = ["Briefing météo & NOTAMs"]
                lines.append("=" * 40)
                for ic, mt in wx_data.items():
                    lines.append(f"\n--- {ic} ---")
                    if mt.metar:
                        lines.append(f"METAR : {mt.metar}")
                    if mt.taf:
                        lines.append(f"TAF   : {mt.taf}")
                if notams:
                    lines.append("\n\nNOTAMs")
                    lines.append("=" * 40)
                    for n in notams:
                        lines.append("")
                        lines.append(_ar.format_notam(n))
                zf.writestr("Briefing.txt", "\n".join(lines))
            else:
                zf.writestr(
                    "Briefing.txt",
                    "Briefing non chargé — clique '🌤️ Charger' en page Preview puis re-génère le ZIP."
                )

        buf.seek(0)
        st.download_button(
            "⬇️ Télécharger pack ZIP",
            data=buf.getvalue(),
            file_name="Pack_" + (mission.get("registration") or "mission").replace("/", "-") + ".zip",
            mime="application/zip",
        )

    st.divider()
    st.markdown("### 🌤️ Briefing (météo, NOTAM, GRAMET, pack PDF)")
    st.caption(
        "Données opérationnelles via autorouter.aero. Aucun appel n'est "
        "effectué tant que tu ne cliques pas sur 'Charger'."
    )
    _render_briefing_section(legs=st.session_state.legs)

    _step_nav_footer()


if page_idx == 3:
    st.caption(
        "Toutes les missions / routes enregistrées en base. Clique sur **Charger** "
        "pour pré-remplir une mission avec les legs sauvegardés."
    )
    rows = db.list_route_templates()
    if not rows:
        st.info("Aucune route en base. Génère une DIC ou clique 💾 Enregistrer "
                "depuis la page Legs pour démarrer la bibliothèque.")
    else:
        # Group by category (dossier pays). Tri pour mettre les plus récents
        # en premier dans chaque dossier — l'id auto-incrément joue ce rôle.
        from collections import defaultdict
        by_cat: dict[str, list] = defaultdict(list)
        for r in rows:
            by_cat[r["category"] or "Divers"].append(r)
        st.caption(f"**{len(rows)} route(s)** réparties sur **{len(by_cat)} dossier(s)**.")
        for cat in sorted(by_cat):
            with st.expander(f"📁 {cat}  ({len(by_cat[cat])} route(s))", expanded=False):
                for r in sorted(by_cat[cat], key=lambda x: x["id"], reverse=True):
                    legs_json = json.loads(r["legs_json"])
                    summary = " → ".join(
                        dict.fromkeys(
                            [legs_json[0]["origin"]] + [l["destination"] for l in legs_json]
                        )
                    )
                    c1, c2, c3 = st.columns([5, 1, 1])
                    with c1:
                        st.markdown(f"**{r['name']}**")
                        st.caption(f"`{summary}` · {len(legs_json)} leg(s)")
                    with c2:
                        if st.button("📂 Charger", key=f"hist_load_{r['id']}"):
                            _apply_template(r["name"], rows)
                            _goto_page(1)
                            st.rerun()
                    with c3:
                        if st.button("🗑️", key=f"hist_del_{r['id']}", help="Supprimer"):
                            with db.connect() as c:
                                c.execute("DELETE FROM route_template WHERE id = ?", (r["id"],))
                            st.rerun()

    _step_nav_footer()


if page_idx == 4:
    st.caption(
        "Configuration et données opérationnelles. Ces sections n'impactent pas "
        "la génération du DIC tant que tu n'en as pas besoin."
    )

    with st.expander("📥 Flotte type — import rapide", expanded=False):
        st.markdown(
            "Insère les appareils standards de la flotte (idempotent : "
            "rejouable, ne crée pas de doublons grâce à l'unicité de "
            "l'immatriculation)."
        )
        # Perf des types ICAO de la flotte. Upsert avant les aircraft pour
        # satisfaire la FK aircraft.type_icao → aircraft_type. Inclut les
        # types absents de la CSV seed (L410, B190).
        FLEET_TYPES = [
            {"icao_designator": "DA62", "full_name": "Diamond DA62",
             "manufacturer": "Diamond", "cruise_tas_kt": 192,
             "service_ceiling_ft": 20000, "range_nm": 1300, "wake_category": "L"},
            {"icao_designator": "DHC6", "full_name": "De Havilland Canada DHC-6 Twin Otter",
             "manufacturer": "De Havilland Canada", "cruise_tas_kt": 160,
             "service_ceiling_ft": 25000, "range_nm": 800, "wake_category": "L"},
            {"icao_designator": "L410", "full_name": "Let L-410 Turbolet",
             "manufacturer": "Let Kunovice", "cruise_tas_kt": 195,
             "service_ceiling_ft": 20000, "range_nm": 770, "wake_category": "L"},
            {"icao_designator": "B190", "full_name": "Beechcraft 1900D",
             "manufacturer": "Beechcraft", "cruise_tas_kt": 280,
             "service_ceiling_ft": 25000, "range_nm": 1500, "wake_category": "M"},
            {"icao_designator": "A321", "full_name": "Airbus A321",
             "manufacturer": "Airbus", "cruise_tas_kt": 447,
             "service_ceiling_ft": 39800, "range_nm": 3200, "wake_category": "M"},
        ]
        DEFAULT_FLEET = [
            # (registration, type_icao, callsign, operator)
            ("F-HJTA",   "DA62",  "HJTA",   "DYNAMI AVIATION OPS"),
            ("TY-BAB",   "DHC6",  "TY-BAB", "AMAZONE AIRLINES / DYNAMI AVIATION OPS"),
            ("7Q-YAE",   "L410",  "7QYAE",  "DYNAMI AVIATION OPS"),
            ("F-WZNA",   "B190",  "FWZNA",  "DYNAMI AVIATION OPS"),
            ("F-HSVA",   "A321",  "SVA21F", "SKYVISION"),
        ]
        if st.button("📥 Importer flotte type"):
            db.upsert_aircraft_types(FLEET_TYPES)
            inserted = 0
            for reg, ty, cs, op in DEFAULT_FLEET:
                db.save_aircraft(reg, ty, cs, op)
                inserted += 1
            st.success(
                f"✅ {inserted} appareil(s) en base — dont l'A321F SkyVision "
                f"(F-HSVA, cruise 447 kt, ceiling FL398, wake M)."
            )
            st.rerun()

    with st.expander("🛩️ Aérodromes opérationnels (sans ICAO)", expanded=True):
        st.markdown(
            "Aérodromes / points de poser sans code ICAO publié (FOB, base militaire). "
            "Ils restent en base après seed et sont utilisables comme origin/destination."
        )
        user_aps = db.list_user_airports()
        if user_aps:
            st.caption(f"{len(user_aps)} aérodrome(s) en base :")
            for ap in user_aps:
                cols = st.columns([3, 1])
                with cols[0]:
                    st.markdown(
                        f"**{ap['icao']}** — {ap['name']}  "
                        f"({ap['country_iso']})  ·  "
                        f"`{ap['lat']:.4f}°, {ap['lon']:.4f}°`"
                    )
                with cols[1]:
                    if st.button("🗑️", key=f"del_uap_{ap['icao']}", help="Supprimer"):
                        db.delete_user_airport(ap["icao"])
                        st.rerun()
        else:
            st.caption("Aucun aérodrome opérationnel en base.")
        st.markdown("**Ajouter :**")
        c1, c2 = st.columns(2)
        with c1:
            new_label = st.text_input(
                "Identifiant (ex. TOUROU, KAINJI)",
                key="uap_label",
                help="Le nom court qu'utilisera l'agent comme origin/destination",
            ).strip().upper()
            new_name = st.text_input(
                "Nom complet (ex. Kainji NAFB)", key="uap_name",
            ).strip()
            new_country = st.text_input(
                "Pays (ISO 2-letter, ex. BJ, NG)", key="uap_country",
                max_chars=2,
            ).strip().upper()
        with c2:
            new_lat = st.number_input(
                "Latitude (°N+ / S-)", value=0.0, format="%.6f", key="uap_lat",
            )
            new_lon = st.number_input(
                "Longitude (°E+ / W-)", value=0.0, format="%.6f", key="uap_lon",
            )
            is_mil = st.checkbox("Militaire", value=True, key="uap_mil")
        if new_label and new_name and st.button("💾 Sauver", key="uap_save"):
            db.save_user_airport(
                icao=new_label, name=new_name,
                country_iso=new_country or "",
                lat=float(new_lat), lon=float(new_lon),
                is_military=is_mil,
            )
            st.success(f"Aérodrome {new_label} ajouté.")
            st.rerun()

    with st.expander("🌐 Autorouter API (suggestion approfondie)", expanded=True):
        from app import autorouter_client
        ar_cfg = autorouter_client.AutorouterConfig.from_secrets(st.secrets)
        if ar_cfg.is_configured():
            st.caption(f"✓ Configuré — `{ar_cfg.base_url}`")
            if st.button("🔌 Test connexion", key="ar_test"):
                try:
                    info = autorouter_client.ping_version(ar_cfg)
                    st.success(
                        f"API v{info.get('major')}.{info.get('minor')}.{info.get('patch')} "
                        f"({'prod' if info.get('production') else 'sandbox'})"
                    )
                    try:
                        autorouter_client._get_token(ar_cfg)
                        st.success("Token OAuth obtenu — credentials valides.")
                    except autorouter_client.AutorouterError as e:
                        st.error(f"Token : {e}")
                except autorouter_client.AutorouterError as e:
                    st.error(str(e))
        else:
            st.caption(
                "Pas configuré. Ajoute dans Streamlit Cloud → Settings → Secrets :"
            )
            st.code(
                "[autorouter]\n"
                'base_url = "https://api.autorouter.aero/v1.0"\n'
                'token_url = "https://api.autorouter.aero/v1.0/oauth2/token"\n'
                'client_id = "ton.email@example.org"\n'
                'client_secret = "ton-mot-de-passe-autorouter"\n',
                language="toml",
            )

    _step_nav_footer()
