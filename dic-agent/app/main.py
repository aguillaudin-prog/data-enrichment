"""Streamlit entrypoint — DIC Agent.

Run: streamlit run app/main.py
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

# Ensure the project root is on sys.path so `from app import ...` works
# regardless of the directory `streamlit run` was launched from.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st

from app import db, docx_generator, fpl_exporter, route_engine, route_suggester

st.set_page_config(page_title="DIC Agent", layout="wide")

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


def _aircraft_picker(key_prefix: str, operator: str | None = None) -> dict:
    rows = db.list_aircraft(operator=operator)
    options = ["— nouveau —"] + [
        f"{r['registration']} / {r['type_icao'] or '?'}" for r in rows
    ]
    label = f"Appareil ({operator})" if operator else "Appareil"
    sel = st.selectbox(label, options, key=f"{key_prefix}_ap_sel")
    if sel != "— nouveau —":
        idx = options.index(sel) - 1
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
            "Type ICAO (ex. DHC6, A400, B738…)", key=f"{key_prefix}_type"
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

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        sel_cdb = st.selectbox("Commandant de bord (CDB)", cdb_options, key=f"{key_prefix}_cdb_sel")
        if sel_cdb == "— autre —":
            cdb_text = st.text_input("CDB (saisie libre)", key=f"{key_prefix}_cdb_text").strip()
        else:
            cdb_text = sel_cdb
    with c2:
        sel_fo = st.selectbox("Copilote (FO)", fo_options, key=f"{key_prefix}_fo_sel")
        if sel_fo == "— autre —":
            fo_text = st.text_input("FO (saisie libre)", key=f"{key_prefix}_fo_text").strip()
        elif sel_fo == "— aucun —":
            fo_text = ""
        else:
            fo_text = sel_fo
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
        if new_name and st.button("💾 Ajouter", key=f"{key_prefix}_save_pilot"):
            db.save_pilot(new_name, new_role, new_rank or None)
            st.success(f"Pilote {new_name} ({new_role}) ajouté.")
            st.rerun()

    return {
        "n_crew": int(n_crew),
        "pilots": pilots_display,
        "cdb": cdb_text,
        "fo": fo_text,
    }


def _poc_picker(key_prefix: str) -> dict:
    rows = db.list_pocs()
    options = ["— nouveau —"] + [f"{r['rank'] or ''} {r['name']}".strip() for r in rows]
    sel = st.selectbox("Profil POC", options, key=f"{key_prefix}_poc_sel")
    if sel != "— nouveau —":
        idx = options.index(sel) - 1
        r = rows[idx]
        return {
            "name": f"{r['rank'] or ''} {r['name']}".strip(),
            "phone": r["phone"] or "",
            "email_personal": r["email_personal"] or "",
            "email_functional": r["email_functional"] or "",
            "fax": r["fax"] or "",
        }
    c1, c2 = st.columns(2)
    with c1:
        rank = st.text_input("Grade", key=f"{key_prefix}_poc_rank")
        name = st.text_input("Nom prénom", key=f"{key_prefix}_poc_name")
        phone = st.text_input("Téléphone", key=f"{key_prefix}_poc_phone")
    with c2:
        email_p = st.text_input("Email perso", key=f"{key_prefix}_poc_emailp")
        email_f = st.text_input("Email fonctionnel", key=f"{key_prefix}_poc_emailf")
        fax = st.text_input("Fax", key=f"{key_prefix}_poc_fax")
    if name and st.button("💾 Sauver POC", key=f"{key_prefix}_save_poc"):
        db.save_poc(rank, name, phone, email_p, email_f, fax)
        st.success("POC enregistré.")
        st.rerun()
    return {
        "name": f"{rank} {name}".strip(),
        "phone": phone,
        "email_personal": email_p,
        "email_functional": email_f,
        "fax": fax,
    }


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
        origin = st.text_input(
            "Origin (ICAO)", value=leg.get("origin", ""), key=f"{kprefix}_orig"
        ).strip().upper()
    with c2:
        destination = st.text_input(
            "Destination (ICAO)", value=leg.get("destination", ""), key=f"{kprefix}_dest"
        ).strip().upper()
    with c3:
        fl = st.number_input("FL", min_value=0, max_value=600, value=int(leg.get("fl", 90)), step=10, key=f"{kprefix}_fl")
    with c4:
        tas = st.number_input("TAS (kt)", min_value=50, max_value=900, value=int(leg.get("tas", 140)), step=10, key=f"{kprefix}_tas")

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

    rc1, rc2 = st.columns([5, 1])
    with rc1:
        route_text = st.text_input(
            "Route texte ICAO (ex. `TYE POLTO LAG L433 IBA R778 TEGDA MNA`)",
            value=leg.get("route_text", ""),
            key=f"{kprefix}_route",
        )
    with rc2:
        st.write("")
        st.write("")
        if st.button("✨ Suggérer", key=f"{kprefix}_suggest", help="A* sur les NAVAID dans le corridor"):
            if origin and destination:
                with st.spinner("Calcul A*…"):
                    sug = route_suggester.suggest_route(origin, destination)
                if sug.waypoints and sug.distance_nm > 0:
                    enroute = sug.route_text
                    enroute_tokens = [t for t in enroute.split() if t and t != "DCT"]
                    first_fix = enroute_tokens[0] if enroute_tokens else None
                    last_fix = enroute_tokens[-1] if enroute_tokens else None
                    sid_pick = route_suggester.pick_procedure(origin, first_fix, "SID")
                    star_pick = route_suggester.pick_procedure(destination, last_fix, "STAR")
                    full_route = enroute
                    extras: list[str] = []
                    if sid_pick:
                        full_route = f"{sid_pick['proc_name']} {full_route}".strip()
                        extras.append(
                            f"SID **{sid_pick['proc_name']}** "
                            f"(rwy {sid_pick['runways_csv'] or '-'} → {sid_pick['connecting_fix']})"
                        )
                    if star_pick:
                        full_route = f"{full_route} {star_pick['proc_name']}".strip()
                        extras.append(
                            f"STAR **{star_pick['proc_name']}** "
                            f"({star_pick['connecting_fix']} → rwy {star_pick['runways_csv'] or '-'})"
                        )
                    st.session_state[f"_pending_route_{sid}_{idx}"] = full_route
                    msg = (
                        f"Route suggérée : `{full_route}` "
                        f"({sug.distance_nm:.0f} NM, {sug.nodes_explored} nœuds explorés)"
                    )
                    if extras:
                        msg += "  \nAuto-sélection : " + " · ".join(extras)
                    st.session_state[f"_pending_suggest_msg_{sid}_{idx}"] = msg
                    st.rerun()
                else:
                    st.error("Pas de route trouvée — vérifie les ICAO d'origine/destination.")

    msg = st.session_state.pop(f"_pending_suggest_msg_{sid}_{idx}", None)
    if msg:
        st.success(msg)

    # SID / STAR pickers — visible only if origin/destination has procedures
    # in DB (i.e. user ran `python -m app.import_cifp`).
    sids = db.list_procedures(origin, "SID") if origin else []
    stars = db.list_procedures(destination, "STAR") if destination else []
    if sids or stars:
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

with st.sidebar:
    st.header("Paramètres")
    template_format = st.radio("Format DIC", ["FRA", "ICAO"], horizontal=True)
    st.divider()
    st.caption(
        "Tip : pour un point de coordonnées brutes, saisir au format "
        "`N 9°34'45.56\" / E 3°14'7.09\"` ou `N9 34 45 / E3 14 7`."
    )

tab_mission, tab_legs, tab_preview = st.tabs(
    ["1️⃣ Mission & profils", "2️⃣ Legs", "3️⃣ Preview & export"]
)

if "legs" not in st.session_state:
    st.session_state.legs = [
        {"origin": "", "destination": "", "fl": 90, "tas": 140, "route_text": ""}
    ]

with tab_mission:
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
    poc = _poc_picker("mission")

    st.divider()
    st.subheader("Indicateurs")
    ic1, ic2, ic3, ic4, ic5 = st.columns(5)
    with ic1:
        sensors = "YES" if st.checkbox("Capteurs / caméras") else "NO"
    with ic2:
        armament = "YES" if st.checkbox("Armement") else "NO"
    with ic3:
        ew = "YES" if st.checkbox("Guerre électronique") else "NO"
    with ic4:
        has_vip = st.checkbox("VIP à bord")
    with ic5:
        has_dg = st.checkbox("Dangerous Goods")

    st.subheader("Vol")
    purpose = st.text_input("Purpose of flight", value="LOGISTIC FLIGHT WITHOUT DANGEROUS GOODS")
    alternates = st.text_input("Alternates (ICAO list)", value="")
    radio_freq = st.text_input("Radio frequencies", value="VHF")
    n_passengers = st.text_input("Number of passengers", value="TBN")
    vip_title = st.text_input("VIP title/rank and name", value="NIL" if not has_vip else "TBN")
    dg_details = st.text_input("DG details", value="NIL" if not has_dg else "TBN")
    remarks = st.text_area("Remarks", value="")

    st.session_state.mission = {
        "reference": reference,
        "amendment": amendment,
        "mission_number": mission_number,
        "template_format": template_format,
        "requesting_state": "FRANCE",
        "operator": ap.get("operator", ""),
        "aircraft_count_type": f"1  {ap.get('type_icao','')}",
        "registration": ap.get("registration", ""),
        "spare_aircraft": f"{ap.get('registration','')} OR SUBSTITUTE",
        "callsign": ap.get("callsign", ""),
        "n_crew": crew.get("n_crew", 2),
        "pilots": crew.get("pilots", ""),
        "sensors": sensors,
        "armament": armament,
        "ew": ew,
        "purpose": purpose,
        "alternates": alternates,
        "radio_frequencies": radio_freq,
        "n_passengers": n_passengers,
        "vip_title": vip_title,
        "dg_details": dg_details,
        "remarks": remarks,
        "poc_name": poc.get("name", ""),
        "poc_phone": poc.get("phone", ""),
        "poc_email_personal": poc.get("email_personal", ""),
        "poc_email_functional": poc.get("email_functional", ""),
        "poc_fax": poc.get("fax", ""),
        "vip_flag": has_vip,
        "dg_flag": has_dg,
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


with tab_legs:
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
        filtered = tpl_rows if cat_sel == "— tous —" else by_cat.get(cat_sel, [])
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
    lc1, lc2, _ = st.columns([1, 1, 4])
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


with tab_preview:
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
        st.dataframe(rows_view, use_container_width=True)

        leg_input = {
            "origin": leg["origin"],
            "destination": leg["destination"],
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
    if date_range_min and date_range_max:
        if date_range_min == date_range_max:
            mission["date_of_flight"] = date_range_min.strftime("%d %b %Y").upper()
        else:
            mission["date_of_flight"] = (
                f"{date_range_min.strftime('%d %b').upper()} TO "
                f"{date_range_max.strftime('%d %b %Y').upper()}"
            )

    st.divider()
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("📄 Générer DIC .docx", type="primary"):
            if all_warnings:
                st.warning("Des warnings subsistent — le doc sera généré mais à vérifier.")
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
            # Dossier de classement : on suit l'opérateur d'abord (sa base
            # opérationnelle), avec fallback sur le code pays de l'origine.
            operator = (mission.get("operator") or "").strip()
            folder = OPERATOR_FOLDER.get(operator)
            if not folder:
                origin_iso = _resolve_country_for_airport(st.session_state.legs[0]["origin"])
                folder = origin_iso or "Divers"
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
                for tok in (mission.get("alternates") or "").split("/"):
                    m = __import__("re").search(r"\b([A-Z]{4})\b", tok.upper())
                    if m:
                        altn_codes.append(m.group(1))
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
