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

# CSS custom — renforce les containers bordés des legs.
# Streamlit change ses sélecteurs data-testid entre versions. On cible
# plusieurs variants connus pour maximiser la chance de match.
st.markdown(
    """
    <style>
    /* TOUS les sélecteurs Streamlit qui correspondent à un container
       avec border=True selon la version */
    div[data-testid="stVerticalBlockBorderWrapper"],
    div[data-testid="stVerticalBlock"][style*="border"],
    div[data-testid="stContainer"][style*="border"],
    section[data-testid="stContainer"][style*="border"],
    .stContainer[style*="border"],
    div[class*="VerticalBlockBorderWrapper"] {
        border: 1.5px solid #c8d0db !important;
        border-left: 6px solid #2563eb !important;
        border-radius: 10px !important;
        padding: 1.4rem 1.6rem !important;
        margin: 0.4rem 0 1.8rem 0 !important;
        background-color: #ffffff !important;
        box-shadow: 0 3px 10px rgba(15, 23, 42, 0.08),
                    0 1px 3px rgba(15, 23, 42, 0.04) !important;
    }
    /* Headers ### dans les containers : très saillants */
    div[data-testid="stVerticalBlockBorderWrapper"] h3,
    div[data-testid="stVerticalBlock"][style*="border"] h3,
    div[class*="VerticalBlockBorderWrapper"] h3 {
        color: #1e3a8a !important;
        margin: 0 0 0.8rem 0 !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 2px solid #dbeafe !important;
        font-weight: 700 !important;
        font-size: 1.25rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _ensure_amazone_data_seeded(force: bool = False) -> int:
    """Seed des données opérateur Amazone. Idempotent. Self-healing : si
    les routes officielles ont disparu (suppression manuelle, wipe, etc.)
    le seed re-tourne au prochain démarrage."""
    n = 0
    try:
        # Force la migration de schéma (ALTER TABLE pour les colonnes
        # ajoutées, dont `official` qui peut manquer sur DB historique).
        db.init_schema()
        # Fix des typos connus dans les templates sauvegardés. Idempotent
        # (UPDATE ... WHERE LIKE est no-op si pas de match). Évite que
        # d'anciennes missions saved avec typo (ex: 'KELI' au lieu de
        # 'KELIG') continuent à empoisonner les routes chargées.
        _fix_known_route_typos()
        if not force:
            # Re-seed si manquant OU si schéma de missions changé.
            # Le catalogue Amazone vise 23 missions (15 routes numérotées
            # + variants). Si on en a moins, force le re-seed pour
            # bénéficier de la dernière version.
            try:
                with db.connect() as c:
                    n_missions = c.execute(
                        "SELECT COUNT(*) FROM route_template "
                        "WHERE official = 1 AND variant = 'mission'"
                    ).fetchone()[0]
                    n_old_cats = c.execute(
                        "SELECT COUNT(*) FROM route_template "
                        "WHERE official = 1 AND variant = 'mission' "
                        "AND category != 'Amazone'"
                    ).fetchone()[0]
                    # Détection format ancien des noms : si un mission a
                    # encore "(maritime" ou "(Abuja)" dans son nom, on
                    # force le re-seed pour appliquer le format uniformisé.
                    n_old_names = c.execute(
                        "SELECT COUNT(*) FROM route_template "
                        "WHERE official = 1 AND variant = 'mission' "
                        "AND (name LIKE '%(maritime%' OR name LIKE '%(Abuja)%' "
                        "OR name LIKE '%(Minna)%' OR name LIKE '%(Ilorin)%' "
                        "OR name LIKE '%(overflight%' OR name LIKE '%(techstop %' "
                        "OR name LIKE '%(évitement%' OR name LIKE '%Yaoundé%')"
                    ).fetchone()[0]
                if (n_missions >= 23 and n_old_cats == 0 and n_old_names == 0
                        and hasattr(db, "count_official_routes")
                        and db.count_official_routes() > 0):
                    return 0  # schéma à jour, rien à faire
            except Exception:
                pass
        from app.seed_db import (
            seed_amazone_waypoints, seed_canonical_routes,
            seed_dhc6_perf_refinements, seed_amazone_missions,
        )
        n += seed_amazone_waypoints()
        n += seed_canonical_routes()
        n += seed_amazone_missions()
        seed_dhc6_perf_refinements()
    except Exception:
        pass
    return n


def _fix_known_route_typos() -> int:
    """Corrige les typos connus dans route_template.legs_json. Liste à
    étendre quand on en croise d'autres. Idempotent.

    Cas réels rencontrés :
    - 'KELI ' au lieu de 'KELIG ' (le G final droppé à la saisie)
    - 'KELI"' au lieu de 'KELIG"' (en fin de string JSON)
    """
    fixes = [
        (" KELI ", " KELIG "),
        ('"KELI"', '"KELIG"'),
        (" KELI,", " KELIG,"),
        (" KELI-", " KELIG-"),
        ("-KELI ", "-KELIG "),
        ("-KELI-", "-KELIG-"),
        ('KELI W951', 'KELIG W951'),
    ]
    n_fixed = 0
    try:
        with db.connect() as c:
            for old, new in fixes:
                cur = c.execute(
                    "UPDATE route_template SET legs_json = REPLACE(legs_json, ?, ?) "
                    "WHERE legs_json LIKE ?",
                    (old, new, f"%{old}%"),
                )
                n_fixed += cur.rowcount
    except Exception:
        pass
    return n_fixed


def _cleanup_and_relabel_user_templates() -> int:
    """Au boot Streamlit, nettoie les user-saved missions :

    1. Supprime les doublons triviaux : 1-leg ou 2-leg user-saved dont
       les (origin, destination) matchent un leg officiel Amazone — ces
       entrées venaient d'auto-saves de l'ancien schéma et polluent
       maintenant le picker.
    2. Re-catégorise les user-saved restants (composites, tours
       multi-mission) sous la catégorie "Amazone (saved)" pour les
       grouper dans le même dossier que les officiels.
    3. Annote leur nom avec les numéros de mission Amazone identifiés
       pour chaque leg : "[13/1.x/9] DBBB → DIAP → DIBK → DIKO".

    Idempotent : ne touche rien si le ménage est déjà fait.
    """
    n_deleted = 0
    n_relabeled = 0
    try:
        with db.connect() as c:
            cols = {r[1] for r in c.execute("PRAGMA table_info(route_template)").fetchall()}
            if "official" not in cols or "variant" not in cols:
                return 0
            # Pull all official Amazone missions for matching
            official = c.execute(
                "SELECT name, legs_json FROM route_template "
                "WHERE official = 1 AND variant = 'mission'"
            ).fetchall()
            # Index : (origin, destination) → liste de tuples (mission_num, leg_idx)
            # mission_num extrait du nom : "[BJ] 13 — DBBB ↔ DNAA" → "13"
            official_legs_idx: dict[tuple[str, str], list[str]] = {}
            official_routes: list[tuple[str, list[tuple[str, str]]]] = []
            import re as _re
            for r in official:
                try:
                    legs = json.loads(r["legs_json"])
                except (json.JSONDecodeError, TypeError):
                    continue
                # Extrait le numéro de mission du nom
                m = _re.search(r"\] (\d+(?:\.[A-Z])?)", r["name"] or "")
                mission_num = m.group(1) if m else "?"
                od_pairs = [
                    ((leg.get("origin") or "").upper().strip(),
                     (leg.get("destination") or "").upper().strip())
                    for leg in (legs or []) if isinstance(leg, dict)
                ]
                official_routes.append((mission_num, od_pairs))
                for od in od_pairs:
                    official_legs_idx.setdefault(od, []).append(mission_num)

            # User-saved missions
            user_rows = c.execute(
                "SELECT id, name, category, legs_json FROM route_template "
                "WHERE official IS NULL OR official = 0"
            ).fetchall()
            for r in user_rows:
                try:
                    legs = json.loads(r["legs_json"])
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(legs, list) or not legs:
                    continue
                user_ods = [
                    ((leg.get("origin") or "").upper().strip(),
                     (leg.get("destination") or "").upper().strip())
                    for leg in legs if isinstance(leg, dict)
                ]
                # 1. Doublon trivial : user_ods == legs d'un official
                is_dup = any(user_ods == off_ods for _, off_ods in official_routes)
                if is_dup:
                    c.execute("DELETE FROM route_template WHERE id = ?", (r["id"],))
                    n_deleted += 1
                    continue
                # 2-3. Re-categorize + annotate avec numéros Amazone matched
                leg_nums = []
                for od in user_ods:
                    nums = official_legs_idx.get(od)
                    leg_nums.append(nums[0] if nums else "?")
                annot = "/".join(leg_nums) if leg_nums else ""
                # Nouveau nom : "[13/1.A/9] DBBB → DIAP → DIBK → DIKO"
                path = " → ".join([user_ods[0][0]] + [od[1] for od in user_ods])
                new_name = f"[{annot}] {path}" if annot else path
                new_cat = "Mes tours"
                if r["name"] != new_name or r["category"] != new_cat:
                    try:
                        c.execute(
                            "UPDATE route_template SET name = ?, category = ? "
                            "WHERE id = ?",
                            (new_name, new_cat, r["id"]),
                        )
                        n_relabeled += 1
                    except Exception:
                        # Probable conflit UNIQUE(name) — on a déjà cette
                        # entrée. Suppression silencieuse du doublon.
                        c.execute("DELETE FROM route_template WHERE id = ?", (r["id"],))
                        n_deleted += 1
    except Exception:
        pass
    return n_deleted + n_relabeled


_ensure_amazone_data_seeded()
_cleanup_and_relabel_user_templates()


def _current_user_email() -> str | None:
    """Email de l'utilisateur connecté via Streamlit Cloud, ou None.

    Streamlit 1.57 expose `st.user` comme un `UserInfoProxy` Mapping-like.
    On accède via `.get("email")` ou `.to_dict().get("email")`, PAS via
    getattr direct (qui ne renvoie que les méthodes du proxy).
    """
    # 1. st.user (Mapping-like dans Streamlit 1.32+)
    try:
        if hasattr(st, "user"):
            # to_dict() est le contrat propre pour récupérer les claims
            data = st.user.to_dict() if hasattr(st.user, "to_dict") else dict(st.user)
            email = data.get("email") if isinstance(data, dict) else None
            if email:
                return email
    except Exception:
        pass
    # 2. Fallback API expérimentale (anciennes versions Streamlit)
    try:
        if hasattr(st, "experimental_user"):
            ex = st.experimental_user
            data = ex.to_dict() if hasattr(ex, "to_dict") else None
            email = (data or {}).get("email") if isinstance(data, dict) else None
            if not email:
                email = getattr(ex, "email", None)
            if email:
                return email
    except Exception:
        pass
    return None


def _is_admin() -> bool:
    """Admin = email matche st.secrets["ADMIN_EMAIL"].

    Détection stricte du contexte :
    - Si on tourne sur Streamlit Cloud (= experimental_user existe avec
      un email) : on EXIGE st.secrets["ADMIN_EMAIL"] et un match exact.
      Pas de secret = pas d'admin. Pas de match = pas d'admin.
    - Si on tourne en local sans auth (= experimental_user n'existe
      pas) : tout le monde est admin pour faciliter le dev.
    """
    user_email = (_current_user_email() or "").strip().lower()
    if not user_email:
        # Pas de session auth → dev local OU prod sans email visible.
        # Par sécurité, on n'accorde pas l'admin sans email connu.
        # Sauf si on est explicitement en dev (variable d'env).
        import os as _os
        return _os.environ.get("DIC_AGENT_DEV", "").strip() == "1"
    try:
        admin_email = (st.secrets.get("ADMIN_EMAIL") or "").strip().lower()
    except Exception:
        admin_email = ""
    return bool(admin_email) and user_email == admin_email


def _show_logged_in_user() -> None:
    """Affiche en sidebar l'email connecté + le statut admin. Aide à
    diagnostiquer pourquoi un email se voit refuser l'accès Admin
    (mismatch case, secret pas chargée, etc.)."""
    email = _current_user_email()
    admin = _is_admin()
    with st.sidebar:
        if email:
            badge = "🛡️ admin" if admin else "👤 user"
            st.caption(f"{badge} · **{email}**")
        else:
            st.caption("👤 _(pas d'auth détectée)_")
        # Bouton debug visible pour tout le monde — pour diagnostiquer
        # ce que Streamlit Cloud expose réellement.
        with st.expander("🔬 Auth debug", expanded=False):
            try:
                admin_email_secret = (st.secrets.get("ADMIN_EMAIL") or "").strip()
            except Exception:
                admin_email_secret = "(error)"
            # Dump tout ce qu'on peut récupérer de st.user et st.experimental_user
            user_dump = "—"
            try:
                if hasattr(st, "user"):
                    if hasattr(st.user, "to_dict"):
                        d = st.user.to_dict()
                        user_dump = f"  st.user.to_dict() = {d!r}"
                    else:
                        user_dump = f"  st.user = {dict(st.user)!r}"
                else:
                    user_dump = "  st.user : not available"
            except Exception as e:
                user_dump = f"  st.user : error {e}"
            exp_dump = "—"
            try:
                if hasattr(st, "experimental_user"):
                    attrs = {k: getattr(st.experimental_user, k, None) for k in dir(st.experimental_user) if not k.startswith("_")}
                    exp_dump = "\n".join(f"  st.experimental_user.{k} = {v!r}" for k, v in attrs.items())
                else:
                    exp_dump = "  st.experimental_user : not available"
            except Exception as e:
                exp_dump = f"  st.experimental_user : error {e}"
            import streamlit as _st_for_ver
            st.code(
                f"streamlit version : {_st_for_ver.__version__}\n"
                f"email détecté     : {email!r}\n"
                f"ADMIN_EMAIL secret: {admin_email_secret!r}\n"
                f"is_admin          : {admin}\n\n"
                f"st.user attrs :\n{user_dump}\n\n"
                f"st.experimental_user attrs :\n{exp_dump}",
                language="text",
            )


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


def _render_leg_map(resolution, leg: dict) -> None:
    """Carte interactive de la route (pydeck, livré avec streamlit, pas
    de dépendance ajoutée). Visualise :

    - Aéro origine (carré vert)
    - Aéro destination (carré rouge)
    - Alternate (losange orange) si saisi
    - Waypoints résolus en base (cercles bleus)
    - Ligne reliant l'ordre du FPL

    Détection visuelle d'erreurs courantes : route qui zigzague, frontière
    manquante, alternate à l'autre bout du continent, waypoint mal résolu.
    """
    import pydeck as pdk

    origin_icao = leg.get("origin", "")
    destination_icao = leg.get("destination", "")
    alternate_icao = (leg.get("alternate") or "").strip().upper()

    # Récupère lat/lon des aéros + waypoints
    ap_o = db.find_airport(origin_icao) if origin_icao else None
    ap_d = db.find_airport(destination_icao) if destination_icao else None
    ap_a = db.find_airport(alternate_icao) if alternate_icao else None
    if not (ap_o and ap_d):
        return  # pas la peine de tracer si aéros manquants

    # Points pour markers : (lat, lon, label, color, size)
    markers: list[dict] = []
    markers.append({
        "lat": float(ap_o["lat"]), "lon": float(ap_o["lon"]),
        "label": origin_icao, "color": [40, 180, 60], "size": 12,
    })
    markers.append({
        "lat": float(ap_d["lat"]), "lon": float(ap_d["lon"]),
        "label": destination_icao, "color": [220, 60, 60], "size": 12,
    })
    if ap_a:
        markers.append({
            "lat": float(ap_a["lat"]), "lon": float(ap_a["lon"]),
            "label": f"ALT {alternate_icao}", "color": [240, 160, 40], "size": 10,
        })
    # Waypoints intermédiaires résolus
    coord_pts: list[tuple[float, float]] = [(float(ap_o["lat"]), float(ap_o["lon"]))]
    for p in (resolution.points or []):
        if p.lat is None or p.lon is None:
            continue
        if p.label in (origin_icao, destination_icao):
            continue
        markers.append({
            "lat": float(p.lat), "lon": float(p.lon),
            "label": p.label, "color": [80, 130, 200], "size": 7,
        })
        coord_pts.append((float(p.lat), float(p.lon)))
    coord_pts.append((float(ap_d["lat"]), float(ap_d["lon"])))

    # Ligne reliant les points dans l'ordre
    line_path = [{"path": [[lon, lat] for lat, lon in coord_pts], "color": [100, 100, 240]}]

    # View centrée sur le midpoint, zoom inversement proportionnel à la distance
    mid_lat = (float(ap_o["lat"]) + float(ap_d["lat"])) / 2
    mid_lon = (float(ap_o["lon"]) + float(ap_d["lon"])) / 2
    dist_nm = resolution.total_distance_nm or 100
    # Heuristique : zoom 5 pour 100 NM, zoom 3 pour 1500 NM, zoom 2 pour 4000 NM
    zoom = max(2, min(6, 7 - (dist_nm / 300)))

    layers = [
        pdk.Layer(
            "PathLayer", line_path,
            get_path="path", get_color="color", get_width=3,
            width_min_pixels=2,
        ),
        pdk.Layer(
            "ScatterplotLayer", markers,
            get_position="[lon, lat]", get_fill_color="color",
            get_radius="size * 1000", pickable=True,
        ),
        pdk.Layer(
            "TextLayer", markers,
            get_position="[lon, lat]", get_text="label",
            get_color=[20, 20, 20], get_size=14, get_alignment_baseline="bottom",
        ),
    ]
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=mid_lat, longitude=mid_lon, zoom=zoom, pitch=0,
        ),
        map_style="light",
        tooltip={"text": "{label}"},
    )
    st.pydeck_chart(deck, width="stretch", height=350)


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


def _render_insert_after_leg(leg_idx: int, leg: dict) -> None:
    """Petit bouton ➕ entre chaque leg pour insérer un nouveau leg.

    Cliquer ouvre un sub-menu compact (expander) avec :
    - "Leg vide" → insère un leg blanc
    - Missions Amazone dont le 1er leg part de la destination du leg
      courant → insère SEULEMENT le 1er leg de cette mission (pour ne
      pas casser le retour final si la mission user prévoit un retour
      à DBBB en fin de course).

    Empreinte UI : un expander collapsed par défaut entre chaque leg.
    Pas affiché si destination du leg pas saisie (rien à proposer).
    """
    dest = (leg.get("destination") or "").strip().upper()
    if not dest:
        return
    # Cherche tous les legs des missions Amazone qui partent de `dest`,
    # pas uniquement le 1er leg. Ex : si current dest = GUCY, on doit
    # proposer aussi le leg 2 de mission "3.A — DIAP ↔ GUCY" qui est
    # GUCY → DIAP (le retour, partant de GUCY).
    matching: list[tuple[str, dict, int, int]] = []  # (name, leg_dict, leg_idx, n_legs)
    try:
        with db.connect() as c:
            cols = {r[1] for r in c.execute("PRAGMA table_info(route_template)").fetchall()}
            if "official" not in cols:
                return
            rows = c.execute(
                "SELECT name, legs_json FROM route_template "
                "WHERE official = 1 AND variant = 'mission'"
            ).fetchall()
        for r in rows:
            try:
                rlegs = json.loads(r["legs_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(rlegs, list):
                continue
            for sub_idx, sub_leg in enumerate(rlegs):
                if (sub_leg.get("origin") or "").strip().upper() == dest:
                    matching.append((r["name"], sub_leg, sub_idx, len(rlegs)))
    except Exception:
        return
    # Tri des suggestions :
    # 1. Retour home (destination = DBBB ou TOUROU) en premier
    # 2. Continuation vers un autre hub (DIAP, GUCY, GOBD, FKYS, etc.)
    # 3. Variant 1.A / 1.B / 1.C dans cet ordre alphabétique
    HOME_BASES = {"DBBB", "TOUROU"}
    def _sug_key(item):
        _name, _sub_leg, _idx, _n = item
        sub_dest = (_sub_leg.get("destination") or "").upper()
        is_home_return = 0 if sub_dest in HOME_BASES else 1
        return (is_home_return, _name)
    matching.sort(key=_sug_key)
    label = (
        f"➕ Insérer un leg après celui-ci"
        + (f" · {len(matching)} suggestion(s) Amazone depuis {dest}" if matching else "")
    )
    with st.expander(label, expanded=False):
        if st.button(
            "⬜ Leg vide", key=f"ins_blank_{leg_idx}_{_legs_sid()}",
            help="Insère un leg vide à pré-remplir manuellement",
        ):
            st.session_state["_pending_insert_leg"] = {
                "after": leg_idx,
                "legs": [{"origin": dest, "destination": "", "fl": 90,
                          "tas": 140, "route_text": ""}],
            }
            st.rerun()
        for name, sub_leg, sub_idx, n_legs in matching:
            short_name = name.split(" / ", 1)[-1] if " / " in name else name
            sub_dest = (sub_leg.get("destination") or "").upper()
            home_marker = " 🏠" if sub_dest in HOME_BASES else ""
            label_btn = (
                f"📌 {short_name} leg {sub_idx + 1}/{n_legs} · "
                f"{sub_leg['origin']} → {sub_leg['destination']}{home_marker}"
            )
            if st.button(
                label_btn, key=f"ins_mis_{leg_idx}_{name}_{sub_idx}_{_legs_sid()}",
                help=(
                    f"Insère ce leg de la mission. "
                    f"Route : {sub_leg.get('route_text', '?')}. "
                    f"Alt : {sub_leg.get('alternate', '?')}."
                    + (" · Retour vers la base." if sub_dest in HOME_BASES else "")
                ),
            ):
                st.session_state["_pending_insert_leg"] = {
                    "after": leg_idx,
                    "legs": [{
                        "origin": sub_leg["origin"],
                        "destination": sub_leg["destination"],
                        "fl": sub_leg.get("fl", 90),
                        "tas": sub_leg.get("tas", 140),
                        "route_text": sub_leg.get("route_text", ""),
                        "alternate": sub_leg.get("alternate", ""),
                    }],
                }
                st.rerun()


def _leg_editor(idx: int, leg: dict) -> dict:
    sid = _legs_sid()
    kprefix = f"leg_s{sid}_{idx}"

    # Apply any pending route suggestion BEFORE the route_text widget renders.
    pending_route = st.session_state.pop(f"_pending_route_{sid}_{idx}", None)
    if pending_route is not None:
        st.session_state[f"{kprefix}_route"] = pending_route

    # Header dynamique : "Leg N — DBBB → DIAP" avec la destination
    # courante visible direct. Si pas encore saisies, on garde un
    # placeholder discret pour ne pas casser la lisibilité.
    cur_orig = (leg.get("origin") or "").strip().upper() or "—"
    cur_dest = (leg.get("destination") or "").strip().upper() or "—"
    st.markdown(f"### Leg {idx + 1}  ·  **{cur_orig} → {cur_dest}**")
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
    # couple O/D. La 1ère variante du catalogue (1.A, 13, etc.) est
    # **appliquée automatiquement** dès que :
    #   (a) origin + destination changent (= nouvelle paire vue), ou
    #   (b) route_text est vide, ou
    #   (c) route_text vaut exactement la dernière valeur auto-appliquée
    #       (= l'OPS n'a pas modifié manuellement entre temps).
    # Sinon, la saisie manuelle de l'OPS reste intacte. L'expander reste
    # affiché pour permettre de switch entre variantes (1.A maritime vs
    # 1.B overflight vs 1.C techstop).
    if origin and destination and hasattr(db, "find_canonical_routes"):
        try:
            canon_rows = db.find_canonical_routes(origin, destination, ac_type or None)
        except Exception:
            canon_rows = []
        # Apply explicite (bouton click au tour précédent)
        pending_canon = st.session_state.pop(f"{kprefix}_pending_canon", None)
        if pending_canon is not None:
            st.session_state[f"{kprefix}_route"] = pending_canon["route_text"]
            st.session_state[f"{kprefix}_alternate"] = pending_canon["alternate"] or ""
            st.session_state[f"{kprefix}_last_auto_route"] = pending_canon["route_text"]
        # Auto-apply de la 1ère variante quand approprié
        if canon_rows:
            best = canon_rows[0]
            best_route = json.loads(best["legs_json"])[0]["route_text"]
            best_alt = best["alternate"] or ""
            last_pair_key = f"{kprefix}_last_canon_pair"
            current_pair = f"{origin}|{destination}|{ac_type or ''}"
            last_pair = st.session_state.get(last_pair_key)
            last_auto = st.session_state.get(f"{kprefix}_last_auto_route", "")
            cur_route = (st.session_state.get(f"{kprefix}_route", "") or "").strip()
            pair_changed = (last_pair is not None and last_pair != current_pair)
            owns_value = (not cur_route) or (cur_route == last_auto)
            if pending_canon is None and (pair_changed or owns_value):
                st.session_state[f"{kprefix}_route"] = best_route
                st.session_state[f"{kprefix}_alternate"] = best_alt
                st.session_state[f"{kprefix}_last_auto_route"] = best_route
            st.session_state[last_pair_key] = current_pair
        if canon_rows:
            with st.expander(
                f"📌 {len(canon_rows)} route(s) officielle(s) "
                f"{origin}→{destination} ({canon_rows[0]['operator'] or '—'}) — "
                f"1ère variante appliquée auto",
                expanded=False,
            ):
                st.caption(
                    "⚠️ **Routes mandatoires** côté opérateur. "
                    "Perfs calibrées **ISA+20°C, still air, OEW DHC6-400 "
                    "TY-BAB 3813 kg** — pas de marge head/tailwind. "
                    "Click sur une autre variante pour la switch."
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
    # Pending apply depuis le tour précédent (click sur un bouton suggestion)
    pending_alt = st.session_state.pop(f"{kprefix}_pending_alt", None)
    if pending_alt is not None:
        st.session_state[f"{kprefix}_alternate"] = pending_alt
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

    # 📍 Alternates auto-suggérés (top 5 aéros IFR proches, filtrés par perf
    # min_runway de l'appareil). Affiché en boutons cliquables seulement si
    # destination saisie et hasattr db.find_alternate_candidates (déploiement
    # partiel safe).
    if destination and hasattr(db, "find_alternate_candidates"):
        try:
            min_rwy = int(ac_perf["min_runway_ft"]) if (ac_perf and ac_perf["min_runway_ft"]) else None
            alt_candidates = db.find_alternate_candidates(
                destination, min_runway_ft=min_rwy, max_distance_nm=250, limit=5,
            )
        except Exception:
            alt_candidates = []
        if alt_candidates:
            # Suggestions cachées par défaut pour réduire le bruit visuel.
            # L'OPS qui veut changer d'alternate déplie ; sinon la valeur
            # auto-suggérée reste appliquée sans le mur de boutons.
            with st.expander(
                f"📍 {len(alt_candidates)} alternates compatibles dans 250 NM",
                expanded=False,
            ):
                cols = st.columns(min(len(alt_candidates), 5))
                for i, ap in enumerate(alt_candidates):
                    with cols[i]:
                        dist_nm = db._haversine_nm(
                            float(db.find_airport(destination)["lat"]),
                            float(db.find_airport(destination)["lon"]),
                            float(ap["lat"]), float(ap["lon"]),
                        )
                        label = f"{ap['icao']}\n{ap['name'][:18]}\n{dist_nm:.0f} NM"
                        if st.button(
                            label, key=f"{kprefix}_alt_{ap['icao']}",
                            help=f"{ap['name']} — rwy max {ap['max_runway_ft'] or '?'} ft",
                            width="stretch",
                        ):
                            st.session_state[f"{kprefix}_pending_alt"] = ap["icao"]
                            st.rerun()

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
if not _is_admin():
    # Masque le badge "Manage app" et la chrome de déploiement Streamlit
    # Cloud aux utilisateurs non-admin. Note : ce n'est PAS une sécurité
    # (utilisateur curieux peut inspecter le DOM ou aller direct sur
    # share.streamlit.io). Sans credentials Streamlit Cloud il ne pourra
    # rien faire de toute façon. C'est juste cosmétique pour ne pas
    # tenter l'OPS qui n'aurait rien à faire dans ces menus.
    st.markdown(
        """
        <style>
        /* Badge "Manage app" Streamlit Cloud — masqué aux non-admins */
        .viewerBadge_container__1QSob,
        .viewerBadge_link__qRIco,
        .viewerBadge_text__1JaDK,
        [data-testid="stDecoration"],
        [data-testid="manage-app-button"],
        [data-testid="stToolbar"] { display: none !important; }
        /* Menu hamburger en haut à droite (Settings, Rerun, etc) */
        #MainMenu { visibility: hidden !important; }
        footer { visibility: hidden !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )


_BASE_PAGES = [
    ("1.", "Mission & profils", "Avion, équipage, compagnie"),
    ("2.", "Legs", "Itinéraire, dates, route"),
    ("3.", "Preview & export", "Récapitulatif des legs, validation, puis export .docx + FPL + briefing"),
]
_ADMIN_PAGE = ("⚙", "Admin", "Historique + APIs + données opérationnelles (admin seulement)")
# Admin reste invisible aux utilisateurs non-admin (st.secrets ADMIN_EMAIL).
# Historique a migré dans Admin (= section sous expander), n'est plus une
# page autonome. Page count fluctue donc selon le profil utilisateur.
PAGES = _BASE_PAGES + ([_ADMIN_PAGE] if _is_admin() else [])


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
    _step_nav_footer()


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

if page_idx == 1:
    # Picker Mission : on filtre pour ne montrer que des MISSIONS
    # complètes (round-trips ou tours multi-legs), pas les 56 routes
    # uni-directionnelles du catalogue qui sont consommées par
    # l'auto-apply au niveau leg. Filtre = user-saved OU multi-leg
    # officielle (>=2 legs).
    tpl_rows_all = db.list_route_templates()
    tpl_rows = []
    for r in tpl_rows_all:
        try:
            legs = json.loads(r["legs_json"])
            n_legs = len(legs) if isinstance(legs, list) else 0
        except (json.JSONDecodeError, TypeError):
            n_legs = 0
        is_official = "official" in r.keys() and r["official"]
        is_multi_leg = n_legs > 1
        # On garde : user-saved (any leg count) OR officielle multi-leg
        if not is_official or is_multi_leg:
            tpl_rows.append(r)
    by_cat: dict[str, list] = {}
    for r in tpl_rows:
        raw_cat = r["category"] if "category" in r.keys() and r["category"] else "Autres"
        # Normalisation des catégories :
        # - Officielles : toutes regroupées sous "Amazone" (les anciennes
        #   "Bénin/Cameroun/..." héritées ou la nouvelle "Amazone").
        # - User-saved : "Mes tours" pour les composites custom.
        if raw_cat.upper() == "AMAZONE" or raw_cat in (
            "Bénin", "Cameroun", "Mauritanie", "Côte d'Ivoire", "Sénégal-Guinée"
        ):
            cat = "Amazone"
        elif raw_cat == "Mes tours" or "saved" in raw_cat.lower():
            cat = "Mes tours"
        else:
            cat = raw_cat
        by_cat.setdefault(cat, []).append(r)
    # Tri intra-dossier : missions Bénin (préfixe [BJ]) en tête, puis le
    # reste alphabétique. Ça met le hub naturel de l'opérateur en premier.
    def _mission_sort_key(r) -> tuple:
        name = r["name"] or ""
        # Priorité 0 si touche le Bénin (préfixe [BJ] ou DBBB/TOUROU dans le nom)
        bj_first = 0 if (
            "[BJ]" in name or "DBBB" in name or "TOUROU" in name
        ) else 1
        return (bj_first, name)
    for cat in by_cat:
        by_cat[cat].sort(key=_mission_sort_key)
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

    # Apply un insert pending depuis le tour précédent (click sur ➕
    # ou sur une mission Amazone proposée en sub-menu).
    pending_insert = st.session_state.pop("_pending_insert_leg", None)
    if pending_insert is not None:
        idx_after = pending_insert["after"]
        new_legs = pending_insert["legs"]
        st.session_state.legs[idx_after + 1:idx_after + 1] = new_legs
        _bump_legs_sid()

    for i, leg in enumerate(st.session_state.legs):
        # Chaque leg dans un container bordé → séparation visuelle
        # nette entre legs, lecture beaucoup plus claire qu'un flat
        # markdown ### Leg N suivi d'un mur d'inputs.
        with st.container(border=True):
            st.session_state.legs[i] = _leg_editor(i, leg)
        _render_insert_after_leg(i, leg)

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
        if valid and st.button("📋 Placer cette route dans l'historique", help="Ajoute cette route à la page Historique pour pouvoir la recharger plus tard"):
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

        # Vent : si |delta_pct| >= 10% du still-air, on affiche le temps
        # wind-corrected à la place. Sinon on garde le still-air sans
        # mention pour ne pas polluer visuellement. Silent fail si
        # Open-Meteo down (network/timeout/J+16 hors fenêtre).
        wind_info: dict | None = None
        try:
            from app import wind_client
            coord_pts = [
                (p.lat, p.lon) for p in (resolution.points or [])
                if p.lat is not None and p.lon is not None
            ]
            if len(coord_pts) >= 2 and isinstance(leg.get("eobt"), dt.datetime):
                wind_info = wind_client.compute_wind_adjusted_time(
                    coord_pts, resolution.total_distance_nm,
                    leg.get("tas", 140), leg["eobt"], int(leg.get("fl", 90)),
                )
        except Exception:
            wind_info = None

        c1, c2, c3 = st.columns(3)
        c1.metric("Distance", f"{resolution.total_distance_nm:.0f} NM")
        # Affiche temps corrigé seulement si le vent change le temps de >=10%
        if wind_info and wind_info["available"] and abs(wind_info["delta_pct"]) >= 10:
            c2.metric("Temps de vol", f"{wind_info['wind_adjusted_min']:.0f} min")
            kind = "headwind" if wind_info["headwind_kt"] >= 0 else "tailwind"
            sign = "+" if wind_info["headwind_kt"] >= 0 else "−"
            c2.caption(
                f"💨 Vent pris en compte ({kind} {sign}{abs(wind_info['headwind_kt']):.0f} kt · "
                f"still-air était {wind_info['still_air_min']:.0f} min)"
            )
        else:
            c2.metric("Temps de vol", f"{resolution.total_time_min:.0f} min")
        c3.metric("Pays traversés", str(len(resolution.segments)))

        # Carte visuelle de la route (origine + destination + waypoints + alt)
        try:
            _render_leg_map(resolution, leg)
        except Exception as e:
            st.caption(f"_(carte indisponible : {type(e).__name__})_")

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


def _render_historique_section() -> None:
    """Bloc Historique des routes en base — utilisé comme expander
    dans la page Admin. Permet à l'OPS admin de gérer ses templates
    sauvegardés et de voir le catalogue officiel."""
    rows = db.list_route_templates()
    if not rows:
        st.info(
            "Aucune route en base. Génère une DIC ou clique sur "
            "« 📋 Placer cette route dans l'historique » en page Legs "
            "pour démarrer la bibliothèque."
        )
        return
    from collections import defaultdict
    by_cat: dict[str, list] = defaultdict(list)
    for r in rows:
        by_cat[r["category"] or "Divers"].append(r)
    st.caption(
        f"**{len(rows)} route(s)** réparties sur **{len(by_cat)} dossier(s)**. "
        f"Les routes **🔒 officielles** (catalogue Amazone) sont protégées."
    )
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
                is_official = "official" in r.keys() and r["official"]
                with c1:
                    prefix = "🔒 " if is_official else ""
                    st.markdown(f"{prefix}**{r['name']}**")
                    st.caption(f"`{summary}` · {len(legs_json)} leg(s)")
                with c2:
                    if st.button("📂 Charger", key=f"hist_load_{r['id']}"):
                        _apply_template(r["name"], rows)
                        _goto_page(1)
                        st.rerun()
                with c3:
                    if is_official:
                        st.caption("🔒 officielle")
                    else:
                        if st.button("🗑️", key=f"hist_del_{r['id']}", help="Supprimer"):
                            with db.connect() as c:
                                c.execute("DELETE FROM route_template WHERE id = ?", (r["id"],))
                            st.rerun()


# Page Admin (anciennement page_idx == 4, maintenant 3 car Historique
# n'est plus une page autonome). Gate sur _is_admin() : si l'utilisateur
# courant n'est pas listé dans st.secrets["ADMIN_EMAIL"], pas d'accès.
if page_idx == 3 and _is_admin():
    st.caption(
        "Configuration et données opérationnelles. Ces sections n'impactent pas "
        "la génération du DIC tant que tu n'en as pas besoin."
    )

    # Historique des routes en base — migré ici depuis l'ancienne page
    # dédiée. Accessible uniquement aux admins maintenant.
    with st.expander("📋 Historique missions / routes en base", expanded=False):
        _render_historique_section()

    # Statut des APIs externes — un coup d'œil rapide pour savoir si
    # quelque chose est cassé côté upstream.
    with st.expander("🌐 Statut APIs externes", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Open-Meteo (vent aloft)**")
            if st.button("🔍 Tester Open-Meteo", key="admin_om_test"):
                from app import wind_client
                hc = wind_client.health_check()
                if hc["ok"]:
                    st.success(f"✅ OK ({hc['latency_ms']} ms)")
                else:
                    st.error(f"❌ KO : {hc['error']}")
            try:
                from app import wind_client
                ls = wind_client.get_last_status()
                if ls["ok"] is None:
                    st.caption("_(aucun appel depuis le boot)_")
                elif ls["ok"]:
                    st.caption(f"✓ dernier appel OK · {ls['last_check'].strftime('%H:%M:%SZ') if ls['last_check'] else '?'}")
                else:
                    st.caption(
                        f"⚠️ dernier appel KO · {ls['last_check'].strftime('%H:%M:%SZ') if ls['last_check'] else '?'} · "
                        f"erreur : {ls['error']}"
                    )
            except Exception:
                pass
        with c2:
            st.markdown("**Autorouter.aero**")
            from app import autorouter_client
            ar_cfg = autorouter_client.AutorouterConfig.from_secrets(st.secrets)
            if not ar_cfg.is_configured():
                st.warning("⚠️ Pas configuré")
                with st.popover("Voir config Secrets requise"):
                    st.caption(
                        "Ajoute ces 4 lignes dans Streamlit Cloud → "
                        "Settings → Secrets :"
                    )
                    st.code(
                        "[autorouter]\n"
                        'base_url = "https://api.autorouter.aero/v1.0"\n'
                        'token_url = "https://api.autorouter.aero/v1.0/oauth2/token"\n'
                        'client_id = "ton.email@example.org"\n'
                        'client_secret = "ton-mot-de-passe-autorouter"\n',
                        language="toml",
                    )
            else:
                st.caption(f"✓ Configuré · `{ar_cfg.base_url}`")
                if st.button("🔍 Tester autorouter", key="admin_ar_test"):
                    try:
                        info = autorouter_client.ping_version(ar_cfg)
                        st.success(
                            f"✅ API v{info.get('major', '?')}."
                            f"{info.get('minor', '?')}."
                            f"{info.get('patch', '?')} "
                            f"({'prod' if info.get('production') else 'sandbox'})"
                        )
                        try:
                            autorouter_client._get_token(ar_cfg)
                            st.success("✅ Token OAuth obtenu — credentials valides.")
                        except autorouter_client.AutorouterError as e:
                            st.error(f"❌ Token KO : {e}")
                    except autorouter_client.AutorouterError as e:
                        st.error(f"❌ Version probe KO : {e}")
        with c3:
            st.markdown("**OpenAIP** _(airspaces P/R/D)_")
            if st.button("🔍 Tester OpenAIP", key="admin_oa_test"):
                try:
                    from app import openaip_client
                    hc = openaip_client.health_check()
                    if hc["ok"]:
                        st.success(f"✅ OK ({hc['latency_ms']} ms)")
                    else:
                        st.error(f"❌ KO : {hc['error']}")
                except Exception as e:
                    st.error(f"❌ Module : {e}")
            if st.button("🔬 Diag secrets", key="admin_oa_debug",
                         help="Affiche les clés top-level présentes dans st.secrets (valeurs masquées)"):
                try:
                    keys = list(st.secrets.keys())
                    if keys:
                        st.code("Clés top-level dans st.secrets :\n" + "\n".join(f"  - {k}" for k in keys))
                    else:
                        st.warning("st.secrets est vide.")
                except Exception as e:
                    st.error(f"Lecture impossible : {e}")
            st.caption(
                "_(pénalise les zones militaires dans le route suggester. "
                "Sans clé `OPENAIP_API_KEY` les routes restent calculées, "
                "juste sans cette pénalisation.)_"
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

    _step_nav_footer()
