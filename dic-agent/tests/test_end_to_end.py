"""End-to-end test: rebuild the 5 example DIC and emit .docx files."""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from app import docx_generator, route_engine

OUT = Path(__file__).resolve().parent.parent / "out"
OUT.mkdir(exist_ok=True)


def _common_mission(fmt: str = "FRA") -> dict:
    return {
        "reference": "MSG DU 10/04/2026",
        "amendment": "V2",
        "mission_number": "06543-26/00",
        "template_format": fmt,
        "requesting_state": "FRANCE",
        "operator": "AMAZONE AIRLINES / DYNAMI AVIATION OPS",
        "aircraft_count_type": "1 DHC6-400",
        "registration": "TY-BAB",
        "spare_aircraft": "TY-BAB OR SUBSTITUTE",
        "callsign": "TY-BAB",
        "n_crew": 2,
        "pilots": "Aditya Tri Hertiawan and Saba Muhammad",
        "sensors": "NO",
        "armament": "NO",
        "ew": "NO",
        "purpose": "LOGISTIC FLIGHT WITHOUT DANGEROUS GOODS",
        "alternates": "ABUJA (DNAA) / LOME (DXXX)",
        "radio_frequencies": "VHF",
        "n_passengers": "TBN",
        "vip_title": "TBN",
        "dg_details": "NIL",
        "remarks": "",
        "poc_name": "OF1 MERLIN",
        "poc_phone": "+ 225 07 15 013 761",
        "poc_email_personal": "cos-det14.j10@intradef.gouv.fr",
        "poc_email_functional": "",
        "poc_fax": "",
        "date_of_flight": "XXXXXXX",
    }


def _build_one(name: str, mission: dict, legs: list[dict]) -> None:
    idx = route_engine._build_country_index()
    leg_payloads = []
    departures, destinations = [], []
    for leg in legs:
        res = route_engine.compute_leg(
            eobt=leg["eobt"],
            origin_icao=leg["origin"],
            destination_icao=leg["destination"],
            route_text=leg["route_text"],
            fl=leg["fl"],
            tas_kt=leg["tas"],
            country_index=idx,
        )
        print(f"\n  {leg['origin']}→{leg['destination']}  {res.total_distance_nm:.0f}NM  {res.total_time_min:.0f}min  warnings={len(res.warnings)}")
        for s in res.segments:
            entry = route_engine.format_zulu(s.entry_time, mission["template_format"]) if s.entry_time else "?"
            exit_ = route_engine.format_zulu(s.exit_time, mission["template_format"]) if s.exit_time else "?"
            print(f"    {s.state_name:12} {s.entry_label} {entry} | {s.route_in_country} | {s.exit_label} {exit_}")
        for w in res.warnings:
            print(f"    ⚠ {w}")
        leg_input = {
            "origin": leg["origin"],
            "destination": leg["destination"],
            "origin_iso": None,
            "destination_iso": None,
            "overrides": {},
        }
        leg_payloads.append(docx_generator.serialize_leg(leg_input, res, mission["template_format"]))
        departures.append(leg["origin"])
        destinations.append(leg["destination"])
    mission["departure_airport"] = " / ".join(departures)
    mission["destination_airport"] = " / ".join(destinations)
    data = docx_generator.build_dic_document(mission, leg_payloads)
    out_path = OUT / f"{name}.docx"
    out_path.write_bytes(data)
    print(f"  → {out_path} ({len(data)} bytes)")


def test_cotonou_minna() -> None:
    print("\n=== Cotonou ↔ Minna ===")
    m = _common_mission("FRA")
    _build_one(
        "DIC_COTONOU_MINNA_COTONOU",
        m,
        [
            {
                "origin": "DBBB", "destination": "DNMN",
                "fl": 90, "tas": 140,
                "eobt": dt.datetime(2026, 4, 20, 6, 0, tzinfo=dt.timezone.utc),
                "route_text": "TYE POLTO LAG L433 IBA R778 TEGDA MNA",
            },
            {
                "origin": "DNMN", "destination": "DBBB",
                "fl": 100, "tas": 140,
                "eobt": dt.datetime(2026, 4, 20, 12, 0, tzinfo=dt.timezone.utc),
                "route_text": "MNA MAGIA V377 LAG POLTO TYE",
            },
        ],
    )


def test_cotonou_abuja() -> None:
    print("\n=== Cotonou ↔ Abuja ===")
    m = _common_mission("FRA")
    _build_one(
        "DIC_COTONOU_ABUJA_COTONOU",
        m,
        [
            {
                "origin": "DBBB", "destination": "DNAA",
                "fl": 90, "tas": 140,
                "eobt": dt.datetime(2026, 4, 21, 6, 0, tzinfo=dt.timezone.utc),
                "route_text": "TYE POLTO LAG R778 KELIG W951 MESES ABC",
            },
            {
                "origin": "DNAA", "destination": "DBBB",
                "fl": 100, "tas": 140,
                "eobt": dt.datetime(2026, 4, 21, 12, 0, tzinfo=dt.timezone.utc),
                "route_text": "ABC VONUK H340 LAG POLTO TYE",
            },
        ],
    )


def test_cotonou_ilorin() -> None:
    print("\n=== Cotonou ↔ Ilorin ===")
    m = _common_mission("FRA")
    _build_one(
        "DIC_COTONOU_ILORIN_COTONOU",
        m,
        [
            {
                "origin": "DBBB", "destination": "DNIL",
                "fl": 90, "tas": 140,
                "eobt": dt.datetime(2026, 4, 22, 6, 0, tzinfo=dt.timezone.utc),
                "route_text": "TYE POLTO LAG L433 IBA ILR",
            },
            {
                "origin": "DNIL", "destination": "DBBB",
                "fl": 100, "tas": 140,
                "eobt": dt.datetime(2026, 4, 22, 12, 0, tzinfo=dt.timezone.utc),
                "route_text": "ILR USGUN V377 LAG POLTO TYE",
            },
        ],
    )


def test_tourou_kainji() -> None:
    print("\n=== TOUROU ↔ KAINJI (coord brutes) ===")
    from app import db
    db.save_user_airport("ZZTR", "TOUROU", "BJ", 9.580000, 3.230000, is_military=True)
    m = _common_mission("FRA")
    m["alternates"] = "ILORIN (DNIL) / COTONOU (DBBB)"
    _build_one(
        "DIC_TOUROU_KAINJI_TOUROU",
        m,
        [
            {
                "origin": "ZZTR", "destination": "DNKJ",
                "fl": 90, "tas": 140,
                "eobt": dt.datetime(2026, 4, 23, 6, 0, tzinfo=dt.timezone.utc),
                "route_text": "N 9°34'45.56\" / E 3°14'7.09\"",
            },
            {
                "origin": "DNKJ", "destination": "ZZTR",
                "fl": 100, "tas": 140,
                "eobt": dt.datetime(2026, 4, 23, 9, 0, tzinfo=dt.timezone.utc),
                "route_text": "N 9°34'45.56\" / E 3°14'7.09\"",
            },
        ],
    )


def test_rci_long() -> None:
    print("\n=== RCI long (ICAO) ===")
    m = _common_mission("ICAO")
    m["alternates"] = "YAMOUSSOUKRO RCI DIYO / BOUAKE RCI DIBK / LOME TOGO DXXX"
    m["radio_frequencies"] = "V/U/HF"
    _build_one(
        "DIC_RCI_long",
        m,
        [
            {"origin": "DBBB", "destination": "DIAP", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 25, 3, 35, tzinfo=dt.timezone.utc),
             "route_text": "TYE EBUSO ENKIT ARABA"},
            {"origin": "DIAP", "destination": "DIBK", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 25, 7, 30, tzinfo=dt.timezone.utc),
             "route_text": "BKY"},
            {"origin": "DIBK", "destination": "DIAP", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 25, 9, 40, tzinfo=dt.timezone.utc),
             "route_text": "BKY"},
            {"origin": "DIAP", "destination": "DIKO", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 25, 12, 30, tzinfo=dt.timezone.utc),
             "route_text": "BKY BONTO KRG"},
            {"origin": "DIKO", "destination": "DIAP", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 25, 15, 40, tzinfo=dt.timezone.utc),
             "route_text": "KRG BONTO BKY"},
            {"origin": "DIAP", "destination": "DBBB", "fl": 90, "tas": 140,
             "eobt": dt.datetime(2026, 4, 26, 7, 0, tzinfo=dt.timezone.utc),
             "route_text": "ARABA ENKIT EBUSO TYE"},
        ],
    )


if __name__ == "__main__":
    test_cotonou_minna()
    test_cotonou_abuja()
    test_cotonou_ilorin()
    test_tourou_kainji()
    test_rci_long()
    print("\nAll tests OK.")
