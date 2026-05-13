"""Generate the DIC .docx file in either FRA short or ICAO long format.

We build the document programmatically with python-docx rather than from a static
template, because (a) the legs/segments are dynamic and (b) we want exact control
over the table grid that DIC ANNEX A requires.
"""
from __future__ import annotations

import datetime as dt
from io import BytesIO
from typing import Any

from docx import Document
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
from docx.shared import Cm, Pt, RGBColor

from app.route_engine import LegResolution, format_zulu

GRAY = RGBColor(0xE0, 0xE0, 0xE0)
BLACK = RGBColor(0x00, 0x00, 0x00)


def _set_cell_bg(cell, hex_color: str) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tc_pr.append(shd)


def _bold(cell, text: str, size: int = 9) -> None:
    cell.text = ""
    p = cell.paragraphs[0]
    run = p.add_run(text)
    run.bold = True
    run.font.size = Pt(size)


def _plain(cell, text: str, size: int = 9, bold: bool = False) -> None:
    cell.text = ""
    p = cell.paragraphs[0]
    run = p.add_run(text or "")
    run.bold = bold
    run.font.size = Pt(size)


def _set_widths(table, widths_cm: list[float]) -> None:
    for row in table.rows:
        for idx, cell in enumerate(row.cells):
            if idx < len(widths_cm):
                cell.width = Cm(widths_cm[idx])


def _kv_row(table, label: str, value: str) -> None:
    row = table.add_row()
    _bold(row.cells[0], label)
    _plain(row.cells[1], value)


def _header_row(table, *labels: str, bg: str = "BFBFBF") -> None:
    row = table.add_row()
    for i, l in enumerate(labels):
        _bold(row.cells[i], l)
        _set_cell_bg(row.cells[i], bg)


def build_dic_document(mission: dict, leg_data: list[dict]) -> bytes:
    fmt = mission.get("template_format", "FRA")
    if fmt == "ICAO":
        return _build_icao_long(mission, leg_data)
    return _build_fra_short(mission, leg_data)


def _build_fra_short(mission: dict, leg_data: list[dict]) -> bytes:
    doc = Document()
    section = doc.sections[0]
    section.top_margin = Cm(1.2)
    section.bottom_margin = Cm(1.2)
    section.left_margin = Cm(1.5)
    section.right_margin = Cm(1.5)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("ANNEX A")
    r.bold = True
    r.font.size = Pt(11)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("FRA DIPLOMATIC CLEARANCE    (DIC) FORM")
    r.bold = True
    r.font.size = Pt(14)

    t = doc.add_table(rows=0, cols=2)
    t.style = "Table Grid"
    _kv_row(t, "(1) Reference number:", mission.get("reference", ""))
    _kv_row(t, "(2) Amendment number:", mission.get("amendment", ""))
    _kv_row(t, "Mission number:", mission.get("mission_number", ""))
    _set_widths(t, [5, 12])

    doc.add_paragraph()

    # State / R / N / L / VIP / DG / A / FR / EXISTING DIC NUMBER — one row per leg per country
    state_tbl = doc.add_table(rows=0, cols=9)
    state_tbl.style = "Table Grid"
    _header_row(state_tbl, "State", "R", "N", "L", "VIP", "DG", "A", "FR", "EXISTING DIC NUMBER")
    for li, leg in enumerate(leg_data, start=1):
        # Leg label row
        row = state_tbl.add_row()
        _bold(row.cells[0], f"Leg {li}")
        for i in range(1, 9):
            _plain(row.cells[i], "")
            _set_cell_bg(row.cells[i], "F2F2F2")
        for seg in leg["segments"]:
            row = state_tbl.add_row()
            _plain(row.cells[0], seg["state_name"])
            _plain(row.cells[1], "X" if seg.get("R") else "")
            _plain(row.cells[2], "X" if seg.get("N") else "")
            _plain(row.cells[3], "X" if seg.get("L") else "")
            _plain(row.cells[4], "X" if seg.get("VIP") else "")
            _plain(row.cells[5], "X" if seg.get("DG") else "")
            _plain(row.cells[6], "X" if seg.get("A") else "")
            _plain(row.cells[7], seg.get("FR", "I"))
            _plain(row.cells[8], seg.get("existing_dic", ""))

    doc.add_paragraph()

    info_tbl = doc.add_table(rows=0, cols=3)
    info_tbl.style = "Table Grid"
    _header_row(info_tbl, "SERIAL", "REQUESTED INFORMATION", "INFORMATION SUBMITTED")

    def _info(serial: str, label: str, value: str) -> None:
        r = info_tbl.add_row()
        _plain(r.cells[0], serial)
        _plain(r.cells[1], label)
        _plain(r.cells[2], value)

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "AIRCRAFT AND CREW")

    _info("(12)", "Requesting state", mission.get("requesting_state", "FRANCE"))
    _info("(12a)", "Operator", mission.get("operator", ""))
    _info("(13)", "Number and type of aircraft", mission.get("aircraft_count_type", ""))
    _info("(14)", "Aircraft registration", mission.get("registration", ""))
    _info("(15)", "Spare aircraft", mission.get("spare_aircraft", ""))
    _info("(16)", "Callsign (including spare if different)", mission.get("callsign", ""))
    _info("(17)", "Number of crew members", str(mission.get("n_crew", "")))
    _info("(18)", "Pilot rank and name", mission.get("pilots", ""))
    _info("(19)", "Photographic sensors and/or cameras", mission.get("sensors", "NO"))
    _info("(20)", "Armament", mission.get("armament", "NO"))
    _info("(21)", "Electronic warfare equipment", mission.get("ew", "NO"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "FLIGHT DETAILS (Detailed routing in Appendix 1)")

    _info("(22)", "Date of flight", mission.get("date_of_flight", ""))
    _info("(23)", "Purpose of flight", mission.get("purpose", ""))
    _info("(24)", "Departure airport", mission.get("departure_airport", ""))
    _info("(25)", "Destination airport(s)", mission.get("destination_airport", ""))
    _info("(26)", "Alternate airport(s)", mission.get("alternates", ""))
    _info("(27)", "Radio frequencies", mission.get("radio_frequencies", "VHF"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "LOAD INFORMATION")

    _info("(28)", "Number of passengers", mission.get("n_passengers", "TBN"))
    _info("(29)", "VIP title/rank and name", mission.get("vip_title", "TBN"))
    _info("(30)", "DG details", mission.get("dg_details", "NIL"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "REMARKS")
    _info("(31)", "", mission.get("remarks", ""))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "POINT OF CONTACT")

    _info("(32)", "Rank, name, first name", mission.get("poc_name", ""))
    _info("(33)", "Telephone number", mission.get("poc_phone", ""))
    _info("(34)", "Personal E-mail", mission.get("poc_email_personal", ""))
    _info("(35)", "Functional E-mail", mission.get("poc_email_functional", ""))
    _info("(36)", "Fax", mission.get("poc_fax", ""))

    _set_widths(info_tbl, [1.5, 6, 9.5])

    doc.add_page_break()

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("ANNEX A, APPENDIX 1 — DETAILED ITINERARY")
    r.bold = True
    r.font.size = Pt(12)

    for li, leg in enumerate(leg_data, start=1):
        head = doc.add_paragraph()
        run = head.add_run(
            f"Leg {li}  {leg.get('origin','')} → {leg.get('destination','')}    Callsign: {mission.get('callsign','')}"
        )
        run.bold = True
        run.font.size = Pt(10)

        leg_tbl = doc.add_table(rows=0, cols=6)
        leg_tbl.style = "Table Grid"
        _header_row(
            leg_tbl,
            "State",
            "Entry point and timing or airfield + EOBT",
            "Route over Territory",
            "Exit point and timing or airfield + EIBT",
            "FL",
            "TAS",
        )
        for seg in leg["segments"]:
            row = leg_tbl.add_row()
            _plain(row.cells[0], seg["state_name"])
            _plain(
                row.cells[1],
                f"{seg['entry_label']}\n{seg['entry_time_str']}",
            )
            _plain(row.cells[2], seg["route_in_country"])
            _plain(
                row.cells[3],
                f"{seg['exit_label']}\n{seg['exit_time_str']}",
            )
            _plain(row.cells[4], str(seg.get("fl") or ""))
            _plain(row.cells[5], str(seg.get("tas") or ""))
        _set_widths(leg_tbl, [2.5, 4.5, 4.5, 4.5, 1.2, 1.2])
        doc.add_paragraph()

    buf = BytesIO()
    doc.save(buf)
    return buf.getvalue()


def _build_icao_long(mission: dict, leg_data: list[dict]) -> bytes:
    doc = Document()
    section = doc.sections[0]
    section.top_margin = Cm(1.2)
    section.bottom_margin = Cm(1.2)
    section.left_margin = Cm(1.5)
    section.right_margin = Cm(1.5)

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("OVERFLIGHT REQUEST")
    r.bold = True
    r.font.size = Pt(14)

    t = doc.add_table(rows=0, cols=2)
    t.style = "Table Grid"
    _kv_row(t, "(1) Reference number", mission.get("reference", ""))
    _kv_row(t, "(2) Amendment number", mission.get("amendment", ""))
    _set_widths(t, [5, 12])

    doc.add_paragraph()

    state_tbl = doc.add_table(rows=0, cols=9)
    state_tbl.style = "Table Grid"
    _header_row(state_tbl, "STATE", "R", "N", "L", "DG", "A", "FR", "EXISTING DIC NUMBER", "LEG")
    for li, leg in enumerate(leg_data, start=1):
        for seg in leg["segments"]:
            row = state_tbl.add_row()
            _plain(row.cells[0], seg["state_name"])
            _plain(row.cells[1], "X" if seg.get("R") else "")
            _plain(row.cells[2], "X" if seg.get("N") else "")
            _plain(row.cells[3], "X" if seg.get("L") else "")
            _plain(row.cells[4], "X" if seg.get("DG") else "")
            _plain(row.cells[5], "X" if seg.get("A") else "")
            _plain(row.cells[6], seg.get("FR", "I"))
            _plain(row.cells[7], seg.get("existing_dic", ""))
            _plain(row.cells[8], f"LEG {li}")

    doc.add_paragraph()

    info_tbl = doc.add_table(rows=0, cols=3)
    info_tbl.style = "Table Grid"
    _header_row(info_tbl, "SERIAL", "REQUESTED INFORMATION", "INFORMATION SUBMITTED")

    def _info(serial: str, label: str, value: str) -> None:
        r = info_tbl.add_row()
        _plain(r.cells[0], serial)
        _plain(r.cells[1], label)
        _plain(r.cells[2], value)

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "AIRCRAFT AND CREW")

    _info("(11)", "Requesting state", mission.get("requesting_state", "FRANCE"))
    _info("(11a)", "Operator", mission.get("operator", ""))
    _info("(12)", "Number and type of aircraft", mission.get("aircraft_count_type", ""))
    _info("(13)", "Aircraft registration", mission.get("registration", ""))
    _info("(14)", "Spare aircraft", mission.get("spare_aircraft", "/"))
    _info("(15)", "Callsign (including spare if different)", mission.get("callsign", ""))
    _info("(16)", "Number of crew members", str(mission.get("n_crew", "")))
    _info("(17)", "Pilot rank and name", mission.get("pilots", ""))
    _info("(18)", "Photographic sensors and/or cameras", mission.get("sensors", "NO"))
    _info("(19)", "Armament", mission.get("armament", "NO"))
    _info("(20)", "Electronic warfare equipment", mission.get("ew", "NO"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "FLIGHT DETAILS")

    _info("(21)", "Date of flight", mission.get("date_of_flight", ""))
    _info("(22)", "Purpose of flight", mission.get("purpose", ""))
    _info("(23)", "Departure airport(s)", mission.get("departure_airport", ""))
    _info("(24)", "Destination airport(s)", mission.get("destination_airport", ""))
    _info("(25)", "Alternate airport(s)", mission.get("alternates", ""))
    _info("(26)", "Radio frequencies", mission.get("radio_frequencies", "V/U/HF"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "LOAD INFORMATION")

    _info("(27)", "Number of passengers", mission.get("n_passengers", "TBN"))
    _info("(28)", "VIP title/rank and name", mission.get("vip_title", "NIL"))
    _info("(29)", "DG details", mission.get("dg_details", "NO DG"))

    r = info_tbl.add_row()
    for c in r.cells:
        _set_cell_bg(c, "BFBFBF")
    _bold(r.cells[1], "POINT OF CONTACT")

    _info("(31)", "Rank, name, first name", mission.get("poc_name", ""))
    _info("(32)", "Telephone number", mission.get("poc_phone", ""))
    _info("(33)", "E-mail", mission.get("poc_email_personal", ""))
    _info("(34)", "Fax", mission.get("poc_fax", ""))

    _set_widths(info_tbl, [1.5, 6, 9.5])

    doc.add_page_break()

    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("DETAILED ITINERARY")
    r.bold = True
    r.font.size = Pt(12)

    for li, leg in enumerate(leg_data, start=1):
        head = doc.add_paragraph()
        run = head.add_run(
            f"LEG {li}  From {leg.get('origin','')} to {leg.get('destination','')}"
        )
        run.bold = True
        run.font.size = Pt(10)

        leg_tbl = doc.add_table(rows=0, cols=4)
        leg_tbl.style = "Table Grid"
        _header_row(
            leg_tbl,
            "State",
            "Entry point and timing or airfield + ETD\n(DD MMM YY, HHMM Z)",
            "Route over territory",
            "Exit point and timing or airfield + ETA\n(DD MMM YY, HHMM Z)",
        )
        for seg in leg["segments"]:
            row = leg_tbl.add_row()
            _plain(row.cells[0], seg["state_name"])
            _plain(
                row.cells[1],
                f"{seg['entry_label']}\n{seg['entry_time_str']}",
            )
            _plain(row.cells[2], seg["route_in_country"])
            _plain(
                row.cells[3],
                f"{seg['exit_label']}\n{seg['exit_time_str']}",
            )
        _set_widths(leg_tbl, [3.0, 4.5, 5.5, 4.5])
        doc.add_paragraph()

    buf = BytesIO()
    doc.save(buf)
    return buf.getvalue()


def serialize_leg(leg_input: dict, resolution: LegResolution, style: str = "FRA") -> dict:
    segs = []
    for seg in resolution.segments:
        entry_t = format_zulu(seg.entry_time, style) if seg.entry_time else ""
        exit_t = format_zulu(seg.exit_time, style) if seg.exit_time else ""
        is_origin_country = seg.state_iso and seg.state_iso == leg_input.get("origin_iso")
        is_dest_country = seg.state_iso and seg.state_iso == leg_input.get("destination_iso")
        overrides = leg_input.get("overrides", {}).get(seg.state_iso, {})
        segs.append(
            {
                "state_name": overrides.get("state_name", seg.state_name),
                "state_iso": seg.state_iso,
                "entry_label": seg.entry_label,
                "exit_label": seg.exit_label,
                "entry_time_str": entry_t,
                "exit_time_str": exit_t,
                "route_in_country": seg.route_in_country,
                "fl": seg.fl,
                "tas": seg.tas,
                "R": overrides.get("R", is_origin_country or is_dest_country),
                "N": overrides.get("N", not (is_origin_country or is_dest_country)),
                "L": overrides.get("L", is_origin_country or is_dest_country),
                "VIP": overrides.get("VIP", False),
                "DG": overrides.get("DG", False),
                "A": overrides.get("A", False),
                "FR": overrides.get("FR", "I"),
                "existing_dic": overrides.get("existing_dic", ""),
            }
        )
    return {
        "origin": leg_input.get("origin", ""),
        "destination": leg_input.get("destination", ""),
        "segments": segs,
    }
