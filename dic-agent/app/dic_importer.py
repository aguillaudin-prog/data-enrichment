"""Import past DIC .docx files into the route_template library.

Two DIC formats supported:
  - FRA short  : entire form is one big nested table (110+ rows)
  - ICAO long  : multiple tables, one per leg, with LEG paragraph headers

The extractor walks ALL rows of ALL tables and ALSO the in-order paragraphs,
correlating them with the leg structure.

Usage:
    python -m app.dic_importer path/to/dic1.docx path/to/dic2.docx
    python -m app.dic_importer --dir /path/to/dic_folder
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Iterable

from app import db

W_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


@dataclass
class ExtractedLeg:
    order: int
    origin: str
    destination: str
    route_text: str
    fl: int | None
    tas: int | None
    eobt_str: str | None
    states: list[str] = field(default_factory=list)


@dataclass
class ExtractedDIC:
    source_file: str
    reference: str | None
    amendment: str | None
    mission_number: str | None
    aircraft_type: str | None
    registration: str | None
    callsign: str | None
    operator: str | None
    pilots: str | None
    purpose: str | None
    alternates: str | None
    departure_airport: str | None
    destination_airport: str | None
    legs: list[ExtractedLeg] = field(default_factory=list)


# -----------------------------------------------------------------------------
# Low-level docx reading
# -----------------------------------------------------------------------------

def _docx_root(docx_path: Path):
    with zipfile.ZipFile(docx_path) as z:
        with z.open("word/document.xml") as f:
            return ET.parse(f).getroot()


def _all_rows(root) -> list[list[str]]:
    """Flatten every <w:tr> from every <w:tbl> into a single list of rows."""
    rows: list[list[str]] = []
    for tr in root.iter(f"{W_NS}tr"):
        cells: list[str] = []
        for tc in tr.iter(f"{W_NS}tc"):
            text = " ".join((t.text or "") for t in tc.iter(f"{W_NS}t"))
            cells.append(_normalize_ws(text))
        rows.append(cells)
    return rows


def _all_paragraphs(root) -> list[str]:
    out: list[str] = []
    for p in root.iter(f"{W_NS}p"):
        text = "".join(t.text or "" for t in p.iter(f"{W_NS}t"))
        text = _normalize_ws(text)
        if text:
            out.append(text)
    return out


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).replace("\xa0", " ").strip()


# -----------------------------------------------------------------------------
# Field extractors
# -----------------------------------------------------------------------------

SERIAL_RE = re.compile(r"^\(\d+\w?\)$")
ICAO_RE = re.compile(r"\b([A-Z]{4})\b")
# Common 4-letter words that look like ICAO but aren't airport codes.
ICAO_BLACKLIST = {"EXIT", "ENTRY", "NAFB", "AFB", "ETD", "ETA", "EOBT", "EIBT",
                  "FROM", "WITH", "AINJI", "TYBE", "ROUT", "INFO", "SHIP"}
TIME_RE = re.compile(r"(\d{1,2})\s*[/.]\s*(\d{1,2})\s*[/.]\s*(\d{4})\s*,?\s*(\d{2})\s*[.:hH]\s*(\d{2})")
LEG_PARA_RE = re.compile(
    r"\bLEG\s*(\d+)\b.*?\bFrom\s+([A-Z][A-Z' .()/-]+?)\s+to\s+([A-Z][A-Z' .()/-]+)",
    re.IGNORECASE,
)
LEG_INLINE_RE = re.compile(r"^\s*Leg\s*(\d+)\b", re.IGNORECASE)


KNOWN_LABELS = {
    "operator": "operator",
    "requesting state": "requesting_state",
    "number and type of aircraft": "aircraft_type",
    "aircraft registration": "registration",
    "spare aircraft": "spare_aircraft",
    "callsign": "callsign",
    "callsign including spare if different": "callsign",
    "number of crew members": "n_crew",
    "pilot rank and name": "pilots",
    "photographic sensors and or cameras": "sensors",
    "armament": "armament",
    "electronic warfare equipment": "ew",
    "date of flight": "date_of_flight",
    "purpose of flight": "purpose",
    "departure airport": "departure_airport",
    "departure airport s": "departure_airport",
    "destination airport": "destination_airport",
    "destination airport s": "destination_airport",
    "alternate airport s": "alternates",
    "alternate airport": "alternates",
    "radio frequencies": "radio_frequencies",
    "number of passengers": "n_passengers",
    "vip title rank and name": "vip_title",
    "vip title or rank and name": "vip_title",
    "dg details": "dg_details",
    "rank name first name": "poc_name",
    "telephone number": "poc_phone",
    "personal e mail": "poc_email_personal",
    "functional e mail": "poc_email_functional",
    "fax": "poc_fax",
    "e mail": "poc_email_personal",
    "reference number": "reference",
    "amendment number": "amendment",
    "mission number": "mission_number",
}


def _label_key(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_info(rows: list[list[str]]) -> dict[str, str]:
    """Scan all rows; collect known label → value pairs.

    Recognises three layouts:
      a) [serial, label, value]   (FRA short main info table)
      b) [label, value]           (the small header table)
      c) row where one cell starts with 'Reference number :' etc.
    """
    out: dict[str, str] = {}

    def _try_label_at(row: list[str], i: int, value_idx: int) -> None:
        if i >= len(row) or value_idx >= len(row):
            return
        label = _label_key(row[i])
        key = KNOWN_LABELS.get(label)
        if key and not out.get(key):
            # Concatenate any remaining cells as the value (e.g. (13) may split)
            val = " ".join(row[value_idx:]).strip()
            if val:
                out[key] = val

    for row in rows:
        if not row:
            continue
        # Layout (a): serial in col 0
        if row[0] and SERIAL_RE.match(row[0]):
            if len(row) >= 3:
                _try_label_at(row, 1, 2)
            continue

        # Layout (b/c): label may include ':' or be inline like "(1) Reference number :"
        for i, cell in enumerate(row):
            if not cell:
                continue
            # "(1) Reference number :"
            m = re.match(r"^\(\d+\)\s*(.*?)\s*:?\s*$", cell)
            if m:
                inner = m.group(1)
                key = KNOWN_LABELS.get(_label_key(inner))
                if key and i + 1 < len(row) and row[i + 1]:
                    out.setdefault(key, row[i + 1])
                    continue
            # "Reference number :" style
            if cell.endswith(":") or cell.endswith(": "):
                inner = cell.rstrip(": ").strip()
                key = KNOWN_LABELS.get(_label_key(inner))
                if key and i + 1 < len(row) and row[i + 1]:
                    out.setdefault(key, row[i + 1])
                    continue
            # bare label match at position i, value at i+1
            key = KNOWN_LABELS.get(_label_key(cell))
            if key and i + 1 < len(row) and row[i + 1] and not SERIAL_RE.match(row[i + 1]):
                out.setdefault(key, row[i + 1])

    return out


# -----------------------------------------------------------------------------
# Leg extraction
# -----------------------------------------------------------------------------

ITINERARY_HEADER_RE = re.compile(
    r"(state).+?(entry point).+?(route over).+?(exit point)",
    re.IGNORECASE | re.DOTALL,
)
IN_CASE_RE = re.compile(r"in\s+case\s+of\s+emergency", re.IGNORECASE)


def _find_itinerary_anchors(rows: list[list[str]]) -> list[int]:
    """Return indices of rows that are itinerary header rows
    (State | Entry point | Route over | Exit point …)."""
    anchors: list[int] = []
    for i, row in enumerate(rows):
        joined = " | ".join(c.lower() for c in row if c)
        if "state" in joined and "entry point" in joined and "route over" in joined and "exit point" in joined:
            anchors.append(i)
    return anchors


def _normalize_route_cell(s: str) -> str:
    s = re.sub(r"\bENTRY\s+[A-Z' \-]+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\bEXIT\s+[A-Z' \-]+", "", s, flags=re.IGNORECASE)
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\bDCT\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*-\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Glue 'G 851' → 'G851', 'UG 851' → 'UG851' (airway letter + number split by docx whitespace).
    s = re.sub(r"\b([A-Z]{1,2})\s+(\d{2,4})\b", r"\1\2", s)
    # Drop 1- and 2-letter tokens that are not valid airways (likely import noise like 'AD').
    tokens = []
    for t in s.split():
        if len(t) < 3 and not re.fullmatch(r"[A-Z]{1,2}\d{1,4}", t):
            continue
        tokens.append(t)
    return " ".join(tokens)


def _column_indices(header_row: list[str]) -> dict[str, int]:
    cols: dict[str, int] = {}
    for i, c in enumerate(header_row):
        lc = c.lower()
        if "state" in lc and "state" not in cols:
            cols["state"] = i
        elif "entry point" in lc:
            cols["entry"] = i
        elif "route over" in lc:
            cols["route"] = i
        elif "exit point" in lc:
            cols["exit"] = i
        elif "flight level" in lc or re.fullmatch(r"fl", lc):
            cols["fl"] = i
        elif lc == "tas":
            cols["tas"] = i
    return cols


def _row_is_leg_marker(row: list[str]) -> int | None:
    """Return the leg number if any non-empty cell starts with 'Leg N'."""
    for c in row:
        m = LEG_INLINE_RE.match(c or "")
        if m:
            return int(m.group(1))
    return None


def _extract_legs(rows: list[list[str]], paragraphs: list[str]) -> list[ExtractedLeg]:
    anchors = _find_itinerary_anchors(rows)
    if not anchors:
        return []

    # Pre-parse all "LEG N From X to Y" paragraphs by order.
    leg_para_pairs: list[tuple[int, str, str]] = []  # (leg_num, orig, dest)
    for p in paragraphs:
        m = LEG_PARA_RE.search(p)
        if m:
            num = int(m.group(1))
            orig = m.group(2).strip()
            dest = m.group(3).strip()
            leg_para_pairs.append((num, orig, dest))

    legs: list[ExtractedLeg] = []
    # The itinerary may have multiple anchor headers (one per leg in ICAO long,
    # or a single header for the FRA short followed by inline 'Leg N' markers).
    # We iterate through rows from the first anchor and segment by "Leg N" markers
    # in cell 0 OR by anchor row repetition.
    end = len(rows)
    # Process each anchor block independently.
    anchor_bounds = anchors + [end]
    leg_order = 0
    for ai in range(len(anchors)):
        start = anchor_bounds[ai]
        stop = anchor_bounds[ai + 1]
        header_row = rows[start]
        cols = _column_indices(header_row)
        if "state" not in cols or "entry" not in cols or "exit" not in cols:
            continue

        # Walk forward, segment by inline "Leg N" rows, otherwise treat the
        # whole block as a single leg.
        cur_leg_rows: list[list[str]] = []
        legs_in_block: list[list[list[str]]] = []
        skip_until_next_leg = False
        for j in range(start + 1, stop):
            row = rows[j]
            if not any(c for c in row):
                continue
            joined = " ".join(row).lower()
            if _row_is_leg_marker(row) is not None:
                if cur_leg_rows:
                    legs_in_block.append(cur_leg_rows)
                    cur_leg_rows = []
                skip_until_next_leg = False
                continue
            if skip_until_next_leg:
                continue
            if IN_CASE_RE.search(joined):
                skip_until_next_leg = True
                continue
            # skip repeated header
            if " ".join(c.lower() for c in row) == " ".join(c.lower() for c in header_row):
                continue
            # skip rows whose only content is serial markers like (39) (40) (41)…
            non_serial = [c for c in row if c and not SERIAL_RE.match(c)]
            if not non_serial:
                continue
            cur_leg_rows.append(row)
        if cur_leg_rows:
            legs_in_block.append(cur_leg_rows)

        for leg_rows in legs_in_block:
            leg_order += 1
            leg = _build_leg(leg_order, leg_rows, cols, leg_para_pairs)
            if leg:
                legs.append(leg)
    return legs


def _build_leg(
    order: int,
    leg_rows: list[list[str]],
    cols: dict[str, int],
    leg_para_pairs: list[tuple[int, str, str]],
) -> ExtractedLeg | None:
    if not leg_rows:
        return None

    # Pre-clean: drop rows that are pure timestamps (FRA short puts the time
    # on a separate row right under the cell with the airfield label).
    valid_state_rows: list[list[str]] = []
    for row in leg_rows:
        state = row[cols["state"]] if cols.get("state") is not None and cols["state"] < len(row) else ""
        if state and not SERIAL_RE.match(state) and not state.lower().startswith(("flight level", "tas")):
            valid_state_rows.append(row)
    if not valid_state_rows:
        return None

    states: list[str] = []
    route_parts: list[str] = []
    origin: str | None = None
    destination: str | None = None
    eobt: str | None = None
    fl_val: int | None = None
    tas_val: int | None = None

    origin_text: str | None = None
    destination_text: str | None = None
    for r in valid_state_rows:
        st = r[cols["state"]].strip()
        states.append(st)
        entry = r[cols["entry"]] if cols.get("entry") is not None and cols["entry"] < len(r) else ""
        exit_ = r[cols["exit"]] if cols.get("exit") is not None and cols["exit"] < len(r) else ""
        route = r[cols["route"]] if cols.get("route") is not None and cols["route"] < len(r) else ""

        if origin is None:
            ms = [m for m in ICAO_RE.findall(entry) if m not in ICAO_BLACKLIST]
            if ms:
                origin = ms[0]
            elif origin_text is None and entry:
                origin_text = entry.strip()
            if eobt is None:
                tm = TIME_RE.search(entry)
                if tm:
                    eobt = tm.group(0)
        ms = [m for m in ICAO_RE.findall(exit_) if m not in ICAO_BLACKLIST]
        if ms:
            destination = ms[-1]
        elif exit_ and not re.search(r"^EXIT\s+", exit_, re.IGNORECASE):
            destination_text = exit_.strip()
        if route.strip():
            route_parts.append(_normalize_route_cell(route))

        if "fl" in cols and cols["fl"] < len(r) and r[cols["fl"]] and fl_val is None:
            m = re.search(r"\d+", r[cols["fl"]])
            if m:
                fl_val = int(m.group(0))
        if "tas" in cols and cols["tas"] < len(r) and r[cols["tas"]] and tas_val is None:
            m = re.search(r"\d+", r[cols["tas"]])
            if m:
                tas_val = int(m.group(0))

    # If we have a LEG N From X to Y paragraph header for this index, use its
    # ICAO codes as authoritative.
    for n, orig_txt, dest_txt in leg_para_pairs:
        if n == order:
            mo = ICAO_RE.search(orig_txt)
            md = ICAO_RE.search(dest_txt)
            if mo:
                origin = mo.group(1)
            if md:
                destination = md.group(1)
            break

    route_text = _normalize_route_cell(" ".join(route_parts))
    # dedupe consecutive duplicate tokens (e.g. POLTO POLTO from cell boundaries)
    deduped: list[str] = []
    for tok in route_text.split():
        if not deduped or deduped[-1] != tok:
            deduped.append(tok)
    route_text = " ".join(deduped)

    return ExtractedLeg(
        order=order,
        origin=origin or origin_text or "????",
        destination=destination or destination_text or "????",
        route_text=route_text,
        fl=fl_val,
        tas=tas_val,
        eobt_str=eobt,
        states=states,
    )


# -----------------------------------------------------------------------------
# Top-level
# -----------------------------------------------------------------------------

def extract_dic(docx_path: Path) -> ExtractedDIC:
    root = _docx_root(docx_path)
    rows = _all_rows(root)
    paragraphs = _all_paragraphs(root)
    info = _extract_info(rows)
    legs = _extract_legs(rows, paragraphs)
    return ExtractedDIC(
        source_file=str(docx_path.name),
        reference=info.get("reference"),
        amendment=info.get("amendment"),
        mission_number=info.get("mission_number"),
        aircraft_type=info.get("aircraft_type"),
        registration=info.get("registration"),
        callsign=info.get("callsign"),
        operator=info.get("operator"),
        pilots=info.get("pilots"),
        purpose=info.get("purpose"),
        alternates=info.get("alternates"),
        departure_airport=info.get("departure_airport"),
        destination_airport=info.get("destination_airport"),
        legs=legs,
    )


def save_as_route_template(dic: ExtractedDIC) -> int | None:
    if not dic.legs:
        return None
    parts = [dic.legs[0].origin]
    for leg in dic.legs:
        if leg.destination and (not parts or parts[-1] != leg.destination):
            parts.append(leg.destination)
    name = " → ".join(parts) or dic.source_file

    payload = {
        "source_file": dic.source_file,
        "reference": dic.reference,
        "amendment": dic.amendment,
        "aircraft_type": dic.aircraft_type,
        "registration": dic.registration,
        "callsign": dic.callsign,
        "operator": dic.operator,
        "pilots": dic.pilots,
        "purpose": dic.purpose,
        "alternates": dic.alternates,
        "legs": [asdict(l) for l in dic.legs],
    }
    with db.connect() as c:
        cur = c.execute(
            """
            INSERT INTO route_template (name, legs_json) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET legs_json = excluded.legs_json
            RETURNING id
            """,
            (name, json.dumps(payload, ensure_ascii=False)),
        )
        return cur.fetchone()[0]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*", type=Path)
    ap.add_argument("--dir", type=Path, help="Import all .docx in a directory")
    ap.add_argument("--dry-run", action="store_true", help="Print only, don't write DB")
    args = ap.parse_args(argv)

    paths: list[Path] = list(args.paths)
    if args.dir:
        paths.extend(sorted(args.dir.glob("*.docx")))
    if not paths:
        print("Usage: python -m app.dic_importer <file.docx> [...] | --dir <folder>")
        return 1

    db.init_schema()
    for p in paths:
        try:
            dic = extract_dic(p)
        except Exception as e:
            print(f"❌ {p.name}: {type(e).__name__}: {e}")
            continue
        print(f"\n📄 {p.name}")
        print(f"   ref={dic.reference!r} | amendment={dic.amendment!r} | mission={dic.mission_number!r}")
        print(f"   aircraft={dic.registration!r} ({dic.aircraft_type!r}) callsign={dic.callsign!r}")
        print(f"   pilots={dic.pilots!r}")
        print(f"   departure={dic.departure_airport!r}")
        print(f"   destination={dic.destination_airport!r}")
        print(f"   alternates={dic.alternates!r}")
        for leg in dic.legs:
            print(f"   leg{leg.order}: {leg.origin} → {leg.destination} | FL{leg.fl} TAS{leg.tas} | {leg.route_text[:90]}")
        if not args.dry_run and dic.legs:
            tid = save_as_route_template(dic)
            if tid:
                print(f"   ✓ saved as route_template id={tid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
