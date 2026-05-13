"""ICAO Flight Plan (FPL) message generator.

Produces a textual message conforming to ICAO Doc 4444, Appendix 2, fields 7-19.
Single leg per FPL: a DIC with N legs yields N FPL messages.

Format reference:
  (FPL-<callsign>-<flight_rules><flight_type>
   -<n_aircraft><aircraft_type>/<wake>-<equipment>/<surveillance>
   -<dep_aerodrome><eobt>
   -<speed><level> <route>
   -<dest_aerodrome><total_eet> <altn1> <altn2>
   -<other_info>)

Each line is one field, prefixed with '-'. The opening '(FPL-' and trailing ')'
delimit the message.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


# Default equipment + surveillance codes by aircraft category.
# Format: equipment letters / surveillance letters. See ICAO Doc 4444.
#   S = Standard (VHF + ADF + VOR + ILS)
#   D = DME, G = GNSS, R = PBN, Y = 8.33 kHz VHF
#   Surveillance: S = Mode S, A = Mode A/C
DEFAULT_EQUIPMENT = {
    "DHC6": ("SDGY", "S"),
    "ATR42": ("SDFGRY", "S"),
    "ATR72": ("SDFGRY", "S"),
    "C130": ("SDFGHIJ4RY", "S"),
    "C30J": ("SDFGHIJ4RY", "S"),
    "A400": ("SDFGHIJ4RWY", "SD1"),
    "A310": ("SDFGHIJ4RWY", "SD1"),
    "A330": ("SDFGHIJ4RWY", "SD1"),
    "A332": ("SDFGHIJ4RWY", "SD1"),
    "A359": ("SDFGHIJ4RWY", "SD1"),
    "B737": ("SDFGHIJ4RWY", "SD1"),
    "B738": ("SDFGHIJ4RWY", "SD1"),
    "B739": ("SDFGHIJ4RWY", "SD1"),
    "B744": ("SDFGHIJ4RWY", "SD1"),
    "B748": ("SDFGHIJ4RWY", "SD1"),
    "B763": ("SDFGHIJ4RWY", "SD1"),
    "B772": ("SDFGHIJ4RWY", "SD1"),
    "B788": ("SDFGHIJ4RWY", "SD1"),
    "F900": ("SDFGRWY", "SD1"),
    "F7X": ("SDFGRWY", "SD1"),
    "F2TH": ("SDFGRWY", "SD1"),
    "C295": ("SDFGRY", "S"),
    "C235": ("SDFGRY", "S"),
}


@dataclass
class FPLData:
    callsign: str
    flight_rules: str           # I / V / Y / Z
    flight_type: str            # S (scheduled) / N (non-scheduled) / G (general) / M (military) / X
    n_aircraft: int
    aircraft_type: str          # ICAO type designator
    wake_category: str          # L / M / H
    equipment: str              # e.g. SDGY
    surveillance: str           # e.g. S
    dep_aerodrome: str          # ICAO 4 letters or 'ZZZZ'
    eobt: dt.datetime           # UTC
    speed_kt: int               # cruise TAS in knots
    level_fl: int               # cruise FL (e.g. 90 → F090)
    route: str                  # ICAO route string
    dest_aerodrome: str
    total_eet_min: int          # estimated total flight time, minutes
    alternates: list[str]       # list of ICAO codes, max 2
    registration: str | None = None
    operator: str | None = None
    remarks: str | None = None
    other_eet: list[tuple[str, int]] | None = None
        # list of (fir_id, minutes_from_eobt) for field 18 EET/
    sts: str | None = None      # special status (e.g. 'MIL', 'PROTECTED')


def _hhmm(d: dt.datetime) -> str:
    return d.strftime("%H%M")


def _dof(d: dt.datetime) -> str:
    return d.strftime("%y%m%d")


def _level(fl: int) -> str:
    return f"F{fl:03d}"


def _speed(kt: int) -> str:
    return f"N{kt:04d}"


def _eet_to_hhmm(minutes: int) -> str:
    h, m = divmod(int(minutes), 60)
    return f"{h:02d}{m:02d}"


def _sanitize_route(route: str, dep: str, dest: str) -> str:
    """Trim depart/dest ICAO from route, normalise spacing, ensure DCT explicit
    where there's only a single token between airfields."""
    parts = [p.strip() for p in route.split() if p.strip()]
    if parts and parts[0] == dep:
        parts = parts[1:]
    if parts and parts[-1] == dest:
        parts = parts[:-1]
    if not parts:
        return "DCT"
    return " ".join(parts)


def build_fpl(data: FPLData) -> str:
    fr = data.flight_rules
    ft = data.flight_type
    eq = data.equipment or "S"
    surv = data.surveillance or "S"

    field7 = data.callsign.upper()
    field8 = f"{fr}{ft}"
    field9 = f"{data.n_aircraft}{data.aircraft_type.upper()}/{data.wake_category.upper()}"
    field10 = f"{eq}/{surv}"
    field13 = f"{data.dep_aerodrome.upper()}{_hhmm(data.eobt)}"
    field15 = f"{_speed(data.speed_kt)}{_level(data.level_fl)} {_sanitize_route(data.route, data.dep_aerodrome, data.dest_aerodrome)}"
    altns = " ".join(a.upper() for a in (data.alternates or [])[:2])
    field16 = f"{data.dest_aerodrome.upper()}{_eet_to_hhmm(data.total_eet_min)}"
    if altns:
        field16 += f" {altns}"

    other: list[str] = []
    if data.other_eet:
        eet_parts = " ".join(f"{fir.upper()}{_eet_to_hhmm(m)}" for fir, m in data.other_eet)
        other.append(f"EET/{eet_parts}")
    other.append(f"DOF/{_dof(data.eobt)}")
    if data.registration:
        other.append(f"REG/{data.registration.upper().replace('-', '')}")
    if data.operator:
        other.append(f"OPR/{data.operator.upper()}")
    if data.sts:
        other.append(f"STS/{data.sts.upper()}")
    if data.remarks:
        other.append(f"RMK/{data.remarks.upper()}")

    field18 = " ".join(other) if other else "0"

    return (
        f"(FPL-{field7}-{field8}\n"
        f"-{field9}-{field10}\n"
        f"-{field13}\n"
        f"-{field15}\n"
        f"-{field16}\n"
        f"-{field18})"
    )


def fpl_for_leg(
    *,
    callsign: str,
    aircraft_type: str,
    registration: str,
    operator: str,
    wake_category: str,
    dep: str, dest: str,
    eobt: dt.datetime,
    tas_kt: int, fl: int,
    route_text: str,
    eet_min: int,
    alternates: list[str],
    fir_eet: list[tuple[str, int]] | None = None,
    remarks: str | None = None,
    sts: str | None = None,
    flight_rules: str = "I", flight_type: str = "N",
) -> str:
    eq, surv = DEFAULT_EQUIPMENT.get(aircraft_type.upper(), ("S", "S"))
    data = FPLData(
        callsign=callsign, flight_rules=flight_rules, flight_type=flight_type,
        n_aircraft=1, aircraft_type=aircraft_type, wake_category=wake_category or "L",
        equipment=eq, surveillance=surv,
        dep_aerodrome=dep, eobt=eobt,
        speed_kt=tas_kt, level_fl=fl,
        route=route_text,
        dest_aerodrome=dest, total_eet_min=eet_min,
        alternates=alternates,
        registration=registration, operator=operator,
        other_eet=fir_eet, sts=sts, remarks=remarks,
    )
    return build_fpl(data)


if __name__ == "__main__":
    import datetime as _dt
    sample = fpl_for_leg(
        callsign="TYBAB",
        aircraft_type="DHC6", registration="TY-BAB",
        operator="AMAZONE",
        wake_category="L",
        dep="DBBB", dest="DNMN",
        eobt=_dt.datetime(2026, 4, 20, 6, 0),
        tas_kt=140, fl=90,
        route_text="TYE POLTO LAG L433 IBA R778 TEGDA MNA",
        eet_min=152,
        alternates=["DNAA", "DXXX"],
        fir_eet=[("DBBB", 0), ("DRRR", 10)],
        remarks="MIL LOGISTIC FLIGHT",
        sts="PROTECTED",
    )
    print(sample)
