import os
import csv
import re
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

import requests
import streamlit as st

# =============================================================================
# CONFIG (env vars)
# =============================================================================
REPORT_URL = os.getenv(
    "IQSMS_REPORT_URL",
    "https://comply365.asqs.net/iqsms/api/stable/reporting/reports"
)
EVENT_CLASS_URL = os.getenv(
    "IQSMS_EVENT_CLASS_URL",
    "https://comply365.asqs.net/iqsms/api/stable/reporting/event-classifications"
)
FORMS_URL = os.getenv(
    "IQSMS_FORMS_URL",
    "https://comply365.asqs.net/iqsms/api/stable/reporting/forms"
)

# NOTE: No ECID→FormID mapping anymore; we use the selected event classification ID (lfnr) directly.
API_KEY = os.getenv("IQSMS_API_KEY", "").strip() or "xTWPwWr4qHB9TXvwDNopqFvRvvQTZIWL"
DEFAULT_CREATOR_ID = int(os.getenv("IQSMS_CREATOR_ID", "141"))
FORM_PASSWORD = os.getenv("FORM_PASSWORD", "123")
KIND_OF_REPORT = os.getenv("IQSMS_KIND_OF_REPORT", "Ground &amp; Cargo Safety Report").strip()

EVENT_CLASS_PAGE_SIZE = int(os.getenv("IQSMS_EVENT_CLASS_PAGE_SIZE", "200"))
EVENT_CLASS_CACHE_TTL_SECONDS = int(os.getenv("EVENT_CLASS_CACHE_TTL_SECONDS", "900"))
IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS = int(os.getenv("IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS", "900"))

# Airport CSV now lives next to this script
AIRPORT_CSV_FILENAME = "iata-icao.csv"

FIELD_DEFAULTS_JSON = os.getenv("IQSMS_FIELD_DEFAULTS_JSON", "").strip()
FALLBACK_FIELD_DEFAULTS = {
    "Flight Phase": "Parking",
    "Location on aerodrome": "Not applicable",
    "Weather Relevant": "NO",
}

IATA_RE = re.compile(r"^[A-Z]{3}$")
ICAO_RE = re.compile(r"^[A-Z]{4}$")


# =============================================================================
# Helpers
# =============================================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_field_defaults_map() -> dict[str, Any]:
    if FIELD_DEFAULTS_JSON:
        try:
            obj = json.loads(FIELD_DEFAULTS_JSON)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
    return dict(FALLBACK_FIELD_DEFAULTS)


def _dedupe_preserve_order(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _is_empty_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, (list, tuple, set)):
        return len([x for x in v if str(x).strip() != ""]) == 0
    return False


def _append_value(values: list, name: str, value: Any):
    if _is_empty_value(value):
        return
    values.append({"name": name, "value": value})


# =============================================================================
# Airport CSV discovery + load (cached)
# =============================================================================
def find_airport_csv_path() -> Path | None:
    """Return the path to the airport CSV that lives next to this script."""
    p = Path(__file__).with_name(AIRPORT_CSV_FILENAME)
    return p if p.exists() else None


def _norm_header(h: str) -> str:
    return (h or "").strip().lower().replace("-", "_").replace(" ", "_")


def airport_label(rec: dict) -> str:
    iata = rec.get("iata", "")
    icao = rec.get("icao", "")
    name = rec.get("name", "")
    city = rec.get("city", "")
    country = rec.get("country", "")

    parts = [p for p in (name, city, country) if p]
    tail = ", ".join(parts) if parts else "Unknown airport"
    return f"{iata} ({icao}) — {tail}" if icao else f"{iata} — {tail}"


def load_airports_from_csv(path: str) -> tuple[list[tuple[str, str, str]], dict[str, str], dict[str, str]]:
    airports: dict[str, dict] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], {}, {}

        field_map = {_norm_header(h): h for h in reader.fieldnames}

        def get(row: dict, norm_key: str) -> str:
            raw_key = field_map.get(norm_key)
            if not raw_key:
                return ""
            v = row.get(raw_key, "")
            return str(v).strip() if v is not None else ""

        for row in reader:
            iata = (get(row, "iata") or get(row, "iata_code") or get(row, "iata3")).strip().upper()
            if not IATA_RE.fullmatch(iata):
                continue

            icao = (
                get(row, "icao") or get(row, "icao_code") or get(row, "icao4")
                or get(row, "ident") or get(row, "gps_code")
            ).strip().upper()

            rec = {
                "iata": iata,
                "icao": icao,
                "name": (get(row, "airport") or get(row, "airport_name") or get(row, "name")).strip(),
                "city": (get(row, "city") or get(row, "municipality") or get(row, "town")).strip(),
                "country": (get(row, "country") or get(row, "country_name") or get(row, "iso_country")).strip(),
            }
            airports[iata] = rec

    airport_search: list[tuple[str, str, str]] = []
    iata_to_label: dict[str, str] = {}
    icao_to_iata: dict[str, str] = {}

    for iata, rec in airports.items():
        lbl = airport_label(rec)
        airport_search.append((iata, lbl, lbl.upper()))
        iata_to_label[iata] = lbl
        icao = (rec.get("icao") or "").strip().upper()
        if ICAO_RE.fullmatch(icao):
            icao_to_iata[icao] = iata

    airport_search.sort(key=lambda x: x[0])
    return airport_search, iata_to_label, icao_to_iata


@st.cache_data(ttl=3600)
def get_airports_cached() -> tuple[list[tuple[str, str, str]], dict[str, str], dict[str, str], str]:
    p = find_airport_csv_path()
    if p and p.exists():
        airport_search, iata_to_label, icao_to_iata = load_airports_from_csv(str(p))
        return airport_search, iata_to_label, icao_to_iata, str(p)
    st.warning(f"Airport CSV not found next to script: {AIRPORT_CSV_FILENAME}")
    return [], {}, {}, ""


def resolve_airport_to_iata(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    q = raw.upper()

    airport_search, iata_to_label, icao_to_iata, _path = get_airports_cached()
    if not airport_search:
        return q if IATA_RE.fullmatch(q) else ""

    # Exact valid IATA
    if IATA_RE.fullmatch(q) and q in iata_to_label:
        return q

    # Exact valid ICAO mapped to IATA
    if ICAO_RE.fullmatch(q):
        mapped = icao_to_iata.get(q, "")
        if mapped:
            return mapped

    # Prefix IATA match
    for iata, _lbl, _lbl_upper in airport_search:
        if iata.startswith(q):
            return iata

    # Label contains query
    for iata, _lbl, lbl_upper in airport_search:
        if q in lbl_upper:
            return iata

    return ""


def airport_suggestions(query: str, limit: int = 20) -> list[str]:
    query = (query or "").strip()
    if not query:
        return []
    q = query.upper()

    airport_search, iata_to_label, icao_to_iata, _path = get_airports_cached()
    out: list[str] = []

    # Exact IATA known
    if IATA_RE.fullmatch(q) and q in iata_to_label:
        return [f"{q} — {iata_to_label[q]}"]

    # Exact ICAO mapped
    if ICAO_RE.fullmatch(q):
        mapped = icao_to_iata.get(q, "")
        if mapped:
            return [f"{mapped} — {iata_to_label.get(mapped, mapped)}"]

    # IATA prefix
    for iata, lbl, _lbl_upper in airport_search:
        if iata.startswith(q):
            out.append(f"{iata} — {lbl}")
            if len(out) >= limit:
                return out

    # Label contains
    for iata, lbl, lbl_upper in airport_search:
        if q in lbl_upper:
            out.append(f"{iata} — {lbl}")
            if len(out) >= limit:
                break

    return out


def try_autoconfirm_airport(query: str) -> Tuple[str, bool]:
    """
    Returns (iata, autoconfirmed).
    - If query is an exact, known IATA (AAA) -> autoconfirm True.
    - If query is an exact, known ICAO (AAAA) -> autoconfirm True (mapped).
    - Otherwise -> ("", False)
    """
    q = (query or "").strip().upper()
    if not q:
        return "", False

    _search, iata_to_label, icao_to_iata, _path = get_airports_cached()

    # Exact IATA known
    if IATA_RE.fullmatch(q) and q in iata_to_label:
        return q, True

    # Exact ICAO mapped
    if ICAO_RE.fullmatch(q):
        mapped = icao_to_iata.get(q, "")
        if mapped:
            return mapped, True

    return "", False


# =============================================================================
# Event classifications (paged fetch) + normalize
# =============================================================================
def fetch_event_classifications_all_pages() -> dict:
    if not API_KEY:
        raise RuntimeError("Missing IQSMS_API_KEY environment variable.")

    headers = {"api-key": API_KEY, "Accept": "application/json"}
    kor = html.unescape(KIND_OF_REPORT).strip() or "Ground &amp; Cargo Safety Report"

    params = {"kind-of-report": kor, "limit": EVENT_CLASS_PAGE_SIZE, "page[number]": 1}
    resp = requests.get(EVENT_CLASS_URL, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    out = resp.json()

    if not isinstance(out, dict):
        return {"data": [], "meta": {}}

    meta = out.get("meta", {}) if isinstance(out.get("meta"), dict) else {}
    data = out.get("data", []) if isinstance(out.get("data"), list) else []

    last_page = meta.get("lastPage", meta.get("last_page", meta.get("totalPages", meta.get("total_pages", 1))))
    try:
        last_page = int(last_page)
    except Exception:
        last_page = 1

    all_rows_by_id: dict[int, dict] = {}

    def ingest(rows: list):
        for rec in rows:
            if not isinstance(rec, dict):
                continue
            try:
                rid = int(str(rec.get("id", "")).strip())
            except Exception:
                continue
            all_rows_by_id[rid] = rec

    ingest(data)

    for page in range(2, last_page + 1):
        params = {"kind-of-report": kor, "limit": EVENT_CLASS_PAGE_SIZE, "page[number]": page}
        r = requests.get(EVENT_CLASS_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        page_out = r.json()
        if not isinstance(page_out, dict):
            continue
        page_data = page_out.get("data", [])
        if isinstance(page_data, list):
            ingest(page_data)

    return {"data": list(all_rows_by_id.values()), "meta": meta}


def normalize_event_classifications(raw: dict) -> tuple[list[dict], dict[int, str], set[int], dict[str, dict]]:
    data = raw.get("data", [])
    if not isinstance(data, list):
        data = []

    def clean(v: object, fallback: str) -> str:
        s = ("" if v is None else str(v)).strip()
        return s if s else fallback

    index: list[dict] = []
    by_id: dict[int, str] = {}
    selectable_ids: set[int] = set()
    seen_leaf_ids: set[int] = set()
    hierarchy: dict[str, dict[str, list[tuple[int, str, str]]]] = {}

    for rec in data:
        if not isinstance(rec, dict):
            continue
        try:
            event_id = int(str(rec.get("id", "")).strip())
        except Exception:
            continue
        if event_id in seen_leaf_ids:
            continue
        seen_leaf_ids.add(event_id)

        area = clean(rec.get("areaOfOccurrence"), "Unknown area")
        typ = clean(rec.get("typeOfOccurrence"), "Unknown type")
        cls = clean(rec.get("eventClassification"), "Unknown classification")

        path = f"{area} > {typ} > {cls}"

        selectable_ids.add(event_id)
        by_id[event_id] = path
        index.append({"id": event_id, "path": path, "area": area, "type": typ, "class": cls})
        hierarchy.setdefault(area, {}).setdefault(typ, []).append((event_id, cls, path))

    for a in hierarchy:
        for t in hierarchy[a]:
            hierarchy[a][t].sort(key=lambda x: x[1].lower())

    index.sort(key=lambda x: x["path"].lower())
    return index, by_id, selectable_ids, hierarchy


@st.cache_data(ttl=EVENT_CLASS_CACHE_TTL_SECONDS)
def get_event_classifications_cached() -> tuple[list[dict], dict[int, str], set[int], dict[str, dict]]:
    raw = fetch_event_classifications_all_pages()
    return normalize_event_classifications(raw)


# =============================================================================
# Dynamic form schema (cached per lfnr) + hardening
# =============================================================================
def fetch_form_schema(lfnr: int) -> dict:
    """
    Fetch the dynamic form schema for a given lfnr.
    (The forms endpoint is addressed by the same lfnr used for the event classification.)
    """
    if not API_KEY:
        raise RuntimeError("Missing IQSMS_API_KEY environment variable.")
    headers = {"api-key": API_KEY, "Accept": "application/json"}
    url = f"{FORMS_URL.rstrip('/')}/{lfnr}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    out = resp.json()
    if not isinstance(out, dict):
        raise RuntimeError("Unexpected forms response (not a JSON object).")
    return out


def normalize_fields_from_schema(schema: dict) -> tuple[list[dict], bool]:
    anon_default = bool(schema.get("anonymousReporter", False))  # parsed but never used
    raw_fields = schema.get("fields", [])
    if not isinstance(raw_fields, list):
        raw_fields = []

    # Map known payload field names to internal names
    name_map = {
        "Title": "Title",
        "Aircraft Registration": "AircraftReg",
        "Flight Phase": "FlightPhase",
        "Departure": "Departure",
        "Destination": "Destination",
        "Airport of Occurrence": "AirportOccurrence",
        "Location on aerodrome": "AerodromeLocation",
        "Diversion (if applicable)": "Diversion",
        # HTML-encoded variants collapsed to same internal name:
        "Date &amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp;amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp;amp;amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp;amp;amp;amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp;amp;amp;amp;amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp; Time of Event (Local)": "DateTimeLocal",
        "Date &amp;amp; Time of Event (Local)": "DateTimeLocal",
        "Date &amp;amp;amp; Time of Event (Local)": "DateTimeLocal",
        "Date &amp;amp;amp;amp; Time of Event (Local)": "DateTimeLocal",
        "Date &amp;amp;amp;amp;amp; Time of Event (Local)": "DateTimeLocal",
        "Flight Number": "FlightNumber",
        "Call Sign": "CallSign",
        "Inflight Return": "InflightReturn",
        "Total number of persons on board": "TotalPersonsOnBoard",
        "Passenger involved?": "PassengerInvolved",
        "Passenger name": "PassengerName",
        "Seat No": "SeatNo",
        "Citizenship": "Citizenship",
        "Passport No": "PassportNo",
        "Sex": "Sex",
        "Passenger Behaviour": "PassengerBehaviour",
        "Actions taken": "ActionsTaken",
        "Weather Relevant": "WeatherRelevant",
        "Damage on aircraft": "DamageOnAircraft",
        "Damage on aerodrome": "DamageOnAerodrome",
        "Third party damage": "ThirdPartyDamage",
        "Object damaged": "ObjectDamaged",
        "Damage caused by 3rd party": "DamageCausedBy3rdParty",
        "Was Fatigue a contributing factor?": "FatigueContributing",
        "Report Text": "ReportText",
    }

    defaults_map = get_field_defaults_map()

    out: list[dict] = []
    out.append({
        "label": "Event Classification",
        "name": "eventClassificationId",
        "type": "event-classification",
        "required": True,
        "hint": "",
    })

    # ✅ Deduplicate by internal name; also apply robust datetime bucketing
    seen_internal: set[str] = set()

    for rf in raw_fields:
        if not isinstance(rf, dict):
            continue

        raw_name = (rf.get("name") or "").strip()
        if not raw_name:
            continue

        label = html.unescape(raw_name)
        payload_name = raw_name

        required = bool(rf.get("mandatory", False))
        options = rf.get("options") if isinstance(rf.get("options"), list) else []
        multiple = bool(rf.get("multiple", False))
        regex = rf.get("regex")

        # Type inference
        if payload_name in ("Departure", "Destination"):
            ftype = "iata"
        elif "Date" in label and "Time" in label:
            ftype = "datetime"
        elif options:
            ftype = "multiselect" if multiple else "select"
        else:
            ftype = "text"

        # Internal name: map known → internal; otherwise a sanitized label
        internal_name = name_map.get(payload_name)
        if not internal_name:
            internal_name = re.sub(r"[^A-Za-z0-9_]", "", re.sub(r"\s+", "_", label)).strip("_") or "Field"

        # Extra safety: collapse any "(UTC)" datetime into DateTimeUTC; any "(Local)" into DateTimeLocal
        if ftype == "datetime":
            lab_low = label.lower()
            if "(utc" in lab_low or " utc" in lab_low:
                internal_name = "DateTimeUTC"
            elif "(local" in lab_low or " local" in lab_low:
                internal_name = "DateTimeLocal"

        # Skip duplicates with same internal name
        if internal_name in seen_internal:
            continue
        seen_internal.add(internal_name)

        field_def = {
            "label": label,
            "name": internal_name,
            "payload_name": payload_name,
            "type": ftype,
            "required": required,
        }

        if options:
            field_def["options"] = _dedupe_preserve_order([str(x) for x in options])

        if isinstance(regex, str) and regex.strip():
            field_def["pattern"] = regex.strip()

        if payload_name in defaults_map:
            field_def["default"] = defaults_map[payload_name]
        elif internal_name in defaults_map:
            field_def["default"] = defaults_map[internal_name]

        if "default" in field_def and options:
            if ftype == "select":
                if str(field_def["default"]) not in field_def["options"]:
                    field_def.pop("default", None)
            elif ftype == "multiselect":
                dvals = field_def["default"]
                if not isinstance(dvals, list):
                    dvals = [dvals]
                dvals = [str(x) for x in dvals if str(x) in field_def["options"]]
                field_def["default"] = dvals

        out.append(field_def)

    # Enforce Report Text field to be required textarea
    for f in out:
        if f.get("name") == "ReportText" or f.get("payload_name") == "Report Text":
            f["required"] = True
            f["type"] = "textarea"

    # Default title if none present
    for f in out:
        if f.get("name") == "Title" and not f.get("default"):
            f["default"] = "External Ground Ops Report"

    return out, anon_default


@st.cache_data(ttl=IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS)
def get_form_fields_cached(lfnr: int) -> tuple[list[dict], bool]:
    try:
        schema = fetch_form_schema(int(lfnr))
        fields, anon_default = normalize_fields_from_schema(schema)
        if not fields:
            raise RuntimeError("No fields extracted from schema.")
        return fields, anon_default
    except Exception:
        fallback = [{
            "label": "Event Classification",
            "name": "eventClassificationId",
            "type": "event-classification",
            "required": True,
            "hint": "",
        }, {
            "label": "Report Text",
            "name": "ReportText",
            "payload_name": "Report Text",
            "type": "textarea",
            "required": True,
            "default": ""
        }]
        return fallback, False


def _build_defaults_once(lfnr: int, fields: list[dict]) -> dict[str, Any]:
    """
    Build defaults only once per lfnr and keep them in session_state to avoid
    value 'thrash' (e.g., datetime defaults changing every rerun).
    """
    ns = f"_defaults_for_lfnr_{lfnr}"
    if ns in st.session_state:
        return st.session_state[ns]

    defaults: dict[str, Any] = {}
    for f in fields:
        if f["type"] == "datetime":
            defaults[f["name"]] = _now_utc()  # tz-aware but will be split to naive time for the widget
        elif f["type"] == "multiselect":
            defaults[f["name"]] = f.get("default", [])
        else:
            defaults[f["name"]] = f.get("default", "")
    defaults["eventClassificationId"] = ""
    st.session_state[ns] = defaults
    return defaults


# =============================================================================
# Streamlit UI
# =============================================================================
st.set_page_config(page_title="SafetyManager365 External Ground Ops Report", layout="centered")
st.title("SafetyManager365")
st.subheader("External Ground Ops Report")

# -------------------- Comply365 Light/Dark Theme Switch ----------------------
if "theme" not in st.session_state:
    st.session_state.theme = "light"

toggle = st.toggle("🌗 Dark Mode", value=(st.session_state.theme == "dark"))
st.session_state.theme = "dark" if toggle else "light"

def apply_c365_theme():
    # NOTE: intentionally NOT overriding BaseWeb Select internals to avoid any measurement/update loops.
    if st.session_state.theme == "dark":
        st.markdown("""
<style>
/* ---------------------------------------------------------
   DARK MODE — Comply365 Branding
--------------------------------------------------------- */
.stApp { background-color: #003B5C !important; color: #FFFFFF !important; }

/* Buttons */
.stButton > button, form .stButton > button {
  background-color: #0077C8 !important; color: #FFFFFF !important;
  border-radius: 6px !important; border: none !important; box-shadow: none !important;
}
.stButton > button:hover, form .stButton > button:hover { background-color: #3399E6 !important; }

/* Headers + text */
h1, h2, h3, h4, h5, h6, label, .stMarkdown, .stText,
.stSelectbox label, .stMultiselect label { color: #FFFFFF !important; }

/* ------------------------------------------------------------------
   STREAMLIT TABS — DARK MODE FIX
   Ensures text is always readable; removes BaseWeb's dark-grey default
   ------------------------------------------------------------------ */
.stTabs [role="tab"] {
    color: #FFFFFF !important;            /* Always white text */
    background-color: transparent !important;
}

.stTabs [role="tab"]:hover {
    color: #FFFFFF !important;
    background-color: #1A2D40 !important; /* subtle navy hover */
}

.stTabs [data-baseweb="tab-highlight"] {
    color: #FFFFFF !important;             /* active tab text */
    background-color: #003B5C !important;  /* your dark navy */
    border-bottom: 3px solid #0077C8 !important; /* Comply365 blue */
}
/* Inputs: text + textarea */
.stTextInput > div > div > input,
.stTextArea textarea {
  background-color: #0E1E2C !important; color: #FFFFFF !important; border: 1px solid #0077C8 !important;
}
.stTextInput > div > div > input::placeholder { color: #B0C4D8 !important; }

/* Alerts */
.stAlert, .stAlert > div { color: #FFFFFF !important; }

/* ===== Submit button ===== */
div[data-testid="stFormSubmitButton"] > button {
  background-color: #0077C8 !important; color: #FFFFFF !important;
  border-radius: 6px !important; border: none !important; box-shadow: none !important;
}
div[data-testid="stFormSubmitButton"] > button:hover { background-color: #3399E6 !important; }
</style>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
<style>
/* ---------------------------------------------------------
   LIGHT MODE — Comply365 Branding
--------------------------------------------------------- */
.stApp { background-color: #F5F7FA !important; color: #1A1A1A !important; }

/* Buttons */
.stButton > button, form .stButton > button {
  background-color: #0077C8 !important; color: #FFFFFF !important;
  border-radius: 6px !important; border: none !important; box-shadow: none !important;
}
.stButton > button:hover, form .stButton > button:hover { background-color: #005A99 !important; }

/* Labels + text */
label, .stText, .stMarkdown, .stSelectbox label, .stMultiselect label { color: #1A1A1A !important; }

/* Inputs */
.stTextInput > div > div > input,
.stTextArea textarea {
  background-color: #FFFFFF !important; color: #1A1A1A !important; border: 1px solid #D7DDE2 !important;
}

/* ===== Submit button ===== */
div[data-testid="stFormSubmitButton"] > button {
  background-color: #0077C8 !important; color: #FFFFFF !important;
  border-radius: 6px !important; border: none !important; box-shadow: none !important;
}
div[data-testid="stFormSubmitButton"] > button:hover { background-color: #005A99 !important; }
</style>
        """, unsafe_allow_html=True)

apply_c365_theme()
# -----------------------------------------------------------------------------


# Init session
if "unlocked" not in st.session_state:
    st.session_state.unlocked = False
if "selected_lfnr" not in st.session_state:
    st.session_state.selected_lfnr = None
if "selected_ec_path" not in st.session_state:
    st.session_state.selected_ec_path = ""
if "_prev_lfnr" not in st.session_state:
    st.session_state._prev_lfnr = None

# Password gate
if not st.session_state.unlocked:
    st.subheader("Protected Form")
    pw = st.text_input("Password", type="password", key="pw_input")
    if st.button("Unlock", key="unlock_btn"):
        if (pw or "").strip() == FORM_PASSWORD:
            st.session_state.unlocked = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

# Logout button (top-right-ish)
c = st.columns([3, 1])
with c[1]:
    if st.button("Lock / Logout", key="logout_btn"):
        st.session_state.unlocked = False
        st.session_state.selected_lfnr = None
        st.session_state.selected_ec_path = ""
        st.rerun()

# API key required
if not API_KEY:
    st.error("Missing IQSMS_API_KEY environment variable.")
    st.stop()

# Load event classifications
with st.spinner("Loading event classifications…"):
    ec_index, ec_by_id, ec_selectable_ids, ec_hierarchy = get_event_classifications_cached()

# Event Classification UI
st.markdown("## 1) Event Classification")
tab_search, tab_browse = st.tabs(["Search", "Browse (Area → Type → Classification)"])

with tab_search:
    q = st.text_input("Search classification path", value="", key="search_path")
    matches = []
    if len(q.strip()) >= 2:
        qq = q.strip().lower()
        matches = [e for e in ec_index if qq in e["path"].lower()][:50]

    if matches:
        options = [f"{m['id']} — {m['path']}" for m in matches]
        chosen = st.selectbox("Matches", options=options, key="matches_select")
        if chosen:
            chosen_lfnr = int(chosen.split(" — ", 1)[0].strip())
            st.session_state.selected_lfnr = chosen_lfnr
            st.session_state.selected_ec_path = ec_by_id.get(chosen_lfnr, "")
    else:
        if len(q.strip()) >= 2:
            st.info("No matches.")

with tab_browse:
    areas = sorted(ec_hierarchy.keys(), key=lambda s: s.lower())
    if areas:
        area = st.selectbox("Area of Occurrence", areas, index=0, key="area_select")
    else:
        area = None
        st.info("No areas available.")

    types = sorted(ec_hierarchy.get(area, {}).keys(), key=lambda s: s.lower()) if area else []
    if types:
        typ = st.selectbox("Type of Occurrence", types, index=0, key="type_select")
    else:
        typ = None

    leaves = ec_hierarchy.get(area, {}).get(typ, []) if area and typ else []
    if leaves:
        leaf_options = [f"{leaf_id} — {cls}" for (leaf_id, cls, _path) in leaves]
        chosen_leaf = st.selectbox("Event Classification", leaf_options, key="leaf_select")
        chosen_lfnr = int(chosen_leaf.split(" — ", 1)[0].strip())
        st.session_state.selected_lfnr = chosen_lfnr
        st.session_state.selected_ec_path = ec_by_id.get(chosen_lfnr, "")
    else:
        st.info("No classifications available for the selected Area/Type.")

# Enforce lfnr selected
lfnr = st.session_state.selected_lfnr
if not lfnr:
    st.warning("Event Classification is required.")
    st.stop()

# Load fields for the lfnr (no mapping, direct usage)
with st.spinner(f"Loading form schema (lfnr {lfnr})…"):
    fields, _anon_default = get_form_fields_cached(int(lfnr))

# When lfnr changes, clear stale datetime widget keys (prevents controlled/uncontrolled flips)
if st.session_state._prev_lfnr != lfnr:
    # Clear any previously cached defaults for a *different* lfnr
    to_del = [k for k in list(st.session_state.keys()) if str(k).startswith("_defaults_for_lfnr_") and not str(k).endswith(str(lfnr))]
    for k in to_del:
        st.session_state.pop(k, None)
    st.session_state._prev_lfnr = lfnr

defaults = _build_defaults_once(lfnr, fields)
st.caption(f"Loaded form for lfnr: **{lfnr}**")

# Namespace for widget keys to avoid collisions when lfnr changes
form_ns = f"lfnr{lfnr}"

# =============================================================================
# FORM UI (dedicated sections inside ONE form)
# =============================================================================
def render_datetime_field(label: str, key_root: str, default_dt: datetime, *, force_utc: bool) -> datetime:
    """
    Renders date+time inputs using naive time for st.time_input to avoid tz issues.
    Returns a datetime; tzinfo is applied only if `force_utc` is True.
    """
    try:
        t_default = default_dt.time().replace(second=0, microsecond=0, tzinfo=None)
        d_default = default_dt.date()
    except Exception:
        now = _now_utc()
        d_default = now.date()
        t_default = now.time().replace(second=0, microsecond=0, tzinfo=None)

    c1, c2 = st.columns(2)
    with c1:
        d = st.date_input(label + " (date)", value=d_default, key=key_root + "_date")
    with c2:
        t = st.time_input(label + " (time)", value=t_default, key=key_root + "_time")

    dt = datetime.combine(d, t)
    return dt.replace(tzinfo=timezone.utc) if force_utc else dt


def render_airport_field(label: str, key_root: str, required: bool) -> str:
    q = st.text_input(label, key=key_root + "_query", placeholder="e.g. VIE, LOWW, Vienna")

    # Auto-confirm exact IATA/ICAO when unambiguous
    iata_auto, autoconfirmed = try_autoconfirm_airport(q)
    if autoconfirmed:
        if required and not iata_auto:
            st.error(f"{label} is required and must resolve to a valid IATA code.")
        st.caption(f"✔ {label} recognized as **{iata_auto}**")
        return iata_auto

    # Fallback to suggestion-driven resolution
    suggestions = airport_suggestions(q, limit=20)
    choice = None
    if suggestions:
        choice = st.selectbox("Suggestions", ["(keep typed value)"] + suggestions, key=key_root + "_choice")

    raw = q
    if choice and choice != "(keep typed value)":
        raw = choice.split(" — ", 1)[0].strip()

    iata = resolve_airport_to_iata(raw)
    if required and not iata:
        st.error(f"{label} is required and must resolve to a valid IATA code.")
    return iata


with st.form("report_form", clear_on_submit=False):
    values_by_internal: dict[str, Any] = {}
    values_by_internal["eventClassificationId"] = int(lfnr)

    # -----------------------------
    # 2) Report Text (DEDICATED)
    # -----------------------------
    st.markdown("## 2) Report Text")
    with st.container():
        st.markdown("### Report Text *")
        report_text = st.text_area(
            " ",
            value=str(defaults.get("ReportText", "")),
            height=240,
            label_visibility="collapsed",
            key=f"{form_ns}:ReportText"
        )
        values_by_internal["ReportText"] = report_text.strip() if isinstance(report_text, str) else report_text

    # -----------------------------
    # 3) Report Details
    # -----------------------------
    st.markdown("## 3) Report Details")
    with st.container():
        for f in fields:
            if f["name"] in ("eventClassificationId", "ReportText"):
                continue

            label = f["label"] + (" *" if f.get("required") else "")
            internal = f["name"]
            ftype = f["type"]
            required = bool(f.get("required"))

            # Stable widget key per field name
            widget_key_root = f"{form_ns}:{internal}"

            if ftype == "text":
                v = st.text_input(label, value=str(defaults.get(internal, "")), key=widget_key_root)
                if internal == "FlightNumber":
                    v = (v or "").strip().upper()[:8]
                elif internal == "CallSign":
                    v = (v or "").strip().upper()[:50]
                values_by_internal[internal] = v.strip() if isinstance(v, str) else v

            elif ftype == "textarea":
                v = st.text_area(label, value=str(defaults.get(internal, "")), key=widget_key_root)
                values_by_internal[internal] = v.strip() if isinstance(v, str) else v

            elif ftype == "select":
                opts = [""] + list(f.get("options", []))
                dv = defaults.get(internal, "")
                idx0 = opts.index(dv) if dv in opts else 0
                v = st.selectbox(label, opts, index=idx0, key=widget_key_root)
                values_by_internal[internal] = v

            elif ftype == "multiselect":
                opts = list(f.get("options", []))
                dv = defaults.get(internal, [])
                if not isinstance(dv, list):
                    dv = [dv]
                v = st.multiselect(label, opts, default=[x for x in dv if x in opts], key=widget_key_root)
                values_by_internal[internal] = v

            elif ftype == "datetime":
                default_dt = defaults.get(internal) or _now_utc()
                if isinstance(default_dt, str):
                    try:
                        default_dt = datetime.fromisoformat(default_dt)
                    except Exception:
                        default_dt = _now_utc()
                dt = render_datetime_field(
                    label,
                    widget_key_root,
                    default_dt,
                    force_utc=(internal == "DateTimeUTC")
                )
                values_by_internal[internal] = dt

            elif ftype == "iata":
                iata = render_airport_field(label, widget_key_root, required)
                values_by_internal[internal] = iata

            else:
                v = st.text_input(label, value=str(defaults.get(internal, "")), key=widget_key_root)
                values_by_internal[internal] = v.strip() if isinstance(v, str) else v

    submitted = st.form_submit_button("Submit Report", use_container_width=False, type="primary", disabled=False, help=None)


# =============================================================================
# SUBMIT
# =============================================================================
if submitted:
    if int(lfnr) not in ec_selectable_ids:
        st.error("Please select a valid tier-4 Event Classification (leaf).")
        st.stop()

    if not (values_by_internal.get("ReportText") or "").strip():
        st.error("Mandatory field missing: Report Text.")
        st.stop()

    dep = (values_by_internal.get("Departure") or "").strip().upper()
    dst = (values_by_internal.get("Destination") or "").strip().upper()
    if dep and not IATA_RE.fullmatch(dep):
        st.error("Departure must be a valid 3-letter IATA code (A–Z).")
        st.stop()
    if dst and not IATA_RE.fullmatch(dst):
        st.error("Destination must be a valid 3-letter IATA code (A–Z).")
        st.stop()

    payload_values = []
    for f in fields:
        if "payload_name" not in f:
            continue

        payload_name = f["payload_name"]
        internal = f["name"]
        required = bool(f.get("required"))
        ftype = f["type"]
        val = values_by_internal.get(internal)

        if ftype == "datetime":
            converted = val.strftime("%Y-%m-%d %H:%M") if isinstance(val, datetime) else ""
            if required and not converted:
                st.error(f"Mandatory field missing: {f.get('label', payload_name)}.")
                st.stop()
            _append_value(payload_values, payload_name, converted)
            continue

        if ftype == "multiselect":
            if required and _is_empty_value(val):
                st.error(f"Mandatory field missing: {f.get('label', payload_name)}.")
                st.stop()
            _append_value(payload_values, payload_name, val)
            continue

        if internal == "Departure":
            val = dep
        elif internal == "Destination":
            val = dst
        elif internal == "FlightNumber":
            val = (val or "").strip().upper()[:8]
        elif internal == "CallSign":
            val = (val or "").strip().upper()[:50]
        elif internal == "ReportText":
            val = (val or "").strip()
        else:
            if isinstance(val, str):
                val = val.strip()

        if required and _is_empty_value(val):
            st.error(f"Mandatory field missing: {f.get('label', payload_name)}.")
            st.stop()

        _append_value(payload_values, payload_name, val)

    payload = {
        "eventClassificationId": int(lfnr),  # lfnr used directly
        "anonymous": False,                   # ALWAYS FORCE FALSE
        "creator": int(DEFAULT_CREATOR_ID),
        "values": payload_values,
    }

    headers = {"Content-Type": "application/json", "api-key": API_KEY}

    with st.spinner("Submitting report…"):
        try:
            resp = requests.post(REPORT_URL, headers=headers, json=payload, timeout=30)
            ok = 200 <= resp.status_code < 300
            if ok:
                st.success("Your report has been submitted successfully. Thank you!")
                st.code(resp.text, language="text")
            else:
                st.error(f"Submission failed (HTTP {resp.status_code}).")
                st.code(resp.text, language="text")
        except requests.RequestException as e:
            st.error(f"Request error: {str(e)}")
