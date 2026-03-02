import os
import csv
import re
import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

FORM_ID = int(os.getenv("IQSMS_FORM_ID", "2149"))
API_KEY = os.getenv("IQSMS_API_KEY", "").strip() or ""
DEFAULT_CREATOR_ID = int(os.getenv("IQSMS_CREATOR_ID", "141"))
FORM_PASSWORD = os.getenv("FORM_PASSWORD", "123")
KIND_OF_REPORT = os.getenv("IQSMS_KIND_OF_REPORT", "Ground & Cargo Safety Report").strip()

EVENT_CLASS_PAGE_SIZE = int(os.getenv("IQSMS_EVENT_CLASS_PAGE_SIZE", "200"))
EVENT_CLASS_CACHE_TTL_SECONDS = int(os.getenv("EVENT_CLASS_CACHE_TTL_SECONDS", "900"))
IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS = int(os.getenv("IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS", "900"))

ECID_FORM_MAP_PATH = os.getenv("ECID_FORM_MAP_PATH", str(Path(__file__).with_name("c365_ecid.csv")))
ECID_FORM_MAP_TTL_SECONDS = int(os.getenv("ECID_FORM_MAP_TTL_SECONDS", "3600"))

# =============================================================================
# Airport CSV now lives next to this script
# =============================================================================
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
    seen = set()
    out = []
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
# ECID -> FormID mapping (cached)
# =============================================================================
def load_ecid_form_map(path: str) -> dict[int, int]:
    p = Path(path).expanduser()
    if not p.exists():
        return {}

    mapping: dict[int, int] = {}
    with open(p, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        if not reader.fieldnames:
            return {}

        def norm(h: str) -> str:
            return (h or "").strip().lower().replace(" ", "_")

        fields = {norm(h): h for h in reader.fieldnames}
        ecid_key = fields.get("lfnr")
        form_key = fields.get("form_id")

        if not ecid_key or not form_key:
            raise RuntimeError(
                f"Unexpected ECID mapping headers: {reader.fieldnames} "
                f"(expected something like 'lfnr;form_id')"
            )

        for row in reader:
            try:
                ecid = int(str(row.get(ecid_key, "")).strip())
                fid = int(str(row.get(form_key, "")).strip())
            except Exception:
                continue
            mapping[ecid] = fid

    return mapping


@st.cache_data(ttl=ECID_FORM_MAP_TTL_SECONDS)
def get_ecid_form_map_cached(path: str) -> dict[int, int]:
    return load_ecid_form_map(path)


def form_id_for_event_classification(ecid: int) -> int:
    mp = get_ecid_form_map_cached(ECID_FORM_MAP_PATH)
    return int(mp.get(int(ecid), FORM_ID))


# =============================================================================
# Airport CSV discovery + load (cached) — simplified
# =============================================================================
def find_airport_csv_path() -> Path | None:
    """
    Return the path to the airport CSV that lives next to this script.
    """
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
            if not re.fullmatch(r"[A-Z]{3}", iata):
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
    # Helpful hint for operators if the file is missing
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

    if IATA_RE.fullmatch(q) and q in iata_to_label:
        return q

    if ICAO_RE.fullmatch(q):
        mapped = icao_to_iata.get(q, "")
        if mapped:
            return mapped

    for iata, _lbl, _lbl_upper in airport_search:
        if iata.startswith(q):
            return iata

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
    out = []

    if IATA_RE.fullmatch(q) and q in iata_to_label:
        return [f"{q} — {iata_to_label[q]}"]

    if ICAO_RE.fullmatch(q):
        mapped = icao_to_iata.get(q, "")
        if mapped:
            return [f"{mapped} — {iata_to_label.get(mapped, mapped)}"]

    for iata, lbl, _lbl_upper in airport_search:
        if iata.startswith(q):
            out.append(f"{iata} — {lbl}")
            if len(out) >= limit:
                return out

    for iata, lbl, lbl_upper in airport_search:
        if q in lbl_upper:
            out.append(f"{iata} — {lbl}")
            if len(out) >= limit:
                break

    return out


# =============================================================================
# Event classifications (paged fetch) + normalize
# =============================================================================
def fetch_event_classifications_all_pages() -> dict:
    if not API_KEY:
        raise RuntimeError("Missing IQSMS_API_KEY environment variable.")

    headers = {"api-key": API_KEY, "Accept": "application/json"}
    kor = html.unescape(KIND_OF_REPORT).strip() or "Ground &amp;amp; Cargo Safety Report"

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

    root_label = html.unescape(KIND_OF_REPORT).strip() or "Kind of report"

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

        path = f"{area} &gt; {typ} &gt; {cls}"

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
# Dynamic forms schema (cached per form_id)
# =============================================================================
def fetch_form_schema(form_id: int) -> dict:
    if not API_KEY:
        raise RuntimeError("Missing IQSMS_API_KEY environment variable.")
    headers = {"api-key": API_KEY, "Accept": "application/json"}
    url = f"{FORMS_URL.rstrip('/')}/{form_id}"
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

    name_map = {
        "Title": "Title",
        "Aircraft Registration": "AircraftReg",
        "Flight Phase": "FlightPhase",
        "Departure": "Departure",
        "Destination": "Destination",
        "Airport of Occurrence": "AirportOccurrence",
        "Location on aerodrome": "AerodromeLocation",
        "Diversion (if applicable)": "Diversion",
        "Date &amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp; Time of Event (Local)": "DateTimeLocal",
        "Date &amp;amp; Time of Event (UTC)": "DateTimeUTC",
        "Date &amp;amp; Time of Event (Local)": "DateTimeLocal",
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

        if payload_name in ("Departure", "Destination"):
            ftype = "iata"
        elif "Date" in label and "Time" in label:
            ftype = "datetime"
        elif options:
            ftype = "multiselect" if multiple else "select"
        else:
            ftype = "text"

        internal_name = name_map.get(payload_name)
        if not internal_name:
            internal_name = re.sub(r"[^A-Za-z0-9_]", "", re.sub(r"\s+", "_", label)).strip("_") or "Field"

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

    for f in out:
        if f.get("name") == "ReportText" or f.get("payload_name") == "Report Text":
            f["required"] = True
            f["type"] = "textarea"

    for f in out:
        if f.get("name") == "Title" and not f.get("default"):
            f["default"] = "External Ground Ops Report"

    return out, anon_default


@st.cache_data(ttl=IQSMS_FORM_FIELDS_CACHE_TTL_SECONDS)
def get_form_fields_cached(form_id: int) -> tuple[list[dict], bool]:
    try:
        schema = fetch_form_schema(form_id)
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


def build_defaults(fields: list[dict]) -> dict[str, Any]:
    defaults: dict[str, Any] = {}
    for f in fields:
        if f["type"] == "datetime":
            defaults[f["name"]] = _now_utc()
        elif f["type"] == "multiselect":
            defaults[f["name"]] = f.get("default", [])
        else:
            defaults[f["name"]] = f.get("default", "")
    defaults["eventClassificationId"] = ""
    return defaults


# =============================================================================
# Streamlit UI
# =============================================================================
st.set_page_config(page_title="SafetyManager365 External Ground Ops Report", layout="centered")
st.title("SafetyManager365")
st.subheader("External Ground Ops Report")

# Init session
if "unlocked" not in st.session_state:
    st.session_state.unlocked = False
if "active_form_id" not in st.session_state:
    st.session_state.active_form_id = FORM_ID
if "selected_ecid" not in st.session_state:
    st.session_state.selected_ecid = None
if "selected_ec_path" not in st.session_state:
    st.session_state.selected_ec_path = ""

# Password gate
if not st.session_state.unlocked:
    st.subheader("Protected Form")
    pw = st.text_input("Password", type="password")
    if st.button("Unlock"):
        if (pw or "").strip() == FORM_PASSWORD:
            st.session_state.unlocked = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()

# Logout button (top-right-ish)
c = st.columns([3, 1])
with c[1]:
    if st.button("Lock / Logout"):
        st.session_state.unlocked = False
        st.session_state.selected_ecid = None
        st.session_state.selected_ec_path = ""
        st.session_state.active_form_id = FORM_ID
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
    q = st.text_input("Search classification path", value="")
    matches = []
    if len(q.strip()) >= 2:
        qq = q.strip().lower()
        matches = [e for e in ec_index if qq in e["path"].lower()][:50]

    if matches:
        options = [f"{m['id']} — {m['path']}" for m in matches]
        chosen = st.selectbox("Matches", options=options)
        if chosen:
            chosen_id = int(chosen.split(" — ", 1)[0].strip())
            st.session_state.selected_ecid = chosen_id
            st.session_state.selected_ec_path = ec_by_id.get(chosen_id, "")
    else:
        if len(q.strip()) >= 2:
            st.info("No matches.")

with tab_browse:
    areas = sorted(ec_hierarchy.keys(), key=lambda s: s.lower())
    area = st.selectbox("Area of Occurrence", areas, index=0 if areas else None)
    types = sorted(ec_hierarchy.get(area, {}).keys(), key=lambda s: s.lower()) if area else []
    typ = st.selectbox("Type of Occurrence", types, index=0 if types else None)
    leaves = ec_hierarchy.get(area, {}).get(typ, []) if area and typ else []

    if leaves:
        leaf_options = [f"{leaf_id} — {cls}" for (leaf_id, cls, _path) in leaves]
        chosen_leaf = st.selectbox("Event Classification", leaf_options)
        chosen_id = int(chosen_leaf.split(" — ", 1)[0].strip())
        st.session_state.selected_ecid = chosen_id
        st.session_state.selected_ec_path = ec_by_id.get(chosen_id, "")
    else:
        st.info("No classifications available for the selected Area/Type.")

# Enforce ECID selected
ecid = st.session_state.selected_ecid
if not ecid:
    st.warning("Event Classification is required.")
    st.stop()

# Resolve dynamic Form ID from ECID mapping
resolved_form_id = form_id_for_event_classification(ecid)
if resolved_form_id != st.session_state.active_form_id:
    st.session_state.active_form_id = resolved_form_id
    st.rerun()

# Load fields for active form
with st.spinner(f"Loading form schema (Form ID {st.session_state.active_form_id})…"):
    fields, _anon_default = get_form_fields_cached(st.session_state.active_form_id)
defaults = build_defaults(fields)
st.caption(f"Loaded Form ID: **{st.session_state.active_form_id}**")


# =============================================================================
# FORM UI (dedicated sections inside ONE form)
# =============================================================================
def render_datetime_field(label: str, key: str, default_dt: datetime) -> datetime:
    c1, c2 = st.columns(2)
    with c1:
        d = st.date_input(label + " (date)", value=default_dt.date(), key=key + "_date")
    with c2:
        t = st.time_input(
            label + " (time)",
            value=default_dt.time().replace(second=0, microsecond=0),
            key=key + "_time"
        )
    return datetime.combine(d, t, tzinfo=timezone.utc)


def render_airport_field(label: str, key: str, required: bool) -> str:
    q = st.text_input(label, key=key + "_query", placeholder="e.g. VIE, LOWW, Vienna")
    suggestions = airport_suggestions(q, limit=20)
    choice = None
    if suggestions:
        choice = st.selectbox("Suggestions", ["(keep typed value)"] + suggestions, key=key + "_choice")

    raw = q
    if choice and choice != "(keep typed value)":
        raw = choice.split(" — ", 1)[0].strip()

    iata = resolve_airport_to_iata(raw)
    if required and not iata:
        st.error(f"{label} is required and must resolve to a valid IATA code.")
    return iata


with st.form("report_form", clear_on_submit=False):
    values_by_internal: dict[str, Any] = {}
    values_by_internal["eventClassificationId"] = int(ecid)

    # -----------------------------
    # 2) Report Text (DEDICATED)
    # -----------------------------
    st.markdown("## 2) Report Text")
    rt_box = st.container(border=True) if "border" in st.container.__code__.co_varnames else st.container()
    with rt_box:
        st.markdown("### Report Text *")
        report_text = st.text_area(
            " ",
            value=str(defaults.get("ReportText", "")),
            height=240,
            label_visibility="collapsed",
        )
        values_by_internal["ReportText"] = report_text

    # -----------------------------
    # 3) Report Details
    # -----------------------------
    st.markdown("## 3) Report Details")
    details_box = st.container(border=True) if "border" in st.container.__code__.co_varnames else st.container()
    with details_box:
        for f in fields:
            if f["name"] in ("eventClassificationId", "ReportText"):
                continue

            label = f["label"] + (" *" if f.get("required") else "")
            key = f["name"]
            ftype = f["type"]
            required = bool(f.get("required"))

            if ftype == "text":
                v = st.text_input(label, value=str(defaults.get(key, "")), key=key)
                if key == "FlightNumber":
                    v = (v or "").strip().upper()[:8]
                elif key == "CallSign":
                    v = (v or "").strip().upper()[:50]
                values_by_internal[key] = v.strip() if isinstance(v, str) else v

            elif ftype == "textarea":
                v = st.text_area(label, value=str(defaults.get(key, "")), key=key)
                values_by_internal[key] = v.strip() if isinstance(v, str) else v

            elif ftype == "select":
                opts = [""] + list(f.get("options", []))
                dv = defaults.get(key, "")
                idx = opts.index(dv) if dv in opts else 0
                v = st.selectbox(label, opts, index=idx, key=key)
                values_by_internal[key] = v

            elif ftype == "multiselect":
                opts = list(f.get("options", []))
                dv = defaults.get(key, [])
                if not isinstance(dv, list):
                    dv = [dv]
                v = st.multiselect(label, opts, default=[x for x in dv if x in opts], key=key)
                values_by_internal[key] = v

            elif ftype == "datetime":
                default_dt = defaults.get(key) or _now_utc()
                if isinstance(default_dt, str):
                    try:
                        default_dt = datetime.fromisoformat(default_dt)
                    except Exception:
                        default_dt = _now_utc()
                dt = render_datetime_field(label, key, default_dt)
                values_by_internal[key] = dt

            elif ftype == "iata":
                iata = render_airport_field(label, key, required)
                values_by_internal[key] = iata

            else:
                v = st.text_input(label, value=str(defaults.get(key, "")), key=key)
                values_by_internal[key] = v.strip() if isinstance(v, str) else v

    submitted = st.form_submit_button("Submit report")


# =============================================================================
# SUBMIT
# =============================================================================
if submitted:
    if int(ecid) not in ec_selectable_ids:
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
        "eventClassificationId": int(ecid),
        "anonymous": False,  # ALWAYS FORCE FALSE
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
