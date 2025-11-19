import os
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Where's My Bus API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# GTFS LOADER (lightweight)
# -----------------------------
import csv
import zipfile
from io import BytesIO

GTFS_CACHE: Dict[str, Any] = {
    "loaded": False,
    "stops": {},  # stop_id -> stop
    "routes": {},  # route_id -> route
    "trips": {},  # trip_id -> trip
    "stop_times": [],  # list of {trip_id, arrival_time, stop_id}
    "by_stop": {},  # stop_id -> { route_id -> True }
}


def _parse_time_to_seconds(t: str) -> Optional[int]:
    try:
        # GTFS may include hours > 24 (e.g., 25:10:00)
        parts = [int(p) for p in t.split(":")]
        if len(parts) != 3:
            return None
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    except Exception:
        return None


def _ensure_gtfs_loaded() -> None:
    if GTFS_CACHE.get("loaded"):
        return

    gtfs_dir = os.path.join(os.getcwd(), "data", "gtfs")
    os.makedirs(gtfs_dir, exist_ok=True)

    # If a GTFS zip URL is provided, fetch and extract minimal files
    gtfs_url = os.getenv("GTFS_URL")
    minimal_files = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}

    def extract_from_zip(content: bytes):
        with zipfile.ZipFile(BytesIO(content)) as zf:
            for name in zf.namelist():
                base = os.path.basename(name)
                if base in minimal_files:
                    with zf.open(name) as src, open(os.path.join(gtfs_dir, base), "wb") as dst:
                        dst.write(src.read())

    try:
        if gtfs_url:
            try:
                resp = requests.get(gtfs_url, timeout=20)
                if resp.ok:
                    extract_from_zip(resp.content)
            except Exception:
                pass  # Fallback to local files if download fails

        # Load CSVs if present
        stops_path = os.path.join(gtfs_dir, "stops.txt")
        routes_path = os.path.join(gtfs_dir, "routes.txt")
        trips_path = os.path.join(gtfs_dir, "trips.txt")
        stop_times_path = os.path.join(gtfs_dir, "stop_times.txt")

        if os.path.exists(stops_path):
            with open(stops_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    GTFS_CACHE["stops"][row.get("stop_id")] = row

        if os.path.exists(routes_path):
            with open(routes_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    GTFS_CACHE["routes"][row.get("route_id")] = row

        if os.path.exists(trips_path):
            with open(trips_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    GTFS_CACHE["trips"][row.get("trip_id")] = row

        if os.path.exists(stop_times_path):
            with open(stop_times_path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    # Keep only minimal fields
                    GTFS_CACHE["stop_times"].append(
                        {
                            "trip_id": row.get("trip_id"),
                            "arrival_time": row.get("arrival_time"),
                            "departure_time": row.get("departure_time"),
                            "stop_id": row.get("stop_id"),
                            "stop_sequence": int(row.get("stop_sequence") or 0),
                        }
                    )

        # Build by_stop index -> set of route_ids
        by_stop: Dict[str, Dict[str, bool]] = {}
        trips = GTFS_CACHE["trips"]
        for st in GTFS_CACHE["stop_times"]:
            sid = st.get("stop_id")
            trip = trips.get(st.get("trip_id"))
            if not trip:
                continue
            rid = trip.get("route_id")
            if not sid or not rid:
                continue
            by_stop.setdefault(sid, {})[rid] = True
        GTFS_CACHE["by_stop"] = by_stop

        GTFS_CACHE["loaded"] = True

    except Exception as e:
        # If anything fails, mark as loaded to avoid repeated attempts; the API will handle absence gracefully
        GTFS_CACHE["loaded"] = True


# -----------------------------
# External Live API helpers
# -----------------------------

BUS_API_BASE = os.getenv("BUS_API_BASE", "https://busapi.59.ae")


def _guess_eta_minutes(value: Any) -> Optional[int]:
    """Heuristic to convert various ETA representations to minutes.
    - If value is None -> None
    - If int/float and <= 180 -> treat as minutes
    - If int/float and > 180 -> treat as seconds and convert to minutes
    - If string like "5m" or "5" -> best-effort parse
    """
    if value is None:
        return None
    try:
        if isinstance(value, str):
            v = value.strip().lower().replace("min", "").replace("m", "").replace("minutes", "").strip()
            if v.endswith("s"):
                # seconds like "120s"
                v = v[:-1]
                seconds = float(v)
                return int(round(seconds / 60))
            num = float(v)
            # assume minutes for plain numbers
            return int(round(num))
        if isinstance(value, (int, float)):
            if value <= 180:
                return int(round(value))
            return int(round(value / 60))
    except Exception:
        return None
    return None


def fetch_live_for_stop(stop_id: str) -> List[Dict[str, Any]]:
    """
    Try a few common patterns to get live vehicles approaching a stop.
    This function normalizes output to a list of dicts:
    { route_id, trip_id, eta_min, occupancy, delay_min }
    If nothing is available, returns [].
    """
    endpoints = [
        f"{BUS_API_BASE}/api/stop/{stop_id}",  # user-reported endpoint
        f"{BUS_API_BASE}/realtime/stop/{stop_id}",
        f"{BUS_API_BASE}/stop/{stop_id}/realtime",
        f"{BUS_API_BASE}/stop/{stop_id}/vehicles",
    ]
    headers = {"Accept": "application/json"}

    for url in endpoints:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if not resp.ok:
                continue
            data = resp.json()
            # Try a few shapes; transform as best as possible
            items: List[Dict[str, Any]] = []
            if isinstance(data, list):
                raw_list = data
            elif isinstance(data, dict):
                # Prefer keys commonly used for this API
                for k in ("results", "trips", "vehicles", "arrivals", "data", "items"):
                    if isinstance(data.get(k), list):
                        raw_list = data.get(k)
                        break
                else:
                    # fallback: any first list value
                    raw_list = []
                    for v in data.values():
                        if isinstance(v, list):
                            raw_list = v
                            break
            else:
                raw_list = []

            for v in raw_list:
                # IDs
                route_id = v.get("route_id") or v.get("routeId") or v.get("route") or v.get("routeShortName")
                trip_id = v.get("trip_id") or v.get("tripId") or v.get("trip") or v.get("trip_id_str")

                # ETA handling: look across multiple keys
                eta_min: Optional[int] = None
                if any(k in v for k in ("eta_min", "etaMinutes", "eta_mins")):
                    for k in ("eta_min", "etaMinutes", "eta_mins"):
                        if k in v:
                            eta_min = _guess_eta_minutes(v[k])
                            break
                elif any(k in v for k in ("eta_sec", "eta", "eta_seconds", "etaSeconds")):
                    for k in ("eta_sec", "eta", "eta_seconds", "etaSeconds"):
                        if k in v:
                            eta_min = _guess_eta_minutes(v[k])
                            break
                elif "arrival_in" in v:
                    eta_min = _guess_eta_minutes(v.get("arrival_in"))

                # Occupancy / load
                occupancy = (
                    v.get("occupancy")
                    or v.get("load")
                    or v.get("occupancy_percentage")
                    or v.get("loadPercent")
                    or v.get("passenger_load")
                )
                if isinstance(occupancy, str):
                    try:
                        if occupancy.endswith("%"):
                            occupancy = float(occupancy.strip("% "))
                        else:
                            occupancy = float(occupancy)
                    except Exception:
                        occupancy = None

                # Delay
                delay_val = None
                for k in ("delay_sec", "delay", "delaySeconds", "lateness"):
                    if k in v and isinstance(v[k], (int, float)):
                        delay_val = v[k]
                        break
                delay_min = None
                if delay_val is not None:
                    delay_min = int(round(delay_val / 60)) if delay_val > 180 else int(round(delay_val))

                items.append(
                    {
                        "route_id": str(route_id) if route_id is not None else None,
                        "trip_id": str(trip_id) if trip_id is not None else None,
                        "eta_min": eta_min,
                        "occupancy": occupancy,
                        "delay_min": delay_min,
                    }
                )
            if items:
                return items
        except Exception:
            continue
    return []


# -----------------------------
# Helpers that use GTFS
# -----------------------------


def get_stop_info(stop_id: str) -> Dict[str, Any]:
    _ensure_gtfs_loaded()
    stop = GTFS_CACHE["stops"].get(stop_id)
    if not stop:
        # Minimal fallback
        return {"stop_id": stop_id, "name": f"Stop {stop_id}", "desc": None}
    name = stop.get("stop_name") or stop.get("stop_desc") or f"Stop {stop_id}"
    return {
        "stop_id": stop_id,
        "name": name,
        "code": stop.get("stop_code"),
        "desc": stop.get("stop_desc"),
        "lat": stop.get("stop_lat"),
        "lon": stop.get("stop_lon"),
    }


def get_routes_for_stop(stop_id: str) -> List[Dict[str, Any]]:
    _ensure_gtfs_loaded()
    route_ids = list(GTFS_CACHE.get("by_stop", {}).get(stop_id, {}).keys())
    routes = []
    for rid in route_ids:
        r = GTFS_CACHE["routes"].get(rid) or {"route_id": rid, "route_short_name": rid}
        routes.append(
            {
                "route_id": rid,
                "short_name": r.get("route_short_name") or rid,
                "long_name": r.get("route_long_name"),
                "color": r.get("route_color"),
                "text_color": r.get("route_text_color"),
            }
        )
    # Sort by short_name natural
    def keyfn(x):
        s = x.get("short_name") or ""
        try:
            return (0, int(s))
        except Exception:
            return (1, s)

    routes.sort(key=keyfn)
    return routes


def get_scheduled_arrivals(stop_id: str, limit_per_route: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    """
    Return upcoming scheduled stop_times grouped by route_id.
    This ignores service calendars for simplicity; it filters by times after 'now' only.
    """
    _ensure_gtfs_loaded()
    now = datetime.now()
    secs_now = now.hour * 3600 + now.minute * 60 + now.second

    trips = GTFS_CACHE["trips"]
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for st in GTFS_CACHE["stop_times"]:
        if st.get("stop_id") != stop_id:
            continue
        arr_s = _parse_time_to_seconds(st.get("arrival_time") or st.get("departure_time") or "")
        if arr_s is None:
            continue
        if arr_s < secs_now:
            continue  # only future times
        trip = trips.get(st.get("trip_id"))
        if not trip:
            continue
        rid = trip.get("route_id")
        eta_min = int(round((arr_s - secs_now) / 60))
        grouped.setdefault(rid, []).append(
            {
                "trip_id": st.get("trip_id"),
                "eta_min": eta_min,
                "scheduled": True,
            }
        )

    # trim per route and sort
    for rid, items in grouped.items():
        items.sort(key=lambda x: x.get("eta_min", 999999))
        grouped[rid] = items[:limit_per_route]
    return grouped


# -----------------------------
# API ROUTES
# -----------------------------


@app.get("/")
def root():
    return {"status": "ok", "service": "Where's My Bus API"}


@app.get("/api/stops/{stop_id}")
def api_stop_info(stop_id: str):
    return get_stop_info(stop_id)


@app.get("/api/stops/{stop_id}/routes")
def api_routes_for_stop(stop_id: str):
    routes = get_routes_for_stop(stop_id)
    return {"stop_id": stop_id, "routes": routes}


@app.get("/api/stops/{stop_id}/arrivals")
def api_stop_arrivals(stop_id: str):
    """
    Returns arrivals grouped by route with two categories:
    - live: from external realtime API (if available)
    - scheduled: from GTFS stop_times (upcoming)
    """
    live = fetch_live_for_stop(stop_id)
    scheduled_grouped = get_scheduled_arrivals(stop_id)

    # Group live by route_id
    live_grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in live:
        rid = item.get("route_id") or "unknown"
        live_grouped.setdefault(rid, []).append(
            {
                "trip_id": item.get("trip_id"),
                "eta_min": item.get("eta_min"),
                "occupancy": item.get("occupancy"),
                "delay_min": item.get("delay_min"),
                "live": True,
            }
        )

    # Consolidate route_ids from both sources
    route_ids = set(list(live_grouped.keys()) + list(scheduled_grouped.keys()))

    # Map route details
    routes_meta = {r["route_id"]: r for r in get_routes_for_stop(stop_id)}

    by_route: List[Dict[str, Any]] = []
    for rid in sorted(route_ids):
        meta = routes_meta.get(rid) or {"route_id": rid, "short_name": rid}
        by_route.append(
            {
                "route": meta,
                "live": sorted(live_grouped.get(rid, []), key=lambda x: (x.get("eta_min") is None, x.get("eta_min") or 999999))[:5],
                "scheduled": scheduled_grouped.get(rid, []),
            }
        )

    return {
        "stop": get_stop_info(stop_id),
        "routes": list(routes_meta.values()),
        "by_route": by_route,
        "generated_at": int(time.time()),
        "live_source": BUS_API_BASE,
    }


@app.get("/test")
def test_database():
    """Simple health info plus env variables presence"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Used (no persistence needed)",
        "database_url": "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set",
        "database_name": "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set",
        "live_api_base": BUS_API_BASE,
    }
    return response


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
