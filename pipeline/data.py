"""Data access: Sentinel-1 scenes (Planetary Computer STAC) and AIS presence
(Global Fishing Watch), scoped to a site bbox and a date window.

No bulk pre-fetch and no baked date range. Callers pass the bbox and the window,
and the flow fetches one day at a time, caching as it goes.
"""

import json
import time

import pystac_client
import planetary_computer
import requests
from shapely.geometry import box, shape

from pipeline.config import GFW_TOKEN

STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"

_MANIFEST_CACHE = {}


def manifest_for_day(path, day):
    """Scenes for a day from a committed catalogue manifest (offline, no PC).

    The manifest holds the same fields as list_scenes, so source='cache' runs
    work cold from chips + manifest, with no Planetary Computer calls.
    """
    if path not in _MANIFEST_CACHE:
        by_day = {}
        with open(path) as f:
            for s in json.load(f):
                by_day.setdefault(s["datetime"][:10], []).append(s)
        _MANIFEST_CACHE[path] = by_day
    return _MANIFEST_CACHE[path].get(day, [])


def list_scenes(west, south, east, north, start, end, retries=3):
    """Sentinel-1 GRD IW/VV scenes intersecting the bbox within [start, end].

    start/end are ISO dates (YYYY-MM-DD); the range covers the whole end day.
    Returns dicts with scene_id, datetime, platform, relative_orbit, footprint_wkt
    and coverage (footprint intersect bbox / bbox), sorted by time. Retries
    transient Planetary Computer errors, since a daily loop hits it a lot.
    """
    catalog = pystac_client.Client.open(STAC_URL, modifier=planetary_computer.sign)
    aoi = box(west, south, east, north)
    for attempt in range(retries):
        try:
            search = catalog.search(
                collections=["sentinel-1-grd"],
                bbox=[west, south, east, north],
                datetime=f"{start}T00:00:00Z/{end}T23:59:59Z",
            )
            scenes = []
            for item in search.items():
                props = item.properties
                if props.get("sar:instrument_mode") != "IW":
                    continue
                if "VV" not in props.get("sar:polarizations", []):
                    continue
                geom = shape(item.geometry)
                scenes.append({
                    "scene_id": item.id,
                    "datetime": props["datetime"],
                    "platform": props.get("platform", "unknown"),
                    "relative_orbit": props.get("sat:relative_orbit"),
                    "footprint_wkt": geom.wkt,
                    "coverage": round(geom.intersection(aoi).area / aoi.area, 4),
                })
            scenes.sort(key=lambda s: s["datetime"])
            return scenes
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))


def _gfw_4wings(geojson, date_start, date_end, temporal="DAILY", spatial="HIGH"):
    resp = requests.post(
        "https://gateway.api.globalfishingwatch.org/v3/4wings/report",
        headers={"Authorization": f"Bearer {GFW_TOKEN}", "Content-Type": "application/json"},
        params={
            "datasets[0]": "public-global-presence:latest",
            "date-range": f"{date_start},{date_end}",
            "temporal-resolution": temporal,
            "spatial-resolution": spatial,
            "group-by": "VESSEL_ID",
            "format": "JSON",
        },
        json={"geojson": geojson},
    )
    if resp.status_code != 200:
        raise RuntimeError(f"GFW {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def fetch_ais_day(geojson, day):
    """Vessel presence inside the polygon for a single day.

    Returns (n_vessels, records) where records is a list of
    (vesselId, day, shipName, flag, vesselType, hours).
    """
    result = _gfw_4wings(geojson, day, day, temporal="DAILY", spatial="HIGH")
    records = []
    for entry in result.get("entries", []):
        if not isinstance(entry, dict):
            continue
        for value in entry.values():
            if not isinstance(value, list):
                continue
            for r in value:
                records.append((
                    str(r.get("vesselId")),
                    r.get("date", day),
                    r.get("shipName"),
                    r.get("flag"),
                    r.get("vesselType"),
                    float(r.get("hours") or 0.0),
                ))
    n_vessels = len({r[0] for r in records})
    return n_vessels, records
