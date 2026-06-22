"""Prefect flows: monitoring as a daily loop over a site, for one CFAR model.

The unit is a day. For each day in the window we fetch that day's AIS once, then
run the chosen CFAR model over the scene(s) that imaged the port that day. Two
levels of skip keep re-runs cheap and free of repeated Planetary Computer calls:

  - a SAR chip is downloaded once per (site, scene)        -> sar_cache
  - a scene is predicted once per (site, model_version)    -> predictions

`process_day` runs one day and is its own deployment; `monitoring` runs a window
sequentially by calling the same per-day logic.
"""

import sys
import json
import warnings
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import numpy as np

warnings.filterwarnings("ignore", message="modifier.*returned a result")
import rasterio
from rasterio.vrt import WarpedVRT
import pystac_client
import planetary_computer
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import (DATE_START, DATE_END, COVERAGE_THRESHOLD, DATA_DIR, CACHE_DIR,
                             SITE, Site, Source, DEFAULT_MODEL, GFW_TOKEN)
from pipeline import data as datasrc
from pipeline import models as modelreg
from pipeline import metrics as mx
from pipeline.detector import to_db

SAR_CACHE_DIR = CACHE_DIR / "sar"
STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"


@lru_cache(maxsize=4)
def get_model(version):
    """Load and cache a registered CFAR model by sha (once per worker process)."""
    return modelreg.load(version)


@lru_cache(maxsize=2)
def load_water_mask(path):
    if not path or not Path(path).exists():
        return None
    with rasterio.open(path) as src:
        return src.read(1).astype(bool)


# ------------------------------------------------------------------ chip access
def _download_chip(scene_id, bbox):
    catalog = pystac_client.Client.open(STAC_URL, modifier=planetary_computer.sign)
    item = list(catalog.search(collections=["sentinel-1-grd"], ids=[scene_id]).items())[0]
    signed = planetary_computer.sign(item)
    href = signed.assets["vv"].href
    with rasterio.open(href) as src:
        with WarpedVRT(src, src_crs=src.gcps[1]) as vrt:
            window = rasterio.windows.from_bounds(*bbox, transform=vrt.transform)
            return vrt.read(1, window=window)


def _get_chip(site, scene_id, bbox):
    """VV chip for (site, scene), from disk cache or Planetary Computer.

    Chips live at a deterministic path, so a cache dropped in by hand (e.g.
    extracted from a shared archive) is found without depending on whatever
    absolute path the db happened to record.
    """
    path = SAR_CACHE_DIR / site / f"{scene_id}.npy"
    if path.exists():
        mx.record_sar_cache(site, scene_id, str(path), path.stat().st_size)
        return np.load(path)
    arr = _download_chip(scene_id, bbox)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.save(path, arr)
    mx.record_sar_cache(site, scene_id, str(path), path.stat().st_size)
    return arr


def _render_thumbnail(chip_db, det_lonlat, bbox):
    """Grayscale chip with red detection markers, as a base64 PNG data URI.

    The chip has square-degree pixels (a ~3:1 array), but at 60N a degree of
    longitude is about half a degree of latitude on the ground. We draw in lon/lat
    and set aspect to 1/cos(lat) so the port looks geographically correct (~1.5:1)
    instead of stretched flat.

    Returns a `data:image/png;base64,...` URI, so the image lives inline in the
    metrics db and Grafana renders it with no separate static server — identical
    behaviour locally and behind the Codespaces port proxy.
    """
    import base64
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    west, south, east, north = bbox
    vmin, vmax = np.nanpercentile(chip_db, [2, 98])
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.imshow(chip_db, cmap="gray", vmin=vmin, vmax=vmax,
              extent=[west, east, south, north], origin="upper")
    if det_lonlat:
        ax.scatter([lon for lon, _ in det_lonlat], [lat for _, lat in det_lonlat],
                   c="red", s=18, marker="x", linewidths=1.1)
    ax.set_aspect(1.0 / np.cos(np.radians((south + north) / 2)))
    ax.axis("off")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0, dpi=90)
    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


# ------------------------------------------------------------------ tasks
@task(name="ensure_ais")
def ensure_ais(site, geojson, day):
    """Day's vessel count, from cache or GFW, or None if AIS is unavailable.

    Returns None (rather than raising) when the day isn't seeded and there's no
    usable token, or when the live fetch fails. A missing AIS day must not sink
    the whole monitoring run — it gets flagged as a gap and the SAR side still
    runs, which is exactly the data-gap-vs-real-change distinction the workshop
    is about. Writes the fleet detail only on a successful fetch.
    """
    logger = get_run_logger()
    n = mx.get_ais_day(site, day)
    if n is not None:
        return n
    if not GFW_TOKEN:
        logger.warning(f"AIS {day}: not in seed and no GFW token; marking unavailable")
        return None
    try:
        n, records = datasrc.fetch_ais_day(geojson, day)
    except Exception as e:
        logger.warning(f"AIS {day}: live fetch failed ({str(e)[:80]}); marking unavailable")
        return None
    mx.write_ais_day(site, day, n, records)
    logger.info(f"AIS {day}: {n} vessels fetched")
    return n


def _scene_flags(count, ais_count, coverage):
    """Per-scene flags. ais_count is None when AIS is unavailable for the day (a
    gap or no token) — kept distinct from a real zero-vessel day (NO_AIS)."""
    flags = []
    if count == 0:
        flags.append("ZERO_DETECTIONS")
    if ais_count is None:
        flags.append("AIS_UNAVAILABLE")
    elif ais_count == 0:
        flags.append("NO_AIS")
    if coverage < 0.999:
        flags.append("PARTIAL_COVERAGE")
    return flags


@task(name="process_scene", retries=2, retry_delay_seconds=10)
def process_scene(site, model_version, scene, bbox, ais_count, water_mask_path):
    logger = get_run_logger()
    scene_id = scene["scene_id"]
    chip = _get_chip(site, scene_id, bbox)
    chip_db = to_db(chip)

    model = get_model(model_version)
    water = load_water_mask(water_mask_path)
    out = model.predict({"image_db": chip_db, "water_mask": water})
    dets, count = out["detections"], out["count"]

    h, w = chip.shape
    west, south, east, north = bbox
    det_rows = [
        (i, west + (c / w) * (east - west), north - (r / h) * (north - south), size)
        for i, (r, c, size) in enumerate(dets)
    ]

    coverage = float(scene.get("coverage", 1.0))
    ratio = count / ais_count if ais_count else None
    flags = _scene_flags(count, ais_count, coverage)

    thumb = _render_thumbnail(chip_db,
                              [(lon, lat) for _, lon, lat, _ in det_rows], bbox)
    try:
        run_id = str(get_run_context().task_run.flow_run_id)
    except Exception:
        run_id = ""

    # scene + detections first; the prediction row is the completion marker, last.
    mx.upsert_scene({
        "scene_id": scene_id, "datetime": scene["datetime"],
        "platform": scene["platform"], "relative_orbit": scene["relative_orbit"],
        "footprint_wkt": scene.get("footprint_wkt"),
    })
    mx.write_detections(site, model_version, scene_id, det_rows)
    mx.write_prediction(site, model_version, {
        "scene_id": scene_id, "datetime": scene["datetime"],
        "platform": scene["platform"], "relative_orbit": scene["relative_orbit"],
        "sar_detections": count, "ais_count": ais_count,
        "detection_ais_ratio": ratio, "coverage_fraction": coverage,
        "flags": ",".join(flags), "flow_run_id": run_id, "thumbnail": thumb,
    })
    logger.info(f"{scene_id}: det={count} ais={ais_count} ratio={ratio} flags={flags}")
    return count


# ------------------------------------------------------------------ day logic
def _run_day(model_version, site_row, day, force, source="live"):
    """List and process one day's scene(s). The atomic unit, used by both flows.

    source="live" lists scenes from Planetary Computer; source="cache" reads the
    already-downloaded scenes from the db, so a re-run needs no PC at all.
    """
    logger = get_run_logger()
    site = site_row["site"]
    bbox = (site_row["west"], site_row["south"], site_row["east"], site_row["north"])
    geojson = json.loads(site_row["geojson"]) if site_row.get("geojson") else None

    if source == "cache":
        manifest = DATA_DIR / "seed" / f"scenes_{site}.json"
        scenes = [s for s in datasrc.manifest_for_day(str(manifest), day)
                  if s["coverage"] >= COVERAGE_THRESHOLD]
    else:
        try:
            scenes = [s for s in datasrc.list_scenes(*bbox, day, day)
                      if s["coverage"] >= COVERAGE_THRESHOLD]
        except Exception as e:
            logger.warning(f"{day}: scene listing failed ({str(e)[:80]}); skipping day")
            return 0, 0
    if not scenes:
        return 0, 0  # no covered scene imaged the port this day

    ais = ensure_ais(site, geojson, day)
    done = mx.existing_scene_ids(site, model_version)
    processed = skipped = 0
    for scene in scenes:
        if not force and scene["scene_id"] in done:
            skipped += 1
            continue
        process_scene(site, model_version, scene, bbox, ais, site_row["water_mask_path"])
        processed += 1
    logger.info(f"{day}: {len(scenes)} scene(s), ais={ais}, {processed} done, {skipped} skipped")
    return processed, skipped


# ------------------------------------------------------------------ flows
@flow(name="process_day")
def process_day(day: str, model: str = DEFAULT_MODEL, site: Site = SITE, force: bool = False,
                source: Source = "cache"):
    """Run one CFAR model over a single day. model is an MLflow alias or sha."""
    site_row = mx.get_site(site)
    if not site_row:
        raise RuntimeError(f"site '{site}' not defined. Run setup_pipeline first")
    return _run_day(modelreg.resolve_version(model), site_row, day, force, source)


@flow(name="monitoring")
def monitoring(model: str = DEFAULT_MODEL, start_date: str = DATE_START, end_date: str = DATE_END,
               site: Site = SITE, force: bool = False, source: Source = "cache"):
    """Run one CFAR model over [start_date, end_date], one day at a time.

    Each day lists and processes its own scene(s), the same unit as a real
    scheduled daily run, just replayed across the window. source is 'cache'
    (default; read the catalogue from the committed manifest and chips from disk,
    so a run needs no Planetary Computer) or 'live' (fetch from PC when healthy).
    """
    logger = get_run_logger()
    ready, msg = mx.setup_ready()
    if not ready:
        raise RuntimeError(f"Cannot run monitoring: {msg}")
    site_row = mx.get_site(site)
    if not site_row:
        raise RuntimeError(f"site '{site}' not defined. Run setup_pipeline first")

    version = modelreg.resolve_version(model)
    d0 = datetime.strptime(start_date, "%Y-%m-%d")
    d1 = datetime.strptime(end_date, "%Y-%m-%d")
    logger.info(f"monitoring {start_date} -> {end_date}, model {model} ({version}), source={source}")
    total_p = total_s = 0
    d = d0
    while d <= d1:
        p, s = _run_day(version, site_row, d.strftime("%Y-%m-%d"), force, source)
        total_p += p
        total_s += s
        d += timedelta(days=1)
    logger.info(f"done: {total_p} processed, {total_s} skipped")
    return {"processed": total_p, "skipped": total_s}
