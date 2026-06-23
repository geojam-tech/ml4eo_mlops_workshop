"""Setup pipeline: builds everything the monitoring pipeline depends on.

Idempotent, and designed for a token-free clean slate: nuke the dbs and caches,
run this, and everything reproducible comes back from git and code.

    1. init_database:      schema + the 'primorsk' site row
    2. ensure_water_mask:  land/water raster from ESA WorldCover (public, no token)
    3. seed_ais:           load the committed AIS seed into the db (no token)
    4. register_models:    register the strict baseline CFAR config into MLflow
    5. validate_environment

Predictions are not produced here; that is the monitoring run. Setup only
rebuilds the foundations.
"""

import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from prefect import flow, task, get_run_logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import GFW_TOKEN, ROOT, CACHE_DIR, DATA_DIR, PORT_BBOX, PORT_GEOJSON
from pipeline import metrics as mx
from pipeline import models as modelreg

SITE = "primorsk"
WATER_MASK_PATH = CACHE_DIR / "water_mask_primorsk.tif"
AIS_SEED = DATA_DIR / "seed" / "ais_primorsk.parquet"
WATER_MASK_SEED = DATA_DIR / "seed" / "water_mask_primorsk.tif"
CHIPS_URL = os.getenv(
    "CHIPS_URL",
    "https://drive.google.com/uc?id=1a4A3mqNtsAS0Dcz3engungxIJqFlYH4z"
)

# ESA WorldCover classes that count as land (everything except permanent water,
# class 80). Open sea reads as nodata and is treated as water too.
LAND_CLASSES = [10, 20, 30, 40, 50, 60, 70, 90, 95, 100]


@task(name="init_database")
def init_database():
    t0 = time.time()
    log = get_run_logger()
    mx.init_db()
    mx.init_setup_status()
    w, s, e, n = PORT_BBOX
    mx.upsert_site(SITE, w, s, e, n, geojson=json.dumps(PORT_GEOJSON),
                   water_mask_path=str(WATER_MASK_PATH))
    log.info("schema ready, site 'primorsk' seeded")
    mx.write_setup_status("database", "ok", "schema + site primorsk", time.time() - t0)
    return {"step": "database", "status": "ok"}


@task(name="ensure_water_mask", retries=2, retry_delay_seconds=10)
def ensure_water_mask():
    t0 = time.time()
    log = get_run_logger()
    if WATER_MASK_PATH.exists() and WATER_MASK_PATH.stat().st_size > 0:
        with rasterio.open(WATER_MASK_PATH) as src:
            detail = f"{src.width}x{src.height} px"
        log.info(f"water mask present, skipping ({detail})")
        mx.write_setup_status("water_mask", "skipped", detail, time.time() - t0)
        return {"step": "water_mask", "status": "skipped"}

    # Prefer the committed seed over Planetary Computer, so a clean slate does not
    # depend on PC for the mask.
    if WATER_MASK_SEED.exists() and WATER_MASK_SEED.stat().st_size > 0:
        import shutil
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(WATER_MASK_SEED, WATER_MASK_PATH)
        with rasterio.open(WATER_MASK_PATH) as src:
            detail = f"{src.width}x{src.height} px from seed"
        log.info(f"water mask copied from seed ({detail})")
        mx.write_setup_status("water_mask", "skipped", detail, time.time() - t0)
        return {"step": "water_mask", "status": "skipped"}

    import pystac_client
    import planetary_computer
    from rasterio.windows import from_bounds

    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign,
    )
    items = list(catalog.search(collections=["esa-worldcover"], bbox=PORT_BBOX).items())
    if not items:
        msg = "No ESA WorldCover items found over the site bbox."
        mx.write_setup_status("water_mask", "failed", msg, time.time() - t0)
        raise RuntimeError(msg)
    item = sorted(items, key=lambda it: it.properties.get("start_datetime", ""))[-1]
    href = planetary_computer.sign(item).assets["map"].href

    with rasterio.open(href) as src:
        window = from_bounds(*PORT_BBOX, transform=src.transform)
        landcover = src.read(1, window=window)
        win_transform = src.window_transform(window)

    water = ~np.isin(landcover, LAND_CLASSES)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        WATER_MASK_PATH, "w", driver="GTiff", dtype="uint8", count=1,
        height=water.shape[0], width=water.shape[1], crs="EPSG:4326",
        transform=win_transform, compress="deflate",
    ) as dst:
        dst.write(water.astype("uint8"), 1)

    detail = f"{water.shape[1]}x{water.shape[0]} px, {water.mean():.0%} water"
    log.info(f"water mask written: {detail}")
    mx.write_setup_status("water_mask", "generated", detail, time.time() - t0)
    return {"step": "water_mask", "status": "generated"}


@task(name="seed_ais")
def seed_ais():
    t0 = time.time()
    log = get_run_logger()
    if mx.n_ais_days(SITE) > 0:
        detail = f"{mx.n_ais_days(SITE)} days already present"
        log.info(f"AIS seeded, skipping ({detail})")
        mx.write_setup_status("ais", "skipped", detail, time.time() - t0)
        return {"step": "ais", "status": "skipped"}

    if not AIS_SEED.exists():
        msg = f"AIS seed missing at {AIS_SEED}. Commit it, or fetch live with a token"
        mx.write_setup_status("ais", "failed", msg, time.time() - t0)
        raise FileNotFoundError(msg)

    df = pd.read_parquet(AIS_SEED)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    days = 0
    for day, grp in df.groupby("date"):
        recs = [
            (str(r.vesselId), day,
             None if pd.isna(r.shipName) else r.shipName,
             None if pd.isna(r.flag) else r.flag,
             None if pd.isna(r.vesselType) else r.vesselType,
             0.0 if pd.isna(r.hours) else float(r.hours))
            for r in grp.itertuples(index=False)
        ]
        mx.write_ais_day(SITE, day, int(grp["vesselId"].nunique()), recs)
        days += 1
    detail = f"seeded {days} days from {AIS_SEED.name}"
    log.info(detail)
    mx.write_setup_status("ais", "generated", detail, time.time() - t0)
    return {"step": "ais", "status": "generated"}


@task(name="register_models")
def register_models():
    """Register only the strict baseline. Other models (e.g. the loose one) are
    promoted by hand in the notebook (the interactive step)."""
    t0 = time.time()
    log = get_run_logger()
    cfg = modelreg.DEMO_CONFIGS[0]
    sha = modelreg.register(cfg, alias="default")
    log.info(f"registered cfar {sha} (alias 'default'): pfa={cfg.pfa}")
    mx.write_setup_status("models", "ok", f"version: {sha} @default (pfa={cfg.pfa})", time.time() - t0)
    return {"step": "models", "status": "ok", "shas": [sha]}


@task(name="validate_environment")
def validate_environment():
    t0 = time.time()
    log = get_run_logger()
    token = "set" if GFW_TOKEN else "not set"
    try:
        n = len(modelreg.list_versions())
        mlf = f"{n} model version(s)"
    except Exception as ex:
        mlf = f"unreachable ({str(ex)[:60]})"
    detail = f"GFW_TOKEN={token}, MLflow: {mlf}"
    log.info(detail)
    mx.write_setup_status("environment", "ok", detail, time.time() - t0)
    return {"step": "environment", "status": "ok"}


@task(name="fetch_chips", retries=1, retry_delay_seconds=15)
def download_chips():
    """Download and extract the cached SAR chips. Opt-in; skips if already present.

    Lets a later monitoring run use source=cache with no Planetary Computer.
    """
    t0 = time.time()
    log = get_run_logger()
    sar_dir = CACHE_DIR / "sar" / SITE
    present = len(list(sar_dir.glob("*.npy"))) if sar_dir.exists() else 0
    if present > 0:
        detail = f"{present} chips already present"
        log.info(f"chips present, skipping ({detail})")
        mx.write_setup_status("chips", "skipped", detail, time.time() - t0)
        return {"step": "chips", "status": "skipped"}

    import gdown
    import tarfile
    archive = ROOT / "primorsk_chips.tar.gz"
    if archive.exists():
        log.info("extracting cached chip archive")
    else:
        log.info("downloading chip archive (~416 MB)")
        gdown.download(CHIPS_URL, str(archive), quiet=True)
    with tarfile.open(archive) as tar:
        tar.extractall(ROOT, filter="data")
    n = len(list(sar_dir.glob("*.npy")))
    detail = f"extracted {n} chips"
    log.info(detail)
    mx.write_setup_status("chips", "generated", detail, time.time() - t0)
    return {"step": "chips", "status": "generated"}


@flow(name="setup_pipeline", log_prints=True)
def setup_pipeline(fetch_chips: bool = True):
    """Rebuild every foundation the monitoring pipeline depends on. Idempotent.

    fetch_chips=True (the default) also downloads the cached SAR chips, so the
    monitoring run can use source=cache (also the default) with no Planetary
    Computer. Set it False for a lighter setup when you intend to run source=live.
    """
    mx.init_db()
    mx.init_setup_status()
    results = [
        init_database(),
        ensure_water_mask(),
        seed_ais(),
        register_models(),
    ]
    if fetch_chips:
        results.append(download_chips())
    results.append(validate_environment())
    summary = {r["step"]: r["status"] for r in results}
    print(f"Setup complete: {summary}")
    return summary


if __name__ == "__main__":
    setup_pipeline()
