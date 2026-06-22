"""SQLite metrics database, the integration point between the flow and Grafana.

Everything is keyed so that work is never repeated:

- a SAR chip is downloaded once per (site, scene)        -> sar_cache
- AIS is fetched once per (site, day)                     -> ais_days
- a CFAR model produces one prediction per scene          -> predictions
                                                             keyed (site, model_version, scene)

A `model_version` is a short content sha of the CFAR config, so identical params
always resolve to the same version and skip-if-present stays correct. The
`predictions` row is written last, as the completion marker for a (site, model,
scene): if it is present, that scene is fully done and is skipped on re-run.
"""

import sqlite3
from datetime import datetime, timezone

from pipeline.config import DB_PATH

MODEL_NAME = "cfar"


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(DB_PATH))


def _connect_ro():
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------- schema
def init_db():
    conn = _connect()
    c = conn.cursor()

    # The area definition. One row per site; "primorsk" for now. The bbox here
    # is what every chip is cropped to, so it is part of the identity of a result.
    c.execute("""
        CREATE TABLE IF NOT EXISTS sites (
            site TEXT PRIMARY KEY,
            west REAL, south REAL, east REAL, north REAL,
            geojson TEXT,
            water_mask_path TEXT
        )
    """)

    # Intrinsic, bbox-independent scene metadata from the STAC catalogue.
    c.execute("""
        CREATE TABLE IF NOT EXISTS scenes (
            scene_id TEXT PRIMARY KEY,
            datetime TEXT,
            platform TEXT,
            relative_orbit INTEGER,
            footprint_wkt TEXT
        )
    """)

    # Download ledger: the small VV window cropped to a site, cached on disk.
    # Keyed (site, scene_id) because the chip depends on the crop, not just the
    # scene. Lets a second model reuse the pixels instead of re-hitting PC.
    c.execute("""
        CREATE TABLE IF NOT EXISTS sar_cache (
            site TEXT,
            scene_id TEXT,
            path TEXT,
            bytes INTEGER,
            fetched_at TEXT,
            PRIMARY KEY (site, scene_id)
        )
    """)

    # Per-day AIS presence and fetch marker. A present row means "already
    # fetched", so the daily loop skips it (distinct from a real zero-vessel day).
    c.execute("""
        CREATE TABLE IF NOT EXISTS ais_days (
            site TEXT,
            day TEXT,
            n_vessels INTEGER,
            fetched_at TEXT,
            PRIMARY KEY (site, day)
        )
    """)

    # Fleet detail behind the daily counts. One row per (site, vessel, day).
    c.execute("""
        CREATE TABLE IF NOT EXISTS port_vessel_days (
            site TEXT,
            vesselId TEXT,
            day TEXT,
            shipName TEXT,
            flag TEXT,
            vesselType TEXT,
            hours REAL,
            PRIMARY KEY (site, vesselId, day)
        )
    """)

    # One row per scene per CFAR model version. What Grafana reads, filtered by
    # site + model_version. Scene fields denormalised so dashboard SQL is simple.
    c.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            site TEXT,
            model_name TEXT,
            model_version TEXT,
            scene_id TEXT,
            datetime TEXT,
            platform TEXT,
            relative_orbit INTEGER,
            sar_detections INTEGER,
            ais_count INTEGER,
            detection_ais_ratio REAL,
            coverage_fraction REAL,
            flags TEXT,
            flow_run_id TEXT,
            mlflow_run_id TEXT,
            thumbnail TEXT,
            computed_at TEXT,
            PRIMARY KEY (site, model_version, scene_id)
        )
    """)

    # Individual detections behind each count, where models actually differ.
    c.execute("""
        CREATE TABLE IF NOT EXISTS detections (
            site TEXT,
            model_version TEXT,
            scene_id TEXT,
            det_id INTEGER,
            lon REAL,
            lat REAL,
            size_pixels INTEGER,
            PRIMARY KEY (site, model_version, scene_id, det_id)
        )
    """)

    conn.commit()
    conn.close()


def init_setup_status():
    """Table holding the latest outcome of each setup step (one row per step)."""
    conn = _connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS setup_status (
            step TEXT PRIMARY KEY,
            run_time TEXT,
            status TEXT,
            detail TEXT,
            duration_s REAL
        )
    """)
    conn.commit()
    conn.close()


# --------------------------------------------------------------------- sites
def upsert_site(site, west, south, east, north, geojson=None, water_mask_path=None):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO sites
        (site, west, south, east, north, geojson, water_mask_path)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (site, west, south, east, north, geojson, water_mask_path))
    conn.commit()
    conn.close()


def get_site(site):
    conn = _connect_ro()
    try:
        row = conn.execute(
            "SELECT site, west, south, east, north, geojson, water_mask_path "
            "FROM sites WHERE site=?", (site,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    keys = ["site", "west", "south", "east", "north", "geojson", "water_mask_path"]
    return dict(zip(keys, row))


# --------------------------------------------------------------------- scenes
def upsert_scene(scene):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO scenes
        (scene_id, datetime, platform, relative_orbit, footprint_wkt)
        VALUES (?, ?, ?, ?, ?)
    """, (scene["scene_id"], scene.get("datetime"), scene.get("platform"),
          scene.get("relative_orbit"), scene.get("footprint_wkt")))
    conn.commit()
    conn.close()


# --------------------------------------------------------------------- sar cache
def record_sar_cache(site, scene_id, path, n_bytes):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO sar_cache (site, scene_id, path, bytes, fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """, (site, scene_id, str(path), int(n_bytes), _now()))
    conn.commit()
    conn.close()


# --------------------------------------------------------------------- ais
def get_ais_day(site, day):
    """Daily vessel count if already fetched, else None (the skip marker)."""
    conn = _connect_ro()
    try:
        row = conn.execute(
            "SELECT n_vessels FROM ais_days WHERE site=? AND day=?", (site, day)
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else None


def n_ais_days(site):
    """How many days of AIS are already cached for a site (for setup idempotency)."""
    if not DB_PATH.exists():
        return 0
    conn = _connect_ro()
    try:
        return conn.execute("SELECT COUNT(*) FROM ais_days WHERE site=?", (site,)).fetchone()[0]
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def write_ais_day(site, day, n_vessels, vessel_records):
    """Mark a day fetched and write its fleet detail. Idempotent on the keys."""
    conn = _connect()
    conn.execute(
        "INSERT OR REPLACE INTO ais_days (site, day, n_vessels, fetched_at) VALUES (?, ?, ?, ?)",
        (site, day, int(n_vessels), _now()),
    )
    if vessel_records:
        conn.executemany(
            "INSERT OR REPLACE INTO port_vessel_days "
            "(site, vesselId, day, shipName, flag, vesselType, hours) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(site, *r) for r in vessel_records],
        )
    conn.commit()
    conn.close()


# --------------------------------------------------------------------- predictions
def existing_scene_ids(site, model_version):
    """scene_ids already predicted for (site, model). Used to skip work."""
    if not DB_PATH.exists():
        return set()
    conn = _connect_ro()
    try:
        rows = conn.execute(
            "SELECT scene_id FROM predictions WHERE site=? AND model_version=?",
            (site, model_version),
        ).fetchall()
        return {r[0] for r in rows}
    except sqlite3.OperationalError:
        return set()
    finally:
        conn.close()


def write_detections(site, model_version, scene_id, dets):
    """Replace the detection set for (site, model, scene).

    dets: iterable of (det_id, lon, lat, size_pixels). Written before the
    prediction row, which is the completion marker.
    """
    conn = _connect()
    conn.execute(
        "DELETE FROM detections WHERE site=? AND model_version=? AND scene_id=?",
        (site, model_version, scene_id),
    )
    conn.executemany("""
        INSERT INTO detections (site, model_version, scene_id, det_id, lon, lat, size_pixels)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [(site, model_version, scene_id, d[0], d[1], d[2], d[3]) for d in dets])
    conn.commit()
    conn.close()


def write_prediction(site, model_version, row):
    """The completion marker. Write detections + thumbnail first, this last."""
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO predictions
        (site, model_name, model_version, scene_id, datetime, platform, relative_orbit,
         sar_detections, ais_count, detection_ais_ratio, coverage_fraction, flags,
         flow_run_id, mlflow_run_id, thumbnail, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        site, MODEL_NAME, model_version, row["scene_id"], row.get("datetime"),
        row.get("platform"), row.get("relative_orbit"), row["sar_detections"],
        row["ais_count"], row.get("detection_ais_ratio"),
        row.get("coverage_fraction", 1.0), row.get("flags", ""),
        row.get("flow_run_id", ""), row.get("mlflow_run_id", ""),
        row.get("thumbnail", ""), _now(),
    ))
    conn.commit()
    conn.close()


# --------------------------------------------------------------------- setup status
def setup_ready():
    """Whether setup_pipeline has run successfully (the monitoring precondition).

    Read-only (?mode=ro), so it never creates or mutates the db. Returns (ok, msg).
    """
    if not DB_PATH.exists():
        return False, "metrics.db not found. Run setup_pipeline first"
    conn = _connect_ro()
    try:
        def has_table(name):
            return conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone() is not None

        if not has_table("setup_status"):
            return False, "setup has not run (no setup_status table). Run setup_pipeline first"
        rows = conn.execute("SELECT step, status FROM setup_status").fetchall()
        if not rows:
            return False, "setup_status is empty. Run setup_pipeline first"
        failed = [step for step, status in rows if status == "failed"]
        if failed:
            return False, f"setup has failed steps ({', '.join(failed)}). Re-run setup_pipeline"
        for t in ("sites", "scenes", "predictions", "ais_days", "port_vessel_days"):
            if not has_table(t):
                return False, f"table '{t}' missing. Run setup_pipeline first"
        return True, "setup ok"
    finally:
        conn.close()


def write_setup_status(step, status, detail="", duration_s=0.0):
    """Upsert the latest result for a setup step. status: generated|skipped|failed|ok."""
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO setup_status (step, run_time, status, detail, duration_s)
        VALUES (?, ?, ?, ?, ?)
    """, (step, _now(), status, detail, round(float(duration_s), 2)))
    conn.commit()
    conn.close()
