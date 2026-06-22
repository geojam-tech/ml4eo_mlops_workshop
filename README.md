# ML4EO Workshop: Introduction to MLOps for Earth Observation

What happens when a model needs to run reliably on new imagery every week, across seasons and sensor changes, without someone checking every output by hand? This workshop works through that with a concrete case: monitoring vessel activity at the Primorsk oil terminal in the Gulf of Finland, using Sentinel-1 SAR and AIS.

The detector is a classical CFAR (Constant False Alarm Rate) algorithm with no training step. That is deliberate: this is an **unsupervised** problem with no labels, so the interesting work is everything around the model. How do you version a detector, run it reproducibly, monitor its output when you have no ground truth, and tell a real change in the world from a sensor dropout or a change in your own pipeline?

It runs entirely in GitHub Codespaces. No local setup, no cloud accounts, no GPU.

## Quick start

Click **Code → Codespaces → New codespace**. The stack builds in a few minutes and comes up running:

| Service | Port | What it does |
|---------|------|--------------|
| Jupyter Lab | 8888 | Run the notebooks here |
| Prefect | 4200 | Orchestration — trigger and watch pipeline runs |
| MLflow | 5001 | Model registry — versioned CFAR detectors |
| Grafana | 3000 | Monitoring dashboard (`admin` / `workshop`) |

It works out of the box: the AIS data ships as a committed seed, so no API token is needed.

Then, in the Prefect UI:
1. Run **`setup`** once — builds the schema, water mask, AIS, registers the baseline CFAR model, and downloads the cached SAR chips (~416 MB, so the pipeline runs without Planetary Computer).
2. Run **`monitoring`** — processes the window with a chosen model, reading from the chip cache; the Grafana dashboard fills in.

> **If the chip download fails** — Google Drive can rate-limit the shared link when many people pull it at once — download **`primorsk_chips.tar.gz`** by hand from [this Google Drive link](https://drive.google.com/file/d/1Mkc6gtTg6l_76rpH0AHL75O6JLHFrJZF/view?usp=sharing), drop it in the **repository root**, and run `setup` again. It extracts your copy instead of downloading, and skips the step entirely once the chips are in place. (A browser download often works even when the automatic one is rate-limited.)

## The idea

A CFAR detector is just a parameter set (mainly `pfa`, the false-alarm rate). We treat it as a real model anyway: a config is **content-hashed into a version sha** and registered in MLflow as a loadable model. The pipeline then runs *a specific version*, and every result is keyed by `(site, model_version, scene)`. Setup registers one (a strict detector); register a looser one yourself, run both, and compare them by flipping the **model** dropdown in Grafana.

Because results are keyed that way, runs are idempotent and cheap to repeat. There are two levels of skip:

- a SAR chip is **downloaded once** per scene (`sar_cache`), so a second model reuses the pixels;
- a scene is **predicted once** per model (`predictions`), so re-running a window only does what's missing.

## Notebooks

**`notebooks/primorsk_data_prep.ipynb`** — explore the two streams: Sentinel-1 scenes (live from Planetary Computer, with coverage by orbit) and AIS presence (the committed seed). Look only; it writes nothing.

**`notebooks/primorsk_modelling.ipynb`** — load a real chip, apply the land/water mask, test CFAR at several `pfa` values to see the false-alarm rate move, then **promote** a chosen config to MLflow with `models.register()`. That registered version is exactly what the pipeline runs.

## Pipeline

`pipeline/` defines three Prefect deployments, served automatically by the `worker` on `docker compose up` (restart it after a code change: `docker compose restart worker`).

**`setup`** rebuilds every foundation, idempotently, with no token needed. Steps:

1. `init_database` — schema + the `primorsk` site row
2. `ensure_water_mask` — 10 m land/water raster from ESA WorldCover (Planetary Computer, public)
3. `seed_ais` — load the committed AIS seed (`data/seed/ais_primorsk.parquet`) into the db
4. `register_models` — register the strict baseline (`pfa=1e-4`) into MLflow; register others (e.g. loose `pfa=1e-2`) by hand in the notebook
5. `validate_environment`

Each step records to `setup_status`, shown in Grafana's **Pipeline Health** row. Monitoring treats setup as a hard prerequisite and refuses to start without it.

**`monitoring`** runs one model over a window, day by day, sequentially. For each day it fetches that day's AIS once, then runs the model on the scene(s) that imaged the port:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `model_version` | strict baseline sha | which registered CFAR model to run |
| `start_date` / `end_date` | the full window | the date range |
| `force` | `false` | recompute even if a prediction already exists |
| `source` | `cache` | `cache` reads the committed manifest + chips from disk (no PC); `live` fetches from Planetary Computer |

**`process-day`** runs a single day (`model_version`, `day`) — handy for a quick demo.

Each scene becomes a prediction plus its individual detections, and a thumbnail stored inline in the db (a base64 PNG data URI, so Grafana renders it with no separate static server — the same locally and in Codespaces). By default the Sentinel-1 catalogue and pixels come from the cached chips (`source=cache`); with `source=live` they are fetched live from Planetary Computer (public). AIS is always seeded.

## Grafana

The dashboard has a **CFAR model** dropdown (`$model_version`) at the top. Everything below filters to the selected version, so you can compare a strict and a loose detector side by side. Bands: Pipeline Health, Sensor & Model Diagnostics (per-orbit), Model Tracking (detected vs AIS, the ratio, per-scene table with thumbnails, flags, seasonality), and Fleet (per-vessel AIS summary, flag mix).

## Clean slate

Everything reproducible lives in git or in code, so you can wipe all state and rebuild token-free:

```bash
scripts/reset.sh
```

It stops the stack, removes the dbs and caches (keeping `data/seed/`), rebuilds, and prints the `setup` → `monitoring` steps. Only dates *outside* the seed window need a GFW token.

## Running from cache vs. live (default: cache)

The default window's SAR chips live in a shared archive, so the pipeline runs end to end with no dependency on Planetary Computer — which is why **cache is the default**. `setup` runs with **`fetch_chips=True`** by default: it downloads and extracts ~416 MB into `data/cache/sar/` (idempotent — it skips when the chips are already present). `monitoring` and `process-day` then run with **`source=cache`** by default: they list scenes from the committed manifest (`data/seed/scenes_primorsk.json`), read chips from disk, and compute everything else, with no PC calls.

To run against live Planetary Computer instead, set **`source=live`** on the monitoring run (and you can set `fetch_chips=False` on setup for a lighter, chip-free provisioning). `live` fetches the catalogue and pixels from PC when it's healthy.

## Running locally

```bash
cp .env.example .env
docker compose up --build
```

Dependencies are managed with [uv](https://docs.astral.sh/uv/); the Dockerfile installs them in the container.

## Project structure

```
ML4EO/
├── data/
│   └── seed/               Committed AIS seed (token-free). Runtime state lives in data/ but is gitignored.
├── grafana/                Provisioned datasource + dashboard
├── notebooks/              primorsk_data_prep · primorsk_modelling (CFAR + promote)
├── pipeline/
│   ├── config.py           Port box, default window, coverage threshold
│   ├── data.py             Per-day scene listing + AIS fetch (no bulk pre-fetch)
│   ├── detector.py         CFAR detector (detect() is the whole thing)
│   ├── models.py           CFARConfig, content-sha versions, MLflow register/load
│   ├── metrics.py          SQLite schema + writers (the integration point)
│   ├── flows.py            process_day + the sequential monitoring flow
│   ├── setup.py            Idempotent provisioning
│   └── deploy.py           Registers the deployments
├── scripts/reset.sh        One-command clean slate
├── docker-compose.yml
└── pyproject.toml
```

## Background

The Primorsk oil terminal is Russia's largest Baltic oil port. The three-year window (Feb 2021 to Feb 2024) spans three events of interest, each falling within the data:

1. **Sentinel-1B end of mission** (December 2021). Sentinel-1B suffered a power-supply anomaly on 23 December 2021 and ESA formally ended the mission in August 2022 ([ESA / Copernicus Sentinel Online, 2022](https://sentinels.copernicus.eu/-/end-of-mission-of-the-copernicus-sentinel-1b-satellite)). A sensor change to account for, not to read as a change in vessel behaviour.
2. **Invasion of Ukraine** (February 2022). AIS counts and the fleet composition shift around this date. It also sits inside the ratio's seasonal cycle, so separate season from event before attributing anything.
3. **EU crude oil embargo** (December 2022, effect through 2023). The seaborne crude ban took effect on 5 December 2022, but the *observable* shift at Primorsk — rerouting and the shadow-fleet ramp, visible in the rise of Gabon- and Cook Islands-flagged tankers — lags well into 2023. That lag is the reason the window runs a third year: the policy date is not when the signal moves. Around the same time the AIS source also switches — GFW moved from OrbComm to Spire on 1 January 2023 — a data-pipeline discontinuity that overlaps the window.

The monitoring framework does not try to explain these. It flags them. Telling "the detector changed" from "the world changed" from "the pipeline changed" is the core teaching point.

## Data sources and attribution

**Sentinel-1 SAR** via [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/). Copernicus Sentinel data processed by ESA.

**AIS vessel presence** from [Global Fishing Watch](https://globalfishingwatch.org/), via the 4Wings API, under GFW's non-commercial terms. Attribute Global Fishing Watch if you publish anything using it.

GFW's SAR detection product does not cover the Gulf of Finland (confirmed by testing across the Gulf), so the workshop compares our own detector against AIS, not a second detector.

## References

- Sculley et al. (2015). *Hidden Technical Debt in Machine Learning Systems.* NeurIPS.
- Google Cloud. *MLOps: Continuous delivery and automation pipelines in machine learning.*
- Huyen, C. (2022). *Designing Machine Learning Systems*, ch. 8 — Data Distribution Shifts and Monitoring.
- Tuia et al. (2023). *Artificial intelligence to advance Earth observation: a perspective.* arXiv:2305.08413.
- Nowosad et al. (2026). *Navigating challenges in spatial machine learning.* Erdkunde. [doi:10.3112/erdkunde.2026.04.01](https://doi.org/10.3112/erdkunde.2026.04.01).
- Ploton et al. (2020). *Spatial validation reveals poor predictive performance of large-scale ecological mapping models.* Nature Communications.
- Meyer & Pebesma (2021). *Predicting into unknown space? Area of applicability of spatial prediction models.* Methods in Ecology and Evolution.
- [ml-ops.org](https://ml-ops.org/). MLOps principles and the end-to-end lifecycle.
