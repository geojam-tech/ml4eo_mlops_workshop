# ML4EO Architecture

How the pieces fit together. Diagrams are [Mermaid](https://mermaid.js.org/) so they render on GitHub and stay in version control. Source of truth: `docker-compose.yml`, `pipeline/`, and `grafana/`.

## 1. Services

`docker compose up` brings up five containers. They share the project's `./data` directory through bind mounts.

```mermaid
flowchart TB
  user(["Participant browser"])

  subgraph compose["Docker Compose"]
    jup["jupyter :8888<br/>notebooks"]
    pref["prefect :4200<br/>orchestration + UI"]
    mlf["mlflow :5000<br/>model registry + UI"]
    worker["worker<br/>serves + runs the flows"]
    graf["grafana :3000<br/>dashboard (admin / workshop)"]
  end

  subgraph data["./data (bind-mounted)"]
    db[("metrics.db")]
    mldb[("mlflow.db + mlartifacts")]
    seed[/"seed/ais_primorsk.parquet"/]
    cache[/"cache (chips, water mask)"/]
  end

  user --> jup
  user --> pref
  user --> mlf
  user --> graf

  worker <-->|"register + run deployments"| pref
  worker <-->|"load model by version"| mlf
  worker -->|"reads"| seed
  worker -->|"caches chips"| cache
  worker -->|"writes results + inline thumbnails"| db
  jup <-->|"promote a model"| mlf
  graf -->|"reads via SQLite datasource"| db
```

`metrics.db` is the integration point: the worker writes results, Grafana reads them. MLflow is the separate model side: the notebook (or setup) registers CFAR versions there, and the worker loads them back by version.

## 2. A CFAR model is a versioned config

CFAR has no training step, so a "model" is a parameter set. We content-hash the config into a short version sha and register it as an MLflow pyfunc model. Identical params always produce the same sha, which is what makes skip-if-present correct.

```mermaid
flowchart LR
  cfg["CFARConfig<br/>pfa, guard, background, min/max px"]
  cfg -->|"sha256(config)[:8]"| sha["version sha<br/>e.g. f087272e"]
  sha --> reg["MLflow registry<br/>name: cfar"]
  nb["primorsk_modelling<br/>(promote)"] --> reg
  setup["setup.register_models<br/>(baseline strict only)"] --> reg
  reg -->|"load by version"| flow["monitoring flow"]
```

## 3. The monitoring flow (a daily loop)

The unit is a day. `monitoring` lists the scenes in the window, groups them by day, and runs each day in order. `process_day` is the same logic for one day and is its own deployment. Two levels of skip avoid repeated work.

```mermaid
flowchart TB
  start["monitoring(model_version, start, end)"] --> guard{"setup ready?"}
  guard -->|no| stop["refuse to start"]
  guard -->|yes| listing["list scenes in window<br/>group by day"]
  listing --> loop["for each day"]
  loop --> ais["ensure_ais(day)<br/>cached? skip : fetch + store"]
  loop --> scene{"prediction exists<br/>for (model, scene)?"}
  scene -->|yes| skip["skip"]
  scene -->|no| chip{"chip cached<br/>for scene?"}
  chip -->|yes| load["load chip from disk"]
  chip -->|no| dl["download from PC + cache"]
  load --> predict
  dl --> predict["model.predict(chip, water mask)"]
  predict --> write["write detections + thumbnail,<br/>then prediction (completion marker)"]
```

The prediction row is written **last**, so its presence means the scene is fully done — a half-failed scene retries cleanly.

## 4. The schema (metrics.db)

Keyed so nothing is recomputed. Scene metadata is intrinsic; the chip, AIS, and results are scoped to the site; predictions and detections are also scoped to the model version.

```mermaid
erDiagram
  sites ||--o{ sar_cache : "site"
  sites ||--o{ ais_days : "site"
  sites ||--o{ port_vessel_days : "site"
  scenes ||--o{ predictions : "scene"
  predictions ||--o{ detections : "(model, scene)"
  sites { text site PK }
  scenes { text scene_id PK }
  sar_cache { text site_scene PK }
  ais_days { text site_day PK }
  port_vessel_days { text site_vessel_day PK }
  predictions { text site_model_scene PK }
  detections { text site_model_scene_det PK }
```

- `scenes` — intrinsic, bbox-independent metadata.
- `sar_cache` — chip download ledger, `(site, scene)`.
- `ais_days` / `port_vessel_days` — AIS count + fleet detail, `(site, …)`, model-independent.
- `predictions` — per `(site, model_version, scene)`; what Grafana reads, filtered by the model dropdown.
- `detections` — per detection; where two models visibly differ.

## 5. Module map

| File | Role |
|------|------|
| `pipeline/config.py` | port box, default window, coverage threshold |
| `pipeline/data.py` | per-day scene listing (live STAC) + per-day AIS fetch |
| `pipeline/detector.py` | CFAR detector; `detect()` is the whole thing |
| `pipeline/models.py` | `CFARConfig`, content-sha versions, MLflow register/load |
| `pipeline/metrics.py` | SQLite schema + writers (the integration point) |
| `pipeline/flows.py` | `process_day` and the sequential `monitoring` flow |
| `pipeline/setup.py` | idempotent provisioning (schema, mask, AIS seed, models) |
| `pipeline/deploy.py` | registers `setup` / `monitoring` / `process-day` |
| `scripts/reset.sh` | token-free clean slate |
| `grafana/` | provisioned datasource + dashboard (with the `$model_version` variable) |
