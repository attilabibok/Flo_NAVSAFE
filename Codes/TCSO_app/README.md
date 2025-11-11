# TCSO NWM–FIM Stack

This repository contains a self-contained stack that:

1. Fetches **NWM Short Range** streamflow forecasts.
2. Filters them to your **AOI flowlines** (`p2fflowlines_tcso.gpkg`).
3. Generates **time-referenced flood inundation maps (FIM)**.
4. Writes outputs to:

   * GeoPackage / GeoJSON
   * PostGIS
   * Optionally S3, including a live FIM file.
5. Publishes layers via **GeoServer** as WFS, including:

   * Full FIM & flowlines
   * `fim_nowcast` & `fim_max` virtual (SQL view) layers.
6. Runs a **maintenance service** that prunes old FIM records but preserves configured “protected events”.

Everything is orchestrated via `docker-compose.yaml` in the project root.

---

## Architecture Overview

Services (names may vary slightly depending on your compose file):

* **`tcso-pgis`**
  PostGIS database used as the authoritative store for flowlines & FIM layers.

  * Data volume: `./_postgres_data`

* **`tcso-geoserver`**
  GeoServer serving WMS/WFS layers from PostGIS (and optionally files).

  * Data dir: `./_geoserver_data`
  * Exposes:

    * `tcso:nwm_sr_flowlines`
    * `tcso:fim_nwm`
    * `tcso:fim_nowcast`
    * `tcso:fim_max`

* **`tcso-geoserver-init`**
  Idempotent initializer:

  * Waits for GeoServer to be ready.
  * Ensures workspace, PostGIS datastore, and WFS layers are published.
  * Creates SQL views for `fim_nowcast` and `fim_max`.

* **`tcso-worker`**
  NWM/FIM pipeline worker:

  * Uses `fim.py` / `nwm_fim_worker.py`.
  * Every interval:

    * Detects latest usable NWM SR cycle (with retries).
    * Retrieves NWM forecasts for AOI COMIDs.
    * Generates:

      * flowlines with `q_fXXX` forecast columns,
      * FIM polygons for each forecast time.
    * Writes to files + PostGIS + optional S3.
  * Output dir: `./_output` (mounted to `/data/output` in the container).

* **`tcso-maintainer`** (optional but recommended)

  * FastAPI service.
  * Periodically deletes old FIM records from PostGIS based on retention.
  * Keeps user-defined protected event ranges.

Volumes are intentionally created as **local folders next to `docker-compose.yaml`**:

* `./_postgres_data`
* `./_geoserver_data`
* `./_output`

These persist data across container rebuilds.

---

## Prerequisites

### Windows (Docker Desktop)

1. Install **Docker Desktop**.
2. Enable:

   * WSL2 backend (recommended).
3. Clone this repository.

### macOS (Docker Desktop)

1. Install **Docker Desktop for Mac**.
2. Ensure enough resources (e.g. 4 CPU / 8 GB RAM) for GeoServer + PostGIS.
3. Clone this repository.

### Linux

1. Install:

   * `docker`
   * `docker compose` plugin (or `docker-compose` if using v1).
2. Add your user to the `docker` group (optional).
3. Clone this repository.

Verify:

```bash
docker compose version
```

---

## Quick Start

From the directory containing `docker-compose.yaml`:

```bash
docker compose up -d --build
```

This will:

* Build the custom Python images.
* Start PostGIS.
* Start GeoServer.
* Initialize GeoServer (workspace, datastore, layers).
* Start the worker to fetch NWM data and generate FIM.
* Start the maintainer (if enabled) to clean history.

To follow logs:

```bash
docker compose logs -f
```

Check specific services:

```bash
docker compose logs -f tcso-worker
docker compose logs -f tcso-geoserver-init
docker compose logs -f tcso-geoserver
docker compose logs -f tcso-maintainer
```

Stop everything:

```bash
docker compose down
```

(Volumes in `./_geoserver_data`, `./_postgres_data`, `./_output` are preserved.)

---

## Accessing the Services

### GeoServer

Once running:

* UI: `http://localhost:8080/geoserver`
* WFS GetCapabilities:

  ```text
  http://localhost:8080/geoserver/tcso/ows?service=WFS&version=1.0.0&request=GetCapabilities
  ```

Exposed feature types typically include:

* `tcso:nwm_sr_flowlines` – Flowlines with forecast attributes.
* `tcso:fim_nwm` – All FIM polygons (all `t0`/`valid_time`).
* `tcso:fim_nowcast` – Latest `t0`, earliest `valid_time` (nowcast layer).
* `tcso:fim_max` – Latest `t0`, max inundation per HydroID.

### Worker Outputs (Files)

On the host:

* `./_output/nwm_fim.gpkg` – Flowlines + forecasts.
* `./_output/flowlines.geojson`
* `./_output/FIM_*.gpkg` / `FIM_*.geojson` – Time-stamped FIM snapshots.

### Maintainer API

If exposed (depends on your compose):

* Base: `http://localhost:8100`
* List protected events:

  ```bash
  curl http://localhost:8100/protected-events
  ```
* Add a protected event:

  ```bash
  curl -X POST http://localhost:8100/protected-events \
    -H "Content-Type: application/json" \
    -d '{
      "name": "Nov2025_Flood",
      "t0_from": "2025-11-08T00:00:00Z",
      "t0_to": "2025-11-12T23:59:59Z"
    }'
  ```
* Manual cleanup trigger:

  ```bash
  curl -X POST http://localhost:8100/cleanup
  ```

---

## Configuration via Environment Variables

Most settings are controlled via env vars referenced in `docker-compose.yaml`.
You can:

* Edit them directly in `docker-compose.yaml`, or
* Create a `.env` file next to it (Docker will auto-load).

Below are the key knobs.

### 1. NWM Fetch Configuration (`NWM_*`)

```env
# S3 bucket and product
NWM_BUCKET=noaa-nwm-pds
NWM_PRODUCT=short_range
NWM_VARIABLE=streamflow

# How many days back to search for candidate cycles
NWM_LOOKBACK_DAYS=1

# Retry behavior for incomplete cycles
NWM_RETRIES=5          # attempts per t0
NWM_RETRY_WAIT=30      # seconds between attempts
```

### 2. FIM & AOI Configuration (`FIM_*`)

```env
# Master GPKG with FIM polygons + rating curves
FIM_GPKG=/data/input/Theme4Data.gpkg

# AOI flowlines subset with feature_id (COMID) filter
FIM_FLOWLINES_GPKG=/data/input/p2fflowlines_tcso.gpkg
FIM_FLOWLINES_LAYER=          # leave empty if single-layer
FIM_FLOWLINES_ID_FIELD=feature_id
FIM_FLOWLINES_HYDRO_FIELD=HydroID

# Rating curves + FIM polygon layer names inside FIM_GPKG
FIM_RATING_TABLE=RatingCurves
FIM_FIM_LAYER=TravisFIM

# Lake handling
FIM_LAKE_FIELD=LakeID
FIM_LAKE_MISSING=-999

# Allow extrapolation beyond rating curve range
FIM_EXTRAPOLATE=true
```

Paths like `/data/input/...` should match your volume mappings in `docker-compose.yaml`
(e.g. mount local `./data` to `/data/input`).

### 3. Output Configuration (`OUT_*`)

```env
# Where to write; "gpkg", "postgis", or "both"
OUT_MODE=both

# Base GPKG & layers (inside container)
OUT_GPKG_PATH=/data/output/nwm_fim.gpkg
OUT_FLOWLINES_LAYER=nwm_sr_flowlines
OUT_FIM_LAYER_PREFIX=fim_
OUT_FIM_FORMATS=gpkg,geojson

# PostGIS connection (used by worker and geoserver_init)
OUT_PG_DSN=postgresql://gis:gis@tcso-pgis:5432/gis
OUT_PG_SCHEMA=public
OUT_PG_FLOWLINES_TABLE=nwm_sr_flowlines
OUT_PG_FIM_TABLE=fim_nwm

# Optional S3 export for snapshots + live file
OUT_FIM_S3_BUCKET=your-s3-bucket-name
OUT_FIM_S3_PREFIX=tcso/fim
OUT_FIM_S3_LIVE_KEY=FIM_TCSO_live.geojson
```

If you don’t need S3, omit those variables.

### 4. GeoServer Init (`GEOSERVER_*`)

```env
# URL from init container POV (service name in compose)
GEOSERVER_URL=http://tcso-geoserver:8080/geoserver
GEOSERVER_USER=admin
GEOSERVER_PASSWORD=geoserver

# Workspace / datastore names created in GeoServer
GEOSERVER_WORKSPACE=tcso
GEOSERVER_DATASTORE=tcso_postgis
```

These must match both your compose service names and the PostGIS settings
so GeoServer can connect.

### 5. Maintainer (`MAINTAINER_*`)

```env
# PostGIS DSN; falls back to OUT_PG_DSN if not set
MAINTAINER_PG_DSN=postgresql://gis:gis@tcso-pgis:5432/gis

# Schema and table where FIM data is stored
MAINTAINER_PG_SCHEMA=public
MAINTAINER_FIM_TABLE=fim_nwm

# Retention as ISO8601 duration (e.g. 3 days)
MAINTAINER_RETENTION=P3D

# Background cleanup loop
MAINTAINER_INTERVAL_SEC=900
MAINTAINER_ENABLED=true
```

Anything with `t0` older than `now - MAINTAINER_RETENTION` is eligible for deletion,
**unless** it falls into a protected event window.

---

## Typical Workflow

1. **Start the stack**

   ```bash
   docker compose up -d --build
   ```

2. **Wait for first successful run**

   Watch:

   ```bash
   docker compose logs -f tcso-worker
   ```

   You should see:

   * “Latest NWM SR cycle found…”
   * “Loaded NWM slice…”
   * “Flowlines written…”
   * “FIM snapshot written…”

3. **Connect your client (QGIS, web app, etc.)**

   * Add WFS endpoint:

     ```text
     http://localhost:8080/geoserver/tcso/ows?service=WFS&version=1.0.0&request=GetCapabilities
     ```
   * Use layers:

     * `tcso:fim_nowcast` for “current” inundation
     * `tcso:fim_max` for max extent for latest cycle
     * `tcso:fim_nwm` and `tcso:nwm_sr_flowlines` for full history/analysis.

4. **Manage history**

   * Set retention via `MAINTAINER_RETENTION=P3D` (or similar).
   * Register key events as protected via the maintainer API.

---

If you’d like, next step I can generate a minimal `.env.example` matching your current compose for you to drop in.
