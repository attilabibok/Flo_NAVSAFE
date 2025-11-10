#!/usr/bin/env python3
"""
get_nwm_sr_forecast.py

End-to-end NWM SR → Flowlines → FIM pipeline.

What this module does
---------------------
1. Fetch NWM short_range channel_rt streamflow:
   - for a given t0 (reference time), or
   - for the latest available cycle in the bucket.

2. Build a Flowlines GeoDataFrame from Theme4Data.gpkg that includes:
   - base attributes + geometry, and
   - one column per forecast lead hour (e.g. q_f001, q_f002, ...),
     OR a long-format table with (t0, valid_time, lead_hour) records.

3. Generate Flood Inundation Map polygons (FIM) per forecast time:
   - Use RatingCurves + FIM polygons + NWM flows.
   - Output to:
       - GeoPackage, and/or
       - PostGIS (for GeoServer), controlled via env vars.

4. Designed so GeoServer can:
   - expose a "latest" layer (latest t0 + earliest valid_time),
   - expose parameterized or time-enabled layers using t0 & valid_time.

Configuration via env vars (pydantic_settings)
----------------------------------------------
NWM_*
  NWM_BUCKET              (default: noaa-nwm-pds)
  NWM_PRODUCT             (default: short_range)
  NWM_VARIABLE            (default: streamflow)
  NWM_LOOKBACK_DAYS       (default: 4)

FIM_*
  FIM_GPKG                (default: ./Codes/TCSO_app/data/Theme4Data.gpkg)
  FIM_FLOWLINES_LAYER     (default: P2FFlowlines)
  FIM_FLOWLINES_ID_FIELD  (default: feature_id)
  FIM_FLOWLINES_HYDRO_FIELD (default: HydroID)
  FIM_RATING_TABLE        (default: RatingCurves)
  FIM_FIM_LAYER           (default: TravisFIM)
  FIM_LAKE_FIELD          (default: LakeID)
  FIM_LAKE_MISSING        (default: -999)
  FIM_EXTRAPOLATE         (default: true)

OUT_*
  OUT_MODE                (gpkg | postgis | both; default: gpkg)

  # GeoPackage outputs
  OUT_GPKG_PATH           (default: ./output/nwm_fim.gpkg)
  OUT_FLOWLINES_LAYER     (default: nwm_sr_flowlines)
  OUT_FIM_LAYER_PREFIX    (default: fim_)  # fim_YYYYmmddHHMM

  # PostGIS outputs (if OUT_MODE in {postgis,both})
  OUT_PG_DSN              (e.g. postgresql://user:pass@host:5432/dbname)
  OUT_PG_SCHEMA           (default: public)
  OUT_PG_FLOWLINES_TABLE  (default: nwm_sr_flowlines)
  OUT_PG_FIM_TABLE        (default: fim_nwm)
  FIM_S3_BUCKET: Optional[str] = None
  FIM_S3_PREFIX: str = ""  # e.g. "tcso/fim/"
  FIM_S3_LIVE_KEY: Optional[str] = None

Programmatic entry points
-------------------------
from datetime import datetime, timezone
from get_nwm_sr_forecast import (
    get_nwm_forecast_for_t0,
    get_latest_nwm_forecast,
    build_flowlines_with_forecast_columns,
    generate_all_fim_layers,
)

t0 = datetime(2025, 7, 8, 12, tzinfo=timezone.utc)
settings = PipelineSettings()  # auto from env

# 1) Forecast table (explicit t0)
df = get_nwm_forecast_for_t0(t0, settings)

# 2) Flowlines GDF with q_fXXX columns
gdf_wide = build_flowlines_with_forecast_columns(df, settings)

# 3) FIM polygons for all times (writes to configured targets)
generate_all_fim_layers(df, settings)

If run as __main__, it:
- auto-loads settings,
- grabs latest forecast,
- writes flowlines + FIM to configured outputs.
"""

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from sqlalchemy import create_engine
except ImportError:  # optional for pure GPKG mode
    create_engine = None  # type: ignore


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("NWM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("nwm_fim_pipeline")


# -----------------------------------------------------------------------------
# Settings (env-driven)
# -----------------------------------------------------------------------------

class NWMSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="NWM_", extra="ignore")

    BUCKET: str = "noaa-nwm-pds"
    PRODUCT: str = "short_range"
    VARIABLE: str = "streamflow"
    LOOKBACK_DAYS: int = 1


class FIMSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FIM_", extra="ignore")

    # Original big GPKG (for RatingCurves, FIM polygons)
    GPKG: str = "./Codes/TCSO_app/data/Theme4Data.gpkg"

    # NEW: lightweight AOI flowlines file (exported subset)
    FLOWLINES_GPKG: str = "./Codes/TCSO_app/data/p2fflowlines_tcso.gpkg"
    FLOWLINES_LAYER: str | None = None  # None = auto (single-layer gpkg)

    FLOWLINES_ID_FIELD: str = "feature_id"   # must exist in FLOWLINES_GPKG
    FLOWLINES_HYDRO_FIELD: str = "HydroID"   # used for FIM logic (if present)

    RATING_TABLE: str = "RatingCurves"
    FIM_LAYER: str = "TravisFIM"

    LAKE_FIELD: str = "LakeID"
    LAKE_MISSING: float = -999.0

    EXTRAPOLATE: bool = True


class OutputSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="OUT_", extra="ignore")

    MODE: str = "both"  # gpkg | postgis | both

    # GeoPackage outputs
    GPKG_PATH: str = "./output/nwm_fim.gpkg"
    FLOWLINES_LAYER: str = "nwm_sr_flowlines"
    FIM_LAYER_PREFIX: str = "fim_"  # fim_YYYYmmddHHMM

    # Which formats to emit for FIM snapshots (comma-separated)
    # Supported: "gpkg", "geojson"
    FIM_FORMATS: str = "gpkg,geojson"

    # PostGIS outputs
    PG_DSN: Optional[str] = None
    PG_SCHEMA: str = "public"
    PG_FLOWLINES_TABLE: str = "nwm_sr_flowlines"
    PG_FIM_TABLE: str = "fim_nwm"

    # S3 output for FIM (optional)
    # If set, per-snapshot files are uploaded as:
    #   s3://FIM_S3_BUCKET/FIM_S3_PREFIX/FIM_<PRODUCT>_<t0>_<time>.<ext>
    FIM_S3_BUCKET: Optional[str] = None
    FIM_S3_PREFIX: str = ""  # e.g. "tcso/fim/"

    # Optional "live" S3 object that always points to the latest snapshot,
    # e.g. "FIM_TCSO_live.geojson"
    FIM_S3_LIVE_KEY: Optional[str] = None

@dataclass
class PipelineSettings:
    nwm: NWMSettings
    fim: FIMSettings
    out: OutputSettings

    @classmethod
    def load(cls) -> "PipelineSettings":
        return cls(nwm=NWMSettings(), fim=FIMSettings(), out=OutputSettings())


# -----------------------------------------------------------------------------
# Core small helpers
# -----------------------------------------------------------------------------

CFS_PER_M3PS = 35.3146667


def _ensure_utc_hour(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def _s3_filesystem() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        anon=True,
        client_kwargs={"region_name": "us-east-1"},
    )


def _build_channel_rt_key(
    bucket: str,
    product: str,
    t0: datetime,
    lead_hour: int,
) -> str:
    date_str = t0.strftime("%Y%m%d")
    cycle = f"{t0.hour:02d}"
    lead = f"{lead_hour:03d}"
    fname = f"nwm.t{cycle}z.short_range.channel_rt.f{lead}.conus.nc"
    return f"{bucket}/nwm.{date_str}/{product}/{fname}"


def _load_streamflow_slice(
    fs: s3fs.S3FileSystem,
    key: str,
    variable: str,
    t0: datetime,
    lead_hour: int,
    comids: Optional[Sequence[int]] = None,
) -> Optional[pd.DataFrame]:
    if not fs.exists(key):
        logger.debug("Missing: s3://%s", key)
        return None

    try:
        # Open the object and load the dataset fully into memory
        with fs.open(key, "rb") as fobj:
            ds = xr.open_dataset(fobj, engine="h5netcdf")
            ds.load()  # <- critical: no lazy reads after fobj is closed
    except Exception as exc:
        logger.warning("Failed to open %s (%s)", key, exc)
        return None

    if variable not in ds:
        logger.warning("Variable '%s' not found in %s", variable, key)
        return None

    # time handling
    try:
        vt = pd.to_datetime(ds["time"].values[0]).tz_localize(timezone.utc)
    except Exception:
        vt = t0 + timedelta(hours=lead_hour)

    # subset
    if comids is not None:
        comids = [int(c) for c in comids]
        try:
            da = ds[variable].sel(feature_id=comids)
        except Exception:
            ds_ids = set(int(v) for v in ds["feature_id"].values)
            present = [c for c in comids if c in ds_ids]
            if not present:
                return None
            da = ds[variable].sel(feature_id=present)
        feat_ids = da["feature_id"].values.astype(int)
        vals = da.values.astype(float)
    else:
        feat_ids = ds["feature_id"].values.astype(int)
        vals = ds[variable].values.astype(float)

    df = pd.DataFrame(
        {
            "feature_id": feat_ids.astype(str),
            "streamflow": vals,
        }
    )
    df["forecast_run"] = t0
    df["forecast_time"] = vt
    df["lead_hour"] = int(lead_hour)

    return df


def _collect_forecasts_for_cycle(
    t0: datetime,
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> pd.DataFrame:
    fs = _s3_filesystem()
    t0 = _ensure_utc_hour(t0)

    if max_lead_hours < 1:
        raise ValueError("max_lead_hours must be >= 1")

    frames: List[pd.DataFrame] = []
    for lead in range(1, max_lead_hours + 1):
        key = _build_channel_rt_key(settings.nwm.BUCKET, settings.nwm.PRODUCT, t0, lead)
        df_slice = _load_streamflow_slice(
            fs=fs,
            key=key,
            variable=settings.nwm.VARIABLE,
            t0=t0,
            lead_hour=lead,
            comids=comids,
        )
        if df_slice is not None:
            frames.append(df_slice)

    if not frames:
        raise RuntimeError(f"No NWM short_range channel_rt files found for t0={t0.isoformat()}")

    df = pd.concat(frames, ignore_index=True)
    df["forecast_run"] = pd.to_datetime(df["forecast_run"], utc=True)
    df["forecast_time"] = pd.to_datetime(df["forecast_time"], utc=True)
    df["lead_hour"] = df["lead_hour"].astype(int)
    df["streamflow"] = pd.to_numeric(df["streamflow"], errors="coerce")
    df["feature_id"] = df["feature_id"].astype(str)

    return df.sort_values(["feature_id", "forecast_time"]).reset_index(drop=True)


def _find_latest_cycle(
    settings: PipelineSettings,
    max_lead_hours: int,
) -> datetime:
    fs = _s3_filesystem()
    today = datetime.utcnow().date()

    run_regex = re.compile(r"nwm\.t(\d{2})z\.short_range\.channel_rt\.f(\d{3})\.conus\.nc$")

    for delta in range(settings.nwm.LOOKBACK_DAYS):
        dt = today - timedelta(days=delta)
        prefix = f"{settings.nwm.BUCKET}/nwm.{dt:%Y%m%d}/{settings.nwm.PRODUCT}/"
        try:
            files = fs.find(prefix)
        except FileNotFoundError:
            continue
        except Exception as exc:
            logger.warning("Failed to list %s (%s)", prefix, exc)
            continue

        run_lead: Dict[Tuple[int, int], str] = {}
        for path in files:
            m = run_regex.search(os.path.basename(path))
            if not m:
                continue
            run_hr = int(m.group(1))
            lead_hr = int(m.group(2))
            if 1 <= lead_hr <= max_lead_hours:
                run_lead[(run_hr, lead_hr)] = path

        if not run_lead:
            continue

        latest_run = max(r for (r, _l) in run_lead.keys())
        t0 = datetime(dt.year, dt.month, dt.day, latest_run, tzinfo=timezone.utc)
        logger.info("Latest NWM SR cycle found: %s", t0.isoformat())
        return t0

    raise RuntimeError("No recent NWM SR cycles found in bucket.")


# -----------------------------------------------------------------------------
# Public API: Forecast retrieval
# -----------------------------------------------------------------------------

def get_nwm_forecast_for_t0(
    t0: datetime,
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> pd.DataFrame:
    """
    Forecast for explicit t0.

    Returns long format:
      feature_id, forecast_run(t0), forecast_time, lead_hour, streamflow
    """
    return _collect_forecasts_for_cycle(t0, settings, comids=comids, max_lead_hours=max_lead_hours)


def get_latest_nwm_forecast(
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> Tuple[datetime, pd.DataFrame]:
    """
    Forecast for latest available t0.

    Returns (t0, df_long).
    """
    t0 = _find_latest_cycle(settings, max_lead_hours=max_lead_hours)
    df = get_nwm_forecast_for_t0(t0, settings, comids=comids, max_lead_hours=max_lead_hours)
    return t0, df


# -----------------------------------------------------------------------------
# Flowlines: join & pivot
# -----------------------------------------------------------------------------

def load_flowlines(settings: PipelineSettings) -> gpd.GeoDataFrame:
    """
    Load AOI flowlines from the lightweight GPKG specified in FIMSettings.

    This file:
      - is small,
      - contains a 'feature_id' column (as per your export),
      - is used both as:
          * the NWM COMID filter, and
          * the geometry source for flowline outputs.
    """
    fim = settings.fim
    path = Path(fim.FLOWLINES_GPKG)
    if not path.exists():
        raise FileNotFoundError(f"AOI flowlines GPKG not found: {path}")

    if fim.FLOWLINES_LAYER:
        gdf = gpd.read_file(path, layer=fim.FLOWLINES_LAYER)
    else:
        # Auto: if single layer, this works; if multiple, user can set FLOWLINES_LAYER
        gdf = gpd.read_file(path)

    if fim.FLOWLINES_ID_FIELD not in gdf.columns:
        raise KeyError(
            f"'{fim.FLOWLINES_ID_FIELD}' column missing in AOI flowlines GPKG {path}"
        )

    gdf = gdf.copy()
    gdf["feature_id"] = gdf[fim.FLOWLINES_ID_FIELD].astype(str)

    # If HydroID missing here but needed for FIM, we try to derive it.
    if fim.FLOWLINES_HYDRO_FIELD not in gdf.columns:
        # attempt numeric from feature_id; if that fails, FIM stage will only work
        # where HydroID isn't strictly needed or comes from elsewhere.
        gdf[fim.FLOWLINES_HYDRO_FIELD] = pd.to_numeric(
            gdf["feature_id"], errors="coerce"
        )

    return gdf

def get_aoi_comids(settings: PipelineSettings) -> tuple[gpd.GeoDataFrame, list[int]]:
    """
    Load AOI flowlines once, return:
      - the AOI flowlines GeoDataFrame
      - list of COMIDs (feature_ids as int) to request from NWM

    This is your "only these features" filter.
    """
    flows = load_flowlines(settings)
    comids = (
        flows["feature_id"]
        .dropna()
        .astype(int)
        .drop_duplicates()
        .tolist()
    )
    return flows, comids



def build_flowlines_with_forecast_columns(
    df_forecast: pd.DataFrame,
    settings: PipelineSettings,
    flowlines: Optional[gpd.GeoDataFrame] = None,
) -> gpd.GeoDataFrame:
    """
    Create AOI flowlines GeoDataFrame with one column per lead_hour:
      q_f001, q_f002, ...

    flowlines:
      - optional pre-loaded AOI flowlines (to avoid rereading GPKG)
    """
    if flowlines is None:
        flowlines = load_flowlines(settings)

    # Use first t0 (should be homogeneous)
    t0s = df_forecast["forecast_run"].drop_duplicates()
    if len(t0s) > 1:
        logger.warning("Multiple t0 in forecast DF; using first: %s", t0s.iloc[0])
    t0 = t0s.iloc[0]

    pivot = (
        df_forecast
        .pivot_table(
            index="feature_id",
            columns="lead_hour",
            values="streamflow",
            aggfunc="mean",
        )
        .rename(columns=lambda h: f"q_f{int(h):03d}")
        .reset_index()
    )

    merged = flowlines.merge(pivot, on="feature_id", how="left")
    merged["t0"] = t0

    return merged



# -----------------------------------------------------------------------------
# FIM logic (adapted from fim_opensource2, no CLI)  :contentReference[oaicite:1]{index=1}
# -----------------------------------------------------------------------------

def _linear_interp_scalar(q, xs, ys, extrapolate=True):
    if len(xs) < 2:
        return np.nan
    if q < xs[0]:
        if not extrapolate:
            return np.nan
        x0, x1 = xs[0], xs[1]
        y0, y1 = ys[0], ys[1]
        return y0 + (q - x0) * (y1 - y0) / (x1 - x0)
    if q > xs[-1]:
        if not extrapolate:
            return np.nan
        x0, x1 = xs[-2], xs[-1]
        y0, y1 = ys[-2], ys[-1]
        return y0 + (q - x0) * (y1 - y0) / (x1 - x0)
    idx = np.searchsorted(xs, q)
    if xs[idx] == q:
        return ys[idx]
    x0, x1 = xs[idx - 1], xs[idx]
    y0, y1 = ys[idx - 1], ys[idx]
    return y0 + (q - x0) * (y1 - y0) / (x1 - x0)


def load_rating_curves(fim: FIMSettings) -> pd.DataFrame:
    import sqlite3

    with sqlite3.connect(fim.GPKG) as con:
        df = pd.read_sql(
            f"""
            SELECT
              CAST(HydroID AS INTEGER)   AS HydroID,
              CAST(discharge_ AS FLOAT)  AS discharge_,
              CAST(stage_m AS FLOAT)     AS stage_m
            FROM {fim.RATING_TABLE}
            """,
            con,
        )
    return df.dropna(subset=["HydroID", "discharge_", "stage_m"])


def compute_stage_for_flows(
    flows: pd.DataFrame,
    rc_df: pd.DataFrame,
    hydro_field: str,
    q_cfs_field: str,
    extrapolate: bool,
) -> pd.DataFrame:
    groups = {}
    for hid, sub in rc_df.groupby("HydroID", sort=False):
        s = sub.sort_values("discharge_")
        xs = s["discharge_"].to_numpy(dtype=float)
        ys = s["stage_m"].to_numpy(dtype=float)
        uniq, idx = np.unique(xs, return_index=True)
        groups[hid] = (uniq, ys[idx])

    H_vals, HIndex_vals = [], []
    for _, row in flows.iterrows():
        hid = row[hydro_field]
        q = row[q_cfs_field]
        if pd.isna(hid) or pd.isna(q) or hid not in groups:
            H_vals.append(np.nan)
            HIndex_vals.append(np.nan)
            continue
        xs, ys = groups[hid]
        h = _linear_interp_scalar(float(q), xs, ys, extrapolate=extrapolate)
        H_vals.append(h)
        HIndex_vals.append(int(round(h * 3.28) + 1) if pd.notna(h) else np.nan)

    out = flows.copy()
    out["H"] = H_vals
    out["HIndex"] = HIndex_vals
    return out


def read_fim_bounds(fim: FIMSettings) -> pd.DataFrame:
    fim_gdf = gpd.read_file(fim.GPKG, layer=fim.FIM_LAYER)[[fim.FLOWLINES_HYDRO_FIELD, "HIndex"]]
    fim_gdf[fim.FLOWLINES_HYDRO_FIELD] = pd.to_numeric(
        fim_gdf[fim.FLOWLINES_HYDRO_FIELD], errors="coerce"
    )
    fim_gdf["HIndex"] = pd.to_numeric(fim_gdf["HIndex"], errors="coerce")
    fim_gdf = fim_gdf.dropna(subset=[fim.FLOWLINES_HYDRO_FIELD, "HIndex"])
    fim_gdf["HIndex"] = fim_gdf["HIndex"].astype("Int64")
    bounds = (
        fim_gdf.groupby(fim.FLOWLINES_HYDRO_FIELD)["HIndex"]
        .agg(Hmin="min", Hmax="max")
        .reset_index()
    )
    return bounds


def select_polys_exact(
    fim: FIMSettings,
    pairs_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    fim_gdf = gpd.read_file(fim.GPKG, layer=fim.FIM_LAYER)
    hf = fim.FLOWLINES_HYDRO_FIELD

    fim_gdf[hf] = pd.to_numeric(fim_gdf[hf], errors="coerce")
    fim_gdf["HIndex"] = pd.to_numeric(fim_gdf["HIndex"], errors="coerce")
    fim_gdf = fim_gdf.dropna(subset=[hf, "HIndex"])
    fim_gdf["HIndex"] = fim_gdf["HIndex"].astype("Int64")

    pairs = pairs_df.copy()
    pairs[hf] = pd.to_numeric(pairs[hf], errors="coerce")
    pairs["HIndex"] = pd.to_numeric(pairs["HIndex"], errors="coerce")
    pairs = pairs.dropna(subset=[hf, "HIndex"]).drop_duplicates()
    pairs["HIndex"] = pairs["HIndex"].astype("Int64")

    key = [hf, "HIndex"]
    return fim_gdf.merge(pairs[key], on=key, how="inner")

def generate_fim_snapshot(
    df_time: pd.DataFrame,
    settings: PipelineSettings,
    t0: datetime,
    valid_time: pd.Timestamp,
    flowlines: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Build FIM polygons for ONE forecast snapshot (valid_time).

    Regular reaches (non-lake):
      - Use RatingCurves + Q_cfs from df_time to compute H/HIndex.
      - Clamp HIndex into [Hmin, Hmax] based on available FIM polygons.
      - Select polygons for those (HydroID, HIndex).

    Lake reaches:
      - Defined as flowlines rows with LakeID != LAKE_MISSING and a valid HydroID.
      - For each such HydroID (all sections of the lake), ALWAYS include
        the polygon with the LOWEST HIndex found in FIM polygons.
      - Does NOT depend on NWM having a flow value there.

    Requirements:
      - flowlines has: feature_id, HydroID, LakeID
      - FIM polygons have: HydroID, HIndex
    """
    fim = settings.fim
    hf = fim.FLOWLINES_HYDRO_FIELD
    lake_field = fim.LAKE_FIELD
    lake_missing = fim.LAKE_MISSING

    # ---------------------------
    # Load all FIM polygons once
    # ---------------------------
    fim_polys_all = gpd.read_file(fim.GPKG, layer=fim.FIM_LAYER)
    fim_polys_all = fim_polys_all.copy()
    fim_polys_all[hf] = pd.to_numeric(fim_polys_all[hf], errors="coerce")
    fim_polys_all["HIndex"] = pd.to_numeric(fim_polys_all["HIndex"], errors="coerce")
    fim_polys_all = fim_polys_all.dropna(subset=[hf, "HIndex"])
    fim_polys_all["HIndex"] = fim_polys_all["HIndex"].astype("Int64")

    if fim_polys_all.empty:
        logger.warning("FIM polygon layer is empty or invalid.")
        return gpd.GeoDataFrame(columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None)

    # Precompute Hmin/Hmax per HydroID (for clamping regular reaches)
    bounds = (
        fim_polys_all
        .groupby(hf)["HIndex"]
        .agg(Hmin="min", Hmax="max")
        .reset_index()
        .set_index(hf)
    )

    # ---------------------------
    # 1) REGULAR REACHES (have flows & RCs)
    # ---------------------------
    regular_pairs = pd.DataFrame(columns=[hf, "HIndex"])

    if not df_time.empty and "streamflow" in df_time.columns:
        # Attach HydroID to forecasted reaches
        if hf not in flowlines.columns:
            logger.warning("Flowlines missing '%s'; cannot map forecast to HydroID.", hf)
        else:
            fl_meta = flowlines[["feature_id", hf]].copy()
            sub = df_time.merge(fl_meta, on="feature_id", how="left")
            sub[hf] = pd.to_numeric(sub[hf], errors="coerce")
            sub = sub.dropna(subset=[hf, "streamflow"])

            if not sub.empty:
                # Convert to cfs
                sub["Q_cfs"] = sub["streamflow"].astype(float) * CFS_PER_M3PS

                # Load rating curves
                rc = load_rating_curves(fim)
                if rc.empty:
                    logger.warning("RatingCurves table empty; skipping RC-based FIM for this snapshot.")
                else:
                    # Compute H, HIndex from RC
                    sub2 = compute_stage_for_flows(
                        flows=sub,
                        rc_df=rc,
                        hydro_field=hf,
                        q_cfs_field="Q_cfs",
                        extrapolate=fim.EXTRAPOLATE,
                    )

                    # Join Hmin/Hmax and clamp
                    sub2 = sub2.join(bounds, on=hf, how="inner")
                    if not sub2.empty:
                        sub2["HIndex"] = np.where(
                            sub2["HIndex"].isna(),
                            sub2["Hmin"],
                            sub2["HIndex"],
                        )
                        sub2["HIndex"] = sub2[["HIndex", "Hmin"]].max(axis=1)
                        sub2["HIndex"] = sub2[["HIndex", "Hmax"]].min(axis=1)

                        regular_pairs = (
                            sub2[[hf, "HIndex"]]
                            .dropna()
                            .drop_duplicates()
                        )

    # ---------------------------
    # 2) LAKE REACHES (choose adjusted min HIndex per lake HydroID)
    # ---------------------------
    lake_pairs = pd.DataFrame(columns=[hf, "HIndex"])

    if lake_field in flowlines.columns:
        # All flowline segments that are part of a real lake
        lake_fl = flowlines[
            (flowlines[lake_field].notna()) &
            (flowlines[lake_field] != lake_missing)
        ].copy()

        if not lake_fl.empty:
            lake_fl[hf] = pd.to_numeric(lake_fl[hf], errors="coerce")
            lake_fl = lake_fl.dropna(subset=[hf])

            # All HydroIDs that belong to any lake (can be multiple sections)
            lake_hids = sorted(lake_fl[hf].drop_duplicates().tolist())

            if lake_hids:
                # Subset FIM polygons to only those HydroIDs
                fim_lake = fim_polys_all[fim_polys_all[hf].isin(lake_hids)].copy()
                # For each HydroID:
                #   - take the lowest HIndex in polygons -> hmin
                #   - candidate = hmin + 1
                #   - if candidate exists as polygon HIndex, use that;
                #     else fall back to hmin
                rows = []
                for hid, grp in fim_lake.groupby(hf):
                    hmin = int(grp["HIndex"].min())
                    hmax = int(grp["HIndex"].max())
                    candidate = min(hmin +4,hmax)
                    if (grp["HIndex"] == candidate).any():
                        chosen = candidate
                    else:
                        chosen = hmin
                    rows.append((hid, chosen))

                if rows:
                    lake_pairs = pd.DataFrame(rows, columns=[hf, "HIndex"]).drop_duplicates()

                # Debug for your specific HydroID
                debug_hid = 25050094
                if debug_hid in lake_hids:
                    present = bool((lake_pairs[hf] == debug_hid).any())
                    chosen_val = (
                        lake_pairs.loc[lake_pairs[hf] == debug_hid, "HIndex"].iloc[0]
                        if present else None
                    )
                    logger.info(
                        "Lake debug: HydroID=%s in lake_hids; in lake_pairs=%s, chosen_HIndex=%s",
                        debug_hid,
                        present,
                        chosen_val,
                    )
    # ---------------------------
    # 3) UNION regular + lake pairs
    # ---------------------------
    if regular_pairs.empty and lake_pairs.empty:
        return gpd.GeoDataFrame(columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None)

    all_pairs = (
        pd.concat([regular_pairs, lake_pairs], ignore_index=True)
        .drop_duplicates()
    )

    # ---------------------------
    # 4) Select polygons for those (HydroID, HIndex)
    # ---------------------------
    key = [hf, "HIndex"]
    fim_sel = fim_polys_all.merge(all_pairs[key], on=key, how="inner")

    if fim_sel.empty:
        return gpd.GeoDataFrame(columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None)

    fim_sel = gpd.GeoDataFrame(fim_sel, geometry="geometry", crs=fim_polys_all.crs)
    fim_sel["t0"] = _ensure_utc_hour(t0)
    fim_sel["valid_time"] = valid_time

    return fim_sel





# -----------------------------------------------------------------------------
# Output writers
# -----------------------------------------------------------------------------

def _ensure_gpkg(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def _ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def _parse_formats(out: OutputSettings) -> list[str]:
    return [f.strip().lower() for f in out.FIM_FORMATS.split(",") if f.strip()]


def _s3_filesystem_rw() -> s3fs.S3FileSystem:
    # Uses standard AWS_* or IAM role; anon=False so we can write
    return s3fs.S3FileSystem(anon=False)


def _upload_to_s3(local_path: Path, bucket: str, key: str):
    fs = _s3_filesystem_rw()
    s3_uri = f"{bucket.rstrip('/')}/{key.lstrip('/')}"
    with local_path.open("rb") as fsrc, fs.open(s3_uri, "wb") as fdst:
        fdst.write(fsrc.read())
    logger.info("Uploaded %s -> s3://%s", local_path, s3_uri)


def write_flowlines(
    gdf: gpd.GeoDataFrame,
    settings: PipelineSettings,
):
    out = settings.out
    fmts = _parse_formats(out)

    # Base dir from legacy GPKG_PATH
    base_dir = Path(out.GPKG_PATH).parent
    _ensure_dir(base_dir)

    # 1) GeoPackage (legacy behavior)
    if out.MODE in ("gpkg", "both") or "gpkg" in fmts:
        path = Path(out.GPKG_PATH)
        # overwrite layer cleanly
        if path.exists():
            try:
                gdf.to_file(path, layer=out.FLOWLINES_LAYER, driver="GPKG")
            except Exception:
                path.unlink()
                gdf.to_file(path, layer=out.FLOWLINES_LAYER, driver="GPKG")
        else:
            gdf.to_file(path, layer=out.FLOWLINES_LAYER, driver="GPKG")
        logger.info("Flowlines written to %s (layer=%s)", path, out.FLOWLINES_LAYER)

    # 2) Optional GeoJSON snapshot
    if "geojson" in fmts:
        gj_path = base_dir / "flowlines.geojson"
        gdf.to_file(gj_path, driver="GeoJSON")
        logger.info("Flowlines GeoJSON written to %s", gj_path)

        # Optional S3 upload (no special naming requested here)
        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/flowlines.geojson"
            _upload_to_s3(gj_path, out.FIM_S3_BUCKET, key)

    # 3) PostGIS (unchanged)
    if out.MODE in ("postgis", "both"):
        if not out.PG_DSN:
            raise RuntimeError("OUT_PG_DSN not set for PostGIS output mode.")
        if create_engine is None:
            raise RuntimeError("sqlalchemy not available for PostGIS output.")

        engine = create_engine(out.PG_DSN)
        gdf.to_postgis(
            name=out.PG_FLOWLINES_TABLE,
            con=engine,
            schema=out.PG_SCHEMA,
            if_exists="replace",
            index=False,
        )
        logger.info(
            "Flowlines written to PostGIS %s.%s",
            out.PG_SCHEMA,
            out.PG_FLOWLINES_TABLE,
        )


def write_fim(
    fim_gdf: gpd.GeoDataFrame,
    settings: PipelineSettings,
    time_suffix: str,
):
    """
    Write one FIM snapshot.

    Behavior:
      - Derives product, t0, valid_time from fim_gdf.
      - Writes per-time files named:
            FIM_<PRODUCT>_<t0:yyyyMMddHHmm>_<time:yyyyMMddHHmm>.gpkg
            FIM_<PRODUCT>_<t0:yyyyMMddHHmm>_<time:yyyyMMddHHmm>.geojson
        depending on OUT.FIM_FORMATS and OUT.MODE.
      - Optionally appends to PostGIS (history table).
      - Optionally uploads snapshot files to S3.
      - Optionally updates a fixed "live" GeoJSON at OUT.FIM_S3_LIVE_KEY.

    Note:
      - 'time_suffix' is kept for backward compatibility with any existing calls,
        but naming now comes from t0/valid_time inside fim_gdf.
    """
    out = settings.out

    if fim_gdf.empty:
        logger.info("No FIM polygons for %s", time_suffix)
        return

    # Ensure t0 / valid_time present
    if "t0" not in fim_gdf.columns or "valid_time" not in fim_gdf.columns:
        raise ValueError("fim_gdf must contain 't0' and 'valid_time' columns.")

    # Use first row (all rows in this snapshot share the same t0 & valid_time)
    t0 = pd.to_datetime(fim_gdf["t0"].iloc[0])
    vt = pd.to_datetime(fim_gdf["valid_time"].iloc[0])

    if t0.tzinfo is None:
        t0 = t0.replace(tzinfo=timezone.utc)
    else:
        t0 = t0.tz_convert(timezone.utc)

    if vt.tzinfo is None:
        vt = vt.replace(tzinfo=timezone.utc)
    else:
        vt = vt.tz_convert(timezone.utc)

    t0_str = t0.strftime("%Y%m%d%H%M")
    vt_str = vt.strftime("%Y%m%d%H%M")

    # Product tag from NWM settings
    product = getattr(settings.nwm, "PRODUCT", "short_range")
    product_tag = str(product).upper()

    basename = f"FIM_{product_tag}_{t0_str}_{vt_str}"

    fmts = _parse_formats(out)
    base_dir = Path(out.GPKG_PATH).parent
    _ensure_dir(base_dir)

    written_geojson: Optional[Path] = None

    # 1) GeoPackage output
    if out.MODE in ("gpkg", "both") or "gpkg" in fmts:
        gpkg_path = base_dir / f"{basename}.gpkg"
        # single-layer gpkg per snapshot
        fim_gdf.to_file(gpkg_path, layer="fim", driver="GPKG")
        logger.info("FIM snapshot written to %s", gpkg_path)

        # Upload to S3 if configured
        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/{gpkg_path.name}"
            _upload_to_s3(gpkg_path, out.FIM_S3_BUCKET, key)

    # 2) GeoJSON output
    if "geojson" in fmts:
        gj_path = base_dir / f"{basename}.geojson"
        fim_gdf.to_file(gj_path, driver="GeoJSON")
        written_geojson = gj_path
        logger.info("FIM snapshot GeoJSON written to %s", gj_path)

        # Upload snapshot to S3
        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/{gj_path.name}"
            _upload_to_s3(gj_path, out.FIM_S3_BUCKET, key)

    # 3) PostGIS history table (unchanged)
    if out.MODE in ("postgis", "both"):
        if not out.PG_DSN:
            raise RuntimeError("OUT_PG_DSN not set for PostGIS output mode.")
        if create_engine is None:
            raise RuntimeError("sqlalchemy not available for PostGIS output.")

        engine = create_engine(out.PG_DSN)
        fim_gdf.to_postgis(
            name=out.PG_FIM_TABLE,
            con=engine,
            schema=out.PG_SCHEMA,
            if_exists="append",
            index=False,
        )
        logger.info(
            "FIM polygons appended to PostGIS %s.%s for t0=%s, time=%s",
            out.PG_SCHEMA,
            out.PG_FIM_TABLE,
            t0_str,
            vt_str,
        )

    # 4) Live GeoJSON on S3 (fixed path), if requested
    #    Uses last written GeoJSON snapshot.
    if out.FIM_S3_BUCKET and out.FIM_S3_LIVE_KEY and written_geojson is not None:
        _upload_to_s3(written_geojson, out.FIM_S3_BUCKET, out.FIM_S3_LIVE_KEY)
        logger.info(
            "Updated live FIM GeoJSON at s3://%s/%s",
            out.FIM_S3_BUCKET,
            out.FIM_S3_LIVE_KEY,
        )



# -----------------------------------------------------------------------------
# Orchestrator: generate FIM for all times
# -----------------------------------------------------------------------------


def generate_all_fim_layers(
    df_forecast: pd.DataFrame,
    settings: PipelineSettings,
    flowlines: Optional[gpd.GeoDataFrame] = None,
):
    """
    Generate FIM polygons for each unique forecast_time in df_forecast.

    - Uses AOI flowlines (from p2fflowlines_tcso.gpkg via load_flowlines / get_aoi_comids)
    - Uses generate_fim_snapshot(...) for each time slice
    - Writes to outputs configured in OutputSettings (GPKG / PostGIS)
    - No legacy shapefile logic, no global 2M-feature processing.
    """
    if df_forecast.empty:
        logger.warning("No forecast data provided to generate_all_fim_layers.")
        return

    # Ensure we have AOI flowlines once
    if flowlines is None:
        flowlines = load_flowlines(settings)

    # Resolve t0 (reference time). We assume single-cycle input; if multiple, pick first.
    t0_vals = df_forecast["forecast_run"].dropna().drop_duplicates()
    if t0_vals.empty:
        raise ValueError("df_forecast must contain 'forecast_run' values.")
    if len(t0_vals) > 1:
        logger.warning(
            "Multiple forecast_run (t0) values in df_forecast; using first: %s",
            t0_vals.iloc[0],
        )
    t0 = t0_vals.iloc[0]

    # Loop by forecast_time with small per-time slices
    for valid_time, df_time in df_forecast.groupby("forecast_time", sort=True):
        fim_gdf = generate_fim_snapshot(
            df_time=df_time,
            settings=settings,
            t0=t0,
            valid_time=valid_time,
            flowlines=flowlines,
        )
        if fim_gdf.empty:
            continue

        suffix = valid_time.strftime("%Y%m%d%H%M")
        write_fim(fim_gdf, settings, time_suffix=suffix)

    logger.info("Finished generating FIM layers for all forecast_time values.")


# -----------------------------------------------------------------------------
# __main__: one-shot pipeline using latest forecast
# -----------------------------------------------------------------------------

def main():
    settings = PipelineSettings.load()
    logger.info("Loaded settings: %s", settings)

    # AOI mask + COMIDs from p2fflowlines_tcso.gpkg
    flowlines, comids = get_aoi_comids(settings)

    # NWM only for AOI
    t0, df = get_latest_nwm_forecast(
        settings=settings,
        comids=comids,
        max_lead_hours=1, # use 18 for the short range NWM, but can be limited it to the first forecast step
    )

    logger.info(
        "Using NWM SR forecast t0=%s; %d AOI records (all times)",
        t0.isoformat(),
        len(df),
    )

    # Flowlines with q_fXXX columns (AOI-only)
    gdf_flow = build_flowlines_with_forecast_columns(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )
    write_flowlines(gdf_flow, settings)

    # Time-sliced FIM (per valid_time, AOI-only)
    generate_all_fim_layers(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )

    logger.info("NWM SR → AOI Flowlines → FIM pipeline completed.")



if __name__ == "__main__":
    main()
