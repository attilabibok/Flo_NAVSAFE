#!/usr/bin/env python3
"""
fim.py

End-to-end NWM Short Range → AOI Flowlines → FIM pipeline.

This module provides:

1. Forecast retrieval
   - `get_nwm_forecast_for_t0(t0, settings, ...)`
   - `get_latest_nwm_forecast(settings, ...)`
   Fetches NWM short_range `channel_rt` streamflow for:
   - a specific reference time `t0`, or
   - the latest available cycle discovered in S3.

2. AOI flowlines with forecast attributes
   - `load_flowlines(settings)`
   - `get_aoi_comids(settings)`
   - `build_flowlines_with_forecast_columns(df_forecast, settings, flowlines)`
   Loads a lightweight AOI `p2fflowlines_tcso.gpkg` and:
   - filters NWM to only those COMIDs,
   - joins forecasts back to flowlines,
   - creates `q_fNNN` columns per lead hour (wide format).

3. FIM generation
   - `generate_fim_snapshot(...)`
   - `generate_all_fim_layers(df_forecast, settings, flowlines)`
   Builds Flood Inundation Map (FIM) polygons per forecast time using:
   - FIM polygons from `Theme4Data.gpkg`,
   - rating curves (`RatingCurves`),
   - AOI flowlines + NWM forecast flows.
   Handles special lake behavior via `LakeID` and `HydroID`.

4. Output writers
   - `write_flowlines(gdf, settings)`
   - `write_fim(fim_gdf, settings, time_suffix)`
   Write to:
   - GeoPackage,
   - GeoJSON,
   - PostGIS,
   - optional S3 snapshots + an optional "live" GeoJSON object.

Configuration (via env, using pydantic-settings)
-------------------------------------------------
NWM_*:
  NWM_BUCKET           (default: noaa-nwm-pds)
  NWM_PRODUCT          (default: short_range)
  NWM_VARIABLE         (default: streamflow)
  NWM_LOOKBACK_DAYS    (default: 1)

FIM_*:
  FIM_GPKG                 (default: ./Codes/TCSO_app/data/Theme4Data.gpkg)
  FIM_FLOWLINES_GPKG       (default: ./Codes/TCSO_app/data/p2fflowlines_tcso.gpkg)
  FIM_FLOWLINES_LAYER      (optional; auto if single-layer GPKG)
  FIM_FLOWLINES_ID_FIELD   (default: feature_id)
  FIM_FLOWLINES_HYDRO_FIELD(default: HydroID)
  FIM_RATING_TABLE         (default: RatingCurves)
  FIM_FIM_LAYER            (default: TravisFIM)
  FIM_LAKE_FIELD           (default: LakeID)
  FIM_LAKE_MISSING         (default: -999)
  FIM_EXTRAPOLATE          (default: true)

OUT_*:
  OUT_MODE                 (gpkg | postgis | both; default: both)

  # GeoPackage / file outputs
  OUT_GPKG_PATH            (default: ./output/nwm_fim.gpkg)
  OUT_FLOWLINES_LAYER      (default: nwm_sr_flowlines)
  OUT_FIM_LAYER_PREFIX     (default: fim_)
  OUT_FIM_FORMATS          ("gpkg,geojson" by default)

  # PostGIS outputs
  OUT_PG_DSN               (e.g. postgresql://user:pass@host:5432/dbname)
  OUT_PG_SCHEMA            (default: public)
  OUT_PG_FLOWLINES_TABLE   (default: nwm_sr_flowlines)
  OUT_PG_FIM_TABLE         (default: fim_nwm)

  # Optional S3 output
  OUT_FIM_S3_BUCKET        (optional)
  OUT_FIM_S3_PREFIX        (optional, e.g. tcso/fim)
  OUT_FIM_S3_LIVE_KEY      (optional fixed key, e.g. FIM_TCSO_live.geojson)

Usage (programmatic)
--------------------
    from datetime import datetime, timezone
    from fim import (
        PipelineSettings,
        get_nwm_forecast_for_t0,
        get_latest_nwm_forecast,
        build_flowlines_with_forecast_columns,
        generate_all_fim_layers,
    )

    settings = PipelineSettings.load()
    t0 = datetime(2025, 7, 8, 12, tzinfo=timezone.utc)

    df = get_nwm_forecast_for_t0(t0, settings)
    flowlines, _ = get_aoi_comids(settings)
    gdf = build_flowlines_with_forecast_columns(df, settings, flowlines)
    generate_all_fim_layers(df, settings, flowlines)

Usage (CLI-style)
-----------------
If run as a script (python fim.py), this will:

  - load env-based settings,
  - determine latest NWM SR forecast for the AOI,
  - write AOI flowlines with forecast attributes,
  - generate FIM polygons for all forecast times.

"""


import logging
import os
import re
import time
import io
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Iterable
from shapely.geometry import Polygon, MultiPolygon


import geopandas as gpd
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from sqlalchemy import TIMESTAMP, String, create_engine, text
except ImportError:  # Optional for pure file-based mode
    create_engine = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("NWM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger("nwm_fim_pipeline")


# -----------------------------------------------------------------------------
# Settings (env-driven)
# -----------------------------------------------------------------------------

class NWMSettings(BaseSettings):
    """Settings for accessing NWM data on S3."""

    model_config = SettingsConfigDict(env_prefix="NWM_", extra="ignore")

    BUCKET: str = "noaa-nwm-pds"
    PRODUCT: str = "short_range"
    VARIABLE: str = "streamflow"
    LOOKBACK_DAYS: int = 1


class FIMSettings(BaseSettings):
    """Settings describing FIM inputs: flowlines, rating curves, FIM grid, etc."""

    model_config = SettingsConfigDict(env_prefix="FIM_", extra="ignore")

    # Source GPKG with FIM polygons + rating curves
    GPKG: str = "./Codes/TCSO_app/data/Theme4Data.gpkg"

    # AOI flowlines subset (small, pre-filtered)
    FLOWLINES_GPKG: str = "./Codes/TCSO_app/data/p2fflowlines_tcso.gpkg"
    FLOWLINES_LAYER: Optional[str] = None  # None => auto-detect if single-layer

    # Identifiers
    FLOWLINES_ID_FIELD: str = "feature_id"      # matches NWM feature_id
    FLOWLINES_HYDRO_FIELD: str = "HydroID"      # link to FIM polygons / rating curves

    # Rating curves + FIM polygons
    RATING_TABLE: str = "RatingCurves"
    FIM_LAYER: str = "TravisFIM"

    # Lake handling
    LAKE_FIELD: str = "LakeID"
    LAKE_MISSING: float = -999.0

    # Allow extrapolation beyond rating curve range
    EXTRAPOLATE: bool = True


class OutputSettings(BaseSettings):
    """Output configuration for flowlines and FIM products."""

    model_config = SettingsConfigDict(env_prefix="OUT_", extra="ignore")

    # Mode selection
    MODE: str = "both"  # "gpkg", "postgis", or "both"

    # File outputs (base)
    GPKG_PATH: str = "./output/nwm_fim.gpkg"
    FLOWLINES_LAYER: str = "nwm_sr_flowlines"
    FIM_LAYER_PREFIX: str = "fim_"  # kept for backwards compatibility

    # Which formats to emit for each FIM snapshot ("gpkg", "geojson", or both)
    FIM_FORMATS: str = "gpkg,geojson"

    # PostGIS outputs
    PG_DSN: Optional[str] = None
    PG_SCHEMA: str = "public"
    PG_FLOWLINES_TABLE: str = "nwm_sr_flowlines"
    PG_FIM_TABLE: str = "fim_nwm"

    # Optional S3 outputs for per-snapshot files
    FIM_S3_BUCKET: Optional[str] = None
    FIM_S3_PREFIX: str = ""  # e.g. "tcso/fim/"

    # Optional S3 key for a "live" GeoJSON pointing to latest snapshot
    FIM_S3_LIVE_KEY: Optional[str] = None


@dataclass
class PipelineSettings:
    """Bundle of all configuration groups."""

    nwm: NWMSettings
    fim: FIMSettings
    out: OutputSettings

    @classmethod
    def load(cls) -> "PipelineSettings":
        """
        Load settings from environment variables.

        Returns
        -------
        PipelineSettings
            Fully-populated settings object.
        """
        return cls(nwm=NWMSettings(), fim=FIMSettings(), out=OutputSettings())


# -----------------------------------------------------------------------------
# Core helpers
# -----------------------------------------------------------------------------

CFS_PER_M3PS: float = 35.3146667


def _ensure_utc_hour(dt: datetime) -> datetime:
    """
    Normalize a datetime to UTC on the hour.

    Parameters
    ----------
    dt : datetime
        Datetime to normalize.

    Returns
    -------
    datetime
        UTC-aware datetime with minute/second/microsecond set to 0.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def _s3_filesystem() -> s3fs.S3FileSystem:
    """
    Create an anonymous S3 filesystem client for reading NWM data.

    Returns
    -------
    s3fs.S3FileSystem
    """
    return s3fs.S3FileSystem(
        anon=True,
        client_kwargs={"region_name": "us-east-1"},
        config_kwargs={"signature_version": "unsigned"},
    )

def _build_channel_rt_key(
    bucket: str,
    product: str,
    t0: datetime,
    lead_hour: int,
) -> str:
    """
    Build the S3 key for a `channel_rt` NWM short_range file.

    Parameters
    ----------
    bucket : str
        NWM bucket name. May be given as "noaa-nwm-pds" or "s3://noaa-nwm-pds"
        with or without trailing slashes; it will be normalized.
    product : str
        NWM product (e.g. "short_range").
    t0 : datetime
        Forecast cycle (UTC).
    lead_hour : int
        Lead time in hours.

    Returns
    -------
    str
        S3 path in the form "<bucket>/nwm.YYYYMMDD/<product>/nwm.tHHz.short_range.channel_rt.fFFF.conus.nc",
        suitable for direct use with s3fs (fs.open).
    """
    # Normalize bucket to a plain name: "noaa-nwm-pds"
    bucket = bucket.replace("s3://", "").strip().strip("/")

    date_str = t0.strftime("%Y%m%d")
    cycle = f"{t0.hour:02d}"
    lead = f"{lead_hour:03d}"
    fname = f"nwm.t{cycle}z.short_range.channel_rt.f{lead}.conus.nc"

    # This is exactly compatible with:
    #   aws s3api head-object --bucket <bucket> --key nwm.YYYYMMDD/<product>/<fname>
    return f"{bucket}/nwm.{date_str}/{product}/{fname}"


def _ensure_multipolygon(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure all geometries are MultiPolygon.

    - Polygon -> MultiPolygon([Polygon])
    - MultiPolygon -> unchanged
    - None/empty -> unchanged
    """
    if gdf.empty:
        return gdf

    df = gdf.copy()

    def _to_multi(geom):
        if geom is None:
            return None
        if isinstance(geom, MultiPolygon):
            return geom
        if isinstance(geom, Polygon):
            return MultiPolygon([geom])
        # If anything weird sneaks in (LineString, etc.), keep it as-is so it fails loudly.
        return geom

    df["geometry"] = df["geometry"].map(_to_multi)
    return df


def _load_streamflow_slice(
    fs: s3fs.S3FileSystem,
    key: str,
    variable: str,
    t0: datetime,
    lead_hour: int,
    comids: Optional[Sequence[int]] = None,
) -> Optional[pd.DataFrame]:
    """
    Load a single forecast lead slice from NWM channel_rt.

    Parameters
    ----------
    fs : s3fs.S3FileSystem
        S3 filesystem (anonymous).
    key : str
        S3 key to the NetCDF file.
    variable : str
        Name of the streamflow variable (e.g. "streamflow").
    t0 : datetime
        Forecast reference time.
    lead_hour : int
        Lead hour for this slice.
    comids : Sequence[int], optional
        Optional list of COMIDs to subset.

    Returns
    -------
    Optional[pd.DataFrame]
        DataFrame with columns:
        [feature_id, streamflow, forecast_run, forecast_time, lead_hour],
        or None if file/variable/subset could not be read.
    """
    

    bucket, path = (key.split("/", 1) + [""])[:2]
    s3_url = f"s3://{bucket}/{path}" if path else f"s3://{bucket}"
    http_url = (
        f"https://{bucket}.s3.amazonaws.com/{path}" if path else
        f"https://{bucket}.s3.amazonaws.com"
    )

    ds = None
    # --- 1) Try S3 via s3fs ---
    try:
        with fs.open(key, "rb") as fobj:
            ds = xr.open_dataset(fobj, engine="h5netcdf")
            ds.load()  # force read while handle is open
        logger.debug(
            "Loaded NWM slice via S3 for t0=%s lead=%03d key=%s",
            t0.isoformat(),
            lead_hour,
            s3_url,
        )
    except FileNotFoundError:
        logger.warning(
            "NWM file missing via S3 for t0=%s lead=%03d key=%s; "
            "will try HTTPS fallback %s",
            t0.isoformat(),
            lead_hour,
            s3_url,
            http_url,
        )
    except Exception as exc:
        logger.warning(
            "Error reading NWM file via S3 for t0=%s lead=%03d key=%s: %r; "
            "will try HTTPS fallback %s",
            t0.isoformat(),
            lead_hour,
            s3_url,
            exc,
            http_url,
        )

    # --- 2) HTTPS fallback if S3 failed ---
    if ds is None:
        try:
            resp = requests.get(http_url, stream=True, timeout=60)
            if resp.status_code == 404:
                logger.warning(
                    "NWM file missing via HTTPS as well for t0=%s lead=%03d url=%s",
                    t0.isoformat(),
                    lead_hour,
                    http_url,
                )
                return None
            resp.raise_for_status()

            bio = io.BytesIO(resp.content)
            ds = xr.open_dataset(bio, engine="h5netcdf")
            ds.load()

            logger.info(
                "Loaded NWM slice via HTTPS fallback for t0=%s lead=%03d url=%s",
                t0.isoformat(),
                lead_hour,
                http_url,
            )
        except Exception as exc:
            logger.warning(
                "HTTPS fallback failed for t0=%s lead=%03d url=%s: %r",
                t0.isoformat(),
                lead_hour,
                http_url,
                exc,
            )
            return None


    if variable not in ds:
        logger.warning(
            "Variable '%s' not found in %s for t0=%s lead=%03d",
            variable,
            s3_url,
            t0.isoformat(),
            lead_hour,
        )
        try:
            ds.close()
        except Exception:
            pass
        return None

    # Derive valid time
    try:
        vt = pd.to_datetime(ds["time"].values[0]).tz_localize(timezone.utc)
    except Exception:  # pragma: no cover - fallback path
        vt = t0 + timedelta(hours=lead_hour)
        logger.debug(
            "Using fallback valid_time for t0=%s lead=%03d -> %s",
            t0.isoformat(),
            lead_hour,
            vt.isoformat(),
        )

    # Subset by COMID if requested
    try:
        if comids is not None:
            comids_int = [int(c) for c in comids]
            try:
                da = ds[variable].sel(feature_id=comids_int)
            except Exception:
                ds_ids = set(int(v) for v in ds["feature_id"].values)
                present = [c for c in comids_int if c in ds_ids]
                if not present:
                    logger.warning(
                        "No requested COMIDs present for t0=%s lead=%03d key=%s",
                        t0.isoformat(),
                        lead_hour,
                        s3_url,
                    )
                    return None
                da = ds[variable].sel(feature_id=present)

            feat_ids = da["feature_id"].values.astype(int)
            vals = da.values.astype(float)
        else:
            feat_ids = ds["feature_id"].values.astype(int)
            vals = ds[variable].values.astype(float)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Failed to subset NWM file for t0=%s lead=%03d key=%s (%s)",
            t0.isoformat(),
            lead_hour,
            s3_url,
            exc,
        )
        return None
    finally:
        try:
            ds.close()
        except Exception:
            pass

    df = pd.DataFrame(
        {
            "feature_id": feat_ids.astype(str),
            "streamflow": vals,
        }
    )
    df["forecast_run"] = t0
    df["forecast_time"] = vt
    df["lead_hour"] = int(lead_hour)

    logger.info(
        "Loaded NWM slice: t0=%s lead=%03d key=%s rows=%d",
        t0.isoformat(),
        lead_hour,
        s3_url,
        len(df),
    )

    return df

def _add_fim_primary_key(fim_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add deterministic 'id' column for FIM rows.

    Pattern:
        <HydroID>_<t0 UTC: YYYYMMDDHHMMZ>_<valid_time UTC: YYYYMMDDHHMMZ>

    The timestamps are normalized to UTC before formatting so the same instant
    always yields the same key, independent of source timezone.

    Parameters
    ----------
    fim_gdf :
        GeoDataFrame with columns 'HydroID', 't0', 'valid_time'.

    Returns
    -------
    geopandas.GeoDataFrame
        Copy of input with an additional 'id' column.

    Raises
    ------
    KeyError
        If required columns are missing.
    """
    required = {"HydroID", "t0", "valid_time"}
    missing = required.difference(fim_gdf.columns)
    if missing:
        raise KeyError(f"FIM GeoDataFrame missing required columns: {sorted(missing)}")

    if fim_gdf.empty:
        return fim_gdf

    df = fim_gdf.copy()

    # Normalize to UTC and format
    t0_utc = pd.to_datetime(df["t0"], utc=True)
    vt_utc = pd.to_datetime(df["valid_time"], utc=True)

    df["id"] = (
        df["HydroID"].astype(str)
        + "_"
        + t0_utc.dt.strftime("%Y%m%d%H%MZ")
        + "_"
        + vt_utc.dt.strftime("%Y%m%d%H%MZ")
    )

    return df

def _add_flowlines_primary_key(flow_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add deterministic 'id' column for AOI flowlines with forecast attributes.

    Pattern
    -------
    <HydroID>_<t0 UTC: YYYYMMDDHHMMZ>

    Assumptions
    -----------
    - Each row represents one river segment for a single NWM forecast cycle.
    - Column 'HydroID' is present and uniquely identifies the segment geometry.
    - Column 't0' is present and is the forecast cycle time (timestamptz or parseable).

    Parameters
    ----------
    flow_gdf:
        GeoDataFrame containing at least 'HydroID' and 't0'.

    Returns
    -------
    geopandas.GeoDataFrame
        Copy of the input with a populated 'id' column.
    """
    required = {"HydroID", "t0"}
    missing = required.difference(flow_gdf.columns)
    if missing:
        raise KeyError(
            f"Flowlines GeoDataFrame missing required columns: {sorted(missing)}"
        )

    if flow_gdf.empty:
        return flow_gdf

    df = flow_gdf.copy()

    # Normalize to UTC to avoid timezone-dependent IDs
    t0_utc = pd.to_datetime(df["t0"], utc=True)

    df["id"] = (
        df["HydroID"].astype(str)
        + "_"
        + t0_utc.dt.strftime("%Y%m%d%H%MZ")
    )

    return df


def _refresh_matviews(
    dsn: str,
    schema: str,
    views: Iterable[str],
) -> None:
    """
    Internal utility to refresh one or more PostGIS materialized views.

    Parameters
    ----------
    dsn : str
        SQLAlchemy/PostgreSQL connection string.
    schema : str
        Target schema for the materialized views.
    views : Iterable[str]
        Materialized view names (unqualified). Each will be referenced as
        `<schema>.<view>`.

    Notes
    -----
    - Uses plain `REFRESH MATERIALIZED VIEW` (non-concurrent) for simplicity
      and portability within a transaction block.
    - Safe to call repeatedly; if a view does not exist, it is skipped.
    """
    if create_engine is None:
        logger.debug("sqlalchemy not available; skipping materialized view refresh.")
        return

    engine = create_engine(dsn)

    with engine.begin() as conn:
        for view in views:
            fq_name = f"{schema}.{view}"
            exists = conn.execute(
                text("SELECT to_regclass(:name)"),
                {"name": fq_name},
            ).scalar()

            if not exists:
                logger.info(
                    "Materialized view %s not found; skipping refresh.", fq_name
                )
                continue

            logger.info("Refreshing materialized view %s", fq_name)
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {fq_name}"))

def _collect_forecasts_for_cycle(
    t0: datetime,
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> pd.DataFrame:
    """
    Collect all forecast lead slices for a given cycle `t0`.

    Retries up to NWM_RETRIES times (default: 5) with NWM_RETRY_WAIT seconds
    (default: 30) between attempts if no slices are successfully loaded.
    This is to handle the case where the t0 directory exists but not all
    channel_rt files have been uploaded yet.

    Parameters
    ----------
    t0 : datetime
        Forecast reference time (UTC).
    settings : PipelineSettings
        Pipeline configuration.
    comids : Sequence[int], optional
        Optional AOI COMID filter.
    max_lead_hours : int
        Number of lead hours to fetch (>= 1).

    Raises
    ------
    RuntimeError
        If no slices could be loaded after all retry attempts.

    Returns
    -------
    pd.DataFrame
        Long-format forecast table.
    """
    if max_lead_hours < 1:
        raise ValueError("max_lead_hours must be >= 1")

    fs = _s3_filesystem()
    t0 = _ensure_utc_hour(t0)

    max_retries = int(os.getenv("NWM_RETRIES", "5"))
    retry_wait = int(os.getenv("NWM_RETRY_WAIT", "30"))

    last_debug_status: List[str] = []

    for attempt in range(1, max_retries + 1):
        frames: List[pd.DataFrame] = []
        debug_status: List[str] = []

        for lead in range(1, max_lead_hours + 1):
            key = _build_channel_rt_key(
                settings.nwm.BUCKET,
                settings.nwm.PRODUCT,
                t0,
                lead,
            )
            s3_url = f"s3://{key}"

            df_slice = _load_streamflow_slice(
                fs=fs,
                key=key,
                variable=settings.nwm.VARIABLE,
                t0=t0,
                lead_hour=lead,
                comids=comids,
            )

            if df_slice is not None and not df_slice.empty:
                frames.append(df_slice)
                debug_status.append(
                    f"lead={lead:03d} key={s3_url} -> {len(df_slice)} rows"
                )
            else:
                debug_status.append(
                    f"lead={lead:03d} key={s3_url} -> no data (missing/failed/empty)"
                )

        # If we got any data, stop retrying
        if frames:
            last_debug_status = debug_status
            break

        last_debug_status = debug_status

        if attempt < max_retries:
            summary = "\n  ".join(debug_status) if debug_status else "no attempts"
            logger.warning(
                "No NWM short_range channel_rt slices loaded for t0=%s on attempt %d/%d.\n"
                "Attempt summary:\n  %s\n"
                "Retrying in %d seconds...",
                t0.isoformat(),
                attempt,
                max_retries,
                summary,
                retry_wait,
            )
            time.sleep(retry_wait)
        else:
            # Final attempt failed; fall through to error below
            break

    if not frames:
        debug_text = "\n  ".join(last_debug_status) if last_debug_status else "no attempts"
        logger.error(
            "No NWM short_range channel_rt slices loaded for t0=%s after %d attempt(s).\n"
            "Final attempt summary:\n  %s",
            t0.isoformat(),
            max_retries,
            debug_text,
        )
        raise RuntimeError(
            f"No NWM short_range channel_rt files found for t0={t0.isoformat()}"
        )

    df = pd.concat(frames, ignore_index=True)
    df["forecast_run"] = pd.to_datetime(df["forecast_run"], utc=True)
    df["forecast_time"] = pd.to_datetime(df["forecast_time"], utc=True)
    df["lead_hour"] = df["lead_hour"].astype(int)
    df["streamflow"] = pd.to_numeric(df["streamflow"], errors="coerce")
    df["feature_id"] = df["feature_id"].astype(str)

    logger.info(
        "Collected NWM forecast for t0=%s after %d attempt(s): %d slices, %d total rows",
        t0.isoformat(),
        attempt,
        len(frames),
        len(df),
    )

    return df.sort_values(["feature_id", "forecast_time"]).reset_index(drop=True)

def _dedupe_on_id(gdf: gpd.GeoDataFrame, label: str) -> gpd.GeoDataFrame:
    """
    Ensure uniqueness of the 'id' column by dropping duplicate ids.

    Behavior
    --------
    - If 'id' is not present or frame is empty: return as-is.
    - If duplicates exist:
        * Log a warning with counts.
        * Keep the first row per 'id'.
        * Return a copy with unique ids.

    Parameters
    ----------
    gdf:
        Input GeoDataFrame.
    label:
        Short label used in log messages (e.g. 'fim_nwm', 'nwm_sr_flowlines').

    Returns
    -------
    geopandas.GeoDataFrame
        DataFrame with unique 'id' values.
    """
    if gdf.empty or "id" not in gdf.columns:
        return gdf

    dup_mask = gdf.duplicated(subset="id", keep=False)
    if not dup_mask.any():
        return gdf

    dup_rows = int(dup_mask.sum())
    dup_ids = int(gdf.loc[dup_mask, "id"].nunique())

    logger.warning(
        "%s: found %d duplicate id values across %d rows; "
        "keeping first occurrence per id and dropping the rest.",
        label,
        dup_ids,
        dup_rows,
    )

    # Keep the first occurrence for each id
    deduped = gdf.drop_duplicates(subset="id", keep="first").copy()
    logger.info(
        "%s: deduplicated on id; %d -> %d rows.",
        label,
        len(gdf),
        len(deduped),
    )
    return deduped


def _find_latest_cycle(
    settings: PipelineSettings,
    max_lead_hours: int,
) -> datetime:
    """
    Discover the latest NWM SR forecast cycle (t0) in the bucket.

    Looks back up to `settings.nwm.LOOKBACK_DAYS` through the `short_range`
    directory structure and finds the most recent cycle that has at least one
    `channel_rt` file within the requested lead hours.

    Parameters
    ----------
    settings : PipelineSettings
        Pipeline configuration.
    max_lead_hours : int
        Maximum lead hour expected.

    Returns
    -------
    datetime
        Latest cycle time (UTC).

    Raises
    ------
    RuntimeError
        If no suitable cycle is found.
    """
    fs = _s3_filesystem()
    today = datetime.utcnow().date()
    run_regex = re.compile(
        r"nwm\.t(\d{2})z\.short_range\.channel_rt\.f(\d{3})\.conus\.nc$"
    )

    for delta in range(settings.nwm.LOOKBACK_DAYS):
        dt = today - timedelta(days=delta)
        prefix = f"{settings.nwm.BUCKET}/nwm.{dt:%Y%m%d}/{settings.nwm.PRODUCT}/"

        try:
            files = fs.find(prefix)
        except FileNotFoundError:
            continue
        except Exception as exc:  # pragma: no cover - robustness
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

def find_latest_t0(
    settings: PipelineSettings,
    max_lead_hours: int = 18,
) -> datetime:
    """
    Return the latest available NWM SR cycle (t0) based on S3 listing only.

    This is intentionally lightweight: it does NOT load any channel_rt data.
    Use this in polling loops to decide whether a new cycle exists before
    triggering the expensive forecast/FIM pipeline.

    Parameters
    ----------
    settings : PipelineSettings
        Pipeline configuration.
    max_lead_hours : int
        Maximum lead hour to consider when detecting candidate cycles.

    Returns
    -------
    datetime
        Latest detected cycle time (UTC).

    Raises
    ------
    RuntimeError
        If no suitable cycle is found.
    """
    return _find_latest_cycle(settings, max_lead_hours=max_lead_hours)



def get_nwm_forecast_for_t0(
    t0: datetime,
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> pd.DataFrame:
    """
    Retrieve NWM SR forecast for an explicit reference time `t0`.

    Parameters
    ----------
    t0 : datetime
        Forecast reference time (UTC).
    settings : PipelineSettings
        Pipeline configuration.
    comids : Sequence[int], optional
        Optional AOI COMID filter.
    max_lead_hours : int
        Number of lead hours to fetch.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with one row per (feature_id, lead_hour).
    """
    return _collect_forecasts_for_cycle(
        t0, settings, comids=comids, max_lead_hours=max_lead_hours
    )


def get_latest_nwm_forecast(
    settings: PipelineSettings,
    comids: Optional[Sequence[int]] = None,
    max_lead_hours: int = 10,
) -> Tuple[datetime, pd.DataFrame]:
    """
    Retrieve NWM SR forecast for the latest available cycle.

    Parameters
    ----------
    settings : PipelineSettings
        Pipeline configuration.
    comids : Sequence[int], optional
        Optional AOI COMID filter.
    max_lead_hours : int
        Number of lead hours to fetch.

    Returns
    -------
    (datetime, pd.DataFrame)
        Tuple of (t0, long-format forecast DataFrame).
    """
    t0 = _find_latest_cycle(settings, max_lead_hours=max_lead_hours)
    df = get_nwm_forecast_for_t0(
        t0, settings, comids=comids, max_lead_hours=max_lead_hours
    )
    return t0, df


# -----------------------------------------------------------------------------
# Flowlines: load, filter, and join forecasts
# -----------------------------------------------------------------------------

def load_flowlines(settings: PipelineSettings) -> gpd.GeoDataFrame:
    """
    Load AOI flowlines from the configured lightweight GPKG.

    Returns a GeoDataFrame with at minimum:
      - 'feature_id' column (string),
      - geometry column,
      - 'HydroID' if available (for FIM linkage).

    Parameters
    ----------
    settings : PipelineSettings
        Pipeline configuration.

    Returns
    -------
    geopandas.GeoDataFrame

    Raises
    ------
    FileNotFoundError
        If the AOI flowlines GPKG is missing.
    KeyError
        If the configured ID field is not present.
    """
    fim = settings.fim
    path = Path(fim.FLOWLINES_GPKG)
    if not path.exists():
        raise FileNotFoundError(f"AOI flowlines GPKG not found: {path}")

    if fim.FLOWLINES_LAYER:
        gdf = gpd.read_file(path, layer=fim.FLOWLINES_LAYER)
    else:
        gdf = gpd.read_file(path)

    if fim.FLOWLINES_ID_FIELD not in gdf.columns:
        raise KeyError(
            f"'{fim.FLOWLINES_ID_FIELD}' column missing in AOI flowlines GPKG {path}"
        )

    gdf = gdf.copy()
    gdf["feature_id"] = gdf[fim.FLOWLINES_ID_FIELD].astype(str)

    if fim.FLOWLINES_HYDRO_FIELD not in gdf.columns:
        # Best-effort: derive numeric HydroID from feature_id if needed.
        gdf[fim.FLOWLINES_HYDRO_FIELD] = pd.to_numeric(
            gdf["feature_id"], errors="coerce"
        )

    return gdf


def get_aoi_comids(settings: PipelineSettings) -> Tuple[gpd.GeoDataFrame, List[int]]:
    """
    Load AOI flowlines and return both the GeoDataFrame and the COMID filter.

    Parameters
    ----------
    settings : PipelineSettings
        Pipeline configuration.

    Returns
    -------
    (geopandas.GeoDataFrame, list[int])
        Flowlines GeoDataFrame and list of COMIDs (as ints).
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
    Merge forecast data into AOI flowlines as wide-format q_fNNN columns.

    Parameters
    ----------
    df_forecast : pd.DataFrame
        Long-format forecast table.
    settings : PipelineSettings
        Pipeline configuration.
    flowlines : geopandas.GeoDataFrame, optional
        Pre-loaded flowlines; if not provided, they are loaded.

    Returns
    -------
    geopandas.GeoDataFrame
        AOI flowlines with q_fNNN columns and t0 column.
    """
    if flowlines is None:
        flowlines = load_flowlines(settings)

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
# FIM logic
# -----------------------------------------------------------------------------

def _linear_interp_scalar(
    q: float,
    xs: np.ndarray,
    ys: np.ndarray,
    extrapolate: bool = True,
) -> float:
    """
    1D linear interpolation / extrapolation helper.

    Parameters
    ----------
    q : float
        Query discharge value.
    xs : np.ndarray
        Sorted discharge breakpoints.
    ys : np.ndarray
        Corresponding stage values.
    extrapolate : bool
        Whether to extrapolate beyond [xs[0], xs[-1]].

    Returns
    -------
    float
        Interpolated stage (or NaN if not computable).
    """
    if len(xs) < 2:
        return float("nan")

    if q < xs[0]:
        if not extrapolate:
            return float("nan")
        x0, x1 = xs[0], xs[1]
        y0, y1 = ys[0], ys[1]
        return float(y0 + (q - x0) * (y1 - y0) / (x1 - x0))

    if q > xs[-1]:
        if not extrapolate:
            return float("nan")
        x0, x1 = xs[-2], xs[-1]
        y0, y1 = ys[-2], ys[-1]
        return float(y0 + (q - x0) * (y1 - y0) / (x1 - x0))

    idx = int(np.searchsorted(xs, q))
    if xs[idx] == q:
        return float(ys[idx])

    x0, x1 = xs[idx - 1], xs[idx]
    y0, y1 = ys[idx - 1], ys[idx]
    return float(y0 + (q - x0) * (y1 - y0) / (x1 - x0))


def load_rating_curves(fim: FIMSettings) -> pd.DataFrame:
    """
    Load rating curves from the GPKG's RatingCurves table.

    Parameters
    ----------
    fim : FIMSettings

    Returns
    -------
    pd.DataFrame
        Columns: HydroID, discharge_, stage_m
    """
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
    """
    Compute stage (H) and HIndex for each flow using rating curves.

    Parameters
    ----------
    flows : pd.DataFrame
        Forecast flows with HydroID and Q in cfs.
    rc_df : pd.DataFrame
        Rating curves (HydroID, discharge_, stage_m).
    hydro_field : str
        Name of HydroID field in `flows`.
    q_cfs_field : str
        Name of discharge field in `flows` (cfs).
    extrapolate : bool
        Whether to extrapolate beyond rating curve range.

    Returns
    -------
    pd.DataFrame
        Input flows + columns H and HIndex.
    """
    groups: Dict[int, Tuple[np.ndarray, np.ndarray]] = {}
    for hid, sub in rc_df.groupby("HydroID", sort=False):
        s = sub.sort_values("discharge_")
        xs = s["discharge_"].to_numpy(dtype=float)
        ys = s["stage_m"].to_numpy(dtype=float)
        uniq, idx = np.unique(xs, return_index=True)
        groups[int(hid)] = (uniq, ys[idx])

    H_vals: List[float] = []
    HIndex_vals: List[float] = []

    for _, row in flows.iterrows():
        hid = row[hydro_field]
        q = row[q_cfs_field]
        if pd.isna(hid) or pd.isna(q):
            H_vals.append(float("nan"))
            HIndex_vals.append(float("nan"))
            continue

        hid_int = int(hid)
        if hid_int not in groups:
            H_vals.append(float("nan"))
            HIndex_vals.append(float("nan"))
            continue

        xs, ys = groups[hid_int]
        h = _linear_interp_scalar(float(q), xs, ys, extrapolate=extrapolate)
        if np.isnan(h):
            H_vals.append(float("nan"))
            HIndex_vals.append(float("nan"))
        else:
            H_vals.append(h)
            # HIndex here is a stage-based index; calibration is domain-specific.
            HIndex_vals.append(int(round(h * 3.28) + 1))

    out = flows.copy()
    out["H"] = H_vals
    out["HIndex"] = HIndex_vals
    return out


def read_fim_bounds(fim: FIMSettings) -> pd.DataFrame:
    """
    Read min/max HIndex per HydroID from FIM polygons.

    Parameters
    ----------
    fim : FIMSettings

    Returns
    -------
    pd.DataFrame
        Columns: [HydroID, Hmin, Hmax]
    """
    fim_gdf = gpd.read_file(fim.GPKG, layer=fim.FIM_LAYER)[
        [fim.FLOWLINES_HYDRO_FIELD, "HIndex"]
    ]
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
    """
    Select FIM polygons exactly matching provided (HydroID, HIndex) pairs.

    Parameters
    ----------
    fim : FIMSettings
    pairs_df : pd.DataFrame
        Must contain FLOWLINES_HYDRO_FIELD and HIndex columns.

    Returns
    -------
    geopandas.GeoDataFrame
        Matching polygons.
    """
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
    Build FIM polygons for a single forecast snapshot (one `valid_time`).

    Regular reaches:
      - Map forecast flows to HydroID via AOI flowlines.
      - Convert streamflow [m³/s] to [cfs], compute stage (H) + HIndex via rating curves.
      - Clamp HIndex into [Hmin, Hmax] derived from FIM polygons.
      - Select polygons by (HydroID, HIndex).

    Lake reaches:
      - For flowline segments with LakeID != LAKE_MISSING and valid HydroID,
        treat them as lakes.
      - For each such HydroID (lake sections), choose an adjusted minimum
        HIndex polygon:
          * let hmin = min(HIndex) for that HydroID in FIM polygons
          * let hmax = max(HIndex)
          * candidate = min(hmin + 4, hmax)
          * if candidate exists, use candidate; else use hmin

    Parameters
    ----------
    df_time : pd.DataFrame
        Forecast rows for a single forecast_time.
    settings : PipelineSettings
        Pipeline configuration.
    t0 : datetime
        Forecast cycle time.
    valid_time : pd.Timestamp
        Forecast valid time for this snapshot.
    flowlines : geopandas.GeoDataFrame
        AOI flowlines with HydroID and LakeID.

    Returns
    -------
    geopandas.GeoDataFrame
        FIM polygons for this snapshot, with t0 and valid_time columns.
    """
    fim = settings.fim
    hf = fim.FLOWLINES_HYDRO_FIELD
    lake_field = fim.LAKE_FIELD
    lake_missing = fim.LAKE_MISSING

    # Load all FIM polygons once
    fim_polys_all = gpd.read_file(fim.GPKG, layer=fim.FIM_LAYER)
    fim_polys_all = fim_polys_all.copy()
    fim_polys_all[hf] = pd.to_numeric(fim_polys_all[hf], errors="coerce")
    fim_polys_all["HIndex"] = pd.to_numeric(fim_polys_all["HIndex"], errors="coerce")
    fim_polys_all = fim_polys_all.dropna(subset=[hf, "HIndex"])
    fim_polys_all["HIndex"] = fim_polys_all["HIndex"].astype("Int64")

    if fim_polys_all.empty:
        logger.warning("FIM polygon layer is empty or invalid.")
        return gpd.GeoDataFrame(
            columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None
        )

    # Bounds per HydroID for regular reaches
    bounds = (
        fim_polys_all.groupby(hf)["HIndex"]
        .agg(Hmin="min", Hmax="max")
        .reset_index()
        .set_index(hf)
    )

    # 1) Regular reaches (with flows & rating curves)
    regular_pairs = pd.DataFrame(columns=[hf, "HIndex"])

    if not df_time.empty and "streamflow" in df_time.columns:
        if hf not in flowlines.columns:
            logger.warning(
                "Flowlines missing '%s'; cannot map forecast to HydroID.", hf
            )
        else:
            fl_meta = flowlines[["feature_id", hf]].copy()
            sub = df_time.merge(fl_meta, on="feature_id", how="left")
            sub[hf] = pd.to_numeric(sub[hf], errors="coerce")
            sub = sub.dropna(subset=[hf, "streamflow"])

            if not sub.empty:
                sub["Q_cfs"] = sub["streamflow"].astype(float) * CFS_PER_M3PS

                rc = load_rating_curves(fim)
                if rc.empty:
                    logger.warning(
                        "RatingCurves table empty; skipping RC-based FIM for this snapshot."
                    )
                else:
                    sub2 = compute_stage_for_flows(
                        flows=sub,
                        rc_df=rc,
                        hydro_field=hf,
                        q_cfs_field="Q_cfs",
                        extrapolate=fim.EXTRAPOLATE,
                    )

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

    # 2) Lake reaches (adjusted min HIndex per lake HydroID)
    lake_pairs = pd.DataFrame(columns=[hf, "HIndex"])

    if lake_field in flowlines.columns:
        lake_fl = flowlines[
            (flowlines[lake_field].notna())
            & (flowlines[lake_field] != lake_missing)
        ].copy()

        if not lake_fl.empty:
            lake_fl[hf] = pd.to_numeric(lake_fl[hf], errors="coerce")
            lake_fl = lake_fl.dropna(subset=[hf])

            lake_hids = sorted(lake_fl[hf].drop_duplicates().tolist())

            if lake_hids:
                fim_lake = fim_polys_all[fim_polys_all[hf].isin(lake_hids)].copy()

                rows: List[Tuple[int, int]] = []
                for hid, grp in fim_lake.groupby(hf):
                    hmin = int(grp["HIndex"].min())
                    hmax = int(grp["HIndex"].max())
                    candidate = min(hmin + 4, hmax)
                    if (grp["HIndex"] == candidate).any():
                        chosen = candidate
                    else:
                        chosen = hmin
                    rows.append((int(hid), int(chosen)))

                if rows:
                    lake_pairs = pd.DataFrame(
                        rows, columns=[hf, "HIndex"]
                    ).drop_duplicates()

    # 3) Union regular + lake pairs
    if regular_pairs.empty and lake_pairs.empty:
        return gpd.GeoDataFrame(
            columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None
        )

    all_pairs = (
        pd.concat([regular_pairs, lake_pairs], ignore_index=True)
        .drop_duplicates()
    )

    # 4) Select polygons for (HydroID, HIndex)
    key = [hf, "HIndex"]
    fim_sel = fim_polys_all.merge(all_pairs[key], on=key, how="inner")

    if fim_sel.empty:
        return gpd.GeoDataFrame(
            columns=[hf, "HIndex", "t0", "valid_time", "geometry"], crs=None
        )

    fim_sel = gpd.GeoDataFrame(
        fim_sel, geometry="geometry", crs=fim_polys_all.crs
    )
    fim_sel["t0"] = _ensure_utc_hour(t0)
    fim_sel["valid_time"] = valid_time

    return fim_sel


# -----------------------------------------------------------------------------
# Output helpers
# -----------------------------------------------------------------------------

def _ensure_gpkg(path: Path) -> None:
    """
    Ensure the parent directory for a GeoPackage exists.

    Parameters
    ----------
    path : Path
        Target GeoPackage path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_dir(path: Path) -> None:
    """
    Ensure a directory exists.

    Parameters
    ----------
    path : Path
        Directory path.
    """
    path.mkdir(parents=True, exist_ok=True)


def _parse_formats(out: OutputSettings) -> List[str]:
    """
    Parse configured FIM formats into a list.

    Parameters
    ----------
    out : OutputSettings

    Returns
    -------
    list[str]
    """
    return [f.strip().lower() for f in out.FIM_FORMATS.split(",") if f.strip()]


def _s3_filesystem_rw() -> s3fs.S3FileSystem:
    """
    Create an S3 filesystem client for writing.

    Returns
    -------
    s3fs.S3FileSystem
    """
    return s3fs.S3FileSystem(anon=False)


def _upload_to_s3(local_path: Path, bucket: str, key: str) -> None:
    """
    Upload a local file to S3.

    Parameters
    ----------
    local_path : Path
        Local file path.
    bucket : str
        S3 bucket name.
    key : str
        S3 object key.
    """
    fs = _s3_filesystem_rw()
    s3_uri = f"{bucket.rstrip('/')}/{key.lstrip('/')}"
    with local_path.open("rb") as fsrc, fs.open(s3_uri, "wb") as fdst:
        fdst.write(fsrc.read())
    logger.info("Uploaded %s -> s3://%s", local_path, s3_uri)

def write_flowlines(
    gdf: gpd.GeoDataFrame,
    settings: PipelineSettings,
) -> None:
    """
    Write AOI flowlines with forecast attributes to configured targets.

    Outputs
    -------
    - GeoPackage (OUT_GPKG_PATH / OUT_FLOWLINES_LAYER)
    - GeoJSON  (flowlines.geojson) if requested via OUT_FIM_FORMATS
    - PostGIS  (OUT_PG_FLOWLINES_TABLE) if MODE includes "postgis"

    PostGIS behavior
    ----------------
    - Expects exactly one forecast cycle (one unique t0) in `gdf`.
    - Normalizes t0 to UTC.
    - Adds stable primary key:

          id = "<HydroID>_<t0 UTC: YYYYMMDDHHMMZ>"

    - Deletes existing rows for that t0 from OUT_PG_FLOWLINES_TABLE.
    - Appends the new rows (idempotent for reruns of the same cycle).
    """
    out = settings.out
    fmts = _parse_formats(out)

    base_dir = Path(out.GPKG_PATH).parent
    _ensure_dir(base_dir)

    # 1) GeoPackage
    if out.MODE in ("gpkg", "both") or "gpkg" in fmts:
        path = Path(out.GPKG_PATH)
        _ensure_gpkg(path)
        gdf.to_file(path, layer=out.FLOWLINES_LAYER, driver="GPKG")
        logger.info(
            "Flowlines written to %s (layer=%s)",
            path,
            out.FLOWLINES_LAYER,
        )

    # 2) GeoJSON snapshot
    if "geojson" in fmts:
        gj_path = base_dir / "flowlines.geojson"
        gdf.to_file(gj_path, driver="GeoJSON")
        logger.info("Flowlines GeoJSON written to %s", gj_path)

        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/flowlines.geojson"
            _upload_to_s3(gj_path, out.FIM_S3_BUCKET, key)
    # 3) PostGIS
    if out.MODE in ("postgis", "both"):
        if not out.PG_DSN:
            raise RuntimeError("OUT_PG_DSN not set for PostGIS output mode.")
        if create_engine is None:
            raise RuntimeError("sqlalchemy not available for PostGIS output.")

        # Must have a single forecast cycle
        if "t0" not in gdf.columns:
            raise KeyError("Flowlines GeoDataFrame must contain 't0' for PostGIS output.")

        t0_vals = pd.to_datetime(gdf["t0"], utc=True).unique()
        if len(t0_vals) != 1:
            raise ValueError(
                "write_flowlines expects a single t0 per snapshot; "
                f"got {len(t0_vals)} unique t0 values."
            )

        t0_utc = t0_vals[0]

        flow_db = _add_flowlines_primary_key(gdf)  # id = f"{HydroID}_{t0UTC}"
        flow_db = _dedupe_on_id(flow_db, "nwm_sr_flowlines")

        engine = create_engine(out.PG_DSN)
        table = out.PG_FLOWLINES_TABLE  # e.g. "nwm_sr_flowlines"
        schema = out.PG_SCHEMA

        from sqlalchemy import text  # ensure imported at top

        regclass = f"{schema}.{table}"

        with engine.begin() as conn:
            # Check if the table exists in this schema
            exists = conn.execute(
                text("SELECT to_regclass(:regclass) IS NOT NULL"),
                {"regclass": regclass},
            ).scalar()

            if exists:
                # Idempotent: clean old rows for this t0
                conn.execute(
                    text(
                        f'DELETE FROM "{schema}"."{table}" '
                        "WHERE t0 = :t0"
                    ),
                    {"t0": t0_utc},
                )
            else:
                logger.info(
                    "PostGIS table %s.%s does not exist yet; "
                    "skipping delete for t0=%s (table will be created by to_postgis).",
                    schema,
                    table,
                    t0_utc.isoformat(),
                )

        # Append (and create table on first run)
        flow_db.to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={
                "id": String(),
                "t0": TIMESTAMP(timezone=True),
            },
        )

        logger.info(
            "Flowlines for t0=%s written to PostGIS %s.%s "
            "(%d features, id=<HydroID>_<t0UTC>)",
            t0_utc.isoformat(),
            schema,
            table,
            len(flow_db),
        )




def write_fim(
    fim_gdf: gpd.GeoDataFrame,
    settings: PipelineSettings,
    time_suffix: str,
) -> None:
    """
    Write a single FIM snapshot to configured outputs.

    File naming:
      FIM_<PRODUCT>_<t0:yyyyMMddHHmm>_<time:yyyyMMddHHmm>.{gpkg,geojson}

    Behavior:
      - Uses t0 and valid_time from `fim_gdf`.
      - Writes GPKG / GeoJSON as configured.
      - Appends to PostGIS history table if enabled.
      - Optionally uploads each snapshot to S3.
      - Optionally updates a fixed "live" GeoJSON object on S3.

    Parameters
    ----------
    fim_gdf : geopandas.GeoDataFrame
        Snapshot polygons with 't0' and 'valid_time' columns.
    settings : PipelineSettings
        Pipeline configuration.
    time_suffix : str
        Legacy suffix (ignored for naming; kept for compatibility).
    """
    out = settings.out

    if fim_gdf.empty:
        logger.info("No FIM polygons for %s", time_suffix)
        return

    if "t0" not in fim_gdf.columns or "valid_time" not in fim_gdf.columns:
        raise ValueError("fim_gdf must contain 't0' and 'valid_time' columns.")

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

    product = getattr(settings.nwm, "PRODUCT", "short_range")
    product_tag = str(product).upper()

    basename = f"FIM_{product_tag}_{t0_str}_{vt_str}"

    fmts = _parse_formats(out)
    base_dir = Path(out.GPKG_PATH).parent
    _ensure_dir(base_dir)

    written_geojson: Optional[Path] = None

    # 1) GeoPackage
    if out.MODE in ("gpkg", "both") or "gpkg" in fmts:
        gpkg_path = base_dir / f"{basename}.gpkg"
        fim_gdf.to_file(gpkg_path, layer="fim", driver="GPKG")
        logger.info("FIM snapshot written to %s", gpkg_path)

        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/{gpkg_path.name}"
            _upload_to_s3(gpkg_path, out.FIM_S3_BUCKET, key)

    # 2) GeoJSON
    if "geojson" in fmts:
        gj_path = base_dir / f"{basename}.geojson"
        fim_gdf.to_file(gj_path, driver="GeoJSON")
        written_geojson = gj_path
        logger.info("FIM snapshot GeoJSON written to %s", gj_path)

        if out.FIM_S3_BUCKET:
            key = f"{out.FIM_S3_PREFIX.rstrip('/')}/{gj_path.name}"
            _upload_to_s3(gj_path, out.FIM_S3_BUCKET, key)

    # 3) PostGIS history (idempotent, PK-aware)
    if out.MODE in ("postgis", "both"):
        if not out.PG_DSN:
            raise RuntimeError("OUT_PG_DSN not set for PostGIS output mode.")
        if create_engine is None:
            raise RuntimeError("sqlalchemy not available for PostGIS output.")

        # Enforce single (t0, valid_time) per snapshot
        if "t0" not in fim_gdf.columns or "valid_time" not in fim_gdf.columns:
            raise KeyError("FIM GeoDataFrame must contain 't0' and 'valid_time'.")

        t0_vals = pd.to_datetime(fim_gdf["t0"], utc=True).unique()
        vt_vals = pd.to_datetime(fim_gdf["valid_time"], utc=True).unique()

        if len(t0_vals) != 1 or len(vt_vals) != 1:
            raise ValueError(
                "write_fim expects a single (t0, valid_time) per snapshot; "
                f"got {len(t0_vals)} t0 and {len(vt_vals)} valid_time values."
            )

        t0_utc = t0_vals[0]
        vt_utc = vt_vals[0]

        # Normalize geometry + add deterministic PK
        fim_db = _ensure_multipolygon(fim_gdf)
        fim_db = _add_fim_primary_key(fim_db)
        fim_db = _dedupe_on_id(fim_db, "fim_nwm")

        engine = create_engine(out.PG_DSN)
        table = out.PG_FIM_TABLE
        schema = out.PG_SCHEMA

        regclass = f"{schema}.{table}"

        with engine.begin() as conn:
            # Check if target table exists
            exists = conn.execute(
                text("SELECT to_regclass(:regclass) IS NOT NULL"),
                {"regclass": regclass},
            ).scalar()

            if exists:
                # Idempotent: remove any existing rows for this snapshot
                conn.execute(
                    text(
                        f'DELETE FROM "{schema}"."{table}" '
                        "WHERE t0 = :t0 AND valid_time = :valid_time"
                    ),
                    {"t0": t0_utc, "valid_time": vt_utc},
                )
            else:
                logger.info(
                    "PostGIS table %s.%s does not exist yet; "
                    "skipping delete for t0=%s, valid_time=%s "
                    "(it will be created by to_postgis).",
                    schema,
                    table,
                    t0_utc.isoformat(),
                    vt_utc.isoformat(),
                )

        # Append snapshot (or create table on first run)
        fim_db.to_postgis(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            dtype={
                "id": String(),
                "t0": TIMESTAMP(timezone=True),
                "valid_time": TIMESTAMP(timezone=True),
            },
        )

        logger.info(
            "FIM snapshot t0=%s valid_time=%s written to PostGIS %s.%s "
            "(%d features, id=<HydroID>_<t0UTC>_<validUTC>)",
            t0_utc.isoformat(),
            vt_utc.isoformat(),
            schema,
            table,
            len(fim_db),
        )


    # 4) Live S3 GeoJSON
    if out.FIM_S3_BUCKET and out.FIM_S3_LIVE_KEY and written_geojson is not None:
        _upload_to_s3(written_geojson, out.FIM_S3_BUCKET, out.FIM_S3_LIVE_KEY)
        logger.info(
            "Updated live FIM GeoJSON at s3://%s/%s",
            out.FIM_S3_BUCKET,
            out.FIM_S3_LIVE_KEY,
        )


def refresh_fim_materialized_views(settings: "PipelineSettings") -> None:
    """
    Refresh PostGIS materialized views that depend on FIM history tables.

    Intended to be called once after a successful FIM generation run so that
    GeoServer WFS layers backed by these materialized views see:
      - the latest rows,
      - stable primary keys (id),
      - no timezone-induced duplication artifacts.

    This is a no-op when:
      - OUT_MODE does not include PostGIS,
      - OUT_PG_DSN is not set,
      - SQLAlchemy is unavailable,
      - the expected materialized views do not exist yet.

    Parameters
    ----------
    settings : PipelineSettings
        Loaded pipeline configuration.
    """
    out = settings.out

    # Only relevant if we're actually writing to PostGIS.
    if out.MODE not in ("postgis", "both"):
        logger.debug(
            "PostGIS output disabled (OUT_MODE=%s); skipping FIM matview refresh.",
            out.MODE,
        )
        return

    if not out.PG_DSN:
        logger.debug("OUT_PG_DSN not set; skipping FIM matview refresh.")
        return

    # These names must match what `geoserver_init.py` creates.
    matviews = ("fim_nowcast", "fim_max")

    logger.info(
        "Refreshing FIM materialized views %s in schema '%s'.",
        ", ".join(matviews),
        out.PG_SCHEMA,
    )

    _refresh_matviews(
        dsn=out.PG_DSN,
        schema=out.PG_SCHEMA,
        views=matviews,
    )

    logger.info("FIM materialized views refreshed successfully.")


# -----------------------------------------------------------------------------
# Orchestrator: generate FIM for all forecast times
# -----------------------------------------------------------------------------

def generate_all_fim_layers(
    df_forecast: pd.DataFrame,
    settings: PipelineSettings,
    flowlines: Optional[gpd.GeoDataFrame] = None,
) -> None:
    """
    Generate and persist FIM polygons for each unique forecast_time.

    Parameters
    ----------
    df_forecast : pd.DataFrame
        Long-format forecast table.
    settings : PipelineSettings
        Pipeline configuration.
    flowlines : geopandas.GeoDataFrame, optional
        Pre-loaded AOI flowlines; if missing, they are loaded.

    Notes
    -----
    - Assumes all rows share a common forecast_run (t0); if not, uses the first.
    - Delegates per-time snapshot creation to `generate_fim_snapshot`.
    - Writes each snapshot via `write_fim`.
    """
    if df_forecast.empty:
        logger.warning("No forecast data provided to generate_all_fim_layers.")
        return

    if flowlines is None:
        flowlines = load_flowlines(settings)

    t0_vals = df_forecast["forecast_run"].dropna().drop_duplicates()
    if t0_vals.empty:
        raise ValueError("df_forecast must contain 'forecast_run' values.")
    if len(t0_vals) > 1:
        logger.warning(
            "Multiple forecast_run (t0) values in df_forecast; using first: %s",
            t0_vals.iloc[0],
        )

    t0 = t0_vals.iloc[0]

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

def main() -> None:
    """
    Default entry point for running the full pipeline once.

    Steps:
      1. Load settings from environment.
      2. Load AOI flowlines and derive COMID filter.
      3. Fetch latest NWM SR forecast for AOI.
      4. Write AOI flowlines with q_fNNN columns.
      5. Generate FIM polygons for all forecast times.
    """
    settings = PipelineSettings.load()
    logger.info("Loaded settings: %s", settings)

    flowlines, comids = get_aoi_comids(settings)

    t0, df = get_latest_nwm_forecast(
        settings=settings,
        comids=comids,
        max_lead_hours=1,  # adjust to 18 for full short_range if desired
    )

    logger.info(
        "Using NWM SR forecast t0=%s; %d AOI records (all times)",
        t0.isoformat(),
        len(df),
    )

    gdf_flow = build_flowlines_with_forecast_columns(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )
    write_flowlines(gdf_flow, settings)

    generate_all_fim_layers(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )

    logger.info("NWM SR → AOI Flowlines → FIM pipeline completed.")


if __name__ == "__main__":
    main()
