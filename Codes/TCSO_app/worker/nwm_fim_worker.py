#!/usr/bin/env python3
"""
nwm_fim_worker.py

FastAPI-based NWM/FIM worker.

Behavior
--------
- On startup (via lifespan):
    * loads pipeline settings,
    * loads AOI flowlines + COMID filter once,
    * starts a background task that periodically:
        - detects latest NWM SR cycle (cheap S3 listing via find_latest_t0),
        - if it's new:
            · loads forecasts for that exact t0 (AOI only),
            · builds AOI flowlines with q_fXXX columns,
            · writes outputs (GPKG/GeoJSON/PostGIS/S3),
            · generates all FIM polygons for that t0.
        - if unchanged:
            · logs and skips heavy work.

- On shutdown:
    * cancels the background task cleanly.

Run with:
    uvicorn nwm_fim_worker:app --host 0.0.0.0 --port 8000

Env knobs
---------
WORKER_INTERVAL_SEC    Polling interval in seconds (default: 60)
WORKER_MAX_LEAD_HOURS  Max SR lead hours to use (default: 18)
NWM_LOG_LEVEL          Log level (default: INFO)
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI

from fim import (
    PipelineSettings,
    get_aoi_comids,
    find_latest_t0,
    get_nwm_forecast_for_t0,
    get_latest_nwm_forecast,
    build_flowlines_with_forecast_columns,
    write_flowlines,
    generate_all_fim_layers,
    refresh_fim_materialized_views, 
    write_point_status_layers
)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("NWM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger("nwm_fim_worker")

# -----------------------------------------------------------------------------
# Worker config
# -----------------------------------------------------------------------------

WORKER_INTERVAL_SEC = int(os.getenv("WORKER_INTERVAL_SEC", "60"))
WORKER_MAX_LEAD_HOURS = int(os.getenv("WORKER_MAX_LEAD_HOURS", "18"))

# -----------------------------------------------------------------------------
# Global state
# -----------------------------------------------------------------------------

# Loaded once and reused to avoid re-reading AOI on every loop.
settings: PipelineSettings = PipelineSettings.load()
flowlines, comids = get_aoi_comids(settings)

_last_processed_t0: Optional[datetime] = None
_last_run_count: int = 0


# -----------------------------------------------------------------------------
# Core polling logic
# -----------------------------------------------------------------------------

async def poll_latest_forecast(interval_seconds: int = WORKER_INTERVAL_SEC) -> None:
    """
    Background task:
    - Detect new NWM SR cycles using only S3 listing (find_latest_t0).
    - If a new cycle appears, run the full forecast + FIM pipeline once.
    - If unchanged, do nothing heavy and log a skip message.
    """
    global _last_processed_t0, _last_run_count

    logger.info(
        "Starting NWM/FIM worker loop: interval=%ds max_lead_hours=%d",
        interval_seconds,
        WORKER_MAX_LEAD_HOURS,
    )

    while True:
        try:
            # 1) Lightweight detection of the latest available t0
            try:
                latest_t0 = find_latest_t0(
                    settings=settings,
                    max_lead_hours=WORKER_MAX_LEAD_HOURS,
                )
            except Exception as exc:
                logger.error("Failed to detect latest NWM SR cycle: %s", exc)
                await asyncio.sleep(interval_seconds)
                continue

            # Normalize to aware UTC
            if latest_t0.tzinfo is None:
                latest_t0_utc = latest_t0.replace(tzinfo=timezone.utc)
            else:
                latest_t0_utc = latest_t0.astimezone(timezone.utc)

            # 2) Skip if we've already processed this cycle
            if _last_processed_t0 is not None and latest_t0_utc <= _last_processed_t0:
                logger.info(
                    "Latest NWM SR cycle unchanged (t0=%s); skipping processing.",
                    latest_t0_utc.isoformat(),
                )
                await asyncio.sleep(interval_seconds)
                continue

            logger.info(
                "Detected new NWM SR cycle t0=%s (previous=%s); starting update.",
                latest_t0_utc.isoformat(),
                _last_processed_t0.isoformat() if _last_processed_t0 else "none",
            )

            # 3) Load forecasts ONLY for this specific (new) t0
            try:
                df = get_nwm_forecast_for_t0(
                    t0=latest_t0_utc,
                    settings=settings,
                    comids=comids,
                    max_lead_hours=WORKER_MAX_LEAD_HOURS,
                )
            except Exception as exc:
                logger.error(
                    "Error loading NWM forecast for candidate t0=%s: %s",
                    latest_t0_utc.isoformat(),
                    exc,
                )

                # Fallback: try to find the latest *usable* cycle (may be older).
                try:
                    fb_t0, fb_df = get_latest_nwm_forecast(
                        settings=settings,
                        comids=comids,
                        max_lead_hours=WORKER_MAX_LEAD_HOURS,
                    )
                except Exception as fb_exc:
                    logger.error(
                        "Fallback get_latest_nwm_forecast failed after candidate t0=%s: %s",
                        latest_t0_utc.isoformat(),
                        fb_exc,
                    )
                    await asyncio.sleep(interval_seconds)
                    continue

                # If the best usable t0 is not newer than what we've already processed,
                # there is nothing new to do. Just log and back off.
                if _last_processed_t0 is not None and fb_t0 <= _last_processed_t0:
                    logger.info(
                        "No newer usable NWM SR cycle than %s "
                        "(fallback t0=%s after candidate t0=%s). Skipping.",
                        _last_processed_t0.isoformat(),
                        fb_t0.isoformat(),
                        latest_t0_utc.isoformat(),
                    )
                    await asyncio.sleep(interval_seconds)
                    continue

                # Otherwise, accept the fallback as the run to process.
                logger.warning(
                    "Using fallback NWM SR cycle t0=%s instead of incomplete candidate t0=%s.",
                    fb_t0.isoformat(),
                    latest_t0_utc.isoformat(),
                )
                latest_t0_utc = fb_t0
                df = fb_df

            if df.empty:
                logger.warning(
                    "Forecast dataframe empty for t0=%s; skipping pipeline.",
                    latest_t0_utc.isoformat(),
                )
                await asyncio.sleep(interval_seconds)
                continue

            logger.info(
                "Running NWM/FIM pipeline for t0=%s with %d forecast rows.",
                latest_t0_utc.isoformat(),
                len(df),
            )

            # 4) Build AOI flowlines with forecast columns & write outputs
            try:
                gdf_flow = build_flowlines_with_forecast_columns(
                    df_forecast=df,
                    settings=settings,
                    flowlines=flowlines,
                )
                write_flowlines(gdf_flow, settings)
            except Exception as exc:
                logger.error(
                    "Error building/writing flowlines for t0=%s: %s",
                    latest_t0_utc.isoformat(),
                    exc,
                )
                await asyncio.sleep(interval_seconds)
                continue

            # 5) Generate all FIM layers for this t0
            try:
                fim_polys_gdf = generate_all_fim_layers(
                    df_forecast=df,
                    settings=settings,
                    flowlines=flowlines,
                )
            except Exception as exc:
                logger.error(
                    "Error generating FIM layers for t0=%s: %s",
                    latest_t0_utc.isoformat(),
                    exc,
                )
                await asyncio.sleep(interval_seconds)
                continue
            # 5b) Generate Point layers for low water crossings and addresspoints 

            try:
                write_point_status_layers(
                    fim_polys_5070=fim_polys_gdf,   # the polygons you just generated/published (EPSG:5070)
                    settings=settings,
                    t0_utc=latest_t0_utc,
                )
            except Exception as exc:
                logger.error(
                    "Error generating Point(AP,LWC) layers for t0=%s: %s",
                    latest_t0_utc.isoformat(),
                    exc,
                )
                await asyncio.sleep(interval_seconds)
                continue

            # 5c) Refresh FIM-related materialized views so GeoServer sees consistent data.
            try:
                refresh_fim_materialized_views(settings)
            except Exception as exc:
                # Do not fail the whole cycle if the refresh hiccups; just log loudly.
                logger.error(
                    "Failed to refresh FIM materialized views after t0=%s: %s",
                    latest_t0_utc.isoformat(),
                    exc,
                )
            

            # 6) Mark success
            _last_processed_t0 = latest_t0_utc
            _last_run_count = len(df)

            logger.info(
                "Completed NWM/FIM pipeline for t0=%s; last_run_count=%d",
                latest_t0_utc.isoformat(),
                _last_run_count,
            )

        except Exception as exc:  # Hard guard: don't kill the loop
            logger.exception("Unhandled error in NWM/FIM worker loop: %s", exc)

        await asyncio.sleep(interval_seconds)


# -----------------------------------------------------------------------------
# FastAPI lifespan (startup/shutdown) – replaces deprecated on_event
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context:
    - start background polling task on startup,
    - cancel it gracefully on shutdown.
    """
    task = asyncio.create_task(
        poll_latest_forecast(interval_seconds=WORKER_INTERVAL_SEC)
    )
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


# -----------------------------------------------------------------------------
# FastAPI app & endpoints
# -----------------------------------------------------------------------------

app = FastAPI(
    title="NWM FIM Worker",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    """
    Lightweight health endpoint for monitoring.
    """
    return {
        "status": "ok",
        "last_processed_t0": (
            _last_processed_t0.isoformat() if _last_processed_t0 else None
        ),
        "last_run_count": _last_run_count,
        "interval_sec": WORKER_INTERVAL_SEC,
        "max_lead_hours": WORKER_MAX_LEAD_HOURS,
    }


@app.get("/last-run")
async def last_run() -> dict:
    """
    Detailed info on the last successful pipeline execution.
    """
    return {
        "last_processed_t0": (
            _last_processed_t0.isoformat() if _last_processed_t0 else None
        ),
        "last_run_count": _last_run_count,
    }
