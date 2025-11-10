#!/usr/bin/env python3
"""
nwm_fim_worker.py

FastAPI-based worker that:

- Every 60 seconds:
    * checks for the latest NWM short_range forecast (AOI-only),
    * if it finds a newer t0 than last run:
        - builds AOI flowlines with q_fXXX columns,
        - writes them (GPKG/PostGIS via fim settings),
        - generates FIM layers for all timesteps.

Run with:
    uvicorn nwm_fim_worker:app --host 0.0.0.0 --port 8000
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI

from fim import (
    PipelineSettings,
    get_aoi_comids,
    get_latest_nwm_forecast,
    build_flowlines_with_forecast_columns,
    write_flowlines,
    generate_all_fim_layers,
)

app = FastAPI(title="NWM FIM Worker")

settings: PipelineSettings = PipelineSettings.load()
flowlines, comids = get_aoi_comids(settings)

_last_processed_t0: Optional[datetime] = None
_last_run_count: int = 0


async def poll_latest_forecast(interval_seconds: int = 60) -> None:
    global _last_processed_t0, _last_run_count

    while True:
        try:
            # Get latest SR forecast for AOI
            t0, df = get_latest_nwm_forecast(
                settings=settings,
                comids=comids,
                max_lead_hours=18,  # or from env
            )
            t0_utc = t0.astimezone(timezone.utc)

            # Run only if new
            if _last_processed_t0 is None or t0_utc > _last_processed_t0:
                # Build AOI flowlines with forecast columns
                gdf_flow = build_flowlines_with_forecast_columns(
                    df_forecast=df,
                    settings=settings,
                    flowlines=flowlines,
                )
                write_flowlines(gdf_flow, settings)

                # Generate FIM for all timesteps
                generate_all_fim_layers(
                    df_forecast=df,
                    settings=settings,
                    flowlines=flowlines,
                )

                _last_processed_t0 = t0_utc
                _last_run_count = len(df)

        except Exception as exc:
            # Log; FastAPI/uvicorn will handle stdout/stderr
            print(f"[worker] Error during NWM/FIM update: {exc}", flush=True)

        await asyncio.sleep(interval_seconds)


@app.on_event("startup")
async def startup_event() -> None:
    # Start background polling task
    asyncio.create_task(poll_latest_forecast(interval_seconds=60))


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "last_processed_t0": _last_processed_t0.isoformat() if _last_processed_t0 else None,
        "last_run_count": _last_run_count,
    }


@app.get("/last-run")
async def last_run():
    return {
        "last_processed_t0": _last_processed_t0.isoformat() if _last_processed_t0 else None,
        "last_run_count": _last_run_count,
    }
