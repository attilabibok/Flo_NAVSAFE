#!/usr/bin/env python3
"""
nwm_fim_t0.py

Run NWM short_range → AOI flowlines → FIM for an explicit t0.

Usage:
    python nwm_fim_t0.py --t0 2025-07-08T12:00:00Z --max-lead-hours 18
"""

import argparse
from datetime import datetime, timezone

from fim import (
    PipelineSettings,
    get_aoi_comids,
    get_nwm_forecast_for_t0,
    build_flowlines_with_forecast_columns,
    write_flowlines,
    generate_all_fim_layers,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--t0",
        required=True,
        help="Forecast reference time (ISO8601, e.g. 2025-07-08T12:00:00Z).",
    )
    p.add_argument(
        "--max-lead-hours",
        type=int,
        default=18,
        help="Max short_range lead hours to fetch (default: 18).",
    )
    return p.parse_args()


def parse_t0(value: str) -> datetime:
    # very small robust parser; expects UTC or naive treated as UTC
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if "Z" in value or "+" in value else datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main() -> None:
    args = parse_args()
    settings = PipelineSettings.load()

    t0 = parse_t0(args.t0)

    # AOI mask + COMIDs from p2fflowlines_tcso.gpkg (inside fim.load_flowlines/get_aoi_comids)
    flowlines, comids = get_aoi_comids(settings)

    # NWM SR forecast for this explicit t0, AOI-only
    df = get_nwm_forecast_for_t0(
        t0=t0,
        settings=settings,
        comids=comids,
        max_lead_hours=args.max_lead_hours,
    )

    # Build AOI flowlines with q_fXXX columns and write (GPKG / PostGIS via env)
    gdf_flow = build_flowlines_with_forecast_columns(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )
    write_flowlines(gdf_flow, settings)

    # Generate FIM per timestep (uses your lake rules, etc.)
    generate_all_fim_layers(
        df_forecast=df,
        settings=settings,
        flowlines=flowlines,
    )


if __name__ == "__main__":
    main()
