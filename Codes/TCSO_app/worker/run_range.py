#!/usr/bin/env python3
"""
nwm_fim_range.py

Generate FIM layers for a time window [FROM, TO].

Logic:
- Load AOI COMIDs from p2fflowlines_tcso.gpkg (via fim.get_aoi_comids).
- Get latest NWM short_range forecast for AOI.
- Optionally also look at analysis_assim to determine default FROM.
- Filter forecast timesteps to [FROM, TO].
- Run FIM only for timesteps in that window.

Usage examples:
    # Explicit range
    python nwm_fim_range.py --from 2025-07-08T13:00:00Z --to 2025-07-08T18:00:00Z

    # Let script choose:
    #   FROM = last analysis_assim time (if configured / available)
    #   TO   = first short_range time
    python nwm_fim_range.py
"""

import argparse
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

import pandas as pd
import s3fs
import xarray as xr

from fim import (
    PipelineSettings,
    get_aoi_comids,
    get_latest_nwm_forecast,
    generate_all_fim_layers,
)


def parse_iso_dt(val: str) -> datetime:
    dt = datetime.fromisoformat(val.replace("Z", "+00:00")) if ("Z" in val or "+" in val) else datetime.fromisoformat(val)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_dt", help="Start datetime (ISO8601, UTC).")
    p.add_argument("--to", dest="to_dt", help="End datetime (ISO8601, UTC).")
    p.add_argument(
        "--max-lead-hours",
        type=int,
        default=18,
        help="Max short_range lead hours to fetch (default: 18).",
    )
    return p.parse_args()


# --- Optional: best-effort latest analysis_assim time (AOI-based) ---


def _s3fs_anon() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(anon=True, client_kwargs={"region_name": "us-east-1"})


def get_latest_analysis_assim_time(
    settings: PipelineSettings,
    comids: list[int],
    lookback_days: int = 2,
) -> Optional[pd.Timestamp]:
    """
    Best-effort:
      - Scan analysis_assim channel_rt for latest available time covering AOI.
      - Returns tz-aware UTC timestamp or None.
    """
    bucket = settings.nwm.BUCKET
    product = "analysis_assim"
    variable = settings.nwm.VARIABLE

    fs = _s3fs_anon()
    today = datetime.utcnow().date()

    for delta in range(lookback_days):
        dt = today - timedelta(days=delta)
        prefix = f"{bucket}/nwm.{dt:%Y%m%d}/{product}/"
        try:
            files = fs.find(prefix)
        except Exception:
            continue

        # Look for latest file; AA is hourly
        nc_files = [f for f in files if f.endswith(".channel_rt.nc") or "channel_rt" in f]
        if not nc_files:
            continue

        latest = sorted(nc_files)[-1]
        try:
            with fs.open(latest, "rb") as fobj:
                ds = xr.open_dataset(fobj, engine="h5netcdf")
                ds.load()
        except Exception:
            continue

        try:
            vt = pd.to_datetime(ds["time"].values[-1])
            if vt.tzinfo is None:
                vt = vt.tz_localize(timezone.utc)
            else:
                vt = vt.tz_convert(timezone.utc)
        except Exception:
            vt = None
        finally:
            try:
                ds.close()
            except Exception:
                pass

        if vt is not None:
            return vt

    return None


def main() -> None:
    args = parse_args()
    settings = PipelineSettings.load()

    # AOI COMIDs
    flowlines, comids = get_aoi_comids(settings)

    # Get latest SR forecast once
    t0_sr, df_sr = get_latest_nwm_forecast(
        settings=settings,
        comids=comids,
        max_lead_hours=args.max_lead_hours,
    )

    # Determine FROM / TO
    if args.from_dt:
        from_dt = parse_iso_dt(args.from_dt)
    else:
        # default FROM: try last analysis_assim time, else first SR time
        aa_last = get_latest_analysis_assim_time(settings, comids)  # optional
        if aa_last is not None:
            from_dt = aa_last.to_pydatetime()
        else:
            from_dt = df_sr["forecast_time"].min().to_pydatetime()

    if args.to_dt:
        to_dt = parse_iso_dt(args.to_dt)
    else:
        # default TO: first SR forecast time (often t0 + 1h)
        to_dt = df_sr["forecast_time"].min().to_pydatetime()

    if from_dt > to_dt:
        raise SystemExit(f"FROM {from_dt} is after TO {to_dt}")

    # Filter SR forecasts into [FROM, TO]
    mask = (df_sr["forecast_time"] >= from_dt) & (df_sr["forecast_time"] <= to_dt)
    df_window = df_sr.loc[mask].copy()

    if df_window.empty:
        raise SystemExit(f"No short_range timesteps within [{from_dt}, {to_dt}]")

    # Reuse existing FIM generator: only processes timesteps present in df_window
    generate_all_fim_layers(
        df_forecast=df_window,
        settings=settings,
        flowlines=flowlines,
    )


if __name__ == "__main__":
    main()
