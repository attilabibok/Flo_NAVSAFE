#!/usr/bin/env python3
"""
nwm_fim_maintainer.py

A small FastAPI-based maintenance service for the NWM / FIM PostGIS store.

Responsibilities
----------------
1. Periodically clean historic FIM records from PostGIS by `t0`:
   - Delete rows older than a configured retention period.
   - Retention is configured via an ISO 8601 duration string
     (e.g. "P3D", "PT12H", "P1DT6H") or equivalent env vars.

2. Respect "protected events":
   - Events are named [t0_from, t0_to] ranges.
   - Any records with `t0` within a protected range are never deleted,
     regardless of age.

3. Expose control & inspection API:
   - `GET  /protected-events`   → list all protected events
   - `POST /protected-events`   → add a new protected event
   - `POST /cleanup`            → run cleanup immediately

The service is designed to be:
- Stateless (besides its Postgres tables),
- Idempotent on startup (auto-creates the protected events table),
- Safe to run alongside the main NWM/FIM worker.

Configuration (env)
-------------------
Uses the same DB config as your pipeline, with maintainer-specific knobs.

DB:
  MAINTAINER_PG_DSN         (optional) full DSN; if absent, falls back to:
    OUT_PG_DSN              (from fim.py / pipeline)
  MAINTAINER_PG_SCHEMA      (default: value of OUT_PG_SCHEMA or "public")
  MAINTAINER_FIM_TABLE      (default: value of OUT_PG_FIM_TABLE or "fim_nwm")

Retention:
  MAINTAINER_RETENTION      ISO 8601 duration, e.g. "P3D" or "P1DT12H"
                            (default: "P3D" = 3 days)

Schedule:
  MAINTAINER_INTERVAL_SEC   Number of seconds between background cleanups.
                            (default: 900 = 15 minutes)
  MAINTAINER_ENABLED        "true"/"false" to enable background loop (default: true)

Example docker-compose service
------------------------------
  tcso-maintainer:
    build:
      context: ./worker
    command: ["uvicorn", "nwm_fim_maintainer:app", "--host", "0.0.0.0", "--port", "8100"]
    environment:
      MAINTAINER_PG_DSN: postgresql://gis:gis@tcso-pgis:5432/gis
      MAINTAINER_PG_SCHEMA: public
      MAINTAINER_FIM_TABLE: fim_nwm
      MAINTAINER_RETENTION: P3D
      MAINTAINER_INTERVAL_SEC: 900
    depends_on:
      - tcso-pgis
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("MAINTAINER_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] maintainer - %(message)s",
)
logger = logging.getLogger("nwm_fim_maintainer")


# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

class MaintainerSettings(BaseSettings):
    """
    Settings for the maintainer service.

    Priority:
      - MAINTAINER_* env vars
      - falls back to OUT_* (shared with pipeline) where appropriate
    """

    model_config = SettingsConfigDict(env_prefix="MAINTAINER_", extra="ignore")

    # Database
    PG_DSN: Optional[str] = None
    PG_SCHEMA: Optional[str] = None
    FIM_TABLE: Optional[str] = None

    # Retention: ISO8601 duration, e.g. "P3D", "PT12H", "P1DT6H"
    RETENTION: str = "P3D"

    # Background cleanup loop
    INTERVAL_SEC: int = 900
    ENABLED: bool = True

    def effective_pg_dsn(self) -> str:
        """
        Return the effective Postgres DSN.

        Falls back to OUT_PG_DSN if MAINTAINER_PG_DSN is not set.
        """
        dsn = self.PG_DSN or os.getenv("OUT_PG_DSN")
        if not dsn:
            raise RuntimeError(
                "MAINTAINER_PG_DSN or OUT_PG_DSN must be set for maintainer."
            )
        return dsn

    def effective_schema(self) -> str:
        """
        Return the effective schema name.

        Falls back to OUT_PG_SCHEMA (or public).
        """
        schema = self.PG_SCHEMA or os.getenv("OUT_PG_SCHEMA") or "public"
        return schema

    def effective_fim_table(self) -> str:
        """
        Return the effective FIM history table name.

        Falls back to OUT_PG_FIM_TABLE (or fim_nwm).
        """
        table = self.FIM_TABLE or os.getenv("OUT_PG_FIM_TABLE") or "fim_nwm"
        return table

    def retention_timedelta(self) -> timedelta:
        """
        Parse the configured ISO8601 duration string into a timedelta.

        Supports:
            PnD
            PTnH / PTnM / PTnS
            PnDTnHnMnS
        """
        return parse_iso8601_duration(self.RETENTION)


# -----------------------------------------------------------------------------
# ISO 8601 duration parsing (minimal implementation)
# -----------------------------------------------------------------------------

_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$"
)


def parse_iso8601_duration(value: str) -> timedelta:
    """
    Parse a simple ISO 8601 duration string into a timedelta.

    Supported forms
    ---------------
    - "P3D"
    - "PT12H"
    - "PT30M"
    - "PT45S"
    - "P1DT6H"
    - "P1DT6H30M10S"

    Parameters
    ----------
    value : str
        ISO 8601 duration.

    Returns
    -------
    timedelta

    Raises
    ------
    ValueError
        If the format is not recognized.
    """
    value = value.strip().upper()
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(f"Invalid ISO 8601 duration: {value!r}")

    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)

    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


# -----------------------------------------------------------------------------
# Protected event models
# -----------------------------------------------------------------------------

class ProtectedEventIn(BaseModel):
    """
    Request body for creating a protected event.

    Attributes
    ----------
    name : str
        Human-readable label for the event.
    t0_from : datetime
        Inclusive start of protected t0 range (UTC or with tz).
    t0_to : datetime
        Inclusive end of protected t0 range (UTC or with tz).
    """

    name: str
    t0_from: datetime
    t0_to: datetime

    @field_validator("t0_from", "t0_to")
    @classmethod
    def ensure_tz(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            # Assume UTC if naive
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("t0_to")
    @classmethod
    def ensure_range(cls, v: datetime, values: dict) -> datetime:
        start = values.get("t0_from")
        if start and v < start:
            raise ValueError("t0_to must be >= t0_from")
        return v


class ProtectedEvent(BaseModel):
    """
    Representation of a stored protected event.
    """

    id: int
    name: str
    t0_from: datetime
    t0_to: datetime


@dataclass
class DBContext:
    """
    Holds DB engine and metadata objects.
    """

    engine: Engine
    metadata: MetaData
    protected_table: Table
    fim_table_full: str


# -----------------------------------------------------------------------------
# DB Initialization
# -----------------------------------------------------------------------------

def init_db(settings: MaintainerSettings) -> DBContext:
    """
    Initialize the DB engine and ensure the protected events table exists.

    Returns
    -------
    DBContext
    """
    dsn = settings.effective_pg_dsn()
    schema = settings.effective_schema()
    fim_table = settings.effective_fim_table()
    fim_table_full = f"{schema}.{fim_table}"

    engine = create_engine(dsn, future=True)
    metadata = MetaData(schema=schema)

    protected_table = Table(
        "fim_protected_event",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", Text, nullable=False),
        Column("t0_from", String, nullable=False),
        Column("t0_to", String, nullable=False),
        extend_existing=True,
    )

    # Create schema and table if not present
    with engine.begin() as conn:
        conn.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        )
        metadata.create_all(conn, checkfirst=True)

    logger.info(
        "Maintainer DB initialized (schema=%s fim_table=%s)",
        schema,
        fim_table_full,
    )

    return DBContext(
        engine=engine,
        metadata=metadata,
        protected_table=protected_table,
        fim_table_full=fim_table_full,
    )


# -----------------------------------------------------------------------------
# Core cleanup logic
# -----------------------------------------------------------------------------

def load_protected_events(ctx: DBContext) -> List[ProtectedEvent]:
    """
    Load all protected events from the database.

    Returns
    -------
    list[ProtectedEvent]
    """
    stmt = ctx.protected_table.select().order_by(ctx.protected_table.c.id.asc())
    with ctx.engine.begin() as conn:
        result: Result = conn.execute(stmt)
        rows = result.mappings().all()

    events: List[ProtectedEvent] = []
    for row in rows:
        events.append(
            ProtectedEvent(
                id=row["id"],
                name=row["name"],
                t0_from=_parse_iso_dt(row["t0_from"]),
                t0_to=_parse_iso_dt(row["t0_to"]),
            )
        )
    return events


def add_protected_event(ctx: DBContext, ev_in: ProtectedEventIn) -> ProtectedEvent:
    """
    Insert a new protected event into the database.

    Returns
    -------
    ProtectedEvent
    """
    values = {
        "name": ev_in.name,
        "t0_from": ev_in.t0_from.astimezone(timezone.utc).isoformat(),
        "t0_to": ev_in.t0_to.astimezone(timezone.utc).isoformat(),
    }

    with ctx.engine.begin() as conn:
        result = conn.execute(ctx.protected_table.insert().values(**values))
        new_id = int(result.inserted_primary_key[0])

    return ProtectedEvent(
        id=new_id,
        name=ev_in.name,
        t0_from=ev_in.t0_from.astimezone(timezone.utc),
        t0_to=ev_in.t0_to.astimezone(timezone.utc),
    )


def cleanup_fim_history(
    ctx: DBContext,
    settings: MaintainerSettings,
    now: Optional[datetime] = None,
) -> int:
    """
    Delete historic FIM records older than retention, excluding protected ranges.

    Deletion rule
    -------------
    DELETE FROM <fim_table>
    WHERE t0 < cutoff
      AND NOT EXISTS (
            SELECT 1 FROM fim_protected_event e
            WHERE t0 BETWEEN e.t0_from AND e.t0_to
      )

    Parameters
    ----------
    ctx : DBContext
        Database context.
    settings : MaintainerSettings
        Maintainer configuration.
    now : datetime, optional
        Override "now" (UTC). If None, uses current UTC time.

    Returns
    -------
    int
        Number of rows deleted.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    retention = settings.retention_timedelta()
    cutoff = now - retention

    fim_table = ctx.fim_table_full
    schema = settings.effective_schema()
    protected_table_name = f"{schema}.fim_protected_event"

    delete_sql = f"""
        DELETE FROM {fim_table} f
        WHERE f.t0 < :cutoff
          AND NOT EXISTS (
              SELECT 1
              FROM {protected_table_name} e
              WHERE f.t0 BETWEEN e.t0_from::timestamptz AND e.t0_to::timestamptz
          )
    """

    with ctx.engine.begin() as conn:
        try:
            result: Result = conn.execute(
                text(delete_sql),
                {"cutoff": cutoff},
            )
            deleted = result.rowcount or 0
        except SQLAlchemyError as exc:
            logger.error("Cleanup failed: %s", exc)
            raise

    logger.info(
        "Cleanup run: cutoff=%s retention=%s -> deleted %d row(s)",
        cutoff.isoformat(),
        settings.RETENTION,
        deleted,
    )
    return deleted


def _parse_iso_dt(value: str) -> datetime:
    """
    Parse an ISO 8601 datetime string into an aware UTC datetime.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# -----------------------------------------------------------------------------
# FastAPI application
# -----------------------------------------------------------------------------

settings = MaintainerSettings()
ctx = init_db(settings)

app = FastAPI(
    title="NWM FIM Maintainer",
    version="1.0.0",
    description=(
        "Maintains FIM history in PostGIS by pruning old records while "
        "preserving protected events."
    ),
)


@app.on_event("startup")
async def startup_event() -> None:
    """
    Optionally start a background cleanup loop on startup.
    """
    if settings.ENABLED:
        logger.info(
            "Background cleanup loop enabled; interval=%d sec, retention=%s",
            settings.INTERVAL_SEC,
            settings.RETENTION,
        )
        asyncio.create_task(_cleanup_loop())
    else:
        logger.info("Background cleanup loop disabled (MAINTAINER_ENABLED=false).")


async def _cleanup_loop() -> None:
    """
    Periodic cleanup loop.

    Runs forever (until process exit), sleeping INTERVAL_SEC between runs.
    """
    while True:
        try:
            cleanup_fim_history(ctx, settings)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Background cleanup error: %s", exc)
        await asyncio.sleep(settings.INTERVAL_SEC)


# -----------------------------------------------------------------------------
# API Endpoints
# -----------------------------------------------------------------------------

@app.get("/protected-events", response_model=List[ProtectedEvent])
def list_protected_events() -> List[ProtectedEvent]:
    """
    List all protected events (name, from, to, id).
    """
    return load_protected_events(ctx)


@app.post("/protected-events", response_model=ProtectedEvent, status_code=201)
def create_protected_event(ev_in: ProtectedEventIn) -> ProtectedEvent:
    """
    Add a new protected event.

    Example body
    ------------
    {
      "name": "Nov 2025 flood",
      "t0_from": "2025-11-08T00:00:00Z",
      "t0_to": "2025-11-12T23:59:59Z"
    }
    """
    try:
        return add_protected_event(ctx, ev_in)
    except SQLAlchemyError as exc:
        logger.error("Failed to insert protected event: %s", exc)
        raise HTTPException(status_code=500, detail="DB error while inserting event")


@app.post("/cleanup")
def trigger_cleanup() -> dict:
    """
    Manually trigger a cleanup run.

    Returns
    -------
    {
      "deleted_rows": <int>,
      "cutoff": "<iso8601>",
      "retention": "<ISO8601 duration>"
    }
    """
    now = datetime.now(timezone.utc)
    try:
        deleted = cleanup_fim_history(ctx, settings, now=now)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Cleanup failed (see logs).")

    return {
        "deleted_rows": deleted,
        "cutoff": (now - settings.retention_timedelta()).isoformat(),
        "retention": settings.RETENTION,
    }
