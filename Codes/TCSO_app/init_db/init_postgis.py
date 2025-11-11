#!/usr/bin/env python3
"""
Database bootstrap for the FIM schema.

Responsibilities
----------------
- Wait for PostgreSQL to be available.
- Ensure PostGIS extensions exist.
- Ensure target schema exists.
- Ensure base table `fim_nwm` exists with the expected structure.
- Ensure deterministic primary key / indexes on:
    - fim_nwm(id, t0, valid_time, HydroID)
    - nwm_sr_flowlines(id, t0, HydroID)
- Keep everything idempotent and non-destructive.

Design
------
- `id` columns are plain TEXT primary keys, populated by upstream writers
  (e.g. fim.py), not GENERATED columns. This avoids conflicts and keeps the
  key logic in one place.
- All operations use IF NOT EXISTS checks or catalog queries; running this
  script multiple times is safe.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Final

import psycopg2
from psycopg2.extensions import connection as PGConnection


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

LOG_LEVEL: Final[str] = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="[initdb] %(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class DBSettings:
    """Configuration for connecting to PostgreSQL/PostGIS."""

    host: str = os.getenv("PGHOST", "tcso-pgis")
    port: int = int(os.getenv("PGPORT", "5432"))
    user: str = os.getenv("PGUSER", "gis")
    password: str = os.getenv("PGPASSWORD", "gis")
    database: str = os.getenv("PGDATABASE", "gis")
    schema: str = os.getenv("PGSCHEMA", "public")

    connect_timeout: int = int(os.getenv("PGCONNECT_TIMEOUT", "5"))
    max_wait_seconds: int = int(os.getenv("PG_MAX_WAIT", "120"))


SETTINGS = DBSettings()


# ------------------------------------------------------------------------------
# Connection helpers
# ------------------------------------------------------------------------------

def get_connection(autocommit: bool = True) -> PGConnection:
    """
    Create a new PostgreSQL connection.

    Parameters
    ----------
    autocommit:
        If True, enable autocommit for convenient DDL execution.

    Returns
    -------
    psycopg2.extensions.connection
    """
    conn = psycopg2.connect(
        host=SETTINGS.host,
        port=SETTINGS.port,
        user=SETTINGS.user,
        password=SETTINGS.password,
        dbname=SETTINGS.database,
        connect_timeout=SETTINGS.connect_timeout,
    )
    conn.autocommit = autocommit
    return conn


def wait_for_db() -> None:
    """
    Wait until PostgreSQL is reachable or fail after max_wait_seconds.

    Uses simple connect attempts; suitable for init containers / jobs.
    """
    logger.info(
        "Waiting for PostgreSQL at %s:%s (db=%s, max_wait=%ss)",
        SETTINGS.host,
        SETTINGS.port,
        SETTINGS.database,
        SETTINGS.max_wait_seconds,
    )

    deadline = time.time() + SETTINGS.max_wait_seconds

    while True:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            logger.info("PostgreSQL is up.")
            return
        except Exception as exc:  # noqa: BLE001
            if time.time() >= deadline:
                logger.error("PostgreSQL did not become ready in time: %s", exc)
                raise
            logger.warning("PostgreSQL not ready yet: %s; retrying...", exc)
            time.sleep(3)


# ------------------------------------------------------------------------------
# Schema / extension helpers
# ------------------------------------------------------------------------------

def ensure_schema() -> None:
    """
    Ensure the target schema exists (idempotent).
    """
    with get_connection() as conn, conn.cursor() as cur:
        logger.info("Ensuring schema '%s' exists", SETTINGS.schema)
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SETTINGS.schema}";')


def ensure_postgis_extensions() -> None:
    """
    Ensure required PostGIS extensions exist (idempotent).
    """
    extensions = [
        "postgis",
        "postgis_raster",
        "postgis_topology",
    ]

    with get_connection() as conn, conn.cursor() as cur:
        for ext in extensions:
            logger.info("Ensuring extension '%s' exists", ext)
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext};")


# ------------------------------------------------------------------------------
# Introspection helpers
# ------------------------------------------------------------------------------

def table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the configured schema.

    Parameters
    ----------
    table_name:
        Table name without schema.

    Returns
    -------
    bool
    """
    query = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (SETTINGS.schema, table_name))
        return cur.fetchone() is not None


def column_exists(table_name: str, column_name: str) -> bool:
    """
    Check if a column exists on a table in the configured schema.
    """
    query = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (SETTINGS.schema, table_name, column_name))
        return cur.fetchone() is not None


def has_primary_key(table_name: str) -> bool:
    """
    Check whether the given table has any primary key defined.
    """
    query = """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE c.contype = 'p'
          AND n.nspname = %s
          AND t.relname = %s
        LIMIT 1;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (SETTINGS.schema, table_name))
        return cur.fetchone() is not None


# ------------------------------------------------------------------------------
# fim_nwm helpers
# ------------------------------------------------------------------------------

def ensure_fim_nwm_table() -> None:
    """
    Ensure the base table `fim_nwm` exists with the expected minimal structure.

    Notes
    -----
    - Does NOT drop or overwrite existing data.
    - If table already exists, missing columns are not force-added here; that
      should be handled via migrations if the schema drifts.
    """
    table = f'"{SETTINGS.schema}"."fim_nwm"'

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        "HValue"       double precision,
        "HUC"          integer,
        "HydroID"      integer NOT NULL,
        "Shape_Length" double precision,
        "Shape_Area"   double precision,
        "HIndex"       bigint,
        geometry       geometry(MultiPolygon, 5070),
        t0             timestamptz NOT NULL,
        valid_time     timestamptz NOT NULL
    );
    """

    with get_connection() as conn, conn.cursor() as cur:
        logger.info("Ensuring table %s exists", table)
        cur.execute(ddl)


def ensure_fim_nwm_id_and_pk() -> None:
    """
    Ensure `id` column and primary key exist on `fim_nwm`.

    Contract
    --------
    - `id` is TEXT, populated by the writer as:
        <HydroID>_<t0 UTC: YYYYMMDDHHMMZ>_<valid_time UTC: YYYYMMDDHHMMZ>
    - Primary key: `fim_nwm_pkey` on (id)
    """
    table = "fim_nwm"
    fq_table = f'"{SETTINGS.schema}"."{table}"'

    if not column_exists(table, "id"):
        logger.info("Adding id column to %s", fq_table)
        ddl = f'ALTER TABLE {fq_table} ADD COLUMN id text;'
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
    else:
        logger.info("Column %s.id already exists", table)

    if not has_primary_key(table):
        logger.info("Adding primary key constraint fim_nwm_pkey on %s.id", fq_table)
        ddl = f"""
        ALTER TABLE {fq_table}
        ADD CONSTRAINT fim_nwm_pkey PRIMARY KEY (id);
        """
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
    else:
        logger.info("Primary key already defined on %s", fq_table)


def ensure_fim_nwm_indexes() -> None:
    """
    Ensure supporting indexes exist for `fim_nwm`.

    - idx_fim_nwm_t0_valid_hydroid on (t0, valid_time, HydroID)
    """
    fq_table = f'"{SETTINGS.schema}"."fim_nwm"'
    ddl = f"""
    CREATE INDEX IF NOT EXISTS idx_fim_nwm_t0_valid_hydroid
        ON {fq_table} (t0, valid_time, "HydroID");
    """
    with get_connection() as conn, conn.cursor() as cur:
        logger.info(
            "Ensuring index idx_fim_nwm_t0_valid_hydroid exists on %s",
            fq_table,
        )
        cur.execute(ddl)


# ------------------------------------------------------------------------------
# nwm_sr_flowlines helpers
# ------------------------------------------------------------------------------

def ensure_flowlines_id_pk_indexes() -> None:
    """
    Ensure `nwm_sr_flowlines` has `id`, `t0`, PK, and useful index.

    This does **not** create the table; it assumes the static geometry /
    attributes table is loaded separately.

    Contract for writers
    --------------------
    - Writers (e.g. fim.py) insert forecasted attributes into this table with:
        - `HydroID` (integer)
        - `t0`      (timestamptz, NWM cycle time)
        - `id`      TEXT:

              <HydroID>_<t0 UTC: YYYYMMDDHHMMZ>

    - One row per (HydroID, t0) per snapshot.
    """
    table = "nwm_sr_flowlines"
    if not table_exists(table):
        logger.info(
            "Table %s.%s does not exist; skipping flowlines PK/index setup",
            SETTINGS.schema,
            table,
        )
        return

    fq_table = f'"{SETTINGS.schema}"."{table}"'

    # Ensure t0 column exists (needed for PK + id scheme)
    if not column_exists(table, "t0"):
        logger.info("Adding t0 column (timestamptz) to %s", fq_table)
        ddl = f'ALTER TABLE {fq_table} ADD COLUMN t0 timestamptz;'
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
    else:
        logger.info("Column %s.t0 already exists", table)

    # Ensure id column exists
    if not column_exists(table, "id"):
        logger.info("Adding id column to %s", fq_table)
        ddl = f'ALTER TABLE {fq_table} ADD COLUMN id text;'
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
    else:
        logger.info("Column %s.id already exists", table)

    # Ensure primary key on id
    if not has_primary_key(table):
        logger.info(
            "Adding primary key constraint nwm_sr_flowlines_pkey on %s.id",
            fq_table,
        )
        ddl = f"""
        ALTER TABLE {fq_table}
        ADD CONSTRAINT nwm_sr_flowlines_pkey PRIMARY KEY (id);
        """
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(ddl)
    else:
        logger.info("Primary key already defined on %s", fq_table)

    # Ensure index on (t0, HydroID)
    ddl_idx = f"""
    CREATE INDEX IF NOT EXISTS idx_nwm_sr_flowlines_t0_hydroid
        ON {fq_table} (t0, "HydroID");
    """
    with get_connection() as conn, conn.cursor() as cur:
        logger.info(
            "Ensuring index idx_nwm_sr_flowlines_t0_hydroid exists on %s",
            fq_table,
        )
        cur.execute(ddl_idx)


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> None:
    """
    Orchestrate full idempotent initialization.

    Steps
    -----
    1. Wait for PostgreSQL.
    2. Ensure schema exists.
    3. Ensure PostGIS extensions are installed.
    4. Ensure `fim_nwm` base table, PK, and indexes.
    5. Ensure `nwm_sr_flowlines` PK / indexes if table is present.
    """
    wait_for_db()
    ensure_schema()
    ensure_postgis_extensions()

    ensure_fim_nwm_table()
    ensure_fim_nwm_id_and_pk()
    ensure_fim_nwm_indexes()

    ensure_flowlines_id_pk_indexes()

    logger.info("PostGIS initialization complete.")


if __name__ == "__main__":
    main()
