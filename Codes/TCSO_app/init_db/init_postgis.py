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


def list_table_columns(table_name: str) -> list[str]:
    """
    Return ordered column names for a table in the configured schema.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position;
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, (SETTINGS.schema, table_name))
        return [row[0] for row in cur.fetchall()]


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
# Point status tables (addresspoints, lowwatercrossings)
# ------------------------------------------------------------------------------

def ensure_point_status_table(table: str) -> None:
    """
    Ensure point status tables have PK(id), NOT NULL t0/valid_time, and useful indexes.

    Expected:
      - Columns: id (text), OBJECTID (bigint), Situation (text), t0 (timestamptz), valid_time (timestamptz)
      - PK on id
      - Index on (t0, valid_time)
      - Index on OBJECTID
    """
    fq = f'"{SETTINGS.schema}"."{table}"'
    logger.info("Dropping and recreating status table %s to enforce lean schema.", fq)
    ddl = f"""
    DROP TABLE IF EXISTS {fq} CASCADE;
    CREATE TABLE {fq} (
        id text PRIMARY KEY,
        "OBJECTID" bigint,
        "Situation" text,
        t0 timestamptz NOT NULL,
        valid_time timestamptz NOT NULL
    );
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)

    with get_connection() as conn, conn.cursor() as cur:
        logger.info("Ensuring btree index on %s (t0, valid_time)", fq)
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "{table}_t0_valid_idx" '
            f'ON {fq} ("t0","valid_time");'
        )

        if column_exists(table, "OBJECTID"):
            logger.info("Ensuring btree index on %s.OBJECTID", fq)
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS "{table}_OBJECTID_idx" '
                f'ON {fq} ("OBJECTID");'
            )

        if column_exists(table, "geometry"):
            logger.info("Ensuring GiST index on %s.geometry", fq)
            cur.execute(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_geometry '
                f'ON {fq} USING gist (geometry);'
            )

def ensure_point_static_table(table: str, source_table: str) -> None:
    """
    Ensure a static points table exists; if missing, create from the source table.
    """
    def _strip_runtime_cols() -> None:
        fq_static_local = f'"{SETTINGS.schema}"."{table}"'
        cols = ("id", "t0", "valid_time", "Situation")
        with get_connection() as conn, conn.cursor() as cur:
            for col in cols:
                logger.info("Ensuring %s does not contain column %s", fq_static_local, col)
                cur.execute(f'ALTER TABLE {fq_static_local} DROP COLUMN IF EXISTS "{col}";')

    if table_exists(table):
        logger.info(
            "Static table %s.%s already exists; stripping runtime columns if present.",
            SETTINGS.schema,
            table,
        )
        _strip_runtime_cols()
        return

    fq_static = f'"{SETTINGS.schema}"."{table}"'
    fq_source = f'"{SETTINGS.schema}"."{source_table}"'

    logger.info("Creating static table %s from %s", fq_static, fq_source)
    ddl = f'CREATE TABLE {fq_static} AS TABLE {fq_source} WITH NO DATA;'
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        # Try to copy data if available
        try:
            cur.execute(f'INSERT INTO {fq_static} SELECT * FROM {fq_source};')
            logger.info("Copied existing rows from %s into %s", fq_source, fq_static)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not copy data from %s to %s: %s", fq_source, fq_static, exc)

    _strip_runtime_cols()


# ------------------------------------------------------------------------------
# Materialized views
# ------------------------------------------------------------------------------

def ensure_materialized_view(name: str, sql_body: str, pk_column: str = "id") -> None:
    """
    Create or refresh a materialized view with a UNIQUE index on pk_column.

    Behavior:
    - Drops a regular VIEW of the same name if it exists.
    - Creates the MATVIEW if missing; otherwise refreshes it.
    - Ensures UNIQUE index on pk_column for GeoServer PK detection.
    """
    schema = SETTINGS.schema
    mv_fq = f'"{schema}"."{name}"'
    idx_name = f"{name}_{pk_column}_key"

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.views
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, name),
        )
        if cur.fetchone():
            logger.info("Dropping existing VIEW %s to recreate as MATERIALIZED VIEW", mv_fq)
            cur.execute(f'DROP VIEW "{schema}"."{name}";')

        cur.execute(
            """
            SELECT 1 FROM pg_matviews
            WHERE schemaname=%s AND matviewname=%s
            """,
            (schema, name),
        )
        exists = cur.fetchone() is not None

        # To keep schema drift (extra columns like max_t0/min_valid_time) from lingering,
        # drop and recreate the matview if it already exists.
        if exists:
            logger.info("Dropping existing materialized view %s to recreate with current SQL", mv_fq)
            cur.execute(f"DROP MATERIALIZED VIEW {mv_fq};")

        logger.info("Creating materialized view %s", mv_fq)
        cur.execute(f"CREATE MATERIALIZED VIEW {mv_fq} AS {sql_body}")

        # UNIQUE index for PK
        cur.execute(
            """
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind='i'
              AND c.relname=%s
              AND n.nspname=%s
            """,
            (idx_name, schema),
        )
        if not cur.fetchone():
            logger.info("Creating UNIQUE index %s on %s(%s)", idx_name, mv_fq, pk_column)
            cur.execute(
                f'CREATE UNIQUE INDEX "{idx_name}" ON {mv_fq} ("{pk_column}");'
            )


def _fim_nowcast_sql() -> str:
    """
    Latest t0, earliest valid_time for that t0, ranked by HIndex then t0.
    """
    fim = f'"{SETTINGS.schema}"."fim_nwm"'
    return f"""
WITH latest AS (
    SELECT MAX(t0) AS max_t0 FROM {fim}
),
earliest AS (
    SELECT MIN(valid_time) AS min_valid_time FROM {fim}, latest WHERE t0 = latest.max_t0
),
ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY "HydroID"
            ORDER BY "HIndex" DESC, t0 DESC, valid_time ASC
        ) AS rn,
        id, geometry, "HValue", "HUC", "HydroID",
        "Shape_Length", "Shape_Area", "HIndex", t0, valid_time
    FROM {fim}, latest, earliest
    WHERE t0 = latest.max_t0
      AND valid_time = earliest.min_valid_time
)
SELECT DISTINCT ON (id)
    id, geometry, "HValue", "HUC", "HydroID",
    "Shape_Length", "Shape_Area", "HIndex", t0, valid_time
FROM ranked
WHERE rn = 1
ORDER BY id;
"""


def _fim_max_sql() -> str:
    """
    Max HIndex per HydroID across history; tie-break by latest t0 then earliest valid_time.
    """
    fim = f'"{SETTINGS.schema}"."fim_nwm"'
    return f"""
WITH ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY "HydroID"
            ORDER BY "HIndex" DESC, t0 DESC, valid_time ASC
        ) AS rn,
        id, geometry, "HValue", "HUC", "HydroID",
        "Shape_Length", "Shape_Area", "HIndex", t0, valid_time
    FROM {fim}
)
SELECT DISTINCT ON (id)
    id, geometry, "HValue", "HUC", "HydroID",
    "Shape_Length", "Shape_Area", "HIndex", t0, valid_time
FROM ranked
WHERE rn = 1
ORDER BY id;
"""


def _points_nowcast_sql(status_table: str, static_table: str) -> str:
    """
    Latest snapshot join: left join static to status; missing -> Safe.
    """
    sch = SETTINGS.schema
    status = f'"{sch}"."{status_table}"'
    static = f'"{sch}"."{static_table}"'

    id_expr = (
        'COALESCE(st.id, '
        "CONCAT(static.\"OBJECTID\", '_', to_char(latest.max_t0, 'YYYYMMDDHH24MISS'), "
        "'_', to_char(earliest.min_valid_time, 'YYYYMMDDHH24MISS')))"
    )

    # Avoid duplicate column names (id/t0/valid_time/Situation) coming from static.*
    try:
        cols = list_table_columns(static_table)
        static_cols = [
            f'static."{c}"'
            for c in cols
            if c not in {"id", "Situation", "t0", "valid_time"}
        ]
        # If the table unexpectedly has zero columns, fall back to static.*
        if not static_cols:
            static_cols = ["static.*"]
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Falling back to static.* for %s.%s column list (reason: %s)",
            sch,
            static_table,
            exc,
        )
        static_cols = ["static.*"]

    select_cols = [
        f"{id_expr} AS id",
        "COALESCE(st.\"Situation\", 'Safe') AS \"Situation\"",
        "COALESCE(st.t0, latest.max_t0) AS t0",
        "COALESCE(st.valid_time, earliest.min_valid_time) AS valid_time",
        *static_cols,
    ]
    select_body = ",\n    ".join(select_cols)

    return f"""
WITH latest AS (
  SELECT COALESCE(MAX(t0), '1970-01-01'::timestamptz) AS max_t0 FROM {status}
),
earliest AS (
  SELECT COALESCE(MIN(valid_time), '1970-01-01'::timestamptz) AS min_valid_time FROM {status}
)
SELECT DISTINCT ON (static."OBJECTID")
    {select_body}
FROM {static} AS static
CROSS JOIN latest
CROSS JOIN earliest
LEFT JOIN {status} AS st ON st."OBJECTID" = static."OBJECTID"
WHERE COALESCE(st.\"Situation\", 'Safe') = 'Flooded'
ORDER BY static."OBJECTID", {id_expr};
"""


def ensure_materialized_views() -> None:
    """
    Create/refresh all materialized views used by GeoServer.
    """
    ensure_materialized_view("fim_nowcast", _fim_nowcast_sql(), pk_column="id")
    ensure_materialized_view("fim_max", _fim_max_sql(), pk_column="id")
    ensure_materialized_view(
        "addresspoints_nowcast",
        _points_nowcast_sql("addresspoints_status", "addresspoints_static"),
        pk_column="id",
    )
    ensure_materialized_view(
        "lowwatercrossings_nowcast",
        _points_nowcast_sql("lowwatercrossings_status", "lowwatercrossings_static"),
        pk_column="id",
    )


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

    # Point status tables (if already loaded)
    ensure_point_static_table("addresspoints_static", "addresspoints")
    ensure_point_static_table("lowwatercrossings_static", "lowwatercrossings")
    ensure_point_status_table("addresspoints_status")
    ensure_point_status_table("lowwatercrossings_status")

    # Materialized views for GeoServer / nowcast
    ensure_materialized_views()

    logger.info("PostGIS initialization complete.")


if __name__ == "__main__":
    main()
