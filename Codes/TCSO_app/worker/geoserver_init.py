#!/usr/bin/env python3
"""
GeoServer / PostGIS bootstrap for TCSO NWM-FIM stack.

This script is intended to run as an init container / sidecar and is idempotent.

Responsibilities
----------------
1. Wait for GeoServer to be reachable.
2. Ensure:
   - workspace:      tcso
   - datastore:      tcso-pgis (PostGIS, points at tcso-pgis service)
3. Ensure base feature types (backed by PostGIS tables):
   - nwm_sr_flowlines
   - fim_nwm
4. Ensure PostGIS views (server-side) + publish them as WFS/WMS:
   - fim_nowcast
       Latest t0 and earliest valid_time for that t0.
   - fim_max
       Latest t0; per HydroID: max HIndex, tie -> earliest valid_time.
       Guaranteed max 1 row per HydroID.
5. All published layers use EPSG:5070 as native/declared SRS.

Environment
-----------
Required (with defaults suitable for docker-compose.yaml in this repo):

GEOSERVER_URL            default: http://tcso-geoserver:8080/geoserver
GEOSERVER_USER           default: admin
GEOSERVER_PASSWORD       default: geoserver

POSTGIS_HOST             default: tcso-pgis
POSTGIS_PORT             default: 5432
POSTGIS_DB               default: tcso
POSTGIS_USER             default: tcso
POSTGIS_PASSWORD         default: tcso
POSTGIS_SCHEMA           default: public

WORKSPACE_NAME           default: tcso
DATASTORE_NAME           default: tcso-pgis

FLOWLINES_TABLE          default: nwm_sr_flowlines
FIM_TABLE                default: fim_nwm

Notes
-----
- Uses real PostGIS views (`CREATE OR REPLACE VIEW`) instead of GeoServer "virtual tables".
- Safe to run multiple times; will recreate views and no-op for existing layers.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2
import requests
from psycopg2.extensions import connection as PGConnection

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("INIT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="[init] %(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("geoserver_init")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@dataclass
class Settings:
    """Configuration loaded from environment variables."""

    gs_url: str = os.getenv("GEOSERVER_URL", "http://tcso-geoserver:8080/geoserver")
    gs_user: str = os.getenv("GEOSERVER_USER", "admin")
    gs_password: str = os.getenv("GEOSERVER_PASSWORD", "geoserver")

    # IMPORTANT: match docker-compose service name (hyphen, no underscore)
    pg_host: str = os.getenv("POSTGIS_HOST", "tcso-pgis")
    pg_port: int = int(os.getenv("POSTGIS_PORT", "5432"))
    pg_db: str = os.getenv("POSTGIS_DB", "gis")
    pg_user: str = os.getenv("POSTGIS_USER", "gis")
    pg_password: str = os.getenv("POSTGIS_PASSWORD", "gis")
    pg_schema: str = os.getenv("POSTGIS_SCHEMA", "public")

    workspace: str = os.getenv("GEOSERVER_WORKSPACE", "tcso")
    gs_datastore: str = os.getenv("GEOSERVER_DATASTORE", "tcso-pgis")
    srs: str = os.getenv("GEOSERVER_DEFAULT_CRS", "EPSG:5070")

    flowlines_table: str = os.getenv("FLOWLINES_TABLE", "nwm_sr_flowlines")
    fim_table: str = os.getenv("FIM_TABLE", "fim_nwm")

    # point-status base tables (written by the pipeline)
    addresspoints_table: str = os.getenv("ADDRESSPOINTS_TABLE", "addresspoints")
    lowwater_table: str = os.getenv("LOWWATERCROSSINGS_TABLE", "lowwatercrossings")

    # materialized view names (published as layers)
    addresspoints_nowcast_view: str = os.getenv("ADDRESSPOINTS_NOWCAST_VIEW", "addresspoints_nowcast")
    lowwater_nowcast_view: str = os.getenv("LOWWATER_NOWCAST_VIEW", "lowwatercrossings_nowcast")

    @property
    def gs_auth(self) -> tuple[str, str]:
        return self.gs_user, self.gs_password


SETTINGS = Settings()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pg_connect() -> PGConnection:
    """
    Create a new psycopg2 connection to PostGIS.

    Returns
    -------
    PGConnection
    """
    return psycopg2.connect(
        host=SETTINGS.pg_host,
        port=SETTINGS.pg_port,
        dbname=SETTINGS.pg_db,
        user=SETTINGS.pg_user,
        password=SETTINGS.pg_password,
    )


def _gs_request(
    method: str,
    path: str,
    *,
    expected: Optional[int] = None,
    **kwargs: Any,
) -> requests.Response:
    """
    Issue a REST call to GeoServer.

    Parameters
    ----------
    method:
        HTTP method (GET, POST, PUT, DELETE).
    path:
        REST path, relative to GeoServer root (e.g. ``"/rest/workspaces.json"``).
    expected:
        Optional HTTP status code to assert. If provided and mismatched,
        an HTTPError is raised.
    **kwargs:
        Passed directly to ``requests.request``.

    Returns
    -------
    Response

    Raises
    ------
    HTTPError
        If `expected` is given and the status code does not match.
    """
    url = f"{SETTINGS.gs_url}{path}"
    auth = (SETTINGS.gs_user, SETTINGS.gs_password)
    headers = kwargs.pop("headers", {})
    if "json" in kwargs and "Content-Type" not in headers:
        headers["Content-Type"] = "application/json"

    resp = requests.request(method, url, auth=auth, headers=headers, **kwargs)

    if expected is not None and resp.status_code != expected:
        logger.error(
            "GeoServer %s %s -> %s: %s",
            method,
            url,
            resp.status_code,
            resp.text[:400],
        )
        resp.raise_for_status()

    return resp


def wait_for_postgis(timeout: int = 300, interval: int = 5) -> None:
    """
    Block until PostGIS is reachable and accepting connections.

    Parameters
    ----------
    timeout:
        Maximum time in seconds to wait.
    interval:
        Polling interval in seconds.
    """
    end = time.time() + timeout

    while time.time() < end:
        try:
            with _pg_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            logger.info("PostGIS is up")
            return
        except Exception as exc:  # noqa: BLE001
            logger.info("Waiting for PostGIS... (%s)", exc)
            time.sleep(interval)

    logger.error("PostGIS did not become ready within %s seconds", timeout)
    sys.exit(1)


def wait_for_geoserver(timeout: int = 300, interval: int = 5) -> None:
    """
    Block until GeoServer's REST endpoint is reachable.

    Parameters
    ----------
    timeout:
        Maximum time in seconds to wait.
    interval:
        Polling interval in seconds.
    """
    end = time.time() + timeout
    url = f"{SETTINGS.gs_url}/rest/about/version.json"
    auth = (SETTINGS.gs_user, SETTINGS.gs_password)

    while time.time() < end:
        try:
            r = requests.get(url, auth=auth, timeout=5)
            if r.ok:
                logger.info("GeoServer is up")
                return
        except Exception:
            pass
        logger.info("Waiting for GeoServer...")
        time.sleep(interval)

    logger.error("GeoServer did not become ready within %s seconds", timeout)
    sys.exit(1)


# ---------------------------------------------------------------------------
# GeoServer: workspace / datastore / layer helpers
# ---------------------------------------------------------------------------


def ensure_workspace() -> None:
    """
    Create the GeoServer workspace if it does not already exist.
    """
    ws = SETTINGS.workspace
    r = _gs_request("GET", f"/rest/workspaces/{ws}.json")
    if r.status_code == 200:
        logger.info("Workspace '%s' exists", ws)
        return

    if r.status_code != 404:
        logger.error(
            "Failed to query workspace '%s': %s %s", ws, r.status_code, r.text[:400]
        )
        r.raise_for_status()

    payload = {"workspace": {"name": ws}}
    r = _gs_request(
        "POST",
        "/rest/workspaces",
        expected=201,
        json=payload,
    )
    logger.info("Created workspace '%s'", ws)


def ensure_datastore() -> None:
    """
    Create or update the PostGIS datastore so that it matches SETTINGS.

    Idempotent:
    - If datastore does not exist -> create it.
    - If it exists -> overwrite connectionParameters (host, db, user, etc.)
      to ensure they are correct (e.g. after hostname changes).
    """
    ws = SETTINGS.workspace
    ds = SETTINGS.gs_datastore

    # Desired connection parameters
    conn_params: Dict[str, str] = {
        "dbtype": "postgis",
        "host": SETTINGS.pg_host,
        "port": str(SETTINGS.pg_port),
        "database": SETTINGS.pg_db,
        "user": SETTINGS.pg_user,
        "passwd": SETTINGS.pg_password,
        "schema": SETTINGS.pg_schema,
        "Expose primary keys": "true",
    }

    # Helper to build GS REST payload
    def build_payload() -> Dict[str, Any]:
        return {
            "dataStore": {
                "name": ds,
                "enabled": True,
                "connectionParameters": {
                    "entry": [{"@key": k, "$": v} for k, v in conn_params.items()]
                },
            }
        }

    # Check if datastore exists
    r = _gs_request("GET", f"/rest/workspaces/{ws}/datastores/{ds}.json")
    if r.status_code == 404:
        # Create new datastore
        logger.info("Datastore '%s' does not exist, creating", ds)
        _gs_request(
            "POST",
            f"/rest/workspaces/{ws}/datastores",
            expected=201,
            json=build_payload(),
        )
        logger.info("Created datastore '%s'", ds)
        return

    if r.status_code != 200:
        logger.error(
            "Failed to query datastore '%s': %s %s",
            ds,
            r.status_code,
            r.text[:400],
        )
        r.raise_for_status()

    # Datastore exists: check if it matches desired connectionParameters
    try:
        data = r.json().get("dataStore", {})
        entries = data.get("connectionParameters", {}).get("entry", [])
        existing = {e.get("@key"): e.get("$") for e in entries if "@key" in e}

        mismatch = any(existing.get(k) != v for k, v in conn_params.items())
        if mismatch:
            logger.info(
                "Datastore '%s' exists but connectionParameters differ; updating",
                ds,
            )
            _gs_request(
                "PUT",
                f"/rest/workspaces/{ws}/datastores/{ds}.json",
                expected=200,
                json=build_payload(),
            )
            logger.info("Updated datastore '%s' connectionParameters", ds)
        else:
            logger.info("Datastore '%s' exists and is correctly configured", ds)
    except Exception as exc:
        logger.error(
            "Failed to inspect/update datastore '%s', attempting overwrite: %s",
            ds,
            exc,
        )
        _gs_request(
            "PUT",
            f"/rest/workspaces/{ws}/datastores/{ds}.json",
            expected=200,
            json=build_payload(),
        )
        logger.info("Overwrote datastore '%s' connectionParameters", ds)


def ensure_featuretype(name: str, srs: Optional[str] = None) -> None:
    """
    Publish a PostGIS table/view as a GeoServer feature type if not already present.

    Parameters
    ----------
    name:
        Name of the PostGIS table or view (in `POSTGIS_SCHEMA`) and GeoServer layer.
    srs:
        EPSG code string for `nativeCRS` / `srs`. Defaults to SETTINGS.srs.
    """
    ws = SETTINGS.workspace
    ds = SETTINGS.gs_datastore
    srs = srs or SETTINGS.srs

    # Check if layer already exists
    r = _gs_request(
        "GET",
        f"/rest/workspaces/{ws}/datastores/{ds}/featuretypes/{name}.json",
    )
    if r.status_code == 200:
        logger.info("Layer '%s' already published", name)
        return

    if r.status_code not in (404,):
        logger.error(
            "Failed to query featuretype '%s': %s %s", name, r.status_code, r.text[:400]
        )
        r.raise_for_status()

    payload = {
        "featureType": {
            "name": name,
            "nativeName": name,
            "srs": srs,
            "nativeCRS": srs,
            "title": name,
        }
    }

    _gs_request(
        "POST",
        f"/rest/workspaces/{ws}/datastores/{ds}/featuretypes",
        expected=201,
        json=payload,
    )
    logger.info("Published feature type '%s' (SRS=%s)", name, srs)


def ensure_featuretype_pk(name: str, pk_column: str = "id") -> None:
    """
    Ensure GeoServer knows which attribute is the primary key for a feature type.

    - Especially important for PostGIS views, where PK cannot be auto-detected.
    - Idempotent: if already set to pk_column, does nothing.
    - If the feature type does not exist yet (404), logs and returns;
      caller is responsible for calling ensure_featuretype(...) first.
    """
    ws = SETTINGS.workspace
    ds = SETTINGS.gs_datastore

    url = f"{SETTINGS.gs_url}/rest/workspaces/{ws}/datastores/{ds}/featuretypes/{name}.json"

    # Direct requests.get so we can handle 404 gracefully
    resp = requests.get(url, auth=(SETTINGS.gs_user, SETTINGS.gs_password))
    if resp.status_code == 404:
        logger.info(
            "FeatureType '%s' not found when setting PK; skipping (will be created first).",
            name,
        )
        return
    if resp.status_code != 200:
        logger.error(
            "Failed to read FeatureType '%s' when setting PK: %s %s",
            name,
            resp.status_code,
            resp.text[:400],
        )
        resp.raise_for_status()

    ft = resp.json().get("featureType", {})
    metadata = ft.get("metadata") or {}
    pk_meta = metadata.get("primaryKeyMetadata") or {}
    current = pk_meta.get("keyColumn")

    if current == pk_column:
        logger.info(
            "FeatureType '%s': primaryKeyMetadata.keyColumn already '%s'",
            name,
            pk_column,
        )
        return

    metadata["primaryKeyMetadata"] = {
        "keyColumn": pk_column,
        "generated": False,
        "unique": True,
    }
    ft["metadata"] = metadata

    put_resp = requests.put(
        url,
        auth=(SETTINGS.gs_user, SETTINGS.gs_password),
        headers={"Content-Type": "application/json"},
        json={"featureType": ft},
    )
    if put_resp.status_code not in (200, 201):
        logger.error(
            "Failed to update FeatureType '%s' PK metadata: %s %s",
            name,
            put_resp.status_code,
            put_resp.text[:400],
        )
        put_resp.raise_for_status()

    logger.info(
        "FeatureType '%s': set primaryKeyMetadata.keyColumn='%s'",
        name,
        pk_column,
    )



# ---------------------------------------------------------------------------
# PostGIS view helpers
# ---------------------------------------------------------------------------
def ensure_materialized_view(name: str, sql: str, pk_column: str = "id") -> None:
    """
    Create or refresh a materialized view with a UNIQUE index on pk_column.

    Behavior
    --------
    - If a regular VIEW with this name exists in the schema, drop it.
    - If the MATERIALIZED VIEW does not exist, create it with `sql`.
    - If it exists, REFRESH it (non-concurrently).
    - Ensure a UNIQUE index on pk_column.

    Safe to run on every startup.

    Note
    ----
    PostgreSQL does not support ADD CONSTRAINT PRIMARY KEY on materialized views.
    A UNIQUE index on `pk_column` is sufficient for GeoServer's primary key
    detection and for our own guarantees.
    """
    schema = SETTINGS.pg_schema
    mv_fq = f'"{schema}"."{name}"'
    idx_name = f"{name}_{pk_column}_key"

    with _pg_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Drop plain VIEW if present
            cur.execute(
                """
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = %s
                  AND table_name = %s
                """,
                (schema, name),
            )
            if cur.fetchone():
                logger.info(
                    "Dropping existing VIEW %s.%s in favor of MATERIALIZED VIEW",
                    schema,
                    name,
                )
                cur.execute(f'DROP VIEW "{schema}"."{name}";')

            # Check for existing MATVIEW
            cur.execute(
                """
                SELECT 1
                FROM pg_matviews
                WHERE schemaname = %s
                  AND matviewname = %s
                """,
                (schema, name),
            )
            exists = cur.fetchone() is not None

            if not exists:
                logger.info("Creating materialized view %s", mv_fq)
                cur.execute(f"CREATE MATERIALIZED VIEW {mv_fq} AS {sql}")
            else:
                logger.info("Refreshing materialized view %s", mv_fq)
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv_fq}")

            # Ensure UNIQUE index on pk_column
            cur.execute(
                """
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'i'
                  AND c.relname = %s
                  AND n.nspname = %s
                """,
                (idx_name, schema),
            )
            if not cur.fetchone():
                logger.info(
                    "Creating UNIQUE index %s on %s(%s)",
                    idx_name,
                    mv_fq,
                    pk_column,
                )
                cur.execute(
                    f'CREATE UNIQUE INDEX "{idx_name}" '
                    f'ON {mv_fq} ("{pk_column}");'
                )



def ensure_db_view(name: str, sql_body: str) -> None:
    """
    Create or replace a PostGIS view in the configured schema.

    Parameters
    ----------
    name:
        View name (without schema).
    sql_body:
        SQL SELECT body (without leading 'CREATE VIEW ... AS').
        Should be a valid SELECT query.

    Notes
    -----
    - Function wraps `CREATE OR REPLACE VIEW schema.name AS {sql_body}`.
    - No-op if creation succeeds; always overwrites existing definition.
    """
    view_fq = f"{SETTINGS.pg_schema}.{name}"

    # Preserve formatting & comments; just trim outer whitespace and a trailing ';'
    sql_clean = sql_body.strip()
    if sql_clean.endswith(";"):
        sql_clean = sql_clean[:-1]

    ddl = f"CREATE OR REPLACE VIEW {view_fq} AS {sql_clean}"

    with _pg_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            logger.info("Creating or replacing view %s", view_fq)
            cur.execute(ddl)


# ---------------------------------------------------------------------------
# View definitions
# ---------------------------------------------------------------------------
def build_nowcast_sql() -> str:
    """
    Base SQL (no CREATE) for fim_nowcast materialized view.

    - Latest t0
    - For that t0, earliest valid_time
    - One row per HydroID by highest HIndex.
    """
    fim = f'{SETTINGS.pg_schema}."{SETTINGS.fim_table}"'

    return f"""
WITH latest AS (
    SELECT MAX(t0) AS max_t0 FROM {fim}
),
earliest AS (
    SELECT MIN(valid_time) AS min_valid_time
    FROM {fim}, latest
    WHERE t0 = latest.max_t0
),
ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY "HydroID"
            ORDER BY
                "HIndex" DESC,
                t0 DESC,
                valid_time ASC
        ) AS rn,
        id,
        geometry,
        "HValue",
        "HUC",
        "HydroID",
        "Shape_Length",
        "Shape_Area",
        "HIndex",
        t0,
        valid_time
    FROM {fim}, latest, earliest
    WHERE t0 = latest.max_t0
      AND valid_time = earliest.min_valid_time
)
SELECT DISTINCT ON (id)
    id,
    geometry,
    "HValue",
    "HUC",
    "HydroID",
    "Shape_Length",
    "Shape_Area",
    "HIndex",
    t0,
    valid_time
FROM ranked
WHERE rn = 1
ORDER BY id;
"""

def build_max_sql() -> str:
    """
    Base SQL for fim_max materialized view.

    For each HydroID across all snapshots:
      - Highest HIndex
      - tie: latest t0, then earliest valid_time.
    """
    fim = f'{SETTINGS.pg_schema}."{SETTINGS.fim_table}"'

    return f"""
WITH ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY "HydroID"
            ORDER BY
                "HIndex" DESC,
                t0 DESC,
                valid_time ASC
        ) AS rn,
        id,
        geometry,
        "HValue",
        "HUC",
        "HydroID",
        "Shape_Length",
        "Shape_Area",
        "HIndex",
        t0,
        valid_time
    FROM {fim}
)
SELECT DISTINCT ON (id)
    id,
    geometry,
    "HValue",
    "HUC",
    "HydroID",
    "Shape_Length",
    "Shape_Area",
    "HIndex",
    t0,
    valid_time
FROM ranked
WHERE rn = 1
ORDER BY id;
"""
def _mv_nowcast_sql_for_points(table_name: str) -> str:
    """
    Build SQL body for a points 'nowcast' materialized view:
      - Latest t0 across the base table
      - For that t0, earliest valid_time
      - DISTINCT ON (id) to guarantee row-uniqueness by id
      - Explicitly selects columns to avoid leaking helper fields (max_t0/min_valid_time)
    """
    schema = SETTINGS.pg_schema
    base = f'"{schema}"."{table_name}"'
    # Column order mirrors the lean status tables.
    base_cols = [
        'id',
        '"Situation"',
        '"OBJECTID"',
        't0',
        'valid_time',
    ]
    col_list = ", ".join(base_cols)

    return f"""
WITH latest AS (
  SELECT MAX(t0) AS max_t0 FROM {base}
),
earliest AS (
  SELECT MIN(valid_time) AS min_valid_time
  FROM {base}, latest
  WHERE t0 = latest.max_t0
)
SELECT DISTINCT ON (id)
    {col_list}
FROM {base}, latest, earliest
WHERE {base}.t0 = latest.max_t0
  AND {base}.valid_time = earliest.min_valid_time
  AND {base}."Situation" = 'Flooded'
ORDER BY id;
"""



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point: orchestrate GeoServer + PostGIS initialization."""
    wait_for_geoserver()
    wait_for_postgis()

    # small grace period so both services finish booting
    time.sleep(30)

    # 1) Workspace & datastore
    ensure_workspace()
    ensure_datastore()

    # 2) Base tables (these are written by fim.py pipeline)
    ensure_featuretype(SETTINGS.flowlines_table)
    ensure_featuretype(SETTINGS.fim_table)

    # 3) Publish materialized views (created/refreshed by init_postgis.py)
    ap_mv = SETTINGS.addresspoints_nowcast_view
    lwc_mv = SETTINGS.lowwater_nowcast_view

    ensure_featuretype("fim_nowcast")
    ensure_featuretype("fim_max")
    ensure_featuretype_pk("fim_nowcast", "id")
    ensure_featuretype_pk("fim_max", "id")

    # 4) AddressPoints & LowWaterCrossings nowcast MVs
    ensure_featuretype(ap_mv)
    ensure_featuretype(lwc_mv)
    ensure_featuretype_pk(ap_mv, "id")
    ensure_featuretype_pk(lwc_mv, "id")


    logger.info("GeoServer/PostGIS initialization completed successfully.")


if __name__ == "__main__":
    main()
