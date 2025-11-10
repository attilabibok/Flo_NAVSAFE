#!/usr/bin/env python3
import os
import time
from typing import Tuple

import requests

from fim import PipelineSettings

# GeoServer config (with sensible defaults for kartoza/geoserver)
GEOSERVER_URL = os.getenv("GEOSERVER_URL", "http://tcso-geoserver:8080/geoserver")
GEOSERVER_USER = os.getenv("GEOSERVER_USER", "admin")
GEOSERVER_PASSWORD = os.getenv("GEOSERVER_PASSWORD", "geoserver")

GEOSERVER_WORKSPACE = os.getenv("GEOSERVER_WORKSPACE", "tcso")
GEOSERVER_DATASTORE = os.getenv("GEOSERVER_DATASTORE", "tcso_postgis")

# PostGIS / datastore connection (should match tcso_pgis service)
PG_HOST = os.getenv("PG_HOST", "tcso_pgis")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "gis")
PG_USER = os.getenv("PG_USER", "gis")
PG_PASS = os.getenv("PG_PASS", "gis")

# From fim.py (OUT_* settings)
settings = PipelineSettings.load()
PG_SCHEMA = settings.out.PG_SCHEMA
FLOWLINES_TABLE = settings.out.PG_FLOWLINES_TABLE   # e.g. nwm_sr_flowlines
FIM_TABLE = settings.out.PG_FIM_TABLE               # e.g. fim_nwm

AUTH = (GEOSERVER_USER, GEOSERVER_PASSWORD)
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def wait_for_geoserver(timeout: int = 300, interval: int = 5):
    deadline = time.time() + timeout
    url = f"{GEOSERVER_URL}/rest/about/version.json"
    while time.time() < deadline:
        try:
            r = requests.get(url, auth=AUTH, timeout=5)
            if r.status_code < 500:
                print("[init] GeoServer is up")
                return
        except requests.RequestException:
            pass
        print("[init] Waiting for GeoServer...")
        time.sleep(interval)
    raise RuntimeError("GeoServer did not become ready in time")


def ensure_workspace():
    url = f"{GEOSERVER_URL}/rest/workspaces/{GEOSERVER_WORKSPACE}.json"
    r = requests.get(url, auth=AUTH, headers={"Accept": "application/json"})

    if r.status_code == 200:
        print(f"[init] Workspace '{GEOSERVER_WORKSPACE}' exists")
        return

    # Some GeoServer builds use 404, some (annoyingly) 400 for "not found"/bad name
    if r.status_code not in (400, 404):
        print(f"[init] Unexpected response checking workspace: {r.status_code} {r.text}")
        r.raise_for_status()

    # Create workspace
    payload = {"workspace": {"name": GEOSERVER_WORKSPACE}}
    create_url = f"{GEOSERVER_URL}/rest/workspaces"
    r = requests.post(create_url, json=payload, headers=JSON_HEADERS, auth=AUTH)
    if r.status_code not in (200, 201):
        print(f"[init] Failed to create workspace: {r.status_code} {r.text}")
        r.raise_for_status()
    print(f"[init] Created workspace '{GEOSERVER_WORKSPACE}'")


def ensure_postgis_datastore():
    base = f"{GEOSERVER_URL}/rest/workspaces/{GEOSERVER_WORKSPACE}/datastores"
    url = f"{base}/{GEOSERVER_DATASTORE}.json"

    r = requests.get(url, auth=AUTH, headers={"Accept": "application/json"})
    if r.status_code == 200:
        print(f"[init] Datastore '{GEOSERVER_DATASTORE}' exists")
        return

    if r.status_code not in (400, 404):
        print(f"[init] Unexpected response checking datastore: {r.status_code} {r.text}")
        r.raise_for_status()

    payload = {
        "dataStore": {
            "name": GEOSERVER_DATASTORE,
            "connectionParameters": {
                "host": PG_HOST,
                "port": str(PG_PORT),
                "database": PG_DB,
                "user": PG_USER,
                "passwd": PG_PASS,
                "dbtype": "postgis",
                "schema": PG_SCHEMA,
                "Expose primary keys": "true",
            },
        }
    }

    r = requests.post(base, json=payload, headers=JSON_HEADERS, auth=AUTH)
    if r.status_code not in (200, 201):
        print(f"[init] Failed to create datastore: {r.status_code} {r.text}")
        r.raise_for_status()
    print(f"[init] Created datastore '{GEOSERVER_DATASTORE}'")

def ensure_featuretype(table: str, srs: str = "EPSG:4326"):
    """
    Publish a PostGIS table as a layer (WMS+WFS).
    Idempotent: if it exists, do nothing.
    """
    name = table
    base = f"{GEOSERVER_URL}/rest/workspaces/{GEOSERVER_WORKSPACE}/datastores/{GEOSERVER_DATASTORE}"

    check_url = f"{base}/featuretypes/{name}.json"
    r = requests.get(check_url, auth=AUTH, headers={"Accept": "application/json"})

    if r.status_code == 200:
        print(f"[init] Layer '{name}' already published")
        return

    if r.status_code not in (400, 404):
        print(f"[init] Unexpected response checking featuretype {name}: {r.status_code} {r.text}")
        r.raise_for_status()

    payload = {
        "featureType": {
            "name": name,
            "nativeName": name,
            "title": name,
            "srs": srs,
        }
    }

    create_url = f"{base}/featuretypes"
    r = requests.post(create_url, json=payload, headers=JSON_HEADERS, auth=AUTH)
    if r.status_code not in (200, 201):
        print(f"[init] Failed to create featuretype {name}: {r.status_code} {r.text}")
        r.raise_for_status()
    print(f"[init] Published layer '{name}'")


def main():
    wait_for_geoserver()
    ensure_workspace()
    ensure_postgis_datastore()

    # Publish AOI flowlines + FIM history table
    ensure_featuretype(FLOWLINES_TABLE)
    ensure_featuretype(FIM_TABLE)


if __name__ == "__main__":
    main()
