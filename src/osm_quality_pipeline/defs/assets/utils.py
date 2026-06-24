import requests
import os
import io
import sys
import zipfile
import geopandas as gpd
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource
import json


def download_from_geoboundaries(list_url, country, level_val, url_val, out_dir):
    print(f"Fetching available levels from {list_url}")
    try:
        resp = requests.get(list_url)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)
    entries = resp.json()
    if not isinstance(entries, list) or not entries:
        print(f"No boundaries found for country '{country}'", file=sys.stderr)
        sys.exit(1)

    any_failures = False

    # 2) Iterate over every entry and download
    for entry in entries:
        level = entry.get(level_val)  # e.g. "ADM0", "ADM1", ...
        url = entry.get(url_val)
        if not level or not url:
            print(f"Skipping malformed entry: {entry}", file=sys.stderr)
            any_failures = True
            continue

        out_path = os.path.join(out_dir, f"boundary_{level}.geojson")

        print(f"[{level}] Downloading from {url}")
        try:
            download = requests.get(url)
            download.raise_for_status()
        except requests.RequestException as e:
            print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
            any_failures = True
            continue

        with open(out_path, "wb") as fp:
            fp.write(download.content)
        print(f"[{level}] Saved to {out_path}")

    if any_failures:
        print("One or more boundaries failed to download.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All available boundaries downloaded successfully.")


def download_bkg_boundaries(list_url, level_val, out_dir):  #layer: vg25_sta, vg25_lan, vg25_gem
    try:
        resp = requests.get(list_url)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        zip_path = "daten/DE_VG25.gpkg"

    gpkg_path = os.path.join(out_dir, "DE_VG25.gpkg")

    with z.open(zip_path) as source:
        with open(gpkg_path, "wb") as target:
            target.write(source.read())

    out_path = os.path.join(out_dir, f"{level_val}.geojson")

    gdf = (
        gpd.read_file(gpkg_path, layer=level_val)
        .to_crs(4326)
    )

    gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]
    gdf.to_file(out_path, driver="GeoJSON")


def oqapi_requests(gdf, topic, indicator, raw_dir):
    success = 0

    for _, row in gdf.iterrows():
        geom_id = row["id"]
        params = {
            "topic": topic,
            "bpolys": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": row.geometry.__geo_interface__,
                        "properties": {},
                    }
                ],
            },
        }

        if indicator == "attribute-completeness":
            params["attributes"] = ["name"]  # TODO: figure out how to pass attribute completeness as optional partition

        ApiResource = OhsomeQualityApiResource()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        url = f"{ApiResource.base_url}/indicators/{indicator}"
        resp = requests.post(url, json=params, headers=headers, timeout=120)
        resp.raise_for_status()

        out_path = raw_dir / f"{topic}__{indicator}__{geom_id}.json"
        with open(out_path, "w") as f:
            json.dump(resp.json(), f)
        success += 1

        return success
