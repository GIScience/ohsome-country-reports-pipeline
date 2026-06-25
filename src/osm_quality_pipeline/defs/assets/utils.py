import requests
import os
import io
import sys
import zipfile
import geopandas as gpd
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource
import json

from attr import dataclass


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


@dataclass
class CountryLayer:
    country: str
    layer: str


def get_country_layer_from_partitionkey(country_layer_partitionkey: str) -> CountryLayer:
    "extracts country and layer from <country>|<layer"
    country: str = ""
    layer: str = ""
    partition: str | None = country_layer_partitionkey
    if partition:
        parts = partition.split("|")
        if len(parts) == 2:
            country, layer = parts
        else:
            raise ValueError("Invalid partition format. Should be <country>|<layer>")

    return CountryLayer(country=country, layer=layer)