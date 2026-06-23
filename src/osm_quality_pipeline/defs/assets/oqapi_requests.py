import json
from pathlib import Path

import dagster as dg
import geopandas as gpd
import requests

from osm_quality_pipeline.defs.constants import ApiRequestConfig
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource


TOPIC = "roads-all-highways"
INDICATOR = "mapping-saturation"


@dg.asset(
    ins={"h3_hexgrid": dg.AssetIn()},
    partitions_def=country_partitions,
)
def oqapi_api_requests(
    context: dg.AssetExecutionContext,
    h3_hexgrid: str,
    ohsome_api: OhsomeQualityApiResource,
    config: ApiRequestConfig,
):
    country = context.partition_key.upper()

    gdf = gpd.read_file(h3_hexgrid)

    raw_dir = Path("data") / country / f"raw_responses_{TOPIC}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)

    success = 0

    for _, row in gdf.iterrows():
        geom_id = row["id"]
        params = {
            "topic": TOPIC,
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

        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        url = f"{ohsome_api.base_url}/indicators/{INDICATOR}"
        resp = requests.post(url, json=params, headers=headers, timeout=120)
        resp.raise_for_status()

        out_path = raw_dir / f"{TOPIC}__{INDICATOR}__{geom_id}.json"
        with open(out_path, "w") as f:
            json.dump(resp.json(), f)
        success += 1

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": TOPIC,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )
