import json
from pathlib import Path

import dagster as dg
import geopandas as gpd
import requests

from osm_quality_pipeline.defs.constants import ApiRequestConfig
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource
from osm_quality_pipeline.defs.partitions import multi_partitions_oqapi_request

@dg.asset(
    deps=["h3_hexgrid"],
    partitions_def=multi_partitions_oqapi_request,
)
def oqapi_api_requests(
    context: dg.AssetExecutionContext,
    h3_hexgrid: str,
    ohsome_api: OhsomeQualityApiResource,
    config: ApiRequestConfig,
):
    keys = context.partition_key.keys_by_dimension

    country = keys["country"]
    topic, indicator = keys["topic"].split("|")

    gdf = gpd.read_file(h3_hexgrid)

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)

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
            params["attributes"] = ["name"]# TODO: figure out how to pass attribute completeness as optional partition

        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        url = f"{ohsome_api.base_url}/indicators/{indicator}"
        resp = requests.post(url, json=params, headers=headers, timeout=120)
        resp.raise_for_status()

        out_path = raw_dir / f"{topic}__{indicator}__{geom_id}.json"
        with open(out_path, "w") as f:
            json.dump(resp.json(), f)
        success += 1

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": indicator,
            "cells_processed": success,
        },
    )
# TODO: how to get all possible partition combinations?