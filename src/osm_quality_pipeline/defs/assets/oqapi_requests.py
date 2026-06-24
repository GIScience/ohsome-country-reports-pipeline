import json
from pathlib import Path

import dagster as dg
import geopandas as gpd
import requests

from osm_quality_pipeline.defs.constants import ApiRequestConfig
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource
from osm_quality_pipeline.defs.partitions import multi_partitions_oqapi_request
from osm_quality_pipeline.defs.assets.utils import oqapi_requests


@dg.asset(
    deps=["h3_hexgrid"],
    partitions_def=multi_partitions_oqapi_request,
)
def responses_mapping_saturation(
    context: dg.AssetExecutionContext,
    h3_hexgrid: str,
    config: ApiRequestConfig,
):
    INDICATOR = "mapping-saturation"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )


@dg.asset(
    deps=["h3_hexgrid"],
    partitions_def=multi_partitions_oqapi_request,
)
def responses_user_activity(
    context: dg.AssetExecutionContext,
    h3_hexgrid: str,
    config: ApiRequestConfig,
):
    INDICATOR = "user-activity"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )

# TODO: how to do attribute completeness?
