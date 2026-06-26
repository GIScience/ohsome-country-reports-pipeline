import os
from pathlib import Path

import dagster as dg
import geopandas as gpd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests


logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    group_name="land_cover"
)
def land_cover_thematic_accuracy(
    context: dg.AssetExecutionContext,
):
    INDICATOR = "land-cover-thematic-accuracy"
    TOPIC = "land-cover"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    raw_dir = Path("data") / country / f"raw_responses_{TOPIC}" / layer
    raw_dir.mkdir(parents=True, exist_ok=True)

    layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)

    success = oqapi_requests(gdf=gdf, topic=TOPIC, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": TOPIC,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )