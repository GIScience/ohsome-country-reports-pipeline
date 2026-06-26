import os
from pathlib import Path

import dagster as dg
import geopandas as gpd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests


logger = dg.get_dagster_logger()


TOPICS_CURRENTNESS = [
    "building-count",
    "roads",
    "land-cover"
]


def make_currentness_asset(topic: str):
    topic_ = topic.replace('-', '_')

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_currentness",
        group_name=topic_,
        deps=["country_layers"]
    )
    def generic_currentness_asset(context: dg.AssetExecutionContext) -> dg.Output:
        f"""Mapping Saturation results as json for topic {topic}"""

        INDICATOR = "currentness"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        raw_dir = Path("data") / country / f"raw_responses_{topic}" / layer
        raw_dir.mkdir(parents=True, exist_ok=True)

        # TODO: make this work for all layers
        # TOOD: store all layers in same data format?
        layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
        gdf = gpd.read_file(layer_path)

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

    return generic_currentness_asset


all_currentness_assets = [
    make_currentness_asset(topic)
    for topic in TOPICS_CURRENTNESS
]