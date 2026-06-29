import os
from pathlib import Path

import dagster as dg
import geopandas as gpd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests


logger = dg.get_dagster_logger()


TOPICS_ATTRIBUTE_COMPLETENESS = [
    "building-count",
    "roads",
    "schools",
    "hospitals"
]


TOPIC_ATTRIBUTES = {
    "building-count": ["height", "building-material"],
    "roads": ["name", "maxspeed", "surface"],
    "schools": ["name", "phone-number", "website"],
    "hospitals": ["emergency", "name", "opening-hours","speciality"]
}


def make_attribute_completeness_asset(topic: str):
    topic_ = topic.replace('-', '_')

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_attribute_completeness",
        group_name=topic_,
        deps=["country_layers"]
    )
    def generic_attribute_completeness_asset(context: dg.AssetExecutionContext) -> dg.Output:
        f"""Mapping Saturation results as json for topic {topic}"""

        INDICATOR = "attribute-completeness"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        raw_dir = Path("data") / country / f"raw_responses_{topic}" / layer
        raw_dir.mkdir(parents=True, exist_ok=True)

        layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
        gdf = gpd.read_file(layer_path)

        for attribute in TOPIC_ATTRIBUTES[topic]:
            success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir, attribute=attribute)

        return dg.Output(
            {"raw_dir": str(raw_dir)},
            metadata={
                "country": country,
                "topic": topic,
                "indicator": INDICATOR,
                "cells_processed": success,
            },
        )

    return generic_attribute_completeness_asset


all_attribute_completeness_assets = [
    make_attribute_completeness_asset(topic)
    for topic in TOPICS_ATTRIBUTE_COMPLETENESS
]