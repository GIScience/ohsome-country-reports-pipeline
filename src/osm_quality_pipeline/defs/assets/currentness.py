import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


TOPICS_CURRENTNESS = [
    "building-count",
    "roads",
    "land-cover",
    "schools",
    "hospitals"
]


def make_currentness_asset(topic: str):
    topic_ = topic.replace('-', '_')

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_currentness",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        }
    )
    def generic_currentness_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
        f"""Mapping Saturation results as json for topic {topic}"""

        INDICATOR = "currentness"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        df = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR)
        return df

    return generic_currentness_asset


all_currentness_assets = [
    make_currentness_asset(topic)
    for topic in TOPICS_CURRENTNESS
]