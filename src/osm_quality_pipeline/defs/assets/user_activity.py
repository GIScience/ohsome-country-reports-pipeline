import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


TOPICS_USER_ACTIVITY = [
    "building-count",
    "roads",
    "land-cover",
    "schools",
    "hospitals"
]


def make_user_activity_asset(topic: str):
    topic_ = topic.replace('-', '_')

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_user_activity",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        },
        io_manager_key="duckdb_io_manager"
    )
    def generic_user_activity_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
        f"""User activity results as json for topic {topic}"""

        INDICATOR = "user-activity"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        df = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR)
        return df

    return generic_user_activity_asset


all_user_activity_assets = [
    make_user_activity_asset(topic)
    for topic in TOPICS_USER_ACTIVITY
]