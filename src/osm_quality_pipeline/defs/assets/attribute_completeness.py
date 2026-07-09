import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.constants import TOPICS_BY_INDICATOR, TOPIC_ATTRIBUTES
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


def make_attribute_completeness_asset(topic: str):
    topic_ = topic.replace("-", "_")

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_attribute_completeness",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        },
        io_manager_key="duckdb_io_manager",
    )
    def generic_attribute_completeness_asset(
        context: dg.AssetExecutionContext,
    ) -> pd.DataFrame:
        f"""Attribute completeness results as json for topic {topic}"""

        INDICATOR = "attribute-completeness"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        df_list = []
        for attribute in TOPIC_ATTRIBUTES[topic]:
            df = oqapi_requests(
                gdf=gdf.copy(), topic=topic, indicator=INDICATOR, attribute=attribute
            )
            df["attribute"] = attribute
            df_list.append(df)

        return pd.concat(df_list)

    return generic_attribute_completeness_asset


all_attribute_completeness_assets = [
    make_attribute_completeness_asset(topic)
    for topic in TOPICS_BY_INDICATOR["attribute-completeness"]
]
