import dagster as dg

from osm_quality_pipeline.defs.constants import TOPICS_BY_INDICATOR
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


def make_mapping_saturation_asset(topic: str):
    topic_ = topic.replace("-", "_")

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_mapping_saturation",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        },
        io_manager_key="duckdb_io_manager",
        pool="ohsome_quality_api"
    )
    def generic_mapping_saturation_asset(
        context: dg.AssetExecutionContext,
    ):
        f"""Mapping Saturation results as json for topic {topic}"""

        INDICATOR = "mapping-saturation"

        partition_key = context.partition_key
        country_layer = get_country_layer_from_partitionkey(partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        df, is_valid = oqapi_requests(gdf, topic, partition_key, indicator=INDICATOR)
        # df["value"] = df["value"].round(4)

        # First, materialize dataframe into DuckDB
        yield dg.MaterializeResult(value=df)

        # Then, fail asset of validation was not successful.
        if not is_valid:
            raise dg.Failure(description="Not all oqapi queries successful.")

    return generic_mapping_saturation_asset


all_mapping_saturation_assets = [
    make_mapping_saturation_asset(topic)
    for topic in TOPICS_BY_INDICATOR["mapping-saturation"]
]
