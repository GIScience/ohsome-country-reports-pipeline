import dagster as dg

from osm_quality_pipeline.defs.constants import TOPICS_BY_INDICATOR
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)
from osm_quality_pipeline.defs.resources import CustomDuckDBResource
from osm_quality_pipeline.defs.utils.ohsome_quality_api import ohsome_quality_api_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


def make_currentness_asset(topic: str):
    topic_ = topic.replace("-", "_")

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_currentness",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        },
        io_manager_key="duckdb_io_manager",
        pool="ohsome_quality_api"
    )
    def generic_currentness_asset(context: dg.AssetExecutionContext, duckdb: CustomDuckDBResource):
        f"""Mapping Saturation results as json for topic {topic}"""

        INDICATOR = "currentness"

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        df, is_valid = ohsome_quality_api_requests(
            duckdb=duckdb,
            gdf=gdf,
            topic=topic,
            indicator=INDICATOR,
            partition_key=context.partition_key
        )

        # First, materialize dataframe into DuckDB
        yield dg.MaterializeResult(value=df)

        # Then, fail asset of validation was not successful.
        if not is_valid:
            raise dg.Failure(description="Not all oqapi queries successful.")

    return generic_currentness_asset


all_currentness_assets = [
    make_currentness_asset(topic) for topic in TOPICS_BY_INDICATOR["currentness"]
]
