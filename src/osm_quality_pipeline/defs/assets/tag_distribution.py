import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.constants import TOPICS_BY_INDICATOR, TOPIC_ATTRIBUTES
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)
from osm_quality_pipeline.defs.resources import OhsomeApiResource
from osm_quality_pipeline.defs.utils.ohsome import request_loop
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


def make_tag_distribution_asset(topic: str):
    topic_ = topic.replace("-", "_")

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_tag_distribution",
        group_name=topic_,
        deps=["country_layers"],
        metadata={
            "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
        },
        io_manager_key="duckdb_io_manager",
        pool="ohsome_quality_api"
    )
    def generic_tag_distribution_asset(
            context: dg.AssetExecutionContext,
            ohsome_api_v2: OhsomeApiResource
    ):

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        filter_expr = "geometry:polygon and building=*"
        grouping_keys = ["building", "levels"]
        all_results = []
        for key in grouping_keys:
            test = request_loop(gdf, ohsome_api_v2, filter_expr, key)
            all_results.append(test)

        df_merged = pd.concat(all_results)

        # First, materialize dataframe into DuckDB
        yield dg.MaterializeResult(value=df_merged)

        # Then, fail asset of validation was not successful.
        #if not all(is_valid_list): # use is valid for potential reruns of faled cells in the future
            #raise dg.Failure(description="Not all oqapi queries successful.")
    
    return generic_tag_distribution_asset


all_attribute_completeness_assets = [
    make_tag_distribution_asset(topic)
    for topic in TOPICS_BY_INDICATOR["tag-distribution"]
]