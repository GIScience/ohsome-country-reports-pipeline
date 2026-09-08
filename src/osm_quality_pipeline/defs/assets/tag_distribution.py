import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.constants import TOPICS_BY_INDICATOR, TOPIC_ATTRIBUTES
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)
from osm_quality_pipeline.defs.resources import CustomDuckDBResource, OhsomeApiResource
from osm_quality_pipeline.defs.utils.ohsome import tag_distribution_requests, extract_yaml_info
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
            duckdb: CustomDuckDBResource,
            ohsome_api_v2: OhsomeApiResource
    ):

        country_layer = get_country_layer_from_partitionkey(context.partition_key)
        country = country_layer.country
        layer = country_layer.layer

        gdf = load_layer_as_gdf(context, country, layer)

        measure, filter_expr, grouping_keys = extract_yaml_info(topic)
        all_results = []
        is_valid_list = []
        for key in grouping_keys:
            df, is_valid = tag_distribution_requests(
                duckdb=duckdb,
                gdf=gdf,
                ohsome_api_v2=ohsome_api_v2,
                topic=topic,
                partition_key=context.partition_key,
                filter_expr=filter_expr,
                grouping_key=key,
                measure=measure
            )
            df["grouping_key"] = key
            all_results.append(df)
            is_valid_list.append(is_valid)

        df_merged = pd.concat(all_results, ignore_index=True)

        # First, materialize dataframe into DuckDB
        yield dg.MaterializeResult(value=df_merged)

        # Then, fail asset if validation was not successful.
        if not all(is_valid_list):
            raise dg.Failure(description="Not all ohsome API tag distribution queries successful.")

    return generic_tag_distribution_asset


all_attribute_completeness_assets = [
    make_tag_distribution_asset(topic)
    for topic in TOPICS_BY_INDICATOR["tag-distribution"]
]