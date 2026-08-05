import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf


logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    group_name="land_cover",
    metadata={
        "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
    },
    io_manager_key="duckdb_io_manager"
)
def land_cover_completeness(
    context: dg.AssetExecutionContext,
) -> pd.DataFrame:
    INDICATOR = "land-cover-completeness"
    TOPIC = "land-cover"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    gdf = load_layer_as_gdf(context, country, layer)

    df, is_valid = oqapi_requests(gdf=gdf, topic=TOPIC, indicator=INDICATOR, partition_key=dynamic_country_layers_partition)
    return df
