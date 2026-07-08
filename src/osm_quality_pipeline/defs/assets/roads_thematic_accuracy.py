import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    group_name="roads",
    deps="country_layers",
    metadata={
        "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
    },
    io_manager_key="duckdb_io_manager"
)
def roads_thematic_accuracy(context: dg.AssetExecutionContext) -> pd.DataFrame:
    INDICATOR = "roads-thematic-accuracy"
    TOPIC = "roads"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    if country != "DEU":
        logger.info(f"Indicator is only available for DEU. Can't process for {country}.")
        return None

    gdf = load_layer_as_gdf(context, country, layer)

    df = oqapi_requests(gdf=gdf, topic=TOPIC, indicator=INDICATOR)
    return df