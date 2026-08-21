import dagster as dg

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.resources import CustomDuckDBResource
from osm_quality_pipeline.defs.utils.ohsome_quality_api import ohsome_quality_api_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf, empty_df

logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    group_name="buildings",
    deps=["country_layers"],
    metadata={
        "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
    },
    io_manager_key="duckdb_io_manager",
    pool="ohsome_quality_api"
)
def building_comparison(
        context: dg.AssetExecutionContext,
        duckdb: CustomDuckDBResource
):
    INDICATOR = "building-comparison"
    TOPIC = "buildings"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    gdf = load_layer_as_gdf(context, country, layer)

    if country != "DEU":
        logger.info(f"Indicator is only available for DEU. Can't process for {country}.")
        df = empty_df(gdf, topic=TOPIC, indicator=INDICATOR)
        is_valid = True
    else:
        df, is_valid = ohsome_quality_api_requests(
            duckdb=duckdb,
            gdf=gdf,
            topic=TOPIC,
            indicator=INDICATOR,
            partition_key=context.partition_key
        )

    # First, materialize dataframe into DuckDB
    yield dg.MaterializeResult(value=df)

    # Then, fail asset of validation was not successful.
    if not is_valid:
        raise dg.Failure(description="Not all oqapi queries successful.")

