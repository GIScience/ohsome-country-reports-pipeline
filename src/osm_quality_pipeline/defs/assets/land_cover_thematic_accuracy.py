import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.constants import THEMATIC_ACCURACY_ATTRIBUTES
from osm_quality_pipeline.defs.partitions import country_layers_partition, get_country_layer_from_partitionkey, get_iso
from osm_quality_pipeline.defs.resources import CustomDuckDBResource
from osm_quality_pipeline.defs.utils.ohsome_quality_api import ohsome_quality_api_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf, empty_df

logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_layers_partition,
    group_name="land_cover",
    metadata={
        "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
    },
    io_manager_key="duckdb_io_manager",
    pool="ohsome_quality_api"
)
def land_cover_thematic_accuracy(context: dg.AssetExecutionContext, duckdb: CustomDuckDBResource):

    INDICATOR = "land-cover-thematic-accuracy"
    TOPIC = "land-cover"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    gdf = load_layer_as_gdf(context, country, layer)
    
    if get_iso(country) != "DEU":
        logger.info(f"Indicator is only available for DEU. Can't process for {country}.")
        df = empty_df(gdf, topic=TOPIC, indicator=INDICATOR)
        df["attribute"] = pd.Series([None] * len(df), dtype="string", index=df.index)
        is_valid = True
    else:
        df_list = []
        is_valid_list = []
        for attribute in THEMATIC_ACCURACY_ATTRIBUTES[INDICATOR]:
            df, is_valid = ohsome_quality_api_requests(
                duckdb=duckdb,
                gdf=gdf,
                topic=TOPIC,
                indicator=INDICATOR,
                partition_key=context.partition_key,
                attribute=attribute,
                table_name=context.asset_key.path[-1]
            )
            df["attribute"] = pd.Series([attribute] * len(df), dtype="string", index=df.index)
            df_list.append(df)
            is_valid_list.append(is_valid)

        df = pd.concat(df_list)
        is_valid = all(is_valid_list)

    # First, materialize dataframe into DuckDB
    yield dg.MaterializeResult(value=df)

    # Then, fail asset of validation was not successful.
    if not is_valid:
        raise dg.Failure(description="Not all oqapi queries successful.")