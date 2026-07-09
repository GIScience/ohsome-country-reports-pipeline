import dagster as dg
import pandas as pd
import geopandas as gpd

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf

logger = dg.get_dagster_logger()

@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    name="topic_gpkg",
    group_name="building_count",
    deps=["building_count_attribute_completeness",
          "building_count_mapping_saturation",
          "building_count_user_activity",
          "building_count_currentness"],
    metadata={
        "partition_expr": "partition_key"  # DuckDB maps partitions to the 'partition_key' column
    },
    io_manager_key="duckdb_io_manager"
)
def topic_gpkg(context: dg.AssetExecutionContext, 
               building_count_attribute_completeness, 
               building_count_mapping_saturation, 
               building_count_user_activity, 
               building_count_currentness) -> None:
    
    df = pd.concat([
        building_count_attribute_completeness,
        building_count_mapping_saturation,
        building_count_user_activity,
        building_count_currentness
    ])

    df['geometry'] = gpd.GeoSeries.from_wkt(df['geometry'])
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    
    country_code = context.partition_key.split("|")[0]
    
    gdf.to_file(f"data/{country_code}/Outputs/{country_code}_building_count.gpkg", layer="building_count", driver="GPKG")