import os
import geopandas as gpd


def load_layer_as_gdf(context, country, layer):
    layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)
    gdf["partition_key"] = context.partition_key
    return gdf