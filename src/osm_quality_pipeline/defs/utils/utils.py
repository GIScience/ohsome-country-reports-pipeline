import os
import geopandas as gpd


def load_layer_as_gdf(context, country, layer):
    layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)

    # drop all columns, but id and geometry
    columns = list(gdf.columns)
    columns.remove("id")
    columns.remove("geometry")

    gdf.drop(columns, axis=1, inplace=True)

    gdf["partition_key"] = context.partition_key
    return gdf