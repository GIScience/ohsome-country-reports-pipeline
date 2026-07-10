import os
import geopandas as gpd
import pandas as pd


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

def empty_df(gdf, topic, indicator):
    gdf["geometry"] = gdf["geometry"].to_wkt()
    df = pd.DataFrame(gdf)
    df["topic"] = topic
    df["indicator"] = indicator
    df["status_code"] = 0
    df["value"] = 0
    df["description"] = "skipped: indicator not available for this country"
    df["quality_class"] = 0
    df["osm_timestamp"] = ""
    return df