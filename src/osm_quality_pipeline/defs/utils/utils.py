import os
from importlib import import_module

import geopandas as gpd
import pandas as pd
import dagster as dg
import datetime


logger = dg.get_dagster_logger()

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


def handle_http_error(geom_id, indicator, resp, topic):
    logger.warning(
        f"API error {resp.status_code} for {topic}/{indicator} on {geom_id}"
    )
    row_results = [
        topic,
        indicator,
        resp.status_code,
        -999,
        resp.text,
        None,
        datetime.datetime.now(),
    ]
    return row_results


def handle_connection_error(geom_id, indicator, topic):
    logger.warning(f"Network failure for {topic}/{indicator} on {geom_id}")
    row_results = [
        topic,
        indicator,
        998,
        -999,
        "Network Failure",
        None,
        datetime.datetime.now(),
    ]
    return row_results


def handle_timeout_error(geom_id, indicator, topic):
    logger.warning(f"Timeout for {topic}/{indicator} on {geom_id}")
    row_results = [
        topic,
        indicator,
        999,
        -999,
        "Timeout Error",
        None,
        datetime.datetime.now(),
    ]
    return row_results


def extract_values_from_oqapi_response(response):
    data = response.json()
    result = data["result"][0]

    return [
        result["topic"]["name"],
        result["metadata"]["name"],  # indicator name
        response.status_code,
        result["result"]["value"],
        result["result"]["description"],
        result["result"]["class"],
        result["result"]["timestampOSM"],
    ]


def load_existing_results_gdf(context, asset_name):
    # try to load result from previous asset execution

    defs = import_module("osm_quality_pipeline").definitions.defs()
    df = defs.load_asset_value(asset_name, instance=context.instance)
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.GeoSeries.from_wkt(df.geometry), crs="EPSG:4326"
    )
    print(gdf)
    return gdf