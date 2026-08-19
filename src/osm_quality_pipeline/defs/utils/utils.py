import os

import duckdb
import geopandas as gpd
import pandas as pd
import dagster as dg
import datetime

from osm_quality_pipeline.defs.constants import DATA_DIR


logger = dg.get_dagster_logger()


def load_layer_as_gdf(context, country, layer):
    layer_path = os.path.join(DATA_DIR, country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)

    gdf.geometry = gdf.geometry.simplify(0.002)

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
    logger.warning(f"API error {resp.status_code} for {topic}/{indicator} on {geom_id}")
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
    if result["result"]["value"] is not None:
        result["result"]["value"] = round(result["result"]["value"], 4)
    else:
        result["result"]["value"] = None
    return [
        result["topic"]["name"],
        result["metadata"]["name"],  # indicator name
        response.status_code,
        result["result"]["value"],
        result["result"]["description"],
        result["result"]["class"],
        result["result"]["timestampOSM"],
        result["result"]["figure"]
    ]


def load_existing_results_gdf(asset_name, partition_key):
    db_path = f"{DATA_DIR}/asset_output.duckdb"
    if not os.path.exists(db_path):
        logger.info(f"DuckDB file does not exist yet: {db_path}")
        return None

    try:
        conn = duckdb.connect(db_path, read_only=True)
        df = conn.execute(f'SELECT * FROM public.{asset_name} WHERE partition_key = \'{partition_key}\'').fetchdf()
        logger.info(f"Loaded {len(df)} existing rows from {asset_name} for partition_key: {partition_key}")
        conn.close()
        logger.info(f"Loaded {len(df)} existing rows from {asset_name}")
        return df
    except Exception as e:
        logger.warning(f"Failed to read {asset_name} from DuckDB: {e}")
        return None


def get_retry_rows(df):
    needs_retry = df["status_code"].isna() | (df["status_code"] != 200)
    retry_df = df[needs_retry]
    skip_df = df[~needs_retry]
    logger.info(
        f"Rows to retry: {len(retry_df)}, rows already successful: {len(skip_df)}"
    )
    return retry_df, skip_df
