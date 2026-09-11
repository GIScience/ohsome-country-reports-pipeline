import json
import os
import shlex

import geopandas as gpd
import pandas as pd
import dagster as dg
import datetime

from osm_quality_pipeline.defs.constants import DATA_DIR


logger = dg.get_dagster_logger()


def build_curl_command(url, headers, json_body):
    """Build a copy-pasteable curl reproducing a failed request. The
    Authorization header is emitted as a $HEIGIT_API_KEY shell variable
    reference, never the real key, so nothing secret ends up in logs -
    it resolves automatically if HEIGIT_API_KEY is exported in the shell
    (e.g. via `set -a; source .env; set +a`)."""
    parts = ["curl", "-X", "POST", shlex.quote(url)]
    for key, value in headers.items():
        if key.lower() == "authorization":
            parts.append(f'-H "{key}: $HEIGIT_API_KEY"')
        else:
            parts.append(f"-H {shlex.quote(f'{key}: {value}')}")
    parts.append(f"-d {shlex.quote(json.dumps(json_body))}")
    return " ".join(parts)


def log_curl_reproduction(url, headers, json_body):
    logger.warning(f"Reproduce with: {build_curl_command(url, headers, json_body)}")


def load_layer_as_gdf(context, country, layer):
    layer_path = os.path.join(DATA_DIR, country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)

    gdf.geometry = gdf.geometry.simplify(0.002)

    # drop all columns, but id and geometry
    columns = list(gdf.columns)
    columns.remove("id")
    columns.remove("geometry")

    gdf.drop(columns, axis=1, inplace=True)

    gdf["id"] = gdf["id"].astype(str)
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
    df["figure"] = None
    return df


def handle_http_error(geom_id, indicator, resp, topic, url, headers, params):
    logger.warning(f"API error {resp.status_code} for {topic}/{indicator} on {geom_id}")
    log_curl_reproduction(url, headers, params)
    row_results = [
        topic,
        indicator,
        resp.status_code,
        -999,
        resp.text,
        None,
        datetime.datetime.now(),
        None
    ]
    return row_results


def handle_connection_error(geom_id, indicator, topic, url, headers, params):
    logger.warning(f"Network failure for {topic}/{indicator} on {geom_id}")
    log_curl_reproduction(url, headers, params)
    row_results = [
        topic,
        indicator,
        998,
        -999,
        "Network Failure",
        None,
        datetime.datetime.now(),
        None
    ]
    return row_results


def handle_timeout_error(geom_id, indicator, topic, url, headers, params):
    logger.warning(f"Timeout for {topic}/{indicator} on {geom_id}")
    log_curl_reproduction(url, headers, params)
    row_results = [
        topic,
        indicator,
        999,
        -999,
        "Timeout Error",
        None,
        datetime.datetime.now(),
        None
    ]
    return row_results


def empty_stats_df(status_code, description):
    return pd.DataFrame([{
        "timestamp": datetime.datetime.now().isoformat(),
        "value": None,
        "tagvalue": None,
        "status_code": status_code,
        "description": description,
    }])


def handle_stats_http_error(resp, url, headers, params):
    logger.warning(f"API error {resp.status_code} for tag distribution request")
    log_curl_reproduction(url, headers, params)
    return empty_stats_df(resp.status_code, resp.text)


def handle_stats_timeout_error(url, headers, params):
    logger.warning("Timeout for tag distribution request")
    log_curl_reproduction(url, headers, params)
    return empty_stats_df(999, "Timeout Error")


def handle_stats_connection_error(url, headers, params):
    logger.warning("Network failure for tag distribution request")
    log_curl_reproduction(url, headers, params)
    return empty_stats_df(998, "Network Failure")


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


def get_retry_rows(df):
    needs_retry = df["status_code"].isna() | (df["status_code"] != 200)
    retry_df = df[needs_retry]
    skip_df = df[~needs_retry]
    logger.info(
        f"Rows to retry: {len(retry_df)}, rows already successful: {len(skip_df)}"
    )
    return retry_df, skip_df
