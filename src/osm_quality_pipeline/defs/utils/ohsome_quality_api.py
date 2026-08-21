import dagster as dg
import pandas as pd

from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource


logger = dg.get_dagster_logger()


def ohsome_quality_api_requests(duckdb, gdf, topic, partition_key, indicator, attribute=None):
    logger.info(f"start ohsome quality API queries for: {partition_key}, {topic}, {indicator}, {attribute}")

    existing_df = duckdb.query_asset_results_df(partition_key, topic, indicator, attribute)
    retry_ids, skip_ids = get_retry_rows_ids(existing_df)

    if len(existing_df) == 0:
        logger.info(f"No existing results, querying all {len(gdf)} rows")
        new_results = query_api_rows(gdf, topic, indicator, attribute)
        df = build_dataframe(gdf, indicator, new_results, topic)
    elif len(existing_df) == len(gdf) and retry_ids.empty:
        logger.info("All rows already successful, skipping API calls")
        df = existing_df.copy()
    else:
        logger.info(f"Rows to query: {len(retry_ids)}, rows skipping: {len(skip_ids)}")
        retry_gdf = gdf[gdf["id"].isin(retry_ids["id"])]
        new_results = query_api_rows(retry_gdf, topic, indicator, attribute)
        df = merge_results(retry_gdf, existing_df, new_results, topic, indicator)

    df, is_valid = validate_df(df)
    return df, is_valid


def get_retry_rows_ids(existing_df):
    existing_df = existing_df.copy()
    existing_df["id"] = existing_df["id"].astype(str)
    needs_retry = existing_df["status_code"].isna() | (
        existing_df["status_code"] != 200
    )
    retry_ids = existing_df[needs_retry][["id"]]
    skip_ids = existing_df[~needs_retry][["id"]]
    return retry_ids, skip_ids


def query_api_rows(gdf, topic, indicator, attribute):
    new_columns = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        geojson_geometry = get_geojson_geometry(row)
        ohsome_quality_api = OhsomeQualityApiResource()
        row_results = ohsome_quality_api.query(
            indicator, topic, attribute, geojson_geometry, row["id"]
        )
        new_columns.append(row_results)
        logger.info(f"finished: {i + 1}/{len(gdf)}")
    return new_columns


def build_dataframe(gdf, indicator, new_columns, topic):
    gdf = gdf.copy()
    gdf["id"] = gdf["id"].astype(str)
    gdf["geometry"] = gdf["geometry"].to_wkt()
    df = pd.DataFrame(gdf)
    df["topic"] = topic
    df["indicator"] = indicator
    df["status_code"] = [col[2] for col in new_columns]
    df["value"] = [col[3] for col in new_columns]
    df["description"] = [col[4] for col in new_columns]
    df["quality_class"] = [col[5] for col in new_columns]
    df["osm_timestamp"] = [col[6] for col in new_columns]
    df["figure"] = [col[7] for col in new_columns]
    return df


def merge_results(gdf, existing_df, new_results, topic, indicator):
    new_df = build_dataframe(gdf, indicator, new_results, topic)
    existing_df = existing_df.copy()
    existing_df["id"] = existing_df["id"].astype(str)
    retry_ids = new_df["id"].tolist()
    kept = existing_df[~existing_df["id"].isin(retry_ids)]
    merged = pd.concat([kept, new_df], ignore_index=True)
    return merged


def prepare_final_df(gdf, existing_df, topic, indicator):
    existing_df = existing_df.copy()
    existing_df["id"] = existing_df["id"].astype(str)
    existing_df["partition_key"] = (
        gdf["partition_key"].iloc[0] if "partition_key" in gdf.columns else None
    )
    return existing_df


def get_geojson_geometry(row):
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": row.geometry.__geo_interface__,
                "properties": {},
            }
        ],
    }


def validate_df(df):
    is_valid = ~(df["status_code"] != 200).any()
    return df, is_valid
