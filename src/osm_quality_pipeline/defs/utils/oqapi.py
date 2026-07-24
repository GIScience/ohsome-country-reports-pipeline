import dagster as dg
import pandas as pd
from typing import Tuple

from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource

logger = dg.get_dagster_logger()



def oqapi_requests(gdf, topic, indicator, attribute=None):
    logger.info(f"start oqapi queries for: {topic}, {indicator}, {attribute}")

    new_columns = []

    for i, row in gdf.iterrows():


        geojson_geometry = get_geojson_geometry(row)

        ohsome_quality_api = OhsomeQualityApiResource()

        row_results = ohsome_quality_api.query(indicator, topic, attribute, geojson_geometry, row["id"])
        new_columns.append(row_results)

        logger.info(f"finished: {i + 1}/{len(gdf)}")

    df = populate_dataframe(gdf, indicator, new_columns, topic)

    df, is_valid = validate_df(df)

    return df, is_valid


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



def populate_dataframe(gdf, indicator, new_columns, topic):
    # transform into a "normal" pandas df so that it can be stored in duckdb out of the box
    gdf["geometry"] = gdf["geometry"].to_wkt()
    df = pd.DataFrame(gdf)
    df["topic"] = topic
    df["indicator"] = indicator
    df["status_code"] = [col[2] for col in new_columns]
    df["value"] = [col[3] for col in new_columns]
    df["description"] = [col[4] for col in new_columns]
    df["quality_class"] = [col[5] for col in new_columns]
    df["osm_timestamp"] = [col[6] for col in new_columns]
    return df


def validate_df(df) -> Tuple[pd.DataFrame, bool]:
    is_valid = ~(df["status_code"] != 200).any()
    return df, is_valid