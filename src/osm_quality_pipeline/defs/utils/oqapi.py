import dagster as dg
import requests as r
import pandas as pd

from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource

logger = dg.get_dagster_logger()


def oqapi_requests(gdf, topic, indicator, attribute=None):
    logger.info(f"start oqapi queries for: {topic}, {indicator}, {attribute}")

    new_columns = []

    for _, row in gdf.iterrows():
        geom_id = row["id"]
        params = {
            "topic": topic,
            "bpolys": {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": row.geometry.__geo_interface__,
                        "properties": {},
                    }
                ],
            },
        }

        if indicator == "attribute-completeness":
            params["attributes"] = [
                attribute
            ]

        ApiResource = OhsomeQualityApiResource()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        url = f"{ApiResource.base_url}/indicators/{indicator}"
        resp = r.post(url, json=params, headers=headers, timeout=120)
        resp.raise_for_status()

        row_results = extract_values_from_oqapi_response(resp)
        new_columns.append(row_results)
        logger.info(f"finished: {_ + 1}/{len(gdf)}")

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
        result["result"]["timestampOSM"]
    ]
 