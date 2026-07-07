import json

import dagster as dg
import requests as r

from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource

logger = dg.get_dagster_logger()


def oqapi_requests(gdf, topic, indicator, raw_dir, attribute = None):
    logger.info(f"start oqapi queries for: {topic}, {indicator}, {attribute}")
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
            params["attributes"] = [attribute]  # TODO: figure out how to pass attribute completeness as optional partition

        ApiResource = OhsomeQualityApiResource()
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        url = f"{ApiResource.base_url}/indicators/{indicator}"
        resp = r.post(url, json=params, headers=headers, timeout=120)
        resp.raise_for_status()

        out_path = raw_dir / f"{topic}__{indicator}__{geom_id}.json"
        with open(out_path, "w") as f:
            json.dump(resp.json(), f)

        # write function to extract values from json response
        row_results = extract_values_from_oqapi_response(resp)

        # do something to add these values to the geopandas df


        logger.info(f"finished: {_+1}/{len(gdf)}")

    # return gdf with additional columns instead of success
    return gdf


def extract_values_from_oqapi_response(response):
    # do something to extrac the values from reponse_json
    # topic, indicator, status_code, value, description
    return ["building_count", "mapping_saturation", 200, 1.0, "this is the description"]
