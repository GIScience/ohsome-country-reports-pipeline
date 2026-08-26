import json
import requests
import dagster as dg

logger = dg.get_dagster_logger()


def ohsome_request(filter_expr, grouping_key, geom, grouping_values=None):
    params = {
        "bpolys": json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": geom}]}),
        "filter": filter_expr, "time": "latest"}
    url = "https://api.ohsome.org/v1/elements/count"
    if grouping_key:
        url = f"{url.rstrip('/')}/groupBy/tag"
        params["groupBy"] =  {"type":"byTag","key":grouping_key},
        if grouping_values:
            params["groupByValues"] = grouping_values
    try:
        r = requests.post(url, data=params, timeout=600)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        #if e.response.status_code == 413:
            #log.error(f"Payload still too large for {country}.")
        raise e


def request_loop(gdf):
    new_columns = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        geometry = row.geometry.__geo_interface__
        row_results = ohsome_request(filter_expr="geometry:polygon and building=*",grouping_key="building", geom=geometry)
        new_columns.append(row_results)
        logger.info(f"finished: {i + 1}/{len(gdf)}")
    return new_columns