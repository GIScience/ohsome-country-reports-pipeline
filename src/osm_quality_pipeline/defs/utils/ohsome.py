import json
import requests
import dagster as dg
import pandas as pd

logger = dg.get_dagster_logger()


def ohsome_request(filter_expr, grouping_key, geom, grouping_values=None):
    params = {
        "bpolys": json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": geom}]}),
        "filter": filter_expr, 
        "time": "latest",
        "groupBy":{"type": "byTag", "key": grouping_key}
    }
    measure = "count"
    url = f"https://api.heigit.org/ohsome-api/v2-rc/stats/features/{measure}.csv"
    try:
        r = requests.post(url, data=params, timeout=600)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        #if e.response.status_code == 413:
            #log.error(f"Payload still too large for {country}.")
        raise e


def request_loop(gdf, ohsome_api_v2, filter_expr, grouping_key):
    new_columns = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        geometry = row.geometry.__geo_interface__
        row_results = ohsome_api_v2.stats_features(
            filter_expr=filter_expr,
            grouping_key=grouping_key,
            geojson_geometry=geometry
        )

        new_columns.append(row_results)
        logger.info(f"finished: {i + 1}/{len(gdf)}")
        # new function call here that makes the plot and saves it as json in a pd df
    return pd.concat(new_columns)