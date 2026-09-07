import json
import requests
import dagster as dg
import pandas as pd
import yaml

from osm_quality_pipeline.defs.resources import ohsome_api_rate_limiter

logger = dg.get_dagster_logger()


def request_loop(gdf, ohsome_api_v2, filter_expr, grouping_key, measure):
    ohsome_api_rate_limiter.log_remaining(len(gdf))
    new_columns = []
    #concurrency??
    for i, (_, row) in enumerate(gdf.iterrows()):
        geometry = row.geometry.__geo_interface__
        row_results = ohsome_api_v2.stats_features(
            geojson_geometry=geometry,
            filter_expr=filter_expr,
            grouping_key=grouping_key,
            measure=measure
        )

        new_columns.append(row_results)
        logger.info(f"finished: {i + 1}/{len(gdf)}")
        # new function call here that makes the plot and saves it as json in a pd df
    return pd.concat(new_columns)


def extract_yaml_info(topic):
    with open("src/osm_quality_pipeline/configs/tag_distribution_config.yaml", "r") as f:
        all_params = yaml.safe_load(f)
    measure = all_params["topics"][topic]["measure"]
    filter_expr = all_params["topics"][topic]["filter_expr"]
    grouping_keys = all_params["topics"][topic]["grouping_keys"]
    return measure, filter_expr, grouping_keys