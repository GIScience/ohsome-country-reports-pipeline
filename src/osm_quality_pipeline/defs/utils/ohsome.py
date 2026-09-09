import json
import requests
import dagster as dg
import pandas as pd
import yaml
import plotly.express as px

logger = dg.get_dagster_logger()


def request_loop(gdf, ohsome_api_v2, filter_expr, grouping_key, measure):
    new_rows = []
    for i, (_, row) in enumerate(gdf.iterrows()):
        geometry = row.geometry.__geo_interface__
        row_df = ohsome_api_v2.stats_features(
            geojson_geometry=geometry,
            filter_expr=filter_expr,
            grouping_key=grouping_key,
            measure=measure
        )
        row_df = row_df.copy()
        row_df["id"] = row["id"]
        plot_df = create_treemap(row_df)
        new_rows.append(plot_df)
        logger.info(f"finished: {i + 1}/{len(gdf)}")

    return pd.concat(new_rows, ignore_index=True)


def create_treemap(row_df):
    df = row_df.copy()
    top = df.dropna(subset=["tagvalue"]).sort_values("value", ascending=False).head(6)
    remainder = df.dropna(subset=["tagvalue"])["value"].sum() - top["value"].sum()

    plot_df = top[["tagvalue", "value"]].copy()

    if remainder > 0:
        plot_df.loc[len(plot_df)] = ["remainder", remainder]

    fig = px.treemap(
        plot_df,
        path=["tagvalue"],
        values="value",
        color="tagvalue",
        color_discrete_map={"remainder": "#929292"}
    )

    fig.update_traces(
        texttemplate="<b>%{label}</b><br>%{value:,.0f} km<br>(%{percentRoot:.1%})",
        marker_line=dict(color="white", width=2)
    )
    fig.update_layout(autosize=True, margin=dict(t=5, l=5, r=5, b=5))
    result = df.iloc[[0]].drop(columns="tagvalue")
    result = result.drop(columns="value")
    result["treemap"] = fig.to_json()

    return result




def get_stats_retry_ids(existing_df):
    existing_df = existing_df.copy()
    existing_df["id"] = existing_df["id"].astype(str)
    # one row per id: every row for the same id shares the same status_code
    per_id = existing_df[["id", "status_code"]].drop_duplicates(subset="id")
    needs_retry = per_id["status_code"].isna() | (per_id["status_code"] != 200)
    retry_ids = per_id[needs_retry][["id"]]
    skip_ids = per_id[~needs_retry][["id"]]
    return retry_ids, skip_ids


def tag_distribution_requests(duckdb, gdf, ohsome_api_v2, topic, partition_key, filter_expr, grouping_key, measure):
    logger.info(f"start ohsome API tag distribution queries for: {partition_key}, {topic}, {grouping_key}")

    indicator = "tag-distribution"
    table_name, table_exists = duckdb.check_if_table_exists(topic, indicator, grouping_key)

    if not table_exists:
        logger.info(f"No existing results, querying all {len(gdf)} rows")
        df = request_loop(gdf, ohsome_api_v2, filter_expr, grouping_key, measure)
    else:
        existing_df = duckdb.query_asset_results_df(
            partition_key, topic, indicator, grouping_key, attribute_column="grouping_key"
        )
        retry_ids, skip_ids = get_stats_retry_ids(existing_df)
        n_existing_ids = existing_df["id"].astype(str).nunique()

        if len(existing_df) == 0:
            logger.info(f"No existing results, querying all {len(gdf)} rows")
            df = request_loop(gdf, ohsome_api_v2, filter_expr, grouping_key, measure)
        elif n_existing_ids == len(gdf) and retry_ids.empty:
            logger.info("All rows already successful, skipping API calls")
            df = existing_df.copy()
        else:
            logger.info(f"Rows to query: {len(retry_ids)}, rows skipping: {len(skip_ids)}")
            existing_df = existing_df.copy()
            existing_df["id"] = existing_df["id"].astype(str)
            retry_gdf = gdf[gdf["id"].isin(retry_ids["id"])]
            new_df = request_loop(retry_gdf, ohsome_api_v2, filter_expr, grouping_key, measure)
            kept = existing_df[~existing_df["id"].isin(retry_ids["id"])]
            df = pd.concat([kept, new_df], ignore_index=True)

    df["partition_key"] = partition_key
    is_valid = ~(df.drop_duplicates("id")["status_code"] != 200).any()
    return df, is_valid


def extract_yaml_info(topic):
    with open("src/osm_quality_pipeline/configs/tag_distribution_config.yaml", "r") as f:
        all_params = yaml.safe_load(f)
    measure = all_params["topics"][topic]["measure"]
    filter_expr = all_params["topics"][topic]["filter_expr"]
    grouping_keys = all_params["topics"][topic]["grouping_keys"]
    return measure, filter_expr, grouping_keys