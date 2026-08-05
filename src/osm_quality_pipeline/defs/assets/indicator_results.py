from pathlib import Path

import dagster as dg
import geopandas as gpd
import pandas as pd

from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.constants import CONFIG

from osm_quality_pipeline.defs.constants import (
    ALL_TOPICS,
    STATIC_TOPIC_ASSETS,
    TOPICS_BY_INDICATOR,
)
from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)


def _get_all_topic_deps() -> list[str]:
    deps = set()
    for topic in ALL_TOPICS:
        topic_ = topic.replace("-", "_")
        for indicator, topics in TOPICS_BY_INDICATOR.items():
            if topic in topics:
                indicator_ = indicator.replace("-", "_")
                deps.add(f"{topic_}_{indicator_}")
        for dep in STATIC_TOPIC_ASSETS.get(topic, []):
            deps.add(dep)
    return sorted(deps)


ALL_TOPIC_DEPS = _get_all_topic_deps()

ins = {dep: dg.AssetIn(key=dep) for dep in ALL_TOPIC_DEPS}

@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    name="indicator_results_and_upload_csv",
    group_name="outputs",
    ins=ins
)
def indicator_results_csv(context: dg.AssetExecutionContext, **kwargs) -> None:
    dfs = [df for df in kwargs.values() if df is not None]
    if not dfs:
        return None
    
    combined = pd.concat(dfs)
    combined = combined.drop("geometry", axis=1)

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer


    Path(f"data/{country_code}/Outputs").mkdir(parents=True, exist_ok=True)
    csv_path = f"data/{country_code}/Outputs/{country_code}_{layer}_indicator_results.csv"

    combined.to_csv(csv_path, encoding='utf-8', index=False)

def upload_csv_to_s3(context: dg.AssetExecutionContext, s3: S3Resource) -> None:
    s3_client = s3.get_client()

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer

    file_path = f"data/{country_code}/Outputs/{country_code}_{layer}_indicator_results.csv"

    bucket_name = CONFIG.s3_config.bucket

    s3_client.upload_file(
        Filename=file_path,
        Bucket=bucket_name,
        Key=f"oqapi_hdx/downloads/{country_code}/{file_path}"
    )


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    name="indicator_results_and_upload_gpkg",
    group_name="outputs",
    ins=ins
)
def indicator_results_gpkg(context: dg.AssetExecutionContext, **kwargs) -> None:
    dfs = [df for df in kwargs.values() if df is not None]
    if not dfs:
        return None

    combined = pd.concat(dfs)
    combined["geometry"] = gpd.GeoSeries.from_wkt(combined["geometry"])

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer

    Path(f"data/{country_code}/Outputs").mkdir(parents=True, exist_ok=True)
    gpkg_path = f"data/{country_code}/Outputs/{country_code}_{layer}_indicator_results.gpkg"

    first_layer = True
    for topic in ALL_TOPICS:
        topic_gdf = combined[combined["topic"] == topic]
        if topic_gdf.empty:
            continue

        topic_ = topic.replace("-", "_")

        topic_gdf["indicator_key"] = topic_gdf.apply(
            lambda r: f"{r['indicator']}_{r['attribute']}"
            if pd.notna(r.get("attribute"))
            else r["indicator"],
            axis=1,
        )
        wide = topic_gdf.pivot(
            index="id",
            columns="indicator_key",
            values=["value", "description"]
        )
        wide.columns = wide.columns.swaplevel(0, 1)
        wide = wide.sort_index(axis=1, level=0)

        wide.columns = [f"{ind}_{col}" for col, ind in wide.columns]
        wide = wide.reset_index()

        geom_map = topic_gdf[["id", "geometry"]].drop_duplicates("id")
        wide = wide.merge(geom_map, on="id", how="left")

        wide_gdf = gpd.GeoDataFrame(wide, geometry="geometry", crs="EPSG:4326")
        mode = "w" if first_layer else "a"
        wide_gdf.to_file(gpkg_path, layer=topic_, driver="GPKG", mode=mode)
        first_layer = False


def upload_gpkg_to_s3(context: dg.AssetExecutionContext, s3: S3Resource) -> None:
    s3_client = s3.get_client()

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer

    file_path = f"data/{country_code}/Outputs/{country_code}_{layer}_indicator_results.gpkg"

    bucket_name = CONFIG.s3_config.bucket

    s3_client.upload_file(
        Filename=file_path,
        Bucket=bucket_name,
        Key=f"oqapi_hdx/downloads/{country_code}/{file_path}"
    )
