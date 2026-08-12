from pathlib import Path

import dagster as dg
import geopandas as gpd
import pandas as pd

from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.constants import CONFIG, DATA_DIR

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
    group_name="outputs",
    ins=ins
)
def indicator_results_csv_s3(context: dg.AssetExecutionContext, s3: S3Resource, **kwargs) -> None:
    dfs = [df for df in kwargs.values() if df is not None]
    if not dfs:
        return None
    
    combined = pd.concat(dfs)
    combined = combined.drop("geometry", axis=1)

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer


    csv_path = f"{DATA_DIR}/{country_code}/{country_code}_{layer}_indicator_results.csv"

    combined.to_csv(csv_path, encoding='utf-8', index=False)

    upload_file_to_s3(csv_path, country_code, s3)


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    group_name="outputs",
    ins=ins
)
def indicator_results_gpkg_s3(context: dg.AssetExecutionContext, s3: S3Resource, **kwargs) -> None:
    dfs = [df for df in kwargs.values() if df is not None]
    if not dfs:
        return None

    combined = pd.concat(dfs)
    combined["geometry"] = gpd.GeoSeries.from_wkt(combined["geometry"])

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer

    gpkg_path = f"{DATA_DIR}/{country_code}/{country_code}_{layer}_indicator_results.gpkg"

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

    upload_file_to_s3(gpkg_path, country_code, s3)




def upload_file_to_s3(file_path: str, country_code:str, s3: S3Resource) -> None:
    s3_client = s3.get_client()

    file_name = Path(file_path).name

    s3_client.upload_file(
        Filename=file_path,
        Bucket=CONFIG.s3_config.bucket,
        Key=f"oqapi_hdx/downloads/{country_code}/{file_name}"
    )


