from pathlib import Path

import dagster as dg
import geopandas as gpd
import pandas as pd

from osm_quality_pipeline.defs.constants import (
    TOPICS_BY_INDICATOR,
    STATIC_TOPIC_ASSETS,
    ALL_TOPICS,
)
from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition


def _get_topic_deps(topic: str) -> list[str]:
    topic_ = topic.replace("-", "_")
    deps = []
    for indicator, topics in TOPICS_BY_INDICATOR.items():
        if topic in topics:
            indicator_ = indicator.replace("-", "_")
            deps.append(f"{topic_}_{indicator_}")
    deps.extend(STATIC_TOPIC_ASSETS.get(topic, []))
    return deps


def make_topic_gpkg_asset(topic: str):
    topic_ = topic.replace("-", "_")
    deps = _get_topic_deps(topic)

    ins = {dep: dg.AssetIn(key=dep) for dep in deps}

    @dg.asset(
        partitions_def=dynamic_country_layers_partition,
        name=f"{topic_}_gpkg",
        group_name=topic_,
        ins=ins,
        metadata={"partition_expr": "partition_key"},
        io_manager_key="duckdb_io_manager",
    )
    def generic_topic_gpkg_asset(context: dg.AssetExecutionContext, **kwargs) -> None:
        dfs = [df for df in kwargs.values() if df is not None]
        if not dfs:
            return None

        df = pd.concat(dfs)
        df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        country_code = context.partition_key.split("|")[0]
        Path(f"data/{country_code}/Outputs").mkdir(parents=True, exist_ok=True)
        gdf.to_file(
            f"data/{country_code}/Outputs/{country_code}_{topic_}.gpkg",
            layer=topic_,
            driver="GPKG",
        )

    return generic_topic_gpkg_asset


all_topic_gpkg_assets = [make_topic_gpkg_asset(topic) for topic in ALL_TOPICS]
