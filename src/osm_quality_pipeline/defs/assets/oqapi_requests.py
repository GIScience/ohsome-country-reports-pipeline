import os
import json
from pathlib import Path

import dagster as dg
import geopandas as gpd
import requests

from osm_quality_pipeline.defs.constants import ApiRequestConfig
from osm_quality_pipeline.defs.partitions import country_layers_partition, dynamic_country_layers_partition
from osm_quality_pipeline.defs.assets.utils import oqapi_requests, get_country_layer_from_partitionkey


logger = dg.get_dagster_logger()


@dg.asset(
    deps=["country_layers"],
    partitions_def=dynamic_country_layers_partition,
)
def responses_mapping_saturation(
    context: dg.AssetExecutionContext,
):
    INDICATOR = "mapping-saturation"

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country = country_layer.country
    layer = country_layer.layer

    topic = "building-count"

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / layer
    raw_dir.mkdir(parents=True, exist_ok=True)

    # TODO: make this work for all layers
    # TOOD: store all layers in same data format?
    layer_path = os.path.join("data", country, f"{country}_{layer}.gpkg")
    gdf = gpd.read_file(layer_path)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )


@dg.asset(
    partitions_def=country_layers_partition,
)
def responses_user_activity(
    context: dg.AssetExecutionContext,
    config: ApiRequestConfig,
):
    INDICATOR = "user-activity"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )


@dg.asset(
    partitions_def=country_layers_partition,
)
def responses_roads_thematic_accuracy(
    context: dg.AssetExecutionContext,
    config: ApiRequestConfig,
):
    INDICATOR = "roads-thematic-accuracy"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )

@dg.asset(
    partitions_def=country_layers_partition,
)
def responses_building_comparison(
    context: dg.AssetExecutionContext,
    config: ApiRequestConfig,
):
    INDICATOR = "building-comparison"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )


@dg.asset(
    partitions_def=country_layers_partition,
)
def responses_land_cover_completeness(
    context: dg.AssetExecutionContext,
    config: ApiRequestConfig,
):
    INDICATOR = "land-cover-completeness"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )


@dg.asset(
    partitions_def=country_layers_partition,
)
def responses_land_cover_thematic_accuracy(
    context: dg.AssetExecutionContext,
    config: ApiRequestConfig,
):
    INDICATOR = "land-cover-thematic-accuracy"
    keys = context.partition_key.keys_by_dimension
    country = keys["country"]
    topic = keys["topic"]

    raw_dir = Path("data") / country / f"raw_responses_{topic}" / "hex"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gdf = gpd.read_file(h3_hexgrid)

    success = oqapi_requests(gdf=gdf, topic=topic, indicator=INDICATOR, raw_dir=raw_dir)

    return dg.Output(
        {"raw_dir": str(raw_dir)},
        metadata={
            "country": country,
            "topic": topic,
            "indicator": INDICATOR,
            "cells_processed": success,
        },
    )
# TODO: how to do attribute completeness?
