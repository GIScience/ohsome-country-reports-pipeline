import os

import dagster as dg

from osm_quality_pipeline.defs.assets.indicator_results import upload_file_to_s3
from osm_quality_pipeline.defs.constants import DATA_DIR
from osm_quality_pipeline.defs.partitions import country_partitions, get_h3_config
from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.utils.pmtiles import write_country_boundaries_pmtiles

logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_partitions,
    group_name="preparation",
    deps=["country_layers"],
)
def country_pmtiles_s3(context: dg.AssetExecutionContext, s3: S3Resource) -> None:
    country = context.partition_key

    if country == "DEU":
        levels = ["vg2500_sta", "vg2500_lan", "vg1000_krs"]
    else:
        levels = ["adm0", "adm1"]

    if get_h3_config(country):
        levels.append("h3")


    layer_gpkg_paths = {}
    for layer in levels:
        gpkg_path = os.path.join(DATA_DIR, country, f"{country}_{layer}.gpkg")
        if os.path.exists(gpkg_path):
            layer_gpkg_paths[layer] = gpkg_path
        else:
            logger.warning(f"[{country}] boundary layer file not found, skipping: {gpkg_path}")

    if not layer_gpkg_paths:
        raise dg.Failure(description=f"No boundary layer files found for {country}.")


    pmtiles_path = os.path.join(DATA_DIR, country, f"{country}_boundaries.pmtiles")

    write_country_boundaries_pmtiles(
        layer_gpkg_paths,
        pmtiles_path
    )

    upload_file_to_s3(
        pmtiles_path,
        country,
        s3
    )
