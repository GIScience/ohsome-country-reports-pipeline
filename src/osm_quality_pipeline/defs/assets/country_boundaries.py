import os

import dagster as dg

from osm_quality_pipeline.defs.assets.indicator_results import upload_file_to_s3
from osm_quality_pipeline.defs.constants import DATA_DIR, BoundaryConfig
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.utils.pmtiles import write_country_boundaries_pmtiles

logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_partitions,
    group_name="preparation",
    deps=["country_layers"],
)
def country_boundaries_pmtiles(context: dg.AssetExecutionContext, config: BoundaryConfig, s3: S3Resource) -> None:
    country = context.partition_key

    levels = config.bkg_boundary_levels if country == "DEU" else config.geoboundaries_levels
    layer_names = [level.lower() for level in levels] + ["h3"]

    layer_gpkg_paths = {}
    for layer in layer_names:
        gpkg_path = os.path.join(DATA_DIR, country, f"{country}_{layer}.gpkg")
        if os.path.exists(gpkg_path):
            layer_gpkg_paths[layer] = gpkg_path
        else:
            logger.warning(f"[{country}] boundary layer file not found, skipping: {gpkg_path}")

    if not layer_gpkg_paths:
        raise dg.Failure(description=f"No boundary layer files found for {country}.")

    pmtiles_path = os.path.join(DATA_DIR, country, f"{country}_boundaries.pmtiles")
    write_country_boundaries_pmtiles(layer_gpkg_paths, pmtiles_path)

    upload_file_to_s3(pmtiles_path, country, s3)
