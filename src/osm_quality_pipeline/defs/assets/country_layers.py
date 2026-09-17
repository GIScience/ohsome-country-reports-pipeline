import os
import dagster as dg
import urllib.request

from osm_quality_pipeline.defs.utils.geoboundaries import download_from_geoboundaries
from osm_quality_pipeline.defs.utils.h3 import create_h3_layer
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.constants import (
    DATA_DIR,
    BoundaryConfig
)
from osm_quality_pipeline.defs.resources import S3Resource


logger = dg.get_dagster_logger()


@dg.asset(partitions_def=country_partitions, group_name="preparation")
def country_layers(context, config: BoundaryConfig, s3: S3Resource) -> dg.MaterializeResult[list[str]]:
    country = context.partition_key

    old_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(f"existing partitions: {old_partitions}")

    out_dir = os.path.join(DATA_DIR, country)
    os.makedirs(out_dir, exist_ok=True)

    if country == "DEU":
        updated_partitions = country_layers_germany(context, config, s3, country, out_dir)
    else:
        updated_partitions = country_layers_geoboundaries(context, config, country, out_dir)

    return dg.MaterializeResult(
        value=updated_partitions, metadata={"partitions": updated_partitions}
    )


def country_layers_geoboundaries(context, config, country, out_dir):
    for level_val in config.geoboundaries_levels:
        logger.info(f"download geo boundaries and create partitions for {country}|{level_val}")

        download_from_geoboundaries(
            country=country,
            level_val=level_val,
            url_val="gjDownloadURL",
            out_dir=out_dir,
        )

    adm0_boundary_path = os.path.join(DATA_DIR, country, f"{country}_adm0.gpkg")
    
    create_h3_layer(country, adm0_boundary_path, out_dir)
    logger.info(f"generated h3 layer for {country}")
    updated_partitions = [
        f"{country}|adm0",
        f"{country}|adm1",
        f"{country}|h3",
    ]
    context.instance.add_dynamic_partitions(
        "dynamic_country_layers", updated_partitions
    )
    logger.info(f"added dynamic partitions for {country}: {updated_partitions}")
    return updated_partitions


def country_layers_germany(context, config, s3, country, out_dir):
    logger.info("download BKG boundaries and create partitions for Germany")
    updated_partitions = []

    s3_client = s3.get_client()

    for level_val in config.bkg_boundary_levels:
        logger.info(f"start download: DEU_{level_val}.gpkg")
        s3_client.download_file(
            "heigit-ohsome-quality-api",
            f"bkg_boundaries/DEU_{level_val}.gpkg",
            f"{DATA_DIR}/DEU/DEU_{level_val}.gpkg"
        )

        updated_partitions.append(f"{country}|{level_val}")

    create_h3_layer(country, f"{DATA_DIR}/DEU/DEU_vg2500_sta.gpkg", out_dir)
    updated_partitions.append(f"{country}|h3")

    context.instance.add_dynamic_partitions(
        "dynamic_country_layers", updated_partitions
    )
    logger.info(f"added dynamic partitions for {country}: {updated_partitions}")
    return updated_partitions



