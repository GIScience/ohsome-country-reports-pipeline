import os
import dagster as dg

import requests as r
import sys
import geopandas as gpd
import io

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
        updated_partitions = country_layers_bkg(context, config, s3, country, out_dir)
    else:
        updated_partitions = country_layers_osm(context, config, country, out_dir)

    return dg.MaterializeResult(
        value=updated_partitions, metadata={"partitions": updated_partitions}
    )

def country_layers_bkg(context, config, s3, country, out_dir):
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


def country_layers_osm(context, config, country, out_dir):
    adm0_boundary_path, gdf_adm0 = download_osm_boundary_by_iso(country)

    download_osm_subnational_boundary(country, gdf_adm0)

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


def download_osm_boundary_by_iso(country):
    # download country boundary with iso code from osm
    level = "adm0"
    logger.info(f"download osm boundaries and create partitions for {country}|{level}")
    adm0_boundary_path = os.path.join(DATA_DIR, country, f"{country}_adm0.gpkg")
    url = f"https://maps.heigit.org/vector/service/ohsome/wfs?service=wfs&request=GetFeature&typeNames=ohsome%3Aadmin%5Fworld%5Fwater&outputFormat=application%2Fjson&version=2%2E0%2E0&srsName=EPSG%3A4326&CQL_FILTER=%22iso%22%20%3D%20%27{country}%27"

    try:
        download = r.get(url)
        download.raise_for_status()
    except r.RequestException as e:
        print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
        sys.exit(1)

    gdf = gpd.read_file(io.BytesIO(download.content))
    gdf["osm_id"] = gdf["id"]
    gdf["id"] = [
        f"{country}_adm{level}_{str(i + 1).zfill(2)}"
        for i in range(len(gdf))
    ]
    gdf.to_file(adm0_boundary_path, driver="GPKG")
    logger.info(f"Saved to {adm0_boundary_path}")

    return adm0_boundary_path, gdf

def download_osm_subnational_boundary(country, gdf_adm0):
    level = "adm1"
    logger.info(f"download osm boundaries and create partitions for {country}|{level}")

    gdf_osm_admin_3 = download_osm_boundary_by_parent(gdf_adm0, 3)
    if len(gdf_osm_admin_3) > 0:
        gdf_adm1 = download_osm_boundary_by_parent(gdf_osm_admin_3, 4)
    else:
        gdf_adm1 = download_osm_boundary_by_parent(gdf_adm0, 4)

    gdf_adm1["id"] = [
        f"{country}_adm1_{str(i + 1).zfill(2)}"
        for i in range(len(gdf_adm1))
    ]
    adm1_boundary_path = os.path.join(DATA_DIR, country, f"{country}_adm1.gpkg")
    gdf_adm1.to_file(adm1_boundary_path, driver="GPKG")
    logger.info(f"Saved to {adm1_boundary_path}")


def download_osm_boundary_by_parent(gdf_parent, osm_admin_level):
    parent_ids = gdf_parent["osm_id"].astype(str).to_list()
    parent_ids_str = ','.join(parent_ids)
    cql_filter = f'"parent" IN ({parent_ids_str}) and admin_level = {osm_admin_level}'
    url = f"https://maps.heigit.org/vector/service/ohsome/wfs?service=wfs&request=GetFeature&typeNames=ohsome%3Aadmin%5Fworld%5Fwater&outputFormat=application%2Fjson&version=2%2E0%2E0&srsName=EPSG%3A4326&CQL_FILTER={cql_filter}"
    try:
        download = r.get(url)
        download.raise_for_status()
    except r.RequestException as e:
        print(f"[{osm_admin_level}] ERROR downloading: {e}", file=sys.stderr)
        sys.exit(1)
    gdf = gpd.read_file(io.BytesIO(download.content))

    if len(gdf) > 0:
        gdf["osm_id"] = gdf["id"]
        return gdf
    else:
        return []