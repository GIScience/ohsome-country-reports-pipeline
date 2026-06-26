import os
import dagster as dg
import pandas as pd
import geopandas as gpd
import requests as r
import zipfile
import io
import sys
import urllib.request


from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.constants import (
    H3_ZOOM_LEVEL,
    DATA_DIR,
    BoundaryConfig
)

import h3
from shapely.geometry import shape, box


logger = dg.get_dagster_logger()


@dg.asset(partitions_def=country_partitions, group_name="preparation")
def country_layers(context, config: BoundaryConfig) -> dg.MaterializeResult[list[str]]:
    country = context.partition_key
    logger.info(country)

    old_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(f"existing partitions: {old_partitions}")

    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    if country == "DEU":
        logger.info("download from BKG for Germany")

        for level_val in config.bkg_boundary_levels:
            download_url = f"{config.bkg_boundary_url}/DEU_{level_val}.gpkg"
            logger.info(f"start download: {download_url}")
            urllib.request.urlretrieve(
                download_url,
                f"{DATA_DIR}/DEU/DEU_{level_val}.gpkg"
            )

        create_h3_layer(country, f"{DATA_DIR}/DEU/DEU_vg25_sta.gpkg", out_dir)

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|bundesländer",
            f"{country}|gemeinden",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions(
            "dynamic_country_layers", updated_partitions
        )

    else:
        logger.info("download from geoboundaries")

        download_from_geoboundaries(
            country=country,
            level_val="boundaryType",
            url_val="gjDownloadURL",
            out_dir=out_dir,
        )

        adm0_boundary_path = os.path.join("data", country, f"{country}_adm0.gpkg")
        create_h3_layer(country, adm0_boundary_path, out_dir)

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|adm1",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions(
            "dynamic_country_layers", updated_partitions
        )

    return dg.MaterializeResult(
        value=updated_partitions, metadata={"partitions": updated_partitions}
    )


def download_from_geoboundaries(country, level_val, url_val, out_dir):

    list_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL"
    print(f"Fetching available levels from {list_url}")
    try:
        resp = r.get(list_url)
        resp.raise_for_status()
        entries = resp.json()
    except r.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(entries, list) or not entries:
        print(f"No boundaries found for country '{country}'", file=sys.stderr)
        sys.exit(1)

    any_failures = False

    # 2) Iterate over every entry and download
    for entry in entries:
        level = entry.get(level_val)  # e.g. "ADM0", "ADM1", ...
        url = entry.get(url_val)
        if not level or not url:
            print(f"Skipping malformed entry: {entry}", file=sys.stderr)
            any_failures = True
            continue

        out_path = os.path.join(out_dir, f"{country}_{level.lower()}.gpkg")

        print(f"[{level}] Downloading from {url}")
        try:
            download = r.get(url)
            download.raise_for_status()
        except r.RequestException as e:
            print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
            any_failures = True
            continue

        gdf = gpd.read_file(io.BytesIO(download.content))
        if "id" not in gdf.columns:
            adm_level = (
                os.path.basename(out_path)
                .split("_")[1]
                .replace("adm", "")
                .replace(".gpkg", "")
            )
            gdf = gdf.reset_index(drop=True)
            gdf["id"] = [
                f"{country}_adm{adm_level}_{str(i + 1).zfill(2)}"
                for i in range(len(gdf))
            ]
            gdf.to_file(out_path, driver="GPKG")
        print(f"[{level}] Saved to {out_path}")

    if any_failures:
        print("One or more boundaries failed to download.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All available boundaries downloaded successfully.")


def create_h3_gdf(gdf, country):
    params = get_dynamic_resolutions(gdf)
    zoom_level = params["h3"]

    minx, miny, maxx, maxy = gdf.total_bounds
    buf = 0.05
    bbox_geom = box(minx - buf, miny - buf, maxx + buf, maxy + buf)

    cell_series = pd.Series(h3.geo_to_cells(bbox_geom, res=zoom_level))
    grid_gdf = gpd.GeoDataFrame(
        geometry=cell_series.apply(lambda c: shape(h3.cells_to_geo([c]))),
        crs="EPSG:4326",
    )
    gdf = gpd.GeoDataFrame(gdf[["geometry"]], geometry="geometry", crs="EPSG:4326")
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection").reset_index(drop=True)

    grid_clipped["country"] = country
    grid_clipped["z"] = zoom_level
    grid_clipped["id"] = f"{country}_hex{zoom_level}_" + (
        grid_clipped.index + 1
    ).astype(str)
    grid_clipped = grid_clipped[["id", "country", "geometry"]]

    return grid_clipped, zoom_level


def get_dynamic_resolutions(gdf):
    """
    Determines grid parameters using an accurate equal-area projection
    calculation before checking config overrides.
    """
    # 1. Calculate accurate area using Mollweide projection (Units: Meters)
    # We use a copy so we don't accidentally modify the original GDF's CRS
    area_m2 = gdf.to_crs("ESRI:54009").area.sum()
    area_km2 = area_m2 / 1_000_000

    # 2. Define smart defaults based on area (same thresholds as before)
    if area_km2 < 50_000:
        smart_sq, smart_h3 = 0.05, 6
    elif area_km2 < 500_000:
        smart_sq, smart_h3 = 0.1, 5
    elif area_km2 < 5_000_000:
        smart_sq, smart_h3 = 0.3, 4
    else:
        smart_sq, smart_h3 = 0.8, 3

    # 3. Extract overrides from the 'grids' config block
    conf_h3 = H3_ZOOM_LEVEL

    return {
        "h3": conf_h3 if conf_h3 is not None else smart_h3,
    }


def create_h3_layer(country, adm0_boundary_path, out_dir):
    gdf = gpd.read_file(adm0_boundary_path).to_crs(4326)
    grid_clipped, zoom_level = create_h3_gdf(gdf=gdf, country=country)
    output_path = os.path.join(out_dir, f"{country}_h3.gpkg")
    grid_clipped.to_file(output_path, driver="GPKG")
