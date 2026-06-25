import os
import dagster as dg
import pandas as pd
import geopandas as gpd
import requests as r
import zipfile
import io


from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.constants import H3_ZOOM_LEVEL, BKG_BOUNDARY_URL, DATA_DIR

import h3
from shapely.geometry import shape, box


logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_partitions,
    group_name="preparation"
)
def country_layers(context) -> dg.MaterializeResult[list[str]]:
    country = context.partition_key
    logger.info(country)

    old_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(f"existing partitions: {old_partitions}")

    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    if country == "DEU":
        logger.info("download from BKG for Germany")

        adm0_boundary_path = download_from_bkg(level_val="vg25_sta")
        logger.info(adm0_boundary_path)

        create_h3_layer(country, adm0_boundary_path, out_dir)

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|bundesländer",
            f"{country}|gemeinden",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions("dynamic_country_layers", updated_partitions)

    else:
        logger.info("download from geoboundaries")
        try:
            download_from_geoboundaries(
                country=country,
                level_val="boundaryType",
                url_val="gjDownloadURL",
                out_dir=out_dir
            )
        except SystemExit as e:
            context.log.warning(f"[{country}] geoBoundaries download failed: {e}")
            raise Exception(f"Failed to fetch boundaries for {country} from geoBoundaries.")

        adm0_boundary_path = os.path.join("data", country, "boundary_ADM0.geojson")
        create_h3_layer(country, adm0_boundary_path, out_dir)

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|adm1",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions("dynamic_country_layers", updated_partitions)


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

        out_path = os.path.join(out_dir, f"boundary_{level}.geojson")

        print(f"[{level}] Downloading from {url}")
        try:
            download = r.get(url)
            download.raise_for_status()
        except r.RequestException as e:
            print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
            any_failures = True
            continue

        with open(out_path, "wb") as fp:
            fp.write(download.content)
        print(f"[{level}] Saved to {out_path}")

    if any_failures:
        print("One or more boundaries failed to download.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All available boundaries downloaded successfully.")


def download_from_bkg(level_val):   #layer: vg25_sta, vg25_lan, vg25_gem

    germany_dir = DATA_DIR / "DEU"
    germany_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = germany_dir / "DE_VG25.gpkg"
    out_path = germany_dir / f"{level_val}.geojson"

    try:
        resp = r.get(BKG_BOUNDARY_URL)
        resp.raise_for_status()
    except r.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        zip_path = "daten/DE_VG25.gpkg"
        with z.open(zip_path) as source:
            gpkg_path.write_bytes(source.read())

    gdf = (
        gpd.read_file(gpkg_path, layer=level_val)
        .to_crs(4326)
    )

    gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]
    gdf.to_file(out_path, driver="GeoJSON")

    gpkg_path.unlink()

    return out_path


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
    gdf = gpd.GeoDataFrame(
        gdf[["geometry"]], geometry="geometry", crs="EPSG:4326"
    )
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





