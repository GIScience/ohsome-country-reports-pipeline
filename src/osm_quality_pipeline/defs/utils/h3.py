import os

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import shape, box

from osm_quality_pipeline.defs.constants import H3_ZOOM_LEVEL


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
