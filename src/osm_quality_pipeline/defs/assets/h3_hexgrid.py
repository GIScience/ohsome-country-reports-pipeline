import os
import dagster as dg
import pandas as pd
import geopandas as gpd
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.assets import constants
from osm_quality_pipeline.defs.assets import geoboundary
import h3
from shapely.geometry import shape, box


@dg.asset(ins={"geoboundary_geojson": dg.AssetIn()}, partitions_def=country_partitions) # TODO: maybe change this to deps instead of ins
def h3_hexgrid(context, geoboundary_geojson: dg.Output[str]) -> dg.Output[str]:
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    try:
        boundary_path = next(os.path.join(geoboundary_geojson, f) for f in os.listdir(geoboundary_geojson)if "ADM0" in f)
    except StopIteration:
        raise FileNotFoundError(f"[{country}] No ADM0 boundary file found.")

    gdf = gpd.read_file(boundary_path).to_crs(4326)
    grid_clipped, zoom_level = create_h3_gdf(gdf=gdf, country=country)

    output_path = os.path.join(out_dir, f"{country}_h3_z{zoom_level}.gpkg")
    grid_clipped.to_file(output_path, driver="GPKG")
    return dg.Output(output_path, metadata={"country": country, "zoom_level": zoom_level, "cell_count": len(grid_clipped), "output_path": output_path})


def create_h3_gdf(gdf, country):
    params = get_dynamic_resolutions(gdf)
    zoom_level = params["h3"]

    minx, miny, maxx, maxy = gdf.total_bounds
    buf = 0.05
    bbox_geom = box(minx - buf, miny - buf, maxx + buf, maxy + buf)

    cell_series = pd.Series(h3.geo_to_cells(bbox_geom, res=zoom_level))
    grid_gdf = gpd.GeoDataFrame(geometry=cell_series.apply(lambda c: shape(h3.cells_to_geo([c]))), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(gdf[["shapeName", "shapeISO", "geometry"]], geometry="geometry", crs="EPSG:4326")
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection").reset_index(drop=True)

    grid_clipped["h3_id"] = f"{country}_hex{zoom_level}_" + (grid_clipped.index + 1).astype(str)
    grid_clipped = grid_clipped[["h3_id", "shapeName", "shapeISO", "geometry"]]
    grid_clipped = grid_clipped.rename(
        columns={"h3_id": "id", "shapeName": "ADM0_name", "shapeISO": "ADM0_iso", "shapeID": "ADM0_id"})

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
    conf_sq = constants.SQUARE_RESOLUTION_DEGREE
    conf_h3 = constants.H3_ZOOM_LEVEL

    return {
        "square": conf_sq if conf_sq is not None else smart_sq,
        "h3": conf_h3 if conf_h3 is not None else smart_h3
    }