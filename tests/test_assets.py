from src.osm_quality_pipeline.defs.assets import h3_hexgrid
import geopandas as gpd
import dagster as dg
from pathlib import Path
import shutil

def test_hexgrid_creation():
    gdf = gpd.read_file("data/test_poly_ADM0.geojson").to_crs(4326)
    grid_clipped, zoom_level = h3_hexgrid.create_h3_gdf(gdf=gdf, country="country")
    assert len(grid_clipped == 15)

def test_hexgrid_asset():
    context = dg.build_asset_context(partition_key="tmp")
    boundary_path = "../tests/data/"
    
    result = h3_hexgrid.h3_hexgrid_asset(
        context=context,
        geoboundary_geojson=str(boundary_path),
    )
    gdf = gpd.read_file(result.value)
    assert len(gdf) == 15
    shutil.rmtree(Path(result.value).parent)