from osm_quality_pipeline.defs.assets.land_cover_completeness import land_cover_completeness
from osm_quality_pipeline.defs.assets.mapping_saturation import make_mapping_saturation_asset
from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests
import geopandas as gpd
import dagster as dg
import pytest
import pandas as pd
from pathlib import Path
import pytest
from osm_quality_pipeline.defs.partitions import get_country_layer_from_partitionkey
from osm_quality_pipeline.defs.utils.utils import load_layer_as_gdf


@pytest.fixture(autouse=True)
def project_root_cwd(monkeypatch):
    project_root = Path(__file__).resolve().parents[1]
    monkeypatch.chdir(project_root)

"""
def test_hexgrid_creation():
    gdf = gpd.read_file("data/test_poly_ADM0.geojson").to_crs(4326)
    grid_clipped, zoom_level = h3_hexgrid.create_h3_gdf(gdf=gdf, country="country")
    assert len(grid_clipped == 15)


def test_hexgrid_asset():
    context = dg.build_asset_context(partition_key="tmp")
    boundary_path = "../tests/data/"
    result = h3_hexgrid.h3_hexgrid(
        context=context,
        geoboundary_geojson=str(boundary_path),
    )
    gdf = gpd.read_file(result.value)
    assert len(gdf) == 15
    shutil.rmtree(Path(result.value).parent)
"""

# for the tests to work, you currently need to have the STP data in the data directory

def test_oqapi_request_mapping_saturation():
    context = dg.build_asset_context(
        partition_key=dg.MultiPartitionKey(
            {
                "country": "STP",
                "layer": "adm0",
            }
        )
    )
    topic = "roads-all-highways"
    INDICATOR = "mapping-saturation"
    partition_key = context.partition_key
    country_layer = get_country_layer_from_partitionkey(partition_key)
    country = country_layer.country
    layer = country_layer.layer

    gdf = load_layer_as_gdf(context, country, layer)

    df, is_valid = oqapi_requests(context, gdf, topic, partition_key, indicator=INDICATOR)
    assert not df.empty
    assert df.iloc[0]["value"] == pytest.approx(1.0, 0.05)


def test_oqapi_request_land_cover_completeness():
    context = dg.build_asset_context(
        partition_key=dg.MultiPartitionKey(
            {
                "country": "STP",
                "layer": "adm0",
            }
        )
    )
    df = land_cover_completeness(context)
    assert not df.empty
    assert df.iloc[0]["value"] == pytest.approx(1.04, 0.05)