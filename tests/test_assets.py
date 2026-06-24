from src.osm_quality_pipeline.defs.assets import h3_hexgrid
from src.osm_quality_pipeline.defs.assets import oqapi_requests
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource
import shutil
from pathlib import Path
import geopandas as gpd
import dagster as dg
import json
import pytest

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


def test_oqapi_request():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "roads-all-highways|mapping-saturation",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.oqapi_api_requests(
        context=context,
        h3_hexgrid=str(boundary_path),
        ohsome_api=OhsomeQualityApiResource(),
    )
    with open(result.value["raw_dir"] + "/roads-all-highways__mapping-saturation__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == pytest.approx(1.0)
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_mapping_saturation():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "roads-all-highways",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_mapping_saturation(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/roads-all-highways__mapping-saturation__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == pytest.approx(1.0)
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_user_activity():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "roads-all-highways",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_user_activity(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/roads-all-highways__user-activity__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == pytest.approx(1.0)# assert not correct yet, indicator still broken
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_roads_thematic_accuracy():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "roads",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_roads_thematic_accuracy(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/roads__roads-thematic-accuracy__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] is None
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_building_comparison():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "building-area",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_building_comparison(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/building-area__building-comparison__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == 0.0
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_land_cover_completeness():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "land-cover",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_land_cover_completeness(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/land-cover__land-cover-completeness__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == 0.0
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)


def test_oqapi_request_land_cover_thematic_accuracy():
    context = dg.build_asset_context(partition_key=dg.MultiPartitionKey(
    {
        "country": "TMP",
        "topic": "land-cover",
    }))

    boundary_path = "../tests/data/TMP_h3_z6.gpkg"
    result = oqapi_requests.responses_land_cover_thematic_accuracy(
        context=context,
        h3_hexgrid=str(boundary_path),
    )
    with open(result.value["raw_dir"] + "/land-cover__land-cover-thematic-accuracy__TMP_hex6_3.json") as f:
        data = json.load(f)
    assert data["result"][0]["result"]["value"] == 0.0
    shutil.rmtree(Path(result.value["raw_dir"]).parent.parent)
    # this currently gives a 500 error, probably due to the test area being outside of germany.
    # consider maybe changing the test polygons/hex cells to a small area in germany.
