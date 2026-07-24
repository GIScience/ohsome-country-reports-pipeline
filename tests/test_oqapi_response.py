import datetime
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
from shapely.geometry import box

from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests


@pytest.fixture
def sample_gdf():
    geom = box(0, 0, 1, 1)
    return gpd.GeoDataFrame({"id": ["test_01"], "geometry": [geom]}, crs="EPSG:4326")


@pytest.fixture
def mock_context():
    ctx = MagicMock()
    ctx.partition_key = "DEU|adm0"
    return ctx


@pytest.fixture
def mock_api_resource():
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.OhsomeQualityApiResource"
    ) as mock:
        yield mock


@pytest.fixture
def mock_load_existing():
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.load_existing_results_gdf",
        return_value=None,
    ):
        yield


def _make_success_query_result():
    return [
        "Roads (cars)",
        "mapping-saturation",
        200,
        0.85,
        "good",
        2,
        "2024-01-01T00:00:00Z",
    ]


def _make_timeout_query_result():
    return [
        "roads",
        "mapping-saturation",
        999,
        -999,
        "Timeout Error",
        None,
        datetime.datetime.now(),
    ]


def _make_connection_error_result():
    return [
        "roads",
        "mapping-saturation",
        998,
        -999,
        "Network Failure",
        None,
        datetime.datetime.now(),
    ]


def _make_http_error_result():
    return [
        "roads",
        "mapping-saturation",
        500,
        -999,
        "Internal Server Error",
        None,
        datetime.datetime.now(),
    ]


def test_success(sample_gdf, mock_context, mock_api_resource, mock_load_existing):
    mock_api_resource.return_value.query.return_value = _make_success_query_result()

    df, is_valid = oqapi_requests(
        mock_context,
        sample_gdf,
        topic="roads",
        partition_key="DEU|adm0",
        indicator="mapping-saturation",
    )

    row = df.iloc[0]
    assert row["status_code"] == 200
    assert row["value"] == 0.85
    assert row["quality_class"] == 2
    assert is_valid
    assert isinstance(row["id"], str)


def test_timeout(sample_gdf, mock_context, mock_api_resource, mock_load_existing):
    mock_api_resource.return_value.query.return_value = _make_timeout_query_result()

    df, is_valid = oqapi_requests(
        mock_context,
        sample_gdf,
        topic="roads",
        partition_key="DEU|adm0",
        indicator="mapping-saturation",
    )

    row = df.iloc[0]
    assert row["status_code"] == 999
    assert row["value"] == -999
    assert row["description"] == "Timeout Error"
    assert row["quality_class"] is None
    assert not is_valid


def test_network_error(sample_gdf, mock_context, mock_api_resource, mock_load_existing):
    mock_api_resource.return_value.query.return_value = _make_connection_error_result()

    df, is_valid = oqapi_requests(
        mock_context,
        sample_gdf,
        topic="roads",
        partition_key="DEU|adm0",
        indicator="mapping-saturation",
    )

    row = df.iloc[0]
    assert row["status_code"] == 998
    assert row["value"] == -999
    assert row["description"] == "Network Failure"
    assert row["quality_class"] is None
    assert not is_valid


def test_http_error(sample_gdf, mock_context, mock_api_resource, mock_load_existing):
    mock_api_resource.return_value.query.return_value = _make_http_error_result()

    df, is_valid = oqapi_requests(
        mock_context,
        sample_gdf,
        topic="roads",
        partition_key="DEU|adm0",
        indicator="mapping-saturation",
    )

    row = df.iloc[0]
    assert row["status_code"] == 500
    assert row["value"] == -999
    assert row["description"] == "Internal Server Error"
    assert row["quality_class"] is None
    assert not is_valid
