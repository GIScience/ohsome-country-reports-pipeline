import datetime
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
import requests as r
from shapely.geometry import box

from osm_quality_pipeline.defs.utils.oqapi import oqapi_requests


@pytest.fixture
def sample_gdf():
    geom = box(0, 0, 1, 1)
    return gpd.GeoDataFrame({"id": ["test_01"], "geometry": [geom]}, crs="EPSG:4326")


@pytest.fixture
def mock_api_resource():
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.OhsomeQualityApiResource"
    ) as mock:
        mock.return_value.base_url = "https://api.quality.ohsome.org/v1-test"
        yield mock


def _make_success_response():
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "result": [
            {
                "topic": {"name": "Roads (cars)"},
                "metadata": {"name": "mapping-saturation"},
                "result": {
                    "value": 0.85,
                    "description": "good",
                    "class": 2,
                    "timestampOSM": "2024-01-01T00:00:00Z",
                },
            }
        ]
    }
    return resp


def _make_http_error_response(status_code=500, text="Internal Server Error"):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.raise_for_status.side_effect = r.HTTPError(response=resp)
    return resp


def test_success(sample_gdf, mock_api_resource):
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.r.post",
        return_value=_make_success_response(),
    ):
        df = oqapi_requests(sample_gdf, topic="roads", indicator="mapping-saturation")

    row = df.iloc[0]
    assert row["status_code"] == 200
    assert row["value"] == 0.85
    assert row["topic"] == "Roads (cars)"
    assert row["quality_class"] == 2


def test_timeout(sample_gdf, mock_api_resource):
    with patch("osm_quality_pipeline.defs.utils.oqapi.r.post", side_effect=r.Timeout):
        df = oqapi_requests(sample_gdf, topic="roads", indicator="mapping-saturation")

    row = df.iloc[0]
    assert row["status_code"] == 999
    assert row["value"] == -999
    assert row["description"] == "Timeout Error"
    assert row["quality_class"] is None
    assert row["topic"] == "roads"
    assert row["indicator"] == "mapping-saturation"
    assert isinstance(row["osm_timestamp"], datetime.datetime)


def test_network_error(sample_gdf, mock_api_resource):
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.r.post", side_effect=r.ConnectionError
    ):
        df = oqapi_requests(sample_gdf, topic="roads", indicator="mapping-saturation")

    row = df.iloc[0]
    assert row["status_code"] == 998
    assert row["value"] == -999
    assert row["description"] == "Network Failure"
    assert row["quality_class"] is None
    assert row["topic"] == "roads"
    assert row["indicator"] == "mapping-saturation"
    assert isinstance(row["osm_timestamp"], datetime.datetime)


def test_http_error(sample_gdf, mock_api_resource):
    with patch(
        "osm_quality_pipeline.defs.utils.oqapi.r.post",
        return_value=_make_http_error_response(),
    ):
        df = oqapi_requests(sample_gdf, topic="roads", indicator="mapping-saturation")

    row = df.iloc[0]
    assert row["status_code"] == 500
    assert row["value"] == -999
    assert row["description"] == "Internal Server Error"
    assert row["quality_class"] is None
    assert row["topic"] == "roads"
    assert row["indicator"] == "mapping-saturation"
    assert isinstance(row["osm_timestamp"], datetime.datetime)
