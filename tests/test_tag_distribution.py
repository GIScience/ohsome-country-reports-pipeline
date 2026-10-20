from unittest.mock import MagicMock, patch

import geopandas as gpd
import pytest
import requests
from shapely.geometry import box

from osm_quality_pipeline.defs.resources import OhsomeApiResource
from osm_quality_pipeline.defs.utils.ohsome import request_loop

SUCCESS_CSV = (
    "# preamble\n# preamble\n# preamble\n"
    "timestamp;group;value\n"
    "2025-01-01T00:00:00Z;;10\n"
    "2025-01-01T00:00:00Z;house;6\n"
    "2025-01-01T00:00:00Z;garage;2\n"
)


def _success():
    resp = MagicMock()
    resp.status_code = 200
    resp.text = SUCCESS_CSV
    resp.headers = {}
    return resp


def _server_error():
    resp = MagicMock()
    resp.status_code = 500
    resp.text = "Internal Server Error"
    resp.headers = {}
    resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


@pytest.fixture
def gdf():
    return gpd.GeoDataFrame({"id": ["test_01"], "geometry": [box(0, 0, 1, 1)]}, crs="EPSG:4326")


def _run(gdf, post_side_effect):
    with (
        patch("osm_quality_pipeline.defs.resources.r.post", side_effect=post_side_effect),
        patch("osm_quality_pipeline.defs.resources.ohsome_api_rate_limiter"),
        patch("osm_quality_pipeline.defs.resources.api_quota_tracker"),
        patch("osm_quality_pipeline.defs.utils.ohsome.ohsome_api_rate_limiter"),
    ):
        return request_loop(gdf, OhsomeApiResource(), "building=*", "building", ["count", "area"])


def test_request_loop_stats_error_is_captured_as_row(gdf):
    # count succeeds, area times out
    df = _run(gdf, [_success(), requests.Timeout()])

    assert len(df) == 1
    row = df.iloc[0]
    assert row["id"] == "test_01"
    assert row["status_code"] == 999
    assert row["description"] == "Timeout Error"
    for measure in ["count", "area"]:
        assert f"treemap_{measure}" in df.columns
        assert f"sum_value_{measure}" in df.columns


def test_request_loop_success(gdf):
    df = _run(gdf, [_success(), _success()])

    row = df.iloc[0]
    assert row["status_code"] == 200
    assert row["description"] is None
    assert row["sum_value_count"] == 10
    assert row["treemap_count"] is not None


def test_request_loop_retries_server_error_once(gdf):
    # count: 500 then success on retry, area: success
    df = _run(gdf, [_server_error(), _success(), _success()])

    assert df.iloc[0]["status_code"] == 200
