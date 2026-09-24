import duckdb as ddb
import pytest

from osm_quality_pipeline.defs.resources import CustomDuckDBResource


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "test.duckdb")
    with ddb.connect(path) as conn:
        conn.execute("CREATE TABLE buildings_currentness (id VARCHAR, partition_key VARCHAR)")
    return path


def test_existing_table_detected_by_default(db_path):
    duckdb = CustomDuckDBResource(database=db_path)
    table_name, table_exists = duckdb.check_if_table_exists("buildings", "currentness", None)
    assert table_name == "buildings_currentness"
    assert table_exists


def test_overwrite_ignores_existing_table(db_path):
    duckdb = CustomDuckDBResource(database=db_path, overwrite=True)
    _, table_exists = duckdb.check_if_table_exists("buildings", "currentness", None)
    assert not table_exists


def test_missing_table_not_detected(db_path):
    duckdb = CustomDuckDBResource(database=db_path)
    _, table_exists = duckdb.check_if_table_exists("roads", "currentness", None)
    assert not table_exists
