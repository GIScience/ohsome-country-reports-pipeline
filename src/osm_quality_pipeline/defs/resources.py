import dagster as dg
import requests as r
import pandas as pd
from io import StringIO

from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager


from osm_quality_pipeline.defs.utils.utils import (
    handle_http_error,
    handle_timeout_error,
    handle_connection_error,
    handle_stats_http_error,
    handle_stats_timeout_error,
    handle_stats_connection_error,
    extract_values_from_oqapi_response,
)
from osm_quality_pipeline.defs.utils.rate_limiter import ApiQuotaTracker
from osm_quality_pipeline.defs.constants import (
    CONFIG,
    DATA_DIR,
    OHSOME_QUALITY_API_URL,
    HEIGIT_API_KEY,
    OHSOME_API_URL,
)


api_quota_tracker = ApiQuotaTracker(db_path=f"{DATA_DIR}/rate_limits.sqlite")


duckdb_io_manager = DuckDBPandasIOManager(
    database=f"{DATA_DIR}/asset_output.duckdb"
)

s3_resource = S3Resource(
    aws_access_key_id=CONFIG.s3_config.key_id,
    aws_secret_access_key=CONFIG.s3_config.secret,
    endpoint_url=f"https://{CONFIG.s3_config.host}"
)


def build_table_name(topic, indicator):

    topic_name = topic.replace('-', '_')
    indicator_name = indicator.replace('-', '_')
    return f"{topic_name}_{indicator_name}"


class CustomDuckDBResource(DuckDBResource):

    def query_asset_results_df(self, partition_key, topic, indicator, attribute, attribute_column="attribute", table_name=None):
        table_name = table_name or build_table_name(topic, indicator)
        query = f"SELECT * FROM public.{table_name} WHERE partition_key = '{partition_key}'"
        if attribute:
            query += f" AND {attribute_column} = '{attribute}'"
        with self.get_connection() as conn:
            df = conn.execute(query).fetchdf()
            return df

    def check_if_table_exists(self, topic, indicator, attribute, table_name=None):
        table_name = table_name or build_table_name(topic, indicator)
        with self.get_connection() as conn:
            count = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()[0]

            table_exists = count == 1

            return table_name, table_exists



duckdb_resource = CustomDuckDBResource(
    database=f"{DATA_DIR}/asset_output.duckdb"
)


class OhsomeQualityApiResource(dg.ConfigurableResource):

    @property
    def base_url(self) -> str:
        return OHSOME_QUALITY_API_URL

    def query(self, indicator, topic, attribute, geojson_geometry, geom_id):
        url = f"{self.base_url}/indicators/{indicator}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": HEIGIT_API_KEY
        }

        params = {
            "topic": topic,
            "bpolys": geojson_geometry,
        }

        if indicator == "attribute-completeness":
            params["attributes"] = [attribute]

        try:
            resp = r.post(url, json=params, headers=headers, timeout=120)
            api_quota_tracker.observe("ohsome_quality_api", resp.headers)
            resp.raise_for_status()
            row_results = extract_values_from_oqapi_response(resp)
        except r.Timeout:
            row_results = handle_timeout_error(geom_id, indicator, topic)
        except r.ConnectionError:
            row_results = handle_connection_error(geom_id, indicator, topic)
        except r.HTTPError:
            row_results = handle_http_error(geom_id, indicator, resp, topic)

        return row_results

ohsome_quality_api = OhsomeQualityApiResource()


class OhsomeApiResource(dg.ConfigurableResource):

    @property
    def base_url(self) -> str:
        return OHSOME_API_URL

    def stats_features(self, geojson_geometry, filter_expr, grouping_key, measure):
        url = f"{self.base_url}/stats/features/{measure}.csv"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": HEIGIT_API_KEY
        }

        params = {
            "aoi":  geojson_geometry,
            "filter": filter_expr,
            "time": "latest",
            "groupBy": {"type": "byTag", "key": grouping_key}
        }

        try:
            resp = r.post(url, json=params, headers=headers, timeout=180)
            api_quota_tracker.observe("ohsome_api", resp.headers)
            resp.raise_for_status()

            df = pd.read_csv(
                StringIO(resp.text),
                delimiter=";",
                header=3,
            )
            df["status_code"] = 200
            df["description"] = None
        except r.Timeout:
            df = handle_stats_timeout_error()
        except r.ConnectionError:
            df = handle_stats_connection_error()
        except r.HTTPError:
            df = handle_stats_http_error(resp)

        return df

ohsome_api = OhsomeApiResource()

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "ohsome_api": ohsome_quality_api,
            "ohsome_api_v2": ohsome_api,
            "s3": s3_resource,
            "duckdb_io_manager": duckdb_io_manager,
            "duckdb": duckdb_resource
        }
    )
