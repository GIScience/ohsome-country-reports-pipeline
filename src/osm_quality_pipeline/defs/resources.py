import dagster as dg
import requests as r

from osm_quality_pipeline.defs.constants import DATA_DIR

from dagster_aws.s3 import S3Resource
from dagster_duckdb_pandas import DuckDBPandasIOManager


from osm_quality_pipeline.defs.utils.utils import handle_http_error, handle_timeout_error, handle_connection_error, extract_values_from_oqapi_response
from osm_quality_pipeline.defs.constants import CONFIG

duckdb_io_manager = DuckDBPandasIOManager(
    database=f"{DATA_DIR}/asset_output.duckdb"
)

s3_resource = S3Resource(
    aws_access_key_id=CONFIG.s3_config.key_id,
    aws_secret_access_key=CONFIG.s3_config.secret,
    endpoint_url=f"https://{CONFIG.s3_config.host}"
)


class OhsomeQualityApiResource(dg.ConfigurableResource):
    api_version: str = "v1-test"

    @property
    def base_url(self) -> str:
        return f"https://api.quality.ohsome.org/{self.api_version}"

    def query(self, indicator, topic, attribute, geojson_geometry, geom_id):
        url = f"{self.base_url}/indicators/{indicator}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        params = {
            "topic": topic,
            "bpolys": geojson_geometry,
        }

        if indicator == "attribute-completeness":
            params["attributes"] = [attribute]

        try:
            resp = r.post(url, json=params, headers=headers, timeout=120)
            resp.raise_for_status()
            row_results = extract_values_from_oqapi_response(resp)
        except r.Timeout:
            row_results = handle_timeout_error(geom_id, indicator, topic)
        except r.ConnectionError:
            row_results = handle_connection_error(geom_id, indicator, topic)
        except r.HTTPError:
            row_results = handle_http_error(geom_id, indicator, resp, topic)

        return row_results


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "ohsome_api": OhsomeQualityApiResource(api_version="v1-test"),
            "s3": s3_resource,
            "duckdb_io_manager": duckdb_io_manager
        }
    )
