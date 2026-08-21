import dagster as dg
import requests as r

from osm_quality_pipeline.defs.constants import DATA_DIR

from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager


from osm_quality_pipeline.defs.utils.utils import handle_http_error, handle_timeout_error, handle_connection_error, extract_values_from_oqapi_response
from osm_quality_pipeline.defs.constants import CONFIG, OHSOME_QUALITY_API_URL, OHSOME_API_KEY

duckdb_io_manager = DuckDBPandasIOManager(
    database=f"{DATA_DIR}/asset_output.duckdb"
)

s3_resource = S3Resource(
    aws_access_key_id=CONFIG.s3_config.key_id,
    aws_secret_access_key=CONFIG.s3_config.secret,
    endpoint_url=f"https://{CONFIG.s3_config.host}"
)


def build_table_name(topic, indicator, attribute):

    topic_name = topic.replace('-', '_')
    indicator_name = indicator.replace('-', '_')
    if attribute:
        attribute_name = attribute.replace('-', '_')
        table_name = f"{topic_name}_{indicator_name}_{attribute_name}"
    else:
        table_name = f"{topic_name}_{indicator_name}"

    return table_name


class CustomDuckDBResource(DuckDBResource):

    def query_asset_results_df(self, partition_key, topic, indicator, attribute):
        table_name = build_table_name(topic, indicator, attribute)
        with self.get_connection() as conn:
            df = conn.execute(
                f"SELECT * FROM public.{table_name} WHERE partition_key = '{partition_key}'"
            ).fetchdf()
            return df

    def check_if_table_exists(self, topic, indicator, attribute):
        table_name = build_table_name(topic, indicator, attribute)
        with self.get_connection() as conn:
            count = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
            ).fetchone()

            print(count)

            if count == 1:
                table_exists = True
            else:
                table_exists = False

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
            "Authorization": OHSOME_API_KEY
        }

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

ohsome_quality_api = OhsomeQualityApiResource()

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "ohsome_api": ohsome_quality_api,
            "s3": s3_resource,
            "duckdb_io_manager": duckdb_io_manager,
            "duckdb": duckdb_resource
        }
    )
