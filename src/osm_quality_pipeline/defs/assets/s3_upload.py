import dagster as dg
from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.constants import CONFIG

from osm_quality_pipeline.defs.partitions import (
    dynamic_country_layers_partition,
    get_country_layer_from_partitionkey,
)


@dg.asset(
    partitions_def=dynamic_country_layers_partition,
    name="upload_to_s3",
    group_name="s3_upload",
    deps=["indicator_results_csv", "indicator_results_gpkg"]
)

def upload_to_s3(context: dg.AssetExecutionContext, s3: S3Resource) -> None:
    s3_client = s3.get_client()

    country_layer = get_country_layer_from_partitionkey(context.partition_key)
    country_code = country_layer.country
    layer = country_layer.layer

    gpkg_file = f"{country_code}_{layer}_indicator_results.gpkg"
    gpkg_path = f"data/{country_code}/Outputs/{country_code}_{layer}_indicator_results.gpkg"

    bucket_name = CONFIG.s3_config.bucket

    s3_client.upload_file(
        Filename=gpkg_path,
        Bucket=bucket_name,
        Key=f"oqapi_hdx/downloads/{country_code}/{gpkg_file}"
    )
