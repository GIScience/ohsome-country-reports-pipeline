import dagster as dg
from osm_quality_pipeline.defs.constants import CONFIG
from osm_quality_pipeline.defs.partitions import (
    country_layers_partition,
    country_partitions,
    get_country_layer_from_partitionkey,
    layers_to_country_mapping,
)
from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.utils.hdx import upload_to_hdx
logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_partitions,
    group_name="HDX_upload",
    # deps only (no data loaded): the files are taken from S3, not from local storage
    deps=[
        dg.AssetDep("indicator_results_gpkg_s3", partition_mapping=layers_to_country_mapping),
        dg.AssetDep("indicator_results_csv_s3", partition_mapping=layers_to_country_mapping),
    ],
)
def hdx_country_dataset(context, s3: S3Resource):
    country = context.partition_key
    layers = [
        get_country_layer_from_partitionkey(p).layer
        for p in country_layers_partition.get_partition_keys()
        if get_country_layer_from_partitionkey(p).country == country
    ]
    expected = [
        f"{country}_{layer}_indicator_results.{ext}"
        for ext in ["gpkg", "csv"]
        for layer in layers
    ]

    prefix = f"{CONFIG.s3_config.prefix}/{country}/"
    response = s3.get_client().list_objects_v2(Bucket=CONFIG.s3_config.bucket, Prefix=prefix)
    on_s3 = {obj["Key"].split("/")[-1] for obj in response.get("Contents", [])}

    missing = [f for f in expected if f not in on_s3]
    if missing:
        raise dg.Failure(description=f"Files missing on S3 for {country}: {missing}")

    links = [
        (f, f"https://{CONFIG.s3_config.host}/{CONFIG.s3_config.bucket}/{prefix}{f}")
        for f in expected
    ]
    logger.info(links)

    return upload_to_hdx(country, links, context)
