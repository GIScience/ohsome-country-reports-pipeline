import dagster as dg
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.constants import S3Config
from osm_quality_pipeline.defs.resources import S3Resource
from osm_quality_pipeline.defs.utils.hdx import get_s3_links
from osm_quality_pipeline.defs.utils.hdx import upload_to_hdx
logger = dg.get_dagster_logger()

@dg.asset(partitions_def=country_partitions, group_name="uploads")
def hdx_upload(context, config: S3Config, s3: S3Resource):
    country = context.partition_key
    links_list = get_s3_links(config, country, s3)

    hdx_link = upload_to_hdx(country, links_list, context)
    return hdx_link


