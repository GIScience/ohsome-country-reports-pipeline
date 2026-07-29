import dagster as dg
from osm_quality_pipeline.defs.partitions import country_partitions
from osm_quality_pipeline.defs.constants import S3Config
from osm_quality_pipeline.defs.resources import S3Resource
import os
import boto3
from dotenv import load_dotenv
logger = dg.get_dagster_logger()

@dg.asset(partitions_def=country_partitions, group_name="uploads")
def HDX_upload(context, config: S3Config, s3: S3Resource):
    logger.info(config.bucket)
    country = context.partition_key
    links_list = get_s3_links(config, country, s3)

    return links_list


def get_s3_links(config, country, s3):
    response = s3.get_client().list_objects_v2(
        Bucket=config.bucket,
        Prefix=f"oqapi_hdx/downloads/{country}/",
    )
    # links_list = get_s3_links(country)
    links_list = [
        f"https://{config.host}/{config.bucket}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].lower().endswith(".zip")
    ]
    logger.info(links_list)
    return links_list


def get_s3_links(country_code: str) -> list[str]:
    load_dotenv()  # can i take these things from the resources file somehow?
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("S3_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET"),
        endpoint_url=f"https://{os.getenv('S3_HOST')}",
    )

    bucket = os.getenv("S3_BUCKET")
    host = os.getenv("S3_HOST")
    prefix = f"oqapi_hdx/downloads/{country_code}/"
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
    )

    return [
        f"https://{host}/{bucket}/{obj['Key']}"
        for obj in response.get("Contents", [])
        if obj["Key"].lower().endswith(".zip")
    ]


""" for testing:
if __name__ == "__main__":
  tza_links = get_country_file_links("TZA")
  for link in tza_links:
    print(link)
"""