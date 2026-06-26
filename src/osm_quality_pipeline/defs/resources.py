import requests
import dagster as dg

from dagster_aws.s3 import S3Resource


s3_resource = S3Resource(
    aws_access_key_id=dg.EnvVar("S3_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("S3_SECRET"),
    endpoint_url="https://" + dg.EnvVar("S3_HOST"),
)


class OhsomeQualityApiResource(dg.ConfigurableResource):
    api_version: str = "v1-test"

    @property
    def base_url(self) -> str:
        return f"https://api.quality.ohsome.org/{self.api_version}"

    def get_topics(self) -> list[str]:
        resp = requests.get(f"{self.base_url}/metadata/topics")
        resp.raise_for_status()
        return list(resp.json()["result"].keys())


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "ohsome_api": OhsomeQualityApiResource(api_version="v1-test"),
            "s3": s3_resource
        }
    )
