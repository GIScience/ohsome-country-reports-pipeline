import requests
import dagster as dg


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
        resources={"ohsome_api": OhsomeQualityApiResource(api_version="v1-test")}
    )
