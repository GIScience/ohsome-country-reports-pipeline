import requests
import dagster as dg


class OhsomeQualityApiResource(dg.ConfigurableResource):
    base_url: str = "https://api.quality.ohsome.org/v1"

    def get_topics(self) -> list[str]:
        resp = requests.get(f"{self.base_url}/metadata/topics")
        resp.raise_for_status()
        return list(resp.json()["result"].keys())


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"ohsome_api": OhsomeQualityApiResource()})
