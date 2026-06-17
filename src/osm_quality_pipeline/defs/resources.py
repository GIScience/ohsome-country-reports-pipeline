import yaml
from importlib.resources import files
import dagster as dg


class CountryConfig(dg.ConfigurableResource):
    def get_country(self, iso3: str) -> dict:
        data = yaml.safe_load(
            files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
        )
        return data.get(iso3.upper(), {})
