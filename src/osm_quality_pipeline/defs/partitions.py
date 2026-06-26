import yaml
from importlib.resources import files
import dagster as dg

from attr import dataclass

_countries = yaml.safe_load(
    files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
)
ALL_COUNTRIES = list(_countries.keys())
country_partitions = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)


dynamic_country_layers_partition = dg.DynamicPartitionsDefinition(name="dynamic_country_layers")


@dataclass
class CountryLayer:
    country: str
    layer: str


def get_country_layer_from_partitionkey(country_layer_partitionkey: str) -> CountryLayer:
    "extracts country and layer from <country>|<layer"
    country: str = ""
    layer: str = ""
    partition: str | None = country_layer_partitionkey
    if partition:
        parts = partition.split("|")
        if len(parts) == 2:
            country, layer = parts
        else:
            raise ValueError("Invalid partition format. Should be <country>|<layer>")

    return CountryLayer(country=country, layer=layer)
