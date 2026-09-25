import yaml
from importlib.resources import files
import dagster as dg

from attr import dataclass


@dataclass
class CountryLayer:
    country: str
    layer: str


def get_iso_codes():
    config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("sensor_countries.yaml").read_text()
    )
    countries = [c["iso"] for c in config["countries"]]
    return countries


def get_country_layer_from_partitionkey(country_layer_partitionkey: str) -> CountryLayer:
    """Extract country and layer from <country>|<layer. """
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

# partition for country preparation job
country_partitions = dg.StaticPartitionsDefinition(partition_keys=get_iso_codes())

# partition for indicator calculation
dynamic_country_layers_partition = dg.DynamicPartitionsDefinition(name="dynamic_country_layers")