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
        files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
    )
    countries = [c["iso"] for c in config["countries"]]
    return countries


def get_h3_config(iso):
    config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
    )
    h3_by_iso = {c["iso"]: c["h3"] for c in config["countries"]}
    return h3_by_iso[iso]


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


def get_country_layers_partitions():
    partitions = []
    for iso in get_iso_codes():
        if iso == "DEU":
            partitions.append("DEU|vg2500_sta")
            partitions.append("DEU|vg2500_lan")
            partitions.append("DEU|vg1000_krs")
        else:
            partitions.append(f"{iso}|adm0")
            partitions.append(f"{iso}|adm1")

        if get_h3_config(iso):
            partitions.append(f"{iso}|h3")

    return partitions

# partition for country preparation job
country_partitions = dg.StaticPartitionsDefinition(partition_keys=get_iso_codes())

country_layers_partition = dg.StaticPartitionsDefinition(partition_keys=get_country_layers_partitions())