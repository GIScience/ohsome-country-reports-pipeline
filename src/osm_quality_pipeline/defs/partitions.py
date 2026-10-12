import yaml
from importlib.resources import files
import dagster as dg

from attr import dataclass


@dataclass
class CountryLayer:
    country: str
    layer: str


def _load_countries():
    config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
    )
    return config["countries"]


def get_country_key(entry):
    """Report key of a countries.yaml entry, defaults to the iso code."""
    return entry.get("key", entry["iso"])


def get_country_keys():
    return [get_country_key(c) for c in _load_countries()]


def _get_country_entry(key):
    entries = {get_country_key(c): c for c in _load_countries()}
    return entries[key]


def get_iso(key):
    return _get_country_entry(key)["iso"]


def get_locale(key):
    """Locale sent to the ohsome quality API (Accept-Language), None for the default (English)."""
    return _get_country_entry(key).get("locale")


def get_h3_config(key):
    return _get_country_entry(key)["h3"]


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
    for key in get_country_keys():
        if get_iso(key) == "DEU":
            partitions.append(f"{key}|vg2500_sta")
            partitions.append(f"{key}|vg2500_lan")
            partitions.append(f"{key}|vg1000_krs")
        else:
            partitions.append(f"{key}|adm0")
            partitions.append(f"{key}|adm1")

        if get_h3_config(key):
            partitions.append(f"{key}|h3")

    return partitions

# partition for country preparation job
country_partitions = dg.StaticPartitionsDefinition(partition_keys=get_country_keys())

country_layers_partition = dg.StaticPartitionsDefinition(partition_keys=get_country_layers_partitions())

# maps every <country>|<layer> partition to its <country> partition, so a per-country
# asset can depend on all layers of that country
layers_to_country_mapping = dg.StaticPartitionMapping(
    {p: get_country_layer_from_partitionkey(p).country for p in get_country_layers_partitions()}
)