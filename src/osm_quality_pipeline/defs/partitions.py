import yaml
from importlib.resources import files
import dagster as dg

_countries = yaml.safe_load(
    files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
)
ALL_COUNTRIES = list(_countries.keys())
country_partitions = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)
