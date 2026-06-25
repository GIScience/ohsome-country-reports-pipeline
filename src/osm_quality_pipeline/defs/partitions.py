import yaml
from importlib.resources import files
import dagster as dg

from osm_quality_pipeline.defs.constants import REQUIRED_TOPICS

_countries = yaml.safe_load(
    files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
)
ALL_COUNTRIES = list(_countries.keys())
country_partitions = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)

topics_partitions = dg.StaticPartitionsDefinition(partition_keys=REQUIRED_TOPICS)

multi_partitions_oqapi_request = dg.MultiPartitionsDefinition({
    "country": country_partitions,
    "topic": topics_partitions,
})


ALL_COUNTRIES_LAYERS = []
for country in ALL_COUNTRIES:
    if country == "DEU":
        ALL_COUNTRIES_LAYERS.append(f"{country}|adm0")
        ALL_COUNTRIES_LAYERS.append(f"{country}|bundesländer")
        ALL_COUNTRIES_LAYERS.append(f"{country}|gemeinden")
        ALL_COUNTRIES_LAYERS.append(f"{country}|h3")
    else:
        ALL_COUNTRIES_LAYERS.append(f"{country}|adm0")
        ALL_COUNTRIES_LAYERS.append(f"{country}|adm1")
        ALL_COUNTRIES_LAYERS.append(f"{country}|h3")

country_layers_partition = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES_LAYERS)



dynamic_country_layers_partition = dg.DynamicPartitionsDefinition(name="dynamic_country_layers")
