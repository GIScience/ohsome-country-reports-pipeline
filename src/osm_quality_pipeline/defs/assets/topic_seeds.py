import dagster as dg

from osm_quality_pipeline.defs.partitions import topics_partitions
from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource


@dg.asset
def topic_partition_seeds(
    context: dg.AssetExecutionContext,
    ohsome_api: OhsomeQualityApiResource,
) -> None:
    topic_keys = ohsome_api.get_topics()
    context.instance.add_dynamic_partitions(
        partitions_def_name=topics_partitions.name,
        partition_keys=topic_keys,
    )
    context.log.info(f"Registered {len(topic_keys)} topic partitions: {topic_keys}")
