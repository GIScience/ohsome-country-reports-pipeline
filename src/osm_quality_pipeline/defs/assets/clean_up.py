import dagster as dg

from osm_quality_pipeline.defs.partitions import country_partitions


logger = dg.get_dagster_logger()


@dg.asset(partitions_def=country_partitions, group_name="preparation")
def clean_up_partitions(context) -> dg.MaterializeResult[list[str]]:
    country = context.partition_key

    old_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(f"existing partitions: {old_partitions}")

    for partition in old_partitions:
        if country in partition:
            context.instance.delete_dynamic_partition("dynamic_country_layers", partition)
            logger.info(f"deleted partition: {partition}")

    updated_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(f"remaining partitions: {updated_partitions}")

    return dg.MaterializeResult(
        value=updated_partitions, metadata={"partitions": updated_partitions}
    )