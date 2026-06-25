import os
import dagster as dg
import pandas as pd
import geopandas as gpd
from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition, country_partitions

import h3
from shapely.geometry import shape, box


logger = dg.get_dagster_logger()


@dg.asset(
    partitions_def=country_partitions
)
def country_layers(context) -> dg.MaterializeResult[list[str]]:
    country = context.partition_key
    logger.info(country)

    old_partitions = context.instance.get_dynamic_partitions("dynamic_country_layers")
    logger.info(old_partitions)

    # download data from BKG
    if country == "DEU":
        logger.info("download from BKG")

        # TODO: add download function

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|bundesländer",
            f"{country}|gemeinden",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions("dynamic_country_layers", updated_partitions)

    else:
        logger.info("download from geoboundaries")

        # TODO: add download function

        updated_partitions = [
            f"{country}|adm0",
            f"{country}|adm1",
            f"{country}|h3",
        ]
        context.instance.add_dynamic_partitions("dynamic_country_layers", updated_partitions)


    return dg.MaterializeResult(
        value=updated_partitions, metadata={"partitions": updated_partitions}
    )




