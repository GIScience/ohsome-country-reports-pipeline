import dagster as dg

from osm_quality_pipeline.defs.partitions import dynamic_country_layers_partition


all_topics_job = dg.define_asset_job(
    name="all_topics_job",
    description="Calculate all quality indicators for the topics building-count, roads and land-cover",
    selection='group:"building_count" or group:"roads" or group:"land_cover"',
    partitions_def=dynamic_country_layers_partition
)


building_count_job = dg.define_asset_job(
    name="building_count_job",
    description="Calculate all quality indicators for the topic building-count",
    selection='group:"building_count"',
    partitions_def=dynamic_country_layers_partition
)


roads_job = dg.define_asset_job(
    name="roads_job",
    description="Calculate all quality indicators for the topic roads",
    selection='group:"roads"',
    partitions_def=dynamic_country_layers_partition
)


land_cover_job = dg.define_asset_job(
    name="land_cover_job",
    description="Calculate all quality indicators for the topic land-cover",
    selection='group:"land_cover"',
    partitions_def=dynamic_country_layers_partition
)