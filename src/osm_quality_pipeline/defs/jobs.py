import dagster as dg

from osm_quality_pipeline.defs.partitions import country_partitions, dynamic_country_layers_partition


country_preparation = dg.define_asset_job(
    name="country_preparation_job",
    description="Downloads country boundaries and builds the boundaries pmtiles.",
    selection='key:"country_layers" or key:"country_boundaries_pmtiles"',
    partitions_def=country_partitions
)


full_workflow = dg.define_asset_job(
    name="full_workflow_job",
    description="Calculate all quality indicators and upload result files to S3 bucket.",
    selection='group:outputs or group:"building_count" or group:"roads" or group:"land_cover" or group:"schools" or group:"hospitals" or group:"buildings"',
    partitions_def=dynamic_country_layers_partition
)

