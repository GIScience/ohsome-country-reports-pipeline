from dagster import Definitions

from assets.workflow_assets import (
    boundary_asset,
    h3_hexgrid_asset,
    square_grid_asset,
    ohsome_api_requests_asset,
    build_outputs_asset,
    indicators_per_topic_asset,
    upload_s3_asset,
    upload_hdx_asset,
    tag_distribution_asset,
    upload_stats_s3_asset,
    verify_and_delete_asset,
)

from assets.jobs import osm_quality_job, osm_history_job, boundaries_job


defs = Definitions(
    assets=[
        boundary_asset,
        h3_hexgrid_asset,
        square_grid_asset,
        ohsome_api_requests_asset,
        build_outputs_asset,
        indicators_per_topic_asset,
        upload_s3_asset,
        upload_hdx_asset,
        tag_distribution_asset,
        upload_stats_s3_asset,
        verify_and_delete_asset,
    ],
    jobs=[osm_quality_job,
          osm_history_job,
          boundaries_job],
)