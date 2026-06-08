from dagster import define_asset_job, AssetSelection

# Job that materializes the osm quality
osm_quality_job = define_asset_job(
    name="osm_quality_job",
    selection=AssetSelection.assets(
        "ohsome_api_requests_asset",
        "build_outputs_asset",
        "upload_s3_asset",
    ),
)

# Job that materializes the osm history
osm_history_job = define_asset_job(
    name="osm_history_job",
    selection=AssetSelection.assets(
        "tag_distribution_asset",
        "upload_stats_s3_asset",
    ),
)

# Job that materializes the boundaries
boundaries_job = define_asset_job(
    name="boundaries_job",
    selection=AssetSelection.assets(
        "boundary_asset",
        "h3_hexgrid_asset",
        "square_grid_asset",
    ),
)

# Job that publishes assets to HDX and cleans up
publish_job = define_asset_job(
    name="publish_job",
    selection=AssetSelection.assets(
        "upload_hdx_asset",
        "verify_and_delete_asset",
    ),
)