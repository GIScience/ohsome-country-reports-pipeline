from pathlib import Path

import dagster as dg
from dagster import definitions, load_from_defs_folder

from osm_quality_pipeline.defs.resources import OhsomeQualityApiResource


@definitions
def defs():
    return dg.Definitions.merge(
        load_from_defs_folder(project_root=Path(__file__).parent.parent.parent),
        dg.Definitions(resources={"ohsome_api": OhsomeQualityApiResource()}),
    )
