from importlib.resources import files

import yaml
import json

import dagster as dg

from osm_quality_pipeline.defs.jobs import country_preparation, full_workflow
from osm_quality_pipeline.defs.partitions import ALL_COUNTRIES
from osm_quality_pipeline.defs.resources import duckdb_resource


IN_PROGRESS_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


@dg.sensor(
    jobs=[country_preparation, full_workflow],
    minimum_interval_seconds=15,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Runs country_preparation_job and then full_workflow_job for every layer of each country "
    "in configs/sensor_countries.yaml, one run at a time. Failed runs are retried once, then skipped. "
    "Clear the cursor to start a new round.",
)
def country_sensor(context: dg.SensorEvaluationContext):
    context.log.info("Starting country sensor")
    country_config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("sensor_countries.yaml").read_text()
    )
    context.log.info(f"Loaded country config: {country_config}")

    overwrite = country_config.get("overwrite", False)

    try:
        state = json.loads(context.cursor)
        context.log.info(f"Resuming from cursor: {state}")
    except json.JSONDecodeError:
        state = {"country_idx": 0}

    runs = context.instance.get_runs(filters=dg.RunsFilter(tags={"country_idx": str(state["country_idx"])}))
    context.log.info(runs)
    run_status = runs[0].status if runs else None

    if run_status == dg.DagsterRunStatus.SUCCESS:
        state["country_idx"] += 1
    elif run_status in IN_PROGRESS_STATUSES:
        context.log.info(f"Run for country index {state['country_idx']} is still in progress, skipping")
        return dg.SkipReason(f"Run for country index {state['country_idx']} is still in progress")


                            
    country = country_config["countries"][state["country_idx"]]

    context.log.info(f"Processing country: {country}")

    run_config = {
        "ops": {
            "country_layers": {
                "config": {
                    "create_h3": country["h3"]
                }
            },
            "country_boundaries_pmtiles": {
                "config": {
                    "create_h3": country["h3"]
                }
            }
        }
    }

    tags = {"country_idx": state["country_idx"]}

    request = dg.RunRequest(
        job_name=country_preparation.name,
        partition_key=country["iso"],
        run_config=run_config,
        tags=tags
    )

    context.log.info(f"state: {state}")
    return dg.SensorResult(run_requests=[request], cursor=json.dumps(state))


