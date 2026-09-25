from importlib.resources import files

import yaml
import json

import dagster as dg

from osm_quality_pipeline.defs.jobs import country_preparation, full_workflow
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



    ################################################################
    # load general config
    ################################################################
    country_config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("sensor_countries.yaml").read_text()
    )
    context.log.info(f"Loaded country config: {country_config}")
    overwrite = country_config.get("overwrite", False)


    ################################################################
    # load state
    ################################################################
    try:
        state = json.loads(context.cursor)
        context.log.info(f"Resuming from cursor: {state}")
    except json.JSONDecodeError:
        state = {"country_idx": 0}


    ################################################################
    # check if past runs finished
    ################################################################
    runs = context.instance.get_runs(filters=dg.RunsFilter(tags={"country_idx": str(state["country_idx"])}))
    context.log.info(runs)
    run_status = runs[0].status if runs else None

    if run_status == dg.DagsterRunStatus.SUCCESS:
        state["country_idx"] += 1
    elif run_status in IN_PROGRESS_STATUSES:
        context.log.info(f"Run for country index {state['country_idx']} is still in progress, skipping")
        return dg.SkipReason(f"Run for country index {state['country_idx']} is still in progress")


    ################################################################
    # request run for country preparation
    ################################################################
    country = country_config["countries"][state["country_idx"]]
    context.log.info(f"Preparation for country: {country}")
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
    tags = {
        "country_idx": state["country_idx"],
        "stage": "preparation"
    }
    country_preparation_request = dg.RunRequest(
        job_name=country_preparation.name,
        partition_key=country["iso"],
        run_config=run_config,
        tags=tags
    )


    ################################################################
    # request run for country workflow admin 0
    ################################################################
    country = country_config["countries"][state["country_idx"]]
    partition_key = f"{country["iso"]}|adm0"
    context.log.info(f"Full workflow for country: {country}")
    run_config = {
        "resources": {
            "duckdb": {
                "config": {
                    "database": duckdb_resource.database,
                    "overwrite": overwrite
                }
            }
        }
    }
    tags = {
        "country_idx": state["country_idx"],
        "stage": "adm0"
    }
    country_workflow_request = dg.RunRequest(
        job_name=full_workflow.name,
        partition_key=partition_key,
        run_config=run_config,
        tags=tags
    )


    context.log.info(f"state: {state}")
    return dg.SensorResult(
        run_requests=[country_preparation_request, country_workflow_request],
        cursor=json.dumps(state)
    )


