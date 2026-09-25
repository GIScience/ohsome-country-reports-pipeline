from importlib.resources import files

import yaml
import json

import dagster as dg

from osm_quality_pipeline.defs.jobs import full_workflow
from osm_quality_pipeline.defs.resources import duckdb_resource
from osm_quality_pipeline.defs.partitions import country_layers_partition


IN_PROGRESS_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]

FAILED_STATUSES = {
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
}


def next_state(state, countries, all_partitions):
    if state["idx"] < len(countries):
        idx = state["idx"] + 1
        state = {
            "idx": idx,
            "iso": countries[idx]["iso"],
            "retries": 0,
            "finished": False
        }
        prefix = state["iso"]
        partitions = [p for p in all_partitions if p.startswith(prefix)]
        state["partitions"] = partitions
    else:
        state["finished"] = True

    return state


@dg.sensor(
    jobs=[full_workflow],
    minimum_interval_seconds=180,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Runs the full_workflow_job for every layer of each country."
)
def country_sensor(context: dg.SensorEvaluationContext):
    context.log.info("Starting country sensor")



    ################################################################
    # load general config
    ################################################################
    country_config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("countries.yaml").read_text()
    )
    context.log.info(f"Loaded country config: {country_config}")
    overwrite = country_config.get("overwrite", False)
    countries = country_config["countries"]


    all_partitions = country_layers_partition.get_partition_keys()

    ################################################################
    # load state
    ################################################################
    try:
        state = json.loads(context.cursor)
        context.log.info(f"Resuming from cursor: {state}")
    except:
        state = {
            "idx": 0,
            "iso": countries[0]["iso"],
            "retries": 0,
            "finished": False
        }
        prefix = state["iso"]
        partitions = [p for p in all_partitions if p.startswith(prefix)]
        state["partitions"] = partitions
        context.log.info(f"initialized state: {state}")

        return dg.SensorResult(
            cursor=json.dumps(state)
        )

    ################################################################
    # check if all runs finished for country
    ################################################################

    if state["finished"]:
        # we are done!
        return dg.SkipReason("We are done with all countries.")

    statuses = [check_run_status(context, p) for p in state["partitions"]]

    if any(s in IN_PROGRESS_STATUSES for s in statuses):
        return dg.SkipReason("Some partitions are still running")

    if all(s == dg.DagsterRunStatus.SUCCESS for s in statuses):
        context.log.info(f"old state: {state}")
        state = next_state(state, countries, all_partitions)
        context.log.info(f"new state: {state}")

        if state["finished"]:
            # we are done!
            # let's upload stuff to HDX now.
            return dg.SkipReason("We are done with all countries.")

    if any(s in FAILED_STATUSES for s in statuses):
        context.log.info("Some partitions failed")
        failed_partitions = [p for p, s in zip(state["partitions"], statuses) if s in FAILED_STATUSES]

        if state["retries"] == 0:
            context.log.info(f"We will retry once now for {failed_partitions}.")
            state["partitions"] = failed_partitions
            state["retries"] = 1
        else:
            context.log.info(f"Retried but still failed partitions: {failed_partitions}.")
            context.log.info(f"Continue with next country.")


    ################################################################
    # request runs for country workflow
    ################################################################

    run_requests = []
    for partition_key in state["partitions"]:
        run_request = create_run_request(state["iso"], overwrite, partition_key)
        run_requests.append(run_request)

    return dg.SensorResult(
        run_requests=run_requests,
        cursor=json.dumps(state)
    )


def check_run_status(context, partition):
    last_runs = context.instance.get_runs(
        filters=dg.RunsFilter(
            tags={
                "dagster/partition": partition
            },
        ),
        limit=1
    )

    if len(last_runs) > 0:
        context.log.info([partition, last_runs[0].status])
        return last_runs[0].status
    else:
        return None

def create_run_request(country, overwrite, partition_key):
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
        "iso": country,
    }
    country_workflow_request = dg.RunRequest(
        job_name=full_workflow.name,
        partition_key=partition_key,
        run_config=run_config,
        tags=tags
    )
    return country_workflow_request


