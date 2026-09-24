import json
import uuid
from importlib.resources import files

import dagster as dg
import yaml

from osm_quality_pipeline.defs.jobs import country_preparation, full_workflow
from osm_quality_pipeline.defs.partitions import ALL_COUNTRIES
from osm_quality_pipeline.defs.resources import duckdb_resource


STEP_TAG = "country_sweep/step_id"
COUNTRY_TAG = "country_sweep/country"

IN_PROGRESS_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]
FAILED_STATUSES = [dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED]


def load_sweep_config():
    config = yaml.safe_load(
        files("osm_quality_pipeline.configs").joinpath("sensor_countries.yaml").read_text()
    )
    unknown = [c["iso"] for c in config["countries"] if c["iso"] not in ALL_COUNTRIES]
    if unknown:
        raise ValueError(f"sensor_countries.yaml: ISO codes not in countries.yaml: {unknown}")
    return config


def initial_state():
    return {
        "country_idx": 0,
        "stage": "prep",  # "prep" = country_preparation_job, "full" = full_workflow_job per layer
        "layer_idx": 0,
        "attempt": 0,  # 0 = first try, 1 = retry
        "step_id": None,  # tag of the last launched run
        "skipped": [],
    }


def next_country(state):
    state.update(country_idx=state["country_idx"] + 1, stage="prep", layer_idx=0, attempt=0)


def advance(state):
    if state["stage"] == "prep":
        state.update(stage="full", layer_idx=0, attempt=0)
    else:
        state.update(layer_idx=state["layer_idx"] + 1, attempt=0)


def step_label(state, countries, layers_for):
    country = countries[state["country_idx"]]
    if state["stage"] == "prep":
        return f"{country['iso']} (preparation)"
    return layers_for(country["iso"], country["h3"])[state["layer_idx"]]


def plan_tick(state, config, last_run_status, layers_for):
    """Update the state based on the last run and return (state, run_request, messages).

    last_run_status: status of the last launched run, None if nothing was launched yet
    or the run can't be found (the same step is then launched again).
    layers_for(iso, h3): sorted dynamic partitions of a country.
    """
    countries = config["countries"]
    messages = []

    if state["step_id"] is not None and last_run_status is not None:
        if last_run_status == dg.DagsterRunStatus.SUCCESS:
            advance(state)
        elif last_run_status in FAILED_STATUSES:
            label = step_label(state, countries, layers_for)
            if state["attempt"] == 0:
                messages.append(f"{label} failed, retrying once")
                state["attempt"] = 1
            else:
                messages.append(f"{label} failed twice, skipping")
                state["skipped"].append(label)
                if state["stage"] == "prep":
                    # without fresh boundaries the full workflow makes no sense
                    next_country(state)
                else:
                    advance(state)
    state["step_id"] = None

    while state["country_idx"] < len(countries):
        country = countries[state["country_idx"]]
        iso, h3 = country["iso"], country["h3"]
        step_id = uuid.uuid4().hex
        tags = {STEP_TAG: step_id, COUNTRY_TAG: iso}

        if state["stage"] == "prep":
            op_config = {"config": {"create_h3": h3}}
            request = dg.RunRequest(
                job_name=country_preparation.name,
                partition_key=iso,
                run_config={"ops": {"country_layers": op_config, "country_boundaries_pmtiles": op_config}},
                tags=tags,
            )
        else:
            layers = layers_for(iso, h3)
            if state["layer_idx"] >= len(layers):
                if not layers:
                    messages.append(f"{iso}: no layer partitions found, skipping")
                next_country(state)
                continue
            # the retry only re-queries failed rows, even when overwrite is set
            overwrite = bool(config.get("overwrite")) and state["attempt"] == 0
            request = dg.RunRequest(
                job_name=full_workflow.name,
                partition_key=layers[state["layer_idx"]],
                # run config replaces the whole resource config, so database is required too
                run_config={
                    "resources": {"duckdb": {"config": {"database": duckdb_resource.database, "overwrite": overwrite}}}
                },
                tags=tags,
            )

        state["step_id"] = step_id
        return state, request, messages

    return state, None, messages


@dg.sensor(
    jobs=[country_preparation, full_workflow],
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description="Runs country_preparation_job and then full_workflow_job for every layer of each country "
    "in configs/sensor_countries.yaml, one run at a time. Failed runs are retried once, then skipped. "
    "Clear the cursor to start a new round.",
)
def country_sweep_sensor(context: dg.SensorEvaluationContext):
    instance = context.instance

    for job in (country_preparation, full_workflow):
        if instance.get_runs(filters=dg.RunsFilter(job_name=job.name, statuses=IN_PROGRESS_STATUSES), limit=1):
            return dg.SkipReason(f"a {job.name} run is in progress")

    config = load_sweep_config()
    state = json.loads(context.cursor) if context.cursor else initial_state()

    last_run_status = None
    if state["step_id"]:
        runs = instance.get_runs(filters=dg.RunsFilter(tags={STEP_TAG: state["step_id"]}), limit=1)
        last_run_status = runs[0].status if runs else None

    def layers_for(iso, h3):
        partitions = instance.get_dynamic_partitions("dynamic_country_layers")
        return sorted(p for p in partitions if p.split("|")[0] == iso and (h3 or p.split("|")[1] != "h3"))

    state, request, messages = plan_tick(state, config, last_run_status, layers_for)
    for message in messages:
        context.log.warning(message)

    if request is None:
        return dg.SensorResult(
            cursor=json.dumps(state),
            skip_reason=f"all {len(config['countries'])} countries done, skipped: {state['skipped'] or 'none'}",
        )

    context.log.info(f"launching {request.job_name} for {request.partition_key}")
    return dg.SensorResult(run_requests=[request], cursor=json.dumps(state))
