import dagster as dg
import pytest

from osm_quality_pipeline.definitions import defs as lazy_defs
from osm_quality_pipeline.defs.sensors import initial_state, load_sweep_config, plan_tick


SUCCESS = dg.DagsterRunStatus.SUCCESS
FAILURE = dg.DagsterRunStatus.FAILURE

CONFIG = {
    "overwrite": False,
    "countries": [{"iso": "KEN", "h3": True}, {"iso": "UGA", "h3": False}],
}
defs = lazy_defs()

LAYERS = {
    "KEN": ["KEN|adm0", "KEN|adm1", "KEN|h3"],
    "UGA": ["UGA|adm0", "UGA|adm1"],
}


def layers_for(iso, h3):
    return LAYERS.get(iso, [])


def tick(state, status, config=CONFIG):
    return plan_tick(state, config, status, layers_for)


def test_full_sweep_order():
    state, request, _ = tick(initial_state(), None)
    launched = [(request.job_name, request.partition_key)]
    while request is not None:
        state, request, _ = tick(state, SUCCESS)
        if request:
            launched.append((request.job_name, request.partition_key))

    assert launched == [
        ("country_preparation_job", "KEN"),
        ("full_workflow_job", "KEN|adm0"),
        ("full_workflow_job", "KEN|adm1"),
        ("full_workflow_job", "KEN|h3"),
        ("country_preparation_job", "UGA"),
        ("full_workflow_job", "UGA|adm0"),
        ("full_workflow_job", "UGA|adm1"),
    ]
    assert state["skipped"] == []


def test_prep_run_config_passes_h3_flag():
    state = initial_state()
    state["country_idx"] = 1
    _, request, _ = tick(state, None)
    assert request.run_config["ops"]["country_layers"]["config"]["create_h3"] is False
    assert request.run_config["ops"]["country_boundaries_pmtiles"]["config"]["create_h3"] is False


def test_failed_run_retried_once_then_skipped():
    state, first, _ = tick(initial_state(), None)
    state, _ = tick(state, SUCCESS)[:2]  # prep done, KEN|adm0 launched

    state, retry, messages = tick(state, FAILURE)
    assert retry.partition_key == "KEN|adm0"
    assert "retrying" in messages[0]

    state, after_skip, messages = tick(state, FAILURE)
    assert after_skip.partition_key == "KEN|adm1"
    assert state["skipped"] == ["KEN|adm0"]


def test_failed_prep_skips_whole_country():
    state, _, _ = tick(initial_state(), None)
    state, retry, _ = tick(state, FAILURE)
    assert retry.partition_key == "KEN"

    state, request, _ = tick(state, FAILURE)
    assert (request.job_name, request.partition_key) == ("country_preparation_job", "UGA")
    assert state["skipped"] == ["KEN (preparation)"]


def test_missing_run_relaunches_same_step():
    state, first, _ = tick(initial_state(), None)
    state, again, _ = tick(state, None)
    assert again.partition_key == first.partition_key
    assert state["attempt"] == 0


def test_overwrite_only_on_first_attempt():
    config = {**CONFIG, "overwrite": True}
    state, _, _ = tick(initial_state(), None, config)
    state, request, _ = tick(state, SUCCESS, config)
    assert request.run_config["resources"]["duckdb"]["config"]["overwrite"] is True

    state, retry, _ = tick(state, FAILURE, config)
    assert retry.run_config["resources"]["duckdb"]["config"]["overwrite"] is False


def test_done_returns_no_request():
    state = initial_state()
    state["country_idx"] = 2
    _, request, _ = tick(state, None)
    assert request is None


def test_country_without_layers_is_skipped():
    config = {"overwrite": False, "countries": [{"iso": "SSD", "h3": True}, {"iso": "UGA", "h3": False}]}
    state, _, _ = tick(initial_state(), None, config)
    state, request, messages = tick(state, SUCCESS, config)
    assert request.partition_key == "UGA"
    assert "no layer partitions" in messages[0]


def test_sweep_config_countries_known():
    config = load_sweep_config()
    assert len(config["countries"]) == 28


@pytest.mark.parametrize("stage", ["prep", "full"])
def test_run_configs_valid_for_jobs(stage):
    state = initial_state()
    state, request, _ = tick(state, None)
    if stage == "full":
        state, request, _ = tick(state, SUCCESS)

    job = defs.resolve_job_def(request.job_name)
    dg.validate_run_config(job, request.run_config)


def test_sensor_registered():
    assert defs.get_sensor_def("country_sweep_sensor")


def test_sensor_tick_on_instance():
    import json
    from osm_quality_pipeline.defs.sensors import country_sweep_sensor

    with dg.instance_for_test() as instance:
        context = dg.build_sensor_context(instance=instance)
        result = country_sweep_sensor(context)
        request = result.run_requests[0]
        assert (request.job_name, request.partition_key) == ("country_preparation_job", "SSD")
        assert json.loads(result.cursor)["step_id"] == request.tags["country_sweep/step_id"]
