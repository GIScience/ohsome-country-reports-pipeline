# [OQAPI HDX PIPELINE](https://giscience.github.io/osm-quality-country-reports/)

## Deployed Page

[OQAPI Country Reports](https://giscience.github.io/osm-quality-country-reports/)

## Overview

This Dagster pipeline computes OSM quality indicators (mapping saturation, currentness, attribute completeness, road comparison) for countries using the [ohsome API](https://docs.ohsome.org/). Results are uploaded to S3 and optionally to HDX.

The pipeline is organized into three jobs:

| Job | Assets | Purpose |
|-----|--------|---------|
| `boundaries_job` | `boundary_asset` → `h3_hexgrid_asset` / `square_grid_asset` | Fetch country boundaries and build analysis grids |
| `osm_quality_job` | `ohsome_api_requests_asset` → `build_outputs_asset` → `upload_s3_asset` | Compute quality indicators per theme and upload to S3 |
| `osm_history_job` | `tag_distribution_asset` → `upload_stats_s3_asset` | Compute OSM history tag distributions and upload stats |
| `publish_job` | `upload_hdx_asset` → `verify_and_delete_asset` | Upload outputs to HDX and clean up local files |

## Setup

### 1. Python environment

Requires Python 3.12.10.

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration files

All configs live in `configs/`. Required files:

- **`countries.yaml`** — Lists every country (ISO3 code) with region and slug. Add or remove countries here to control which are processed.
- **`assets_config.yaml`** — Pipeline settings: boundary API source (`github` or `geoboundaries`), grid type (`h3` or `square`), admin levels, and `required_topics` (which topics to run).
- **`theme_config.yaml`** — Theme definitions: ohsome query keys/values, measure types, OSM history base URLs, and theme expansion mappings (e.g. `school` → `school_isced` + `school_operator`).
- **`matrix.yaml`** — Maps each topic to its indicators (e.g. `mapping-saturation`, `currentness`) and attributes. Edit this to change which indicators are computed per topic.

### 3. S3 config

Required for S3 uploads. Create `configs/s3_config.yaml`:

```yaml
s3_asset:
    endpoint: warm.storage.heigit.org
    bucket: heigit-hdx-public
    access_key: <ACCESS_KEY>
    secret_key: <SECRET_KEY>
    dest_prefix: oqapi_hdx
    secure: true
```

### 4. HDX config

Required for HDX uploads. Create `configs/hdx_config.yaml`:

```yaml
hdx:
  site: "prod"
  api_key: <YOUR API KEY>
  owner_org: <YOUR OWNER_ORG>
  data_update_frequency: "Every six months"
  maintainer: <MAINTAINER NAME>
  maintainer_email: <MAINTAINER E-MAIL>
  private: true
  url: "https://data.humdata.org/"
  tags:
    - indicators
    - openstreetmap
```

## Run the pipeline

### Start the Dagster UI

```sh
export DAGSTER_HOME="$PWD/.dagster"
dagster dev -w workspace.yml -p 4444
```

Open http://localhost:4444 in your browser.

### Materialize assets

1. In the UI, go to the **Assets** page.
2. To run everything in order, click **Materialize all**.
3. To run a specific job, go to **Launchpad** → select a job → **Launch Run**.
4. To run individual assets, select them and click **Materialize selected**.

### Run via CLI

```sh
dagster job materialize -f repository.py -j osm_quality_job
dagster job materialize -f repository.py -j osm_history_job
dagster job materialize -f repository.py -j boundaries_job
dagster job materialize -f repository.py -j publish_job
```
