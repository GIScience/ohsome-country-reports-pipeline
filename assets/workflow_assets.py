import plotly.io as pio
import plotly.graph_objects as go
import logging
import os
import glob
import tempfile
import yaml
import asyncio
import aiohttp
import numpy as np
import pandas as pd
import geopandas as gpd
import geojson
import requests
import json
import h3
import time
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import shutil
import gzip
import s3fs
import re
from pathlib import Path
from statistics import median
from shapely.geometry import mapping, shape, box, Polygon, MultiPolygon
from shapely.validation import make_valid
from shapely.ops import unary_union
from filelock import FileLock
import folium
from folium.features import GeoJson
from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition, asset, Output, AssetIn, Config
from scripts.helper_functions import download_from_geoboundaries, download_from_github, upload_to_s3, upload_to_hdx, simplify_geometries, count_vertices, geojson_to_multilayer_pmtiles, upload_stats_to_s3, get_dynamic_resolutions 
from scripts.smart_request_functions import handle_request_error, build_api_request_jobs, default_target_dir, check_failed_files, generate_curl_command, manage_failed_request_file, ensure_valid_geometry
from typing import Optional, Any, Tuple, Dict


def strip_html_tags(text):
    """Remove HTML tags from text and return plain text."""
    if not text or not isinstance(text, str):
        return text
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"

ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
with open(ASSET_CONFIG_YAML_PATH) as _fp:
    _asset_config = yaml.safe_load(_fp)

COUNTRIES_YAML_PATH = os.path.join(os.getcwd(), "configs", "countries.yaml")
with open(COUNTRIES_YAML_PATH) as _fp:
    _mapping = yaml.safe_load(_fp)
# Use the top‑level keys in the YAML as partition keys
ALL_COUNTRIES = list(_mapping.keys())
country_partitions = StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)

MATRIX_YAML_PATH = os.path.join(os.getcwd(), "configs", "matrix.yaml")
with open(MATRIX_YAML_PATH) as _fp:
    _matrix = yaml.safe_load(_fp)

ALL_TOPICS = list(_matrix.get("topics", {}).keys())
topics_partitions = StaticPartitionsDefinition(partition_keys=ALL_TOPICS)

s3_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "s3_config.yaml")

with open(s3_CONFIG_YAML_PATH) as _fp:
    _s3_config = yaml.safe_load(_fp)

OSMHISTORY_BASE = {
    "count": "https://api.ohsome.org/v1/elements/count",
    "length": "https://api.ohsome.org/v1/elements/length",
    "area": "https://api.ohsome.org/v1/elements/area",
}

THEME_CONFIG = {
    "building": {
        "key": "building=* or building",
        "grouping_key": "building",
        "values": ["residential","public","commercial","industrial","house","yes","shed"],
        "measure": "area",
    },
    "highway": {
        "key": "highway=* or highway",
        "grouping_key": "highway",
        "values": ["motorway","trunk","primary","secondary","tertiary","unclassified","residential"],
        "measure": "length",
    },
    "school_isced": {
        "key": "amenity=school or isced:level",
        "grouping_key": "isced:level",
        "values": [str(v) for v in ["0","1","2","3","0;1"]],
        "measure": "count",
    },
    "school_operator": {
        "key": "amenity=school and operator:type=* or amenity=school and operator:type",
        "grouping_key": "operator:type",
        "values": ["public","private","government","community","religious","business","university"],
        "measure": "count",
    },
    "hospital_count": {
        "key": "amenity=hospital or healthcare=hospital",
        "grouping_key": None, # Signal that this is a total count, not a breakdown
        "values": [],
        "measure": "count",
    },
    "hospital_speciality": {
        "key": "(amenity=hospital or healthcare=hospital) and healthcare:speciality=* or (amenity=hospital or healthcare=hospital) and healthcare:speciality",
        "grouping_key": "healthcare:speciality",
        "values": ["general","chiropractic","ophthalmology","paediatrics","gynaecology","psychiatry","dentist","internal"],
        "measure": "count",
    },
    "hospital_operator": {
        "key": "(amenity=hospital or healthcare=hospital) and operator:type=* or (amenity=hospital or healthcare=hospital) and operator:type",
        "grouping_key": "operator:type",
        "values": ["public","private","government","community","religious","business","university"],
        "measure": "count",
    },
    "healthcare-primary_count": {
        "key": "amenity in (clinic, doctors, health_post) or healthcare in (clinic, doctors, doctor, midwife, nurse, center)",
        "grouping_key": None,
        "values": [],
        "measure": "count",
    },
    "healthcare-primary_speciality": {
        "key": "(amenity in (clinic, doctors, health_post) or healthcare in (clinic, doctors, doctor, midwife, nurse, center)) and healthcare:speciality=* or (amenity in (clinic, doctors, health_post) or healthcare in (clinic, doctors, doctor, midwife, nurse, center)) and healthcare:speciality",
        "grouping_key": "healthcare:speciality",
        "values": ["general","chiropractic","ophthalmology","paediatrics","gynaecology","psychiatry","dentist","internal"],
        "measure": "count",
    },
    "healthcare-primary_operator": {
        "key": "(amenity in (clinic, doctors, health_post) or healthcare in (clinic, doctors, doctor, midwife, nurse, center)) and operator:type=* or (amenity in (clinic, doctors, health_post) or healthcare in (clinic, doctors, doctor, midwife, nurse, center)) and operator:type",
        "grouping_key": "operator:type",
        "values": ["public","private","government","community","religious","business","university"],
        "measure": "count",
    },
}
key_partitions = StaticPartitionsDefinition(["highway", "building", "school", "hospital", "healthcare-primary"])

multi_partitions = MultiPartitionsDefinition(
    {
        "country": country_partitions,  # assuming defined elsewhere
        "topic": topics_partitions,
    }
)

second_multi_partitions = MultiPartitionsDefinition(
    {
        "country": country_partitions,  # assuming defined elsewhere
        "key": key_partitions,

    }
)

oqapi_version = _asset_config.get("oqapi-version", "v1")

@asset(
    partitions_def=country_partitions,
)
def boundary_asset(context) -> Output[list[str]]:
    """
    Download *all* ADM boundaries for this country (ADM0, ADM1, ADM2…)
    using either the GeoBoundaries API or GitHub depending on
    the 'api' value in the assets_config.yaml under boundary_asset.
    Adds a unique 'id' column to each ADM layer that includes ADM level.
    """

    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    api_choice = _asset_config.get("boundary_asset", {}).get("api", [])
    context.log.info(f"[{country}] fetching ADM boundaries using API source: {api_choice}")

    # ---- GEOBOUNDARIES option ----
    if api_choice == "geoboundaries":
        list_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL"
        try:
            download_from_geoboundaries(
                list_url=list_url,
                country=country,
                level_val="boundaryType",
                url_val="gjDownloadURL",
                out_dir=out_dir,
            )
        except SystemExit as e:
            context.log.warning(f"[{country}] geoBoundaries download failed: {e}")
            raise Exception(f"Failed to fetch boundaries for {country} from geoBoundaries.")

    # ---- GITHUB option ----
    elif api_choice == "github":
        list_url = (
            f"https://api.github.com/repos/wmgeolab/geoBoundaries/contents/"
            f"releaseData/gbOpen/{country}?ref=main"
        )
        try:
            download_from_github(
                list_url=list_url,
                country=country,
                level_val="name",
                url_val="git_url",
                out_dir=out_dir,
            )
        except SystemExit as e:
            context.log.warning(f"[{country}] GitHub download failed: {e}")
            raise Exception(f"Failed to fetch boundaries for {country} from GitHub.")

    else:
        raise ValueError(
            f"Invalid API choice '{api_choice}' in assets_config.yaml. "
            f"Valid options: 'geoboundaries' or 'github'."
        )

    # ---- Verify results ----
    pattern = os.path.join(out_dir, "boundary_ADM*.geojson")
    paths = sorted(glob.glob(pattern))

    if not paths:
        raise FileNotFoundError(
            f"[{country}] No boundary files found at {pattern}. "
            "Download may have failed."
        )

    context.log.info(f"[{country}] Downloaded {len(paths)} boundary files via {api_choice}")

    # ---- Simplify geometries and add unique 'id' with ADM level ----
    updated_paths = []
    for p in paths:
        simplify_geometries(p)

        # Load GeoJSON
        gdf = gpd.read_file(p)

        # Only add 'id' if not already present
        if "id" not in gdf.columns:
            # Determine ADM level from filename: boundary_ADM0.geojson -> 0
            adm_level = os.path.basename(p).split("_")[1].replace("ADM", "").replace(".geojson", "")
            gdf = gdf.reset_index(drop=True)
            gdf["id"] = [
                f"{country}_adm{adm_level}_{str(i+1).zfill(2)}" for i in range(len(gdf))
            ]

            # Overwrite file with new 'id' column
            gdf.to_file(p, driver="GeoJSON")

        updated_paths.append(p)

    return Output(
        updated_paths,
        metadata={
            "paths": updated_paths,
            "count": len(updated_paths),
            "source": api_choice,
        },
    )


@asset(
    ins={"boundary_asset": AssetIn()},
    partitions_def=country_partitions,
)
def h3_hexgrid_asset(context, boundary_asset: list[str]) -> Output[str]:
    """
    Create an H3 hexagonal grid over the ADM0 boundary and save as GeoPackage.
    """
    
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    # Find ADM0 boundary
    try:
        boundary_path = next(
            p for p in boundary_asset if "ADM0" in os.path.basename(p)
        )
    except StopIteration:
        raise FileNotFoundError(f"[{country}] No ADM0 boundary file found.")

    gdf = gpd.read_file(boundary_path).to_crs(4326)
    
    # Get config section for grids
    grid_config = _asset_config.get("grids", {})
    context.log.info(f"[{country}]grid_config:{grid_config}")
    params = get_dynamic_resolutions(gdf, grid_config)
    
    zoom_level = params["h3"]
    context.log.info(f"[{country}] Using h3 zoom-level: {zoom_level}")

    bounds = gdf.total_bounds
    minx, miny, maxx, maxy = bounds
    buffer_size = 0.05
    bbox_geom = box(minx - buffer_size, miny - buffer_size, maxx + buffer_size, maxy + buffer_size)

    # Generate H3 cells intersecting the bounding box
    # (For large countries, this can be heavy — consider tiling)
    bbox_cell_column = h3.geo_to_cells(bbox_geom, res=zoom_level)
    cell_series = pd.Series(bbox_cell_column)
    cell_geoms = cell_series.apply(lambda c: h3.cells_to_geo([c]))
    geoms = cell_geoms.apply(shape)

    grid_gdf = gpd.GeoDataFrame(geometry=geoms, crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(
        gdf[["shapeName", "shapeISO", "geometry"]],
        geometry="geometry",
        crs="EPSG:4326",
    )

    # Clip to ADM0
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection")
    print(bbox_cell_column)

    # Create simple unique IDs
    grid_clipped = grid_clipped.reset_index(drop=True)

    grid_clipped["h3_id"] = (
        f"{country}_hex{zoom_level}_"
        + (grid_clipped.index + 1).astype(str)
    )
    
    grid_clipped = grid_clipped[["h3_id", "shapeName", "shapeISO", "geometry"]]

    output_path = os.path.join(out_dir, f"{country}_h3_z{zoom_level}.gpkg")

    grid_clipped = grid_clipped.rename(
        columns={
            "h3_id": "id",
            "shapeName": "ADM0_name",
            "shapeISO": "ADM0_iso",
            "shapeID": "ADM0_id",
        }
    )

    grid_clipped.to_file(output_path, driver="GPKG")

    context.log.info(f"[{country}] H3 grid saved with {len(grid_clipped)} cells.")

    return Output(
        output_path,
        metadata={
            "country": country,
            "zoom_level": zoom_level,
            "cell_count": len(grid_clipped),
            "output_path": output_path,
        },
    )


@asset(
    ins={"boundary_asset": AssetIn()},
    partitions_def=country_partitions,
)
def square_grid_asset(context, boundary_asset: list[str]) -> Output[str]:
    """
    Create a degree-based square grid (e.g., 0.1°) clipped to ADM0 boundary.
    """

    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    # Get ADM0 boundary
    try:
        boundary_path = next(
            p for p in boundary_asset if "ADM0" in os.path.basename(p)
        )
    except StopIteration:
        raise FileNotFoundError(f"[{country}] No ADM0 boundary found.")

    gdf = gpd.read_file(boundary_path).to_crs(4326)
    
    # Get config section for grids
    grid_config = _asset_config.get("grids", {})
    params = get_dynamic_resolutions(gdf, grid_config)
    
    res_deg = params["square"]
    context.log.info(f"[{country}] Using Square Res: {res_deg}°")
    minx, miny, maxx, maxy = gdf.total_bounds

    # Generate global-like grid cells within bounding box
    x0 = np.floor(minx / res_deg) * res_deg
    y0 = np.floor(miny / res_deg) * res_deg
    xs = np.arange(x0, maxx + res_deg, res_deg)
    ys = np.arange(y0, maxy + res_deg, res_deg)

    cells = []
    for x in xs:
        for y in ys:
            cells.append(box(x, y, x + res_deg, y + res_deg))

    grid_gdf = gpd.GeoDataFrame(geometry=cells, crs="EPSG:4326")

    
    gdf = gpd.GeoDataFrame(
        gdf[["shapeName", "shapeISO", "geometry"]],
        geometry="geometry",
        crs="EPSG:4326",
    )
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection")

    # Create simple unique IDs
    grid_clipped = grid_clipped.reset_index(drop=True)

    grid_clipped["cell_id"] = (
        f"{country}_sqr{res_deg}_"
        + (grid_clipped.index + 1).astype(str)
    )

    grid_clipped = enrich_grid_with_admin_levels(grid_clipped, boundary_asset, country, context)
    
    output_path = os.path.join(out_dir, f"{country}_grid_{res_deg}deg.gpkg")

    grid_clipped = grid_clipped.rename(
        columns={
            "cell_id": "id",
            "shapeName": "ADM0_name",
            "shapeISO": "ADM0_iso",
            "shapeID": "ADM0_id",
        }
    )

    grid_clipped.to_file(output_path, driver="GPKG")

    context.log.info(f"[{country}] Square grid saved with {len(grid_clipped)} cells.")

    return Output(
        output_path,
        metadata={
            "country": country,
            "resolution_deg": res_deg,
            "cell_count": len(grid_clipped),
            "output_path": output_path,
        },
    )

def enrich_grid_with_admin_levels(grid_gdf, boundary_asset, country, context):
    """
    Enrich a grid GeoDataFrame (square or hex) with attributes from ADM1–ADM10 boundaries.
    Keeps only the boundary with the largest overlap per grid cell.

    Parameters
    ----------
    grid_gdf : GeoDataFrame
        The grid (already clipped to ADM0).
    boundary_asset : list[str]
        Paths to boundary files for all ADM levels.
    country : str
        ISO country code (for logging).
    context : dagster op context
        For logging.

    Returns
    -------
    GeoDataFrame
        Grid with added ADM-level attributes (e.g., ADM1_name, ADM2_id, ...).
    """
    for adm_path in boundary_asset:
        adm_level = os.path.splitext(os.path.basename(adm_path))[0].split("_")[-1]
        if adm_level == "ADM0":
            continue

        context.log.info(f"[{country}] Enriching grid with {adm_level} attributes")

        adm_gdf = gpd.read_file(adm_path).to_crs(4326)[["shapeName", "shapeISO", "shapeID", "geometry"]]
        adm_gdf.columns = ["name", "iso", "id", "geometry"]

        # Spatial intersection to determine largest overlap
        joined = gpd.overlay(grid_gdf, adm_gdf, how="intersection", keep_geom_type=False)
        joined["intersect_area"] = joined.geometry.area

        joined_sorted = joined.sort_values("intersect_area", ascending=False).drop_duplicates(subset="cell_id")

        grid_gdf = grid_gdf.merge(
            joined_sorted[["cell_id", "name", "iso", "id"]],
            on="cell_id",
            how="left",
            suffixes=("", f"_{adm_level}"),
        )

        grid_gdf = grid_gdf.rename(
            columns={
                "name": f"{adm_level}_name",
                "iso": f"{adm_level}_iso",
                "id": f"{adm_level}_id",
            }
        )

    return grid_gdf


class OhsomeConfig(Config):
    """Dagster config schema for the OHSOME grid indicator asset."""
    unit_of_analysis: str = "square"   # or "h3"
    indicator: str = "road-comparison"
    topic: str = "roads-all-highways"
    max_workers: int = 10


@asset
def indicators_per_topic_asset(context) -> Output[dict]:
    """Fetch indicators per topic and available attributes for attribute-completeness."""

    yaml_file = "configs/matrix.yaml"

    # Step 1: Load existing YAML
    with open(yaml_file, "r") as file:
        matrix_data = yaml.safe_load(file)

    # Ensure the structure exists
    if "topics" not in matrix_data:
        matrix_data["topics"] = {}

    base_url = f"https://api.quality.ohsome.org/{oqapi_version}"
    headers = {"accept": "application/json"}
    runtime_info = {}

    # Step 2: Fetch indicators per topic
    for topic in matrix_data["topics"].keys():
        url = f"{base_url}/metadata/topics/{topic}"

        def fetch(index):
            for attempt in range(4):
                try:
                    start_t = time.time()
                    r = requests.get(url, headers=headers, timeout=120)
                    r.raise_for_status()
                    result = r.json()
                    end_t = time.time()
                    indicators = result["result"][topic]["indicators"]
                    matrix_data["topics"][topic]["indicators"] = indicators
                    runtime_info[topic] = end_t - start_t
                    return index, indicators, end_t - start_t
                except requests.RequestException as e:
                    if attempt < 3:
                        time.sleep(2)
                    else:
                        context.log.warning(f"Max retries reached for {topic}: {e}")
                        return index, [], None

        _, _, _ = fetch(0)

    # Step 3: Fetch attributes for topics with 'attribute-completeness'
    attr_url = f"{base_url}/metadata/attributes"
    try:
        context.log.info("Fetching all attribute metadata...")
        r = requests.get(attr_url, headers=headers, timeout=120)
        r.raise_for_status()
        attr_data = r.json().get("result", {})
    except requests.RequestException as e:
        context.log.warning(f"Failed to fetch attributes metadata: {e}")
        attr_data = {}

    # Step 4: Add attributes for topics that have 'attribute-completeness'
    for topic, data in matrix_data["topics"].items():
        indicators = data.get("indicators", [])
        if "attribute-completeness" in indicators and topic in attr_data:
            # Extract all available attributes for this topic
            attributes = list(attr_data[topic].keys())
            matrix_data["topics"][topic]["attributes"] = attributes

    # Step 5: Write updated topics back to YAML
    with open(yaml_file, "w") as file:
        yaml.dump(matrix_data, file, sort_keys=False)

    # ----- Create indicator matrix png from yaml -----
    with open(yaml_file, "r") as f:
        data = yaml.safe_load(f)

    out_png = "data/indicator_matrix.png"

    topics = data["topics"]

    indicator_counts = {}

    # get and count indicaotrs
    for topic_data in topics.values():
        for indicator in topic_data.get("indicators", []):
            indicator_counts[indicator] = indicator_counts.get(indicator, 0) + 1

    # sort indicators by frequence
    all_indicators = sorted(indicator_counts.keys(), key=lambda x: indicator_counts[x], reverse=True)

    rows = []

    for topic_name, topic_data in topics.items():
        row = {"topic": topic_name}
        indicators = topic_data.get("indicators", [])

        for ind in all_indicators:
            row[ind] = "X" if ind in indicators else "-"

        rows.append(row)

    df = pd.DataFrame(rows)

    # save as png
    _, ax = plt.subplots(figsize=(12, 4))
    ax.axis('off')

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        loc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(6)

    # make table nicer with color and bigger text
    for (row, _), cell in table.get_celld().items():
        txt = cell.get_text()
        txt.set_horizontalalignment('center')
        txt.set_weight("bold")

        if txt.get_text() == "X":
            txt.set_color("green")
        elif txt.get_text() == "-":
            txt.set_color("red")

    table.scale(1.4, 2)

    plt.savefig(out_png, bbox_inches='tight', dpi=200)
    plt.close()

    return Output(
        {
        "yaml_file": yaml_file,
        "png_file": out_png,
        "metadata": {"topics_processed": len(matrix_data["topics"]),
        "has_attributes": sum(1 for v in matrix_data["topics"].values() if "attributes" in v and v["attributes"]),
        "runtime_sec_total": sum(runtime_info.values()), },
        },
    )

@asset(partitions_def=multi_partitions)
async def ohsome_api_requests_asset(
    context,
    square_grid_asset: Optional[str] = None,
    h3_hexgrid_asset: Optional[str] = None,
) -> Output[dict]:
    log = context.log
    country, topic = context.partition_key.split("|")

    unit = _asset_config.get("grids", {}).get("unit", "h3")

    if unit in ["h3", "square"]:
        log.info(f"[{country}] UNIT: Using '{unit}' grid cells.")

    grid_path = h3_hexgrid_asset if unit == "h3" else square_grid_asset
    max_workers = int(_asset_config.get("grids", {}).get("max_workers", 10))

    raw_root = Path("data") / country / f"raw_responses_{topic}"
    
    paths = {
        "sqr_raw": raw_root / "sqr",
        "hex_raw": raw_root / "hex",
        "ADM0_raw": raw_root / "ADM0",
        "ADM1_raw": raw_root / "ADM1"
    }
    for p in paths.values(): p.mkdir(parents=True, exist_ok=True)

    handle_500_as_na = _asset_config.get("grids", {}).get("handle_500_as_na", False)
    
    if handle_500_as_na:
        log.info(f"[{country}] CONFIG: 500 Errors will be handled as 'NA' (Dummy JSON).")
    else:
        log.info(f"[{country}] CONFIG: 500 Errors will trigger standard retries and failure.")

    with open(MATRIX_YAML_PATH) as f:
        matrix = yaml.safe_load(f)

    indicators = matrix["topics"][topic]["indicators"]
    attributes = matrix["topics"][topic].get("attributes", [])
    relevant = [a for a in matrix["relevant_attributes"] if a in attributes]
    
    base_url = f"https://api.quality.ohsome.org/{oqapi_version}"
    headers = {"accept": "application/json", "Content-Type": "application/json"}

    async def fetch_and_save(session, semaphore, job, max_attempts=3):
        path = Path(job["path"])
        indicator = job["indicator"]
        geom_id = job["geom_id"]
        attr = job.get("attribute")

        if path.exists() and path.stat().st_size > 0:
            manage_failed_request_file(raw_root, job, success=True)
            return None
        
        original_geometry = ensure_valid_geometry(job["geometry"])
        geometry = original_geometry
        
        params = {
            "topic": topic,
            "bpolys": geojson.FeatureCollection([geojson.Feature(geometry=geometry)]),
        }
        if attr:
            params["attributes"] = [attr]

        url = f"{base_url}/indicators/{indicator}"
        job["curl_command"] = generate_curl_command(url, headers, params)

        simplified_geometry = None

        for attempt in range(1, max_attempts + 1):
            async with semaphore:
                try:
                    if asyncio.current_task().cancelling():
                        raise asyncio.CancelledError("Shutdown requested")
                    log.debug(f"[{country}] Requesting {indicator} for {geom_id} (Attempt {attempt})")
                    async with session.post(url, headers=headers, json=params, timeout=aiohttp.ClientTimeout(total=120)) as r:
                        if r.status == 500 and handle_500_as_na:
                            log.warning(f"[{country}] Server Error 500 for {geom_id}. Config set to save as NA.")
                            with open(path, "w") as f:
                                json.dump({
                                    "indicator": indicator, 
                                    "status": "error", 
                                    "value": None, 
                                    "error": "500 Server Error"
                                }, f)
                            manage_failed_request_file(raw_root, job, success=True)
                            return None

                        r.raise_for_status()
                        data = await r.json()
                        with open(path, "w") as f:
                            json.dump(data, f)
                        manage_failed_request_file(raw_root, job, success=True)
                        return None

                except asyncio.CancelledError:
                    manage_failed_request_file(raw_root, job, success=False)
                    raise
                except aiohttp.ClientResponseError:
                    if attempt < max_attempts:
                        await asyncio.sleep(2 * attempt)
                        continue
                    manage_failed_request_file(raw_root, job, success=False)
                    raise
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt < max_attempts:
                        await asyncio.sleep(2 * attempt)
                        continue
                    if simplified_geometry is None:
                        log.warning(f"[{country}] Request failed after {max_attempts} attempts for {geom_id}. Simplifying geometry and retrying...")
                        simplify_tolerances = [0.01, 0.05, 0.1]
                        geom_shape = shape(original_geometry)
                        simplified_geometry = geom_shape.simplify(tolerance=simplify_tolerances[0], preserve_topology=True)
                        for simplify_try in range(3):
                            geometry = mapping(simplified_geometry)
                            params["bpolys"] = geojson.FeatureCollection([geojson.Feature(geometry=geometry)])
                            job["curl_command"] = generate_curl_command(url, headers, params)
                            log.debug(f"[{country}] Retrying {indicator} for {geom_id} with simplified geometry (attempt {simplify_try + 1}, tolerance={simplify_tolerances[simplify_try]})")
                            try:
                                async with semaphore:
                                    if asyncio.current_task().cancelling():
                                        raise asyncio.CancelledError("Shutdown requested")
                                    async with session.post(url, headers=headers, json=params, timeout=aiohttp.ClientTimeout(total=120)) as r:
                                        if r.status == 500 and handle_500_as_na:
                                            log.warning(f"[{country}] Server Error 500 for {geom_id} (simplified, attempt {simplify_try + 1}). Config set to save as NA.")
                                            with open(path, "w") as f:
                                                json.dump({
                                                    "indicator": indicator, 
                                                    "status": "error", 
                                                    "value": None, 
                                                    "error": "500 Server Error"
                                                }, f)
                                            manage_failed_request_file(raw_root, job, success=True)
                                            return None

                                        r.raise_for_status()
                                        data = await r.json()
                                        with open(path, "w") as f:
                                            json.dump(data, f)
                                        manage_failed_request_file(raw_root, job, success=True)
                                        return None
                            except (asyncio.CancelledError, TimeoutError, aiohttp.ClientError):
                                if simplify_try < 2:
                                    log.warning(f"[{country}] Simplified geometry attempt {simplify_try + 1} failed for {geom_id} (tolerance={simplify_tolerances[simplify_try]}). Simplifying further with tolerance={simplify_tolerances[simplify_try + 1]}...")
                                    simplified_geometry = simplified_geometry.simplify(tolerance=simplify_tolerances[simplify_try + 1], preserve_topology=True)
                                    continue
                                manage_failed_request_file(raw_root, job, success=False)
                                raise
                            except Exception:
                                manage_failed_request_file(raw_root, job, success=False)
                                raise
                        manage_failed_request_file(raw_root, job, success=False)
                        raise
                    manage_failed_request_file(raw_root, job, success=False)
                    raise
                except Exception:
                    if attempt < max_attempts:
                        await asyncio.sleep(2 * attempt)
                        continue
                    manage_failed_request_file(raw_root, job, success=False)
                    raise

    async def run_jobs(jobs, max_workers):
        total = len(jobs)
        semaphore = asyncio.Semaphore(max_workers)

        connector = aiohttp.TCPConnector(
            limit=max_workers * 2,
            limit_per_host=max_workers,
            keepalive_timeout=30
        )
        timeout = aiohttp.ClientTimeout(total=120, connect=10)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            futures = []
            for job in jobs:
                fut = asyncio.ensure_future(fetch_and_save(session, semaphore, job))
                futures.append(fut)

            completed_count = 0
            failed_list = []

            for coro in asyncio.as_completed(futures):
                try:
                    result = await coro
                    completed_count += 1
                    if total <= 10 or completed_count % 10 == 0:
                        log.info(f"[{country}] {completed_count}/{total} requests done")
                except Exception as e:
                    pass

            for idx, fut in enumerate(futures):
                exc = fut.exception()
                if exc:
                    job = jobs[idx]
                    job["error"] = str(exc)
                    failed_list.append(job)
                elif fut.cancelled():
                    job = jobs[idx]
                    job["error"] = "Cancelled (likely due to timeout)"
                    failed_list.append(job)

            if failed_list:
                failed_geom_ids = {j["geom_id"] for j in failed_list}
                log.warning(f"[{country}] {len(failed_list)} requests failed ({failed_geom_ids}).")

            return completed_count, failed_list

    start = time.time()
    done_count = 0  
    fail_list = []  
    
    gdf = gpd.read_file(grid_path)
    all_jobs = build_api_request_jobs(
        gdf=gdf, indicators=indicators, relevant=relevant, 
        topic=topic, default_target_dir=default_target_dir, **paths
    )
    
    for level, p_key in [("ADM1", "ADM1_raw"), ("ADM0", "ADM0_raw")]:
        b_path = f"data/{country}/boundary_{level}.geojson"
        if os.path.exists(b_path):
            b_gdf = gpd.read_file(b_path)
            if level == "ADM0": b_gdf = b_gdf.dissolve()

            all_jobs.extend(build_api_request_jobs(
                gdf=b_gdf, indicators=indicators, relevant=relevant, 
                topic=topic, default_target_dir=default_target_dir, **paths 
            ))

    current_unit_label = "hex" if unit == "h3" else "sqr"
    relevant_units = {current_unit_label, "ADM0", "ADM1"}

    final_jobs_to_run = []
    skipped_count = 0

    for job in all_jobs:
        if job["unit"] not in relevant_units:
            continue
            
        geom_obj = shape(job["geometry"])
        minx, miny, maxx, maxy = geom_obj.bounds
        
        width = maxx - minx
        height = maxy - miny
        
        is_degenerate = (
            geom_obj.area < 1e-10 or 
            width < 1e-6 or 
            height < 1e-6
        )

        if is_degenerate:
            log.info(f"[{country}] Skipping degenerate geom {job['geom_id']}: Bounds {geom_obj.bounds}")
            continue

        if job["path"].exists() and job["path"].stat().st_size > 0:
            skipped_count += 1
            continue

        final_jobs_to_run.append(job)
    
    if not final_jobs_to_run:
        log.info(f"[{country}] All data for {relevant_units} is already present on disk. Nothing to do.")
    else:
        log.info(f"[{country}] Found {len(final_jobs_to_run)} jobs to process ({skipped_count} already exist).")
        done_count, fail_list = await run_jobs(final_jobs_to_run, max_workers)

        failed_adm_jobs = [j for j in fail_list if j["unit"] in ("ADM0", "ADM1")]
        
        if failed_adm_jobs:
            failed_geom_ids = {j["geom_id"] for j in failed_adm_jobs}
            log.warning(f"[{country}] {len(failed_adm_jobs)} ADM0/ADM1 requests failed ({failed_geom_ids}). Retrying with island-filtered boundaries...")
            
            from scripts.helper_functions import filter_small_islands
            
            for level in ["ADM0", "ADM1"]:
                b_path = f"data/{country}/boundary_{level}.geojson"
                if os.path.exists(b_path):
                    gdf = gpd.read_file(b_path)
                    gdf["geometry"] = gdf.geometry.apply(
                        lambda g: filter_small_islands(g, min_area_sq_deg=0.05, min_area_ratio=0.005) if g is not None else g
                    )
                    gdf.to_file(b_path, driver="GeoJSON")
                    log.info(f"[{country}] Filtered small islands from {b_path}")
            
            adm0_path = f"data/{country}/boundary_ADM0.geojson"
            adm1_path = f"data/{country}/boundary_ADM1.geojson"
            
            b_gdfs = []
            for level, b_path in [("ADM0", adm0_path), ("ADM1", adm1_path)]:
                if os.path.exists(b_path):
                    gdf = gpd.read_file(b_path)
                    if level == "ADM0":
                        gdf = gdf.dissolve()
                    b_gdfs.append(gdf)
            
            if b_gdfs:
                combined_gdf = gpd.GeoDataFrame(pd.concat(b_gdfs, ignore_index=True))
                combined_gdf = combined_gdf[combined_gdf.geometry.notna()]
                
                all_retry_jobs = build_api_request_jobs(
                    gdf=combined_gdf, indicators=indicators, relevant=relevant, 
                    topic=topic, default_target_dir=default_target_dir, **paths 
                )
                retry_jobs = [j for j in all_retry_jobs if j["unit"] in ("ADM0", "ADM1") and j["geom_id"] in failed_geom_ids]
                
                for job in retry_jobs:
                    if job["path"].exists():
                        job["path"].unlink()
                
                if retry_jobs:
                    log.info(f"[{country}] Retrying {len(retry_jobs)} failed ADM0/ADM1 jobs with filtered boundaries...")
                    done_count_retry, fail_list_retry = await run_jobs(retry_jobs, max_workers)
                    done_count += done_count_retry
                    fail_list = fail_list_retry

        for label in relevant_units:
            unit_fail_dir = raw_root / f"failed_requests_{label}"
            if unit_fail_dir.exists() and not any(unit_fail_dir.iterdir()):
                try:
                    unit_fail_dir.rmdir()
                except OSError:
                    pass

    if 'fail_list' in locals() and fail_list:
        failed_requests = [j["path"].name for j in fail_list]
        error_msg = f"[{country}] CRITICAL: {len(fail_list)} requests failed: {failed_requests}."
        raise RuntimeError(error_msg)

    return Output(
        {k: str(v) for k, v in paths.items()},
        metadata={
            "runtime": round(time.time() - start, 1), 
            "completed": done_count,
            "failed": len(fail_list)
        }
    )


@asset(partitions_def=multi_partitions)
def build_outputs_asset(
    context,
    ohsome_api_requests_asset: dict,
    square_grid_asset: Optional[str] = None,
    h3_hexgrid_asset: Optional[str] = None,
) -> Output[dict]:

    log = context.log
    country, topic = context.partition_key.split("|")

    log.info(f"[{country}] Starting build_outputs asset for topic '{topic}'")

    # -------------------------
    # Helper
    # -------------------------
    def to_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    # Grid settings & Raw Path Mapping
    # -------------------------
    unit = _asset_config.get("grids", {}).get("unit", "h3")
    
    # NEW: Map the grid and the raw folder based on the unit configuration
    if unit == "h3":
        grid_path = h3_hexgrid_asset
        grid_raw = Path(ohsome_api_requests_asset["hex_raw"])
    else:
        grid_path = square_grid_asset
        grid_raw = Path(ohsome_api_requests_asset["sqr_raw"])

    # NEW: Define ADM paths explicitly from the upstream asset dictionary
    adm0_raw = Path(ohsome_api_requests_asset["ADM0_raw"])
    adm1_raw = Path(ohsome_api_requests_asset["ADM1_raw"])

    log.info(f"[{country}] Using '{unit}' grid for building outputs")
    log.info(f"[{country}] Grid raw responses from: {grid_raw}")

    if not os.path.exists(grid_path):
        raise FileNotFoundError(f"Grid file not found: {grid_path}")

    gdf = gpd.read_file(grid_path)
    log.info(f"[{country}] Loaded grid with {len(gdf)} cells")

    # -------------------------
    # Load matrix
    # -------------------------
    log.info(f"[{country}] Loading indicator matrix: {MATRIX_YAML_PATH}")

    with open(MATRIX_YAML_PATH) as f:
        matrix = yaml.safe_load(f)

    indicators = matrix["topics"][topic]["indicators"]
    attributes = matrix["topics"][topic].get("attributes", [])
    relevant = [a for a in matrix["relevant_attributes"] if a in attributes]

    start = time.time()

    # -------------------------
    # Populate grid dataframe
    # -------------------------
    log.info(f"[{country}] Populating grid result columns")

    for ind in indicators:
            is_attr = (ind == "attribute-completeness")
            suffixes = relevant if is_attr else [None]
            
            for sfx in suffixes:
                col = f"result_value_{ind}_{sfx}" if sfx else f"result_value_{ind}"
                gdf[col] = pd.Series([None] * len(gdf), dtype="float64")
                
                for idx, row in gdf.iterrows():
                    geom_id = row["id"]
                    filename = f"{topic}__{ind}__{sfx}__{geom_id}.json" if sfx else f"{topic}__{ind}__{geom_id}.json"
                    p = grid_raw / filename
                    
                    if p.exists():
                        with open(p) as f:
                            j = json.load(f)
                        
                        # SAFE EXTRACTION: Check for standard result or "NA" error format
                        res = j.get("result", [{}])[0].get("result", {})
                        # If it's a 500-error dummy file, use the top-level 'value' key
                        val = res.get("value") if res else j.get("value")
                        
                        gdf.at[idx, col] = to_float(val)

    runtime = round(time.time() - start, 1)
    log.info(f"[{country}] Grid values populated in {runtime}s")

    # -------------------------
    # Save grid outputs
    # -------------------------
    out_dir = Path("data") / country / "Output"
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = "h3" if unit == "h3" else "square"
    gpkg = out_dir / f"{country}_{topic}_{suffix}.gpkg"
    csv = out_dir / f"{country}_{topic}_{suffix}.csv"

    gdf.to_file(gpkg)
    gdf.drop(columns="geometry").to_csv(csv, index=False)

    # -------------------------
    # ADM0 dataframe
    # -------------------------
    adm0_boundary = gpd.read_file(f"data/{country}/boundary_ADM0.geojson").dissolve()
    adm0_df = gpd.GeoDataFrame({"geometry": adm0_boundary.geometry, "id": adm0_boundary.get("id", ["MLI_adm0"]*len(adm0_boundary))})
    
    for idx, row in adm0_df.iterrows():
        geom_id = row["id"]
        for ind in indicators:
            if ind == "attribute-completeness":
                for attr in relevant:
                    p = adm0_raw / f"{topic}__{ind}__{attr}__{geom_id}.json"
                    if p.exists():
                        j = json.load(open(p))
                        adm0_df.at[idx, f"result_value_{ind}_{attr}"] = to_float(j["result"][0]["result"].get("value"))
                        adm0_df.at[idx, f"description_{ind}_{attr}"] = strip_html_tags(j["result"][0]["result"].get("description"))
            else:
                p = adm0_raw / f"{topic}__{ind}__{geom_id}.json"
                if p.exists():
                    j = json.load(open(p))
                    adm0_df.at[idx, f"result_value_{ind}"] = to_float(j["result"][0]["result"].get("value"))
                    adm0_df.at[idx, f"description_{ind}"] = strip_html_tags(j["result"][0]["result"].get("description"))

    # ADM1 dataframe
    # -------------------------
    if os.path.exists(f"data/{country}/boundary_ADM1.geojson"):
        adm1_boundary = gpd.read_file(f"data/{country}/boundary_ADM1.geojson")
        adm1_df = gpd.GeoDataFrame({"geometry": adm1_boundary.geometry, "id": adm1_boundary.get("id", [])})
        
        for idx, row in adm1_df.iterrows():
            geom_id = row["id"]
            for ind in indicators:
                if ind == "attribute-completeness":
                    for attr in relevant:
                        p = adm1_raw / f"{topic}__{ind}__{attr}__{geom_id}.json"
                        if p.exists():
                            j = json.load(open(p))
                            adm1_df.at[idx, f"result_value_{ind}_{attr}"] = to_float(j["result"][0]["result"].get("value"))
                            adm1_df.at[idx, f"description_{ind}_{attr}"] = strip_html_tags(j["result"][0]["result"].get("description"))
                else:
                    p = adm1_raw / f"{topic}__{ind}__{geom_id}.json"
                    if p.exists():
                        j = json.load(open(p))
                        adm1_df.at[idx, f"result_value_{ind}"] = to_float(j["result"][0]["result"].get("value"))
                        adm1_df.at[idx, f"description_{ind}"] = strip_html_tags(j["result"][0]["result"].get("description"))
    else:
        adm1_df = gpd.GeoDataFrame(columns=["geometry", "id"])

    # -------------------------
    # Long-format parquet rows
    # -------------------------
    long_rows = []

    # 1. Add Grid rows from populated gdf
    for idx, row in gdf.iterrows():
        for ind in indicators:
            if ind == "attribute-completeness":
                for attr in relevant:
                    long_rows.append({
                        "geomID": row["id"], "topic": topic,
                        "indicator": f"{ind}_{attr}", "value": row[f"result_value_{ind}_{attr}"]
                    })
            else:
                long_rows.append({
                    "geomID": row["id"], "topic": topic,
                    "indicator": ind, "value": row[f"result_value_{ind}"]
                })

    # 2. Add Country-level rows (Scanning ADM0 and ADM1 folders)
    for folder in [adm0_raw, adm1_raw]:
        for file_path in folder.glob(f"{topic}__*.json"):
            fname = file_path.stem  
            parts = fname.split("__")
            geom_id = parts[-1]

            if "attribute-completeness" in parts[1]:
                indicator_name = f"{parts[1]}_{parts[2]}"
            else:
                indicator_name = parts[1]

            try:
                with open(file_path) as f:
                    j = json.load(f)
                
                # Check for standard response vs dummy NA response
                res = j.get("result", [{}])[0].get("result", {})
                
                long_rows.append({
                    "geomID": geom_id,
                    "topic": topic,
                    "indicator": indicator_name,
                    # Fallback to j.get("value") if the nested result is missing
                    "value": to_float(res.get("value") if res else j.get("value")),
                    "description": strip_html_tags(res.get("description") if res else j.get("error")),
                })
            except Exception:
                continue
    
    parquet_path = out_dir / f"{country}_long.parquet"
    new_df = pd.DataFrame(long_rows)

    with FileLock(parquet_path.with_suffix(".lock")):
        if parquet_path.exists():
            old_df = pd.read_parquet(parquet_path)
            combined = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates(
                subset=["geomID", "topic", "indicator"], keep="last"
            )            
            combined.to_parquet(parquet_path, index=False)
        else:
            new_df.to_parquet(parquet_path, index=False)

    # -------------------------
    # Collect and gzip individual figures
    # -------------------------
    figures_dir = out_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    figures = {unit: {}, "adm0": {}, "adm1": {}}
    figures[unit][topic] = {}

    # Grid Figures
    for ind in indicators:
        keys = [f"{ind}_{a}" for a in relevant] if ind == "attribute-completeness" else [ind]
        for k in keys:
            figures[unit][topic][k] = []
            for idx, row in gdf.iterrows():
                geom_id = row["id"]
                # Adjusting logic to find the file based on indicator type
                filename = f"{topic}__{ind}__{k.split('_')[-1]}__{geom_id}.json" if ind == "attribute-completeness" else f"{topic}__{ind}__{geom_id}.json"
                p = grid_raw / filename
                if p.exists():
                    try:
                        with open(p) as f:
                            j = json.load(f)
                        res = j.get("result", [{}])[0].get("result", {})
                        fig = res.get("figure") if res else None # 500 errors have no figure
                        if fig: 
                            figures[unit][topic][k].append({"geom_id": geom_id, "figure": fig})
                    except Exception: continue

    # ADM0/ADM1 Figures
    for folder, g_type in [(adm0_raw, "adm0"), (adm1_raw, "adm1")]:
        for file_path in folder.glob(f"{topic}__*.json"):
            parts = file_path.stem.split("__")
            ind_name = f"{parts[1]}_{parts[2]}" if "attribute-completeness" in parts[1] else parts[1]
            
            if g_type not in figures: figures[g_type] = {}
            if topic not in figures[g_type]: figures[g_type][topic] = {}
            if ind_name not in figures[g_type][topic]: figures[g_type][topic][ind_name] = []

            try:
                j = json.load(open(file_path))
                fig = j["result"][0]["result"].get("figure")
                if fig: figures[g_type][topic][ind_name].append({"geom_id": parts[-1], "figure": fig})
            except Exception: continue

    for g_type, g_topics in figures.items():
        for t_name, t_inds in g_topics.items():
            for i_name, fig_list in t_inds.items():
                if fig_list:
                    gz_path = figures_dir / f"{g_type}__{t_name}__{i_name}.json.gz"
                    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
                        json.dump(fig_list, f)

# -------------------------
    # Build PMTiles
    # -------------------------
    pmtiles_path = out_dir / f"{country}_boundaries.pmtiles"
    
    if not pmtiles_path.exists():
        layers = {
            "ADM0": f"data/{country}/boundary_ADM0.geojson",
            "ADM1": f"data/{country}/boundary_ADM1.geojson",
            "square_grid": square_grid_asset,
            "h3_hexgrid": h3_hexgrid_asset,
        }

        clean_layers = {}
        tmp_dir = Path(tempfile.mkdtemp(prefix="pmtiles_layers_"))
        for name, path in layers.items():
            if path and os.path.exists(path):
                tmp_gdf = gpd.read_file(path)
                if len(tmp_gdf) > 0:
                    if tmp_gdf.crs and tmp_gdf.crs.to_epsg() != 4326:
                        tmp_gdf = tmp_gdf.to_crs(4326)
                    tmp_gdf["geometry"] = tmp_gdf.geometry.buffer(0)
                    out_path = tmp_dir / f"{name}.geojson"
                    tmp_gdf.to_file(out_path, driver="GeoJSON")
                    clean_layers[name] = str(out_path)

        geojson_to_multilayer_pmtiles(layers=clean_layers, pmtiles_path=str(pmtiles_path))
    else:
        log.info(f"[{country}] PMTiles already exists, skipping build")

    # Final Boundary Outputs
    adm0_gpkg = out_dir / f"{country}_{topic}_ADM0.gpkg"
    adm0_csv = out_dir / f"{country}_{topic}_ADM0.csv"
    adm0_df.to_file(adm0_gpkg)
    adm0_df.drop(columns="geometry").to_csv(adm0_csv, index=False)

    adm1_gpkg = out_dir / f"{country}_{topic}_ADM1.gpkg"
    adm1_csv = out_dir / f"{country}_{topic}_ADM1.csv"
    if len(adm1_df) > 0:
        adm1_df.to_file(adm1_gpkg)
        adm1_df.drop(columns="geometry").to_csv(adm1_csv, index=False)

    return Output({
        "grid_gpkg_path": str(gpkg),
        "grid_csv_path": str(csv),
        "adm0_gpkg_path": str(adm0_gpkg),
        "adm0_csv_path": str(adm0_csv),
        "adm1_gpkg_path": str(adm1_gpkg),
        "adm1_csv_path": str(adm1_csv),
        "parquet_path": str(parquet_path),
        "pmtiles_path": str(pmtiles_path),
        "figures_gzip_path": str(figures_dir),
    })


@asset(
    deps=["boundary_asset"],
    partitions_def=second_multi_partitions,
)
def tag_distribution_asset(context) -> Output[dict]:
    log = context.log
    country, theme = context.partition_key.split("|")

    # Theme expansion logic remains the same
    if theme == "school":
        themes = ["school_isced", "school_operator"]
    elif theme == "hospital":
        themes = ["hospital_speciality", "hospital_operator", "hospital_count"]
    elif theme == "healthcare-primary":
        themes = ["healthcare-primary_speciality", "healthcare-primary_operator", "healthcare-primary_count"]
    else:
        themes = [theme]

    boundary_path = f"data/{country}/boundary_ADM0.geojson"
    gdf = gpd.read_file(boundary_path).dissolve()

    # --- FIX: SIMPLIFY GEOMETRY ---
    # 0.001 is roughly 111 meters. This drastically reduces the string length 
    # of the GeoJSON and prevents the 413 "Payload Too Large" error.
    simplified_gdf = gdf.simplify(tolerance=0.005, preserve_topology=True)
    geom_json = mapping(simplified_gdf.iloc[0])
    # ------------------------------

    out_dir = Path("data") / country / "Output"
    out_dir.mkdir(parents=True, exist_ok=True)

    def call_ohsome(filter_expr, grouping_key, grouping_values, measure):
        # We send as data=params which is application/x-www-form-urlencoded
        params = {
            "bpolys": json.dumps({
                "type": "FeatureCollection",
                "features": [{"type": "Feature", "geometry": geom_json}]
            }),
            "filter": filter_expr,
            "time": "2026-01-01",
        }

        url = OSMHISTORY_BASE[measure]
        if grouping_key:
            url = f"{url.rstrip('/')}/groupBy/tag"
            params["groupByKey"] = grouping_key
            if grouping_values:
                params["groupByValues"] = grouping_values
        
        log.info(f"[{country}] Calling OHSOME: {url} | Payload size: {len(str(params))} chars")
        
        try:
            r = requests.post(url, data=params, timeout=600) # Increased timeout for large regions
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 413:
                log.error(f"Payload still too large for {country}. Try increasing simplification tolerance.")
            raise e

    def sum_groupby(resp):
        if "groupByResult" in resp:
            return sum(
                entry["value"]
                for group in resp["groupByResult"]
                for entry in group["result"]
            )
        elif "result" in resp:
            return sum(entry["value"] for entry in resp["result"])
        return 0
    
    results = []

    for subtheme in themes:
        cfg = THEME_CONFIG[subtheme]
        key = cfg["key"]
        grouping_key = cfg.get("grouping_key")
        values = cfg.get("values", [])
        measure = cfg.get("measure", "count")

        if values:
            filter_expr = f"{key} in ({', '.join(f'\"{v}\"' for v in values)})"
        else:
            filter_expr = key

        log.info(f"[{country}] Processing {subtheme} with filter: {filter_expr}")

        # Execute API calls
        count_resp = call_ohsome(
            filter_expr, 
            grouping_key, 
            ",".join(values) if values else None, 
            "count"
        )
        total_count = sum_groupby(count_resp)

        if measure in ["area", "length"]:
            measure_resp = call_ohsome(filter_expr, grouping_key, ",".join(values) if values else None, measure)
            total_measure = round(sum_groupby(measure_resp), 2)
        else:
            measure_resp = count_resp
            total_measure = total_count

        measure_resp["total"] = total_count
        measure_resp[f"total_{measure}"] = total_measure

        out_file = out_dir / f"{country}_{subtheme}_tag_distribution.json.gz"
        with gzip.open(out_file, "wt", encoding="utf-8") as f:
            json.dump(measure_resp, f)

        results.append(str(out_file))

    return Output(
        {"files": results, "country": country, "theme": theme},
        metadata={
            "country": country,
            "theme": theme,
            "files_written": len(results),
        },
    )


@asset(
    deps=["build_outputs_asset"],
    partitions_def=multi_partitions,
)
def upload_s3_asset(context, build_outputs_asset):
    """Uploads outputs returned by build_outputs_asset to S3."""

    country, _ = context.partition_key.split("|")
    output_paths = build_outputs_asset  # dict returned by build_outputs_asset

    upload_to_s3(country=country, paths=output_paths, context=context.log)

    context.log.info(f"[{country}] Uploaded dataset(s) to S3 successfully.")


@asset(
    deps=["tag_distribution_asset"],
    partitions_def=second_multi_partitions,
)
def upload_stats_s3_asset(context, tag_distribution_asset):
    """
    Uploads OSM tag distribution (.json.gz) to S3 under
    oqapi_hdx/osm_stats/{COUNTRY}/
    """

    log = context.log
    country, theme = context.partition_key.split("|")

    file_paths = tag_distribution_asset["files"]
    if isinstance(file_paths, str):
        file_paths = [file_paths]  # wrap single file in a list

    for file_path in file_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        log.info(f"[{country}] Uploading OSM stats ({theme}) → S3: {file_path}")

        upload_stats_to_s3(
            country=country,
            file_path=file_path,
            log=log,
        )

        log.info(f"[{country}] OSM stats uploaded successfully: {file_path}")


@asset(
    deps=['upload_s3_asset'],
    partitions_def=country_partitions,
)
def upload_hdx_asset(context):
    country = context.partition_key.upper()

    hdx_config_path = "configs/hdx_config.yaml"
    countries_config = "configs/countries.yaml"

    context.log.info(f"[{country}] Uploading dataset to HDX")

    hdx_country_datasets, links = upload_to_hdx(country, hdx_config_path, countries_config, context)

    context.log.info(f"[{country}] Upload to HDX complete: {hdx_country_datasets}")
    return hdx_country_datasets, links


@asset(
    deps=["upload_hdx_asset",
          "upload_s3_asset",
          "ohsome_api_requests_asset",
          "build_outputs_asset"],
    partitions_def=country_partitions,
)
def verify_and_delete_asset(context):
    """
    Check that uploaded datasets are accessible on HDX.
    Only deletes dataset if all necessary topics are present and accessible.
    """

    country = context.partition_key.upper()

    # Get all files which are listed on RustFS of each country using s3fs
    endpoint = f"https://{_s3_config['endpoint']}"

    fs = s3fs.S3FileSystem(
        anon=True,
        client_kwargs={'endpoint_url': endpoint})
    path_for_s3 = f"{_s3_config['bucket']}/{_s3_config['dest_prefix']}/downloads/{country.upper()}"
    
    context.log.info(f"[{country}] Checking HDX files on {endpoint}/{path_for_s3}")
    files = fs.ls(path_for_s3)

    # necessary topics for cleanup
    must_have_topics = ["roads-all-highways",
                        "building-count",
                        "schools",
                        "hospitals",
                        "healthcare-primary"]

    local_folder = f"data/{country}"

    # give server some time to register all files
    time.sleep(5)

    failed_links = []
    existing_topics = []

    for file_path in files:
        fname = os.path.basename(file_path)
        try:
            # check if file exists without downloading it
            r = requests.head(f"{endpoint}/{file_path}", timeout=30)
            if r.status_code in [200, 301, 302]:
                context.log.info(f"[{country}] HDX file accessible: {fname}")
                # extract topic name from file name
                topic = fname.rsplit(".", 1)[0].split("_",)[1]
                if topic not in existing_topics:
                    existing_topics.append(str(topic))
            else:
                context.log.warning(f"[{country}] HDX file returned {r.status_code}: {fname}")
                context.log.info(f"[{country}] HDX URL: {file_path}")
                failed_links.append((fname, f"HTTP {r.status_code}"))
        except Exception as e:
            context.log.error(f"[{country}] Error accessing HDX file {fname}: {e}")
            failed_links.append((fname, str(e)))

    # make sure all necessary topics are present before cleanup
    if not all(topic in existing_topics for topic in must_have_topics):
        context.log.error(f"[{country}] Not all required topics are present on HDX. Existing topics: {existing_topics}"
                          f"Required topics: {must_have_topics}")
        raise Exception(f"HDX verification failed: Missing required topics. Found: {existing_topics}")
    else:
        context.log.info(f"[{country}] All required topics are present on HDX: {existing_topics}")

    # delete local files if all links work
    if not failed_links:
        context.log.info(f"All datasets for [{country}] are accessible on HDX.")
        if os.path.exists(local_folder):
            shutil.rmtree(local_folder)
            context.log.info(f"[{country.upper()}] All local files deleted.")
        else:
            context.log.warning(f"[{country.upper()}] Cleanup-Path {local_folder} not found.")
            raise FileNotFoundError(f"Cleanup failed: {local_folder} not found.")
    else:
        # if links failed, keep files and raise error
        context.log.error(f"[{country.upper()}] Cleanup failed!"
                          f"The following links are not accessible: {failed_links}")
        raise Exception(f"Cleanup failed for {failed_links}")
