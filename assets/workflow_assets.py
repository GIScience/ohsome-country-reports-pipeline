import os, glob, tempfile, yaml, asyncio, aiohttp, json, time, shutil, gzip, s3fs, re
import numpy as np
import pandas as pd
import geopandas as gpd
import geojson
import requests
import h3
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import mapping, shape, box
from filelock import FileLock
from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition, asset, Output, AssetIn
from scripts.helper_functions import download_from_geoboundaries, download_from_github, upload_to_s3, upload_to_hdx, simplify_geometries, geojson_to_multilayer_pmtiles, upload_stats_to_s3, get_dynamic_resolutions 
from scripts.smart_request_functions import build_api_request_jobs, default_target_dir, generate_curl_command, manage_failed_request_file, ensure_valid_geometry
from typing import Optional, Dict


def strip_html_tags(text):
    if not text or not isinstance(text, str):
        return text
    return re.sub('<.*?>', '', text)

os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"

def _load_yaml(name):
    with open(os.path.join(os.getcwd(), "configs", name)) as f:
        return yaml.safe_load(f)

MATRIX_YAML_PATH = os.path.join(os.getcwd(), "configs", "matrix.yaml")

_asset_config = _load_yaml("assets_config.yaml")
_mapping = _load_yaml("countries.yaml")
ALL_COUNTRIES = list(_mapping.keys())
country_partitions = StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)

_matrix = _load_yaml("matrix.yaml")
ALL_TOPICS = list(_matrix.get("topics", {}).keys())
topics_partitions = StaticPartitionsDefinition(partition_keys=ALL_TOPICS)

_s3_config = _load_yaml("s3_config.yaml")
must_have_topics = _asset_config.get("required_topics", [])

_theme_config = _load_yaml("theme_config.yaml")
OSMHISTORY_BASE = _theme_config.get("osmhistory_base", {})
THEME_CONFIG = _theme_config.get("themes", {})
THEME_EXPANSION = _theme_config.get("theme_expansion", {})
key_partitions = StaticPartitionsDefinition(partition_keys=_theme_config.get("partition_keys", []))

multi_partitions = MultiPartitionsDefinition({
    "country": country_partitions,
    "topic": topics_partitions,
})
second_multi_partitions = MultiPartitionsDefinition({
    "country": country_partitions,
    "key": key_partitions,
})

oqapi_version = _asset_config.get("oqapi-version", "v1")

@asset(
    partitions_def=country_partitions,
)
def boundary_asset(context) -> Output[list[str]]:
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    api_choice = _asset_config.get("boundary_asset", {}).get("api", [])
    context.log.info(f"[{country}] fetching ADM boundaries using API source: {api_choice}")

    if api_choice == "geoboundaries":
        list_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL"
        try:
            download_from_geoboundaries(list_url=list_url, country=country, level_val="boundaryType", url_val="gjDownloadURL", out_dir=out_dir)
        except SystemExit as e:
            context.log.warning(f"[{country}] geoBoundaries download failed: {e}")
            raise Exception(f"Failed to fetch boundaries for {country} from geoBoundaries.")
    elif api_choice == "github":
        list_url = f"https://api.github.com/repos/wmgeolab/geoBoundaries/contents/releaseData/gbOpen/{country}?ref=main"
        try:
            download_from_github(list_url=list_url, country=country, level_val="name", url_val="git_url", out_dir=out_dir)
        except SystemExit as e:
            context.log.warning(f"[{country}] GitHub download failed: {e}")
            raise Exception(f"Failed to fetch boundaries for {country} from GitHub.")
    else:
        raise ValueError(f"Invalid API choice '{api_choice}' in assets_config.yaml. Valid options: 'geoboundaries' or 'github'.")

    pattern = os.path.join(out_dir, "boundary_ADM*.geojson")
    paths = sorted(glob.glob(pattern))

    if not paths:
        raise FileNotFoundError(f"[{country}] No boundary files found at {pattern}.")

    context.log.info(f"[{country}] Downloaded {len(paths)} boundary files via {api_choice}")

    updated_paths = []
    for p in paths:
        simplify_geometries(p)
        gdf = gpd.read_file(p)
        if "id" not in gdf.columns:
            adm_level = os.path.basename(p).split("_")[1].replace("ADM", "").replace(".geojson", "")
            gdf = gdf.reset_index(drop=True)
            gdf["id"] = [f"{country}_adm{adm_level}_{str(i+1).zfill(2)}" for i in range(len(gdf))]
            gdf.to_file(p, driver="GeoJSON")
        updated_paths.append(p)

    return Output(updated_paths, metadata={"paths": updated_paths, "count": len(updated_paths), "source": api_choice})


@asset(ins={"boundary_asset": AssetIn()}, partitions_def=country_partitions)
def h3_hexgrid_asset(context, boundary_asset: list[str]) -> Output[str]:
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    try:
        boundary_path = next(p for p in boundary_asset if "ADM0" in os.path.basename(p))
    except StopIteration:
        raise FileNotFoundError(f"[{country}] No ADM0 boundary file found.")

    gdf = gpd.read_file(boundary_path).to_crs(4326)
    grid_config = _asset_config.get("grids", {})
    params = get_dynamic_resolutions(gdf, grid_config)
    zoom_level = params["h3"]

    minx, miny, maxx, maxy = gdf.total_bounds
    buf = 0.05
    bbox_geom = box(minx - buf, miny - buf, maxx + buf, maxy + buf)

    cell_series = pd.Series(h3.geo_to_cells(bbox_geom, res=zoom_level))
    grid_gdf = gpd.GeoDataFrame(geometry=cell_series.apply(lambda c: shape(h3.cells_to_geo([c]))), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(gdf[["shapeName", "shapeISO", "geometry"]], geometry="geometry", crs="EPSG:4326")
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection").reset_index(drop=True)

    grid_clipped["h3_id"] = f"{country}_hex{zoom_level}_" + (grid_clipped.index + 1).astype(str)
    grid_clipped = grid_clipped[["h3_id", "shapeName", "shapeISO", "geometry"]]

    output_path = os.path.join(out_dir, f"{country}_h3_z{zoom_level}.gpkg")
    grid_clipped = grid_clipped.rename(columns={"h3_id": "id", "shapeName": "ADM0_name", "shapeISO": "ADM0_iso", "shapeID": "ADM0_id"})
    grid_clipped.to_file(output_path, driver="GPKG")

    return Output(output_path, metadata={"country": country, "zoom_level": zoom_level, "cell_count": len(grid_clipped), "output_path": output_path})


@asset(ins={"boundary_asset": AssetIn()}, partitions_def=country_partitions)
def square_grid_asset(context, boundary_asset: list[str]) -> Output[str]:
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    try:
        boundary_path = next(p for p in boundary_asset if "ADM0" in os.path.basename(p))
    except StopIteration:
        raise FileNotFoundError(f"[{country}] No ADM0 boundary found.")

    gdf = gpd.read_file(boundary_path).to_crs(4326)
    params = get_dynamic_resolutions(gdf, _asset_config.get("grids", {}))
    res_deg = params["square"]

    minx, miny, maxx, maxy = gdf.total_bounds
    x0, y0 = np.floor(minx / res_deg) * res_deg, np.floor(miny / res_deg) * res_deg
    xs, ys = np.arange(x0, maxx + res_deg, res_deg), np.arange(y0, maxy + res_deg, res_deg)

    grid_gdf = gpd.GeoDataFrame(geometry=[box(x, y, x + res_deg, y + res_deg) for x in xs for y in ys], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(gdf[["shapeName", "shapeISO", "geometry"]], geometry="geometry", crs="EPSG:4326")
    grid_clipped = gpd.overlay(grid_gdf, gdf, how="intersection").reset_index(drop=True)

    grid_clipped["cell_id"] = f"{country}_sqr{res_deg}_" + (grid_clipped.index + 1).astype(str)
    grid_clipped = enrich_grid_with_admin_levels(grid_clipped, boundary_asset, country, context)

    output_path = os.path.join(out_dir, f"{country}_grid_{res_deg}deg.gpkg")
    grid_clipped = grid_clipped.rename(columns={"cell_id": "id", "shapeName": "ADM0_name", "shapeISO": "ADM0_iso", "shapeID": "ADM0_id"})
    grid_clipped.to_file(output_path, driver="GPKG")

    return Output(output_path, metadata={"country": country, "resolution_deg": res_deg, "cell_count": len(grid_clipped), "output_path": output_path})

def enrich_grid_with_admin_levels(grid_gdf, boundary_asset, country, context):
    for adm_path in boundary_asset:
        adm_level = os.path.splitext(os.path.basename(adm_path))[0].split("_")[-1]
        if adm_level == "ADM0":
            continue

        adm_gdf = gpd.read_file(adm_path).to_crs(4326)[["shapeName", "shapeISO", "shapeID", "geometry"]]
        adm_gdf.columns = ["name", "iso", "id", "geometry"]

        joined = gpd.overlay(grid_gdf, adm_gdf, how="intersection", keep_geom_type=False)
        joined["intersect_area"] = joined.geometry.area
        joined_sorted = joined.sort_values("intersect_area", ascending=False).drop_duplicates(subset="cell_id")

        grid_gdf = grid_gdf.merge(joined_sorted[["cell_id", "name", "iso", "id"]], on="cell_id", how="left", suffixes=("", f"_{adm_level}"))
        grid_gdf = grid_gdf.rename(columns={"name": f"{adm_level}_name", "iso": f"{adm_level}_iso", "id": f"{adm_level}_id"})

    return grid_gdf


@asset
def indicators_per_topic_asset(context) -> Output[dict]:
    yaml_file = "configs/matrix.yaml"
    with open(yaml_file) as file:
        matrix_data = yaml.safe_load(file)
    if "topics" not in matrix_data:
        matrix_data["topics"] = {}

    base_url = f"https://api.quality.ohsome.org/{oqapi_version}"
    headers = {"accept": "application/json"}
    runtime_info = {}

    for topic in matrix_data["topics"]:
        url = f"{base_url}/metadata/topics/{topic}"
        for attempt in range(4):
            try:
                start = time.time()
                r = requests.get(url, headers=headers, timeout=120)
                r.raise_for_status()
                result = r.json()
                indicators = result["result"][topic]["indicators"]
                matrix_data["topics"][topic]["indicators"] = indicators
                runtime_info[topic] = time.time() - start
                break
            except requests.RequestException as e:
                if attempt < 3:
                    time.sleep(2)
                else:
                    context.log.warning(f"Max retries reached for {topic}: {e}")

    try:
        r = requests.get(f"{base_url}/metadata/attributes", headers=headers, timeout=120)
        r.raise_for_status()
        attr_data = r.json().get("result", {})
    except requests.RequestException as e:
        context.log.warning(f"Failed to fetch attributes metadata: {e}")
        attr_data = {}

    for topic, data in matrix_data["topics"].items():
        inds = data.get("indicators", [])
        if "attribute-completeness" in inds and topic in attr_data:
            matrix_data["topics"][topic]["attributes"] = list(attr_data[topic].keys())

    with open(yaml_file, "w") as file:
        yaml.dump(matrix_data, file, sort_keys=False)

    data = _load_yaml("matrix.yaml")
    topics = data["topics"]
    indicator_counts = {}
    for td in topics.values():
        for ind in td.get("indicators", []):
            indicator_counts[ind] = indicator_counts.get(ind, 0) + 1

    all_indicators = sorted(indicator_counts, key=indicator_counts.get, reverse=True)
    rows = [{"topic": tn, **{ind: "X" if ind in td.get("indicators", []) else "-" for ind in all_indicators}} for tn, td in topics.items()]

    df = pd.DataFrame(rows)
    _, ax = plt.subplots(figsize=(12, 4))
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(6)
    for (row, _), cell in table.get_celld().items():
        txt = cell.get_text()
        txt.set_horizontalalignment('center')
        txt.set_weight("bold")
        if txt.get_text() == "X":
            txt.set_color("green")
        elif txt.get_text() == "-":
            txt.set_color("red")
    table.scale(1.4, 2)
    plt.savefig("data/indicator_matrix.png", bbox_inches='tight', dpi=200)
    plt.close()

    return Output({
        "yaml_file": yaml_file,
        "png_file": "data/indicator_matrix.png",
        "metadata": {
            "topics_processed": len(matrix_data["topics"]),
            "has_attributes": sum(1 for v in matrix_data["topics"].values() if "attributes" in v and v["attributes"]),
            "runtime_sec_total": sum(runtime_info.values()),
        },
    })

@asset(partitions_def=multi_partitions)
async def ohsome_api_requests_asset(
    context,
    square_grid_asset: Optional[str] = None,
    h3_hexgrid_asset: Optional[str] = None,
) -> Output[dict]:
    log = context.log
    country, topic = context.partition_key.split("|")

    unit = _asset_config.get("grids", {}).get("unit", "h3")

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

    def to_float(val):
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _populate_adm_results(adm_df, raw_folder, topic, indicators, relevant):
        for idx, row in adm_df.iterrows():
            geom_id = row["id"]
            for ind in indicators:
                if ind == "attribute-completeness":
                    for attr in relevant:
                        p = raw_folder / f"{topic}__{ind}__{attr}__{geom_id}.json"
                        if p.exists():
                            j = json.load(open(p))
                            adm_df.at[idx, f"result_value_{ind}_{attr}"] = to_float(j["result"][0]["result"].get("value"))
                            adm_df.at[idx, f"description_{ind}_{attr}"] = strip_html_tags(j["result"][0]["result"].get("description"))
                else:
                    p = raw_folder / f"{topic}__{ind}__{geom_id}.json"
                    if p.exists():
                        j = json.load(open(p))
                        adm_df.at[idx, f"result_value_{ind}"] = to_float(j["result"][0]["result"].get("value"))
                        adm_df.at[idx, f"description_{ind}"] = strip_html_tags(j["result"][0]["result"].get("description"))
        return adm_df

    unit = _asset_config.get("grids", {}).get("unit", "h3")
    if unit == "h3":
        grid_path = h3_hexgrid_asset
        grid_raw = Path(ohsome_api_requests_asset["hex_raw"])
    else:
        grid_path = square_grid_asset
        grid_raw = Path(ohsome_api_requests_asset["sqr_raw"])

    adm0_raw = Path(ohsome_api_requests_asset["ADM0_raw"])
    adm1_raw = Path(ohsome_api_requests_asset["ADM1_raw"])

    if not os.path.exists(grid_path):
        raise FileNotFoundError(f"Grid file not found: {grid_path}")

    gdf = gpd.read_file(grid_path)
    log.info(f"[{country}] Loaded grid with {len(gdf)} cells")

    with open(MATRIX_YAML_PATH) as f:
        matrix = yaml.safe_load(f)
    indicators = matrix["topics"][topic]["indicators"]
    attributes = matrix["topics"][topic].get("attributes", [])
    relevant = [a for a in matrix["relevant_attributes"] if a in attributes]

    start = time.time()
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
                    res = j.get("result", [{}])[0].get("result", {})
                    val = res.get("value") if res else j.get("value")
                    gdf.at[idx, col] = to_float(val)

    out_dir = Path("data") / country / "Output"
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = "h3" if unit == "h3" else "square"
    gpkg = out_dir / f"{country}_{topic}_{suffix}.gpkg"
    csv = out_dir / f"{country}_{topic}_{suffix}.csv"
    gdf.to_file(gpkg)
    gdf.drop(columns="geometry").to_csv(csv, index=False)

    adm0_boundary = gpd.read_file(f"data/{country}/boundary_ADM0.geojson").dissolve()
    adm0_df = gpd.GeoDataFrame({"geometry": adm0_boundary.geometry, "id": adm0_boundary.get("id", ["MLI_adm0"]*len(adm0_boundary))})
    adm0_df = _populate_adm_results(adm0_df, adm0_raw, topic, indicators, relevant)

    if os.path.exists(f"data/{country}/boundary_ADM1.geojson"):
        adm1_boundary = gpd.read_file(f"data/{country}/boundary_ADM1.geojson")
        adm1_df = gpd.GeoDataFrame({"geometry": adm1_boundary.geometry, "id": adm1_boundary.get("id", [])})
        adm1_df = _populate_adm_results(adm1_df, adm1_raw, topic, indicators, relevant)
    else:
        adm1_df = gpd.GeoDataFrame(columns=["geometry", "id"])

    long_rows = []
    for idx, row in gdf.iterrows():
        for ind in indicators:
            if ind == "attribute-completeness":
                for attr in relevant:
                    long_rows.append({"geomID": row["id"], "topic": topic, "indicator": f"{ind}_{attr}", "value": row[f"result_value_{ind}_{attr}"]})
            else:
                long_rows.append({"geomID": row["id"], "topic": topic, "indicator": ind, "value": row[f"result_value_{ind}"]})

    for folder in [adm0_raw, adm1_raw]:
        for file_path in folder.glob(f"{topic}__*.json"):
            fname = file_path.stem
            parts = fname.split("__")
            geom_id = parts[-1]
            indicator_name = f"{parts[1]}_{parts[2]}" if "attribute-completeness" in parts[1] else parts[1]
            try:
                with open(file_path) as f:
                    j = json.load(f)
                res = j.get("result", [{}])[0].get("result", {})
                long_rows.append({
                    "geomID": geom_id, "topic": topic, "indicator": indicator_name,
                    "value": to_float(res.get("value") if res else j.get("value")),
                    "description": strip_html_tags(res.get("description") if res else j.get("error")),
                })
            except Exception:
                continue

    parquet_path = out_dir / f"{country}_long.parquet"
    new_df = pd.DataFrame(long_rows)
    with FileLock(parquet_path.with_suffix(".lock")):
        if parquet_path.exists():
            combined = pd.concat([pd.read_parquet(parquet_path), new_df], ignore_index=True).drop_duplicates(subset=["geomID", "topic", "indicator"], keep="last")
            combined.to_parquet(parquet_path, index=False)
        else:
            new_df.to_parquet(parquet_path, index=False)

    figures_dir = out_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    figures = {unit: {}, "adm0": {}, "adm1": {}}
    figures[unit][topic] = {}

    for ind in indicators:
        keys = [f"{ind}_{a}" for a in relevant] if ind == "attribute-completeness" else [ind]
        for k in keys:
            figures[unit][topic][k] = []
            for idx, row in gdf.iterrows():
                geom_id = row["id"]
                filename = f"{topic}__{ind}__{k.split('_')[-1]}__{geom_id}.json" if ind == "attribute-completeness" else f"{topic}__{ind}__{geom_id}.json"
                p = grid_raw / filename
                if p.exists():
                    try:
                        j = json.load(open(p))
                        fig = j.get("result", [{}])[0].get("result", {}).get("figure") if j.get("result", [{}])[0].get("result", {}) else None
                        if fig:
                            figures[unit][topic][k].append({"geom_id": geom_id, "figure": fig})
                    except Exception:
                        continue

    for folder, g_type in [(adm0_raw, "adm0"), (adm1_raw, "adm1")]:
        for file_path in folder.glob(f"{topic}__*.json"):
            parts = file_path.stem.split("__")
            ind_name = f"{parts[1]}_{parts[2]}" if "attribute-completeness" in parts[1] else parts[1]
            figures.setdefault(g_type, {}).setdefault(topic, {}).setdefault(ind_name, [])
            try:
                fig = json.load(open(file_path))["result"][0]["result"].get("figure")
                if fig:
                    figures[g_type][topic][ind_name].append({"geom_id": parts[-1], "figure": fig})
            except Exception:
                continue

    for g_type, g_topics in figures.items():
        for t_name, t_inds in g_topics.items():
            for i_name, fig_list in t_inds.items():
                if fig_list:
                    with gzip.open(figures_dir / f"{g_type}__{t_name}__{i_name}.json.gz", "wt", encoding="utf-8") as f:
                        json.dump(fig_list, f)

    pmtiles_path = out_dir / f"{country}_boundaries.pmtiles"
    if not pmtiles_path.exists():
        layers = {"ADM0": f"data/{country}/boundary_ADM0.geojson", "ADM1": f"data/{country}/boundary_ADM1.geojson", "square_grid": square_grid_asset, "h3_hexgrid": h3_hexgrid_asset}
        clean_layers = {}
        tmp_dir = Path(tempfile.mkdtemp(prefix="pmtiles_layers_"))
        for name, path in layers.items():
            if path and os.path.exists(path):
                g = gpd.read_file(path)
                if len(g) > 0:
                    g = g.to_crs(4326) if (g.crs and g.crs.to_epsg() != 4326) else g
                    g["geometry"] = g.geometry.buffer(0)
                    g.to_file(tmp_dir / f"{name}.geojson", driver="GeoJSON")
                    clean_layers[name] = str(tmp_dir / f"{name}.geojson")
        geojson_to_multilayer_pmtiles(layers=clean_layers, pmtiles_path=str(pmtiles_path))

    adm0_gpkg, adm0_csv = out_dir / f"{country}_{topic}_ADM0.gpkg", out_dir / f"{country}_{topic}_ADM0.csv"
    adm0_df.to_file(adm0_gpkg)
    adm0_df.drop(columns="geometry").to_csv(adm0_csv, index=False)

    adm1_gpkg, adm1_csv = out_dir / f"{country}_{topic}_ADM1.gpkg", out_dir / f"{country}_{topic}_ADM1.csv"
    if len(adm1_df) > 0:
        adm1_df.to_file(adm1_gpkg)
        adm1_df.drop(columns="geometry").to_csv(adm1_csv, index=False)

    return Output({
        "grid_gpkg_path": str(gpkg), "grid_csv_path": str(csv),
        "adm0_gpkg_path": str(adm0_gpkg), "adm0_csv_path": str(adm0_csv),
        "adm1_gpkg_path": str(adm1_gpkg), "adm1_csv_path": str(adm1_csv),
        "parquet_path": str(parquet_path), "pmtiles_path": str(pmtiles_path),
        "figures_gzip_path": str(figures_dir),
    })


@asset(
    deps=["boundary_asset"],
    partitions_def=second_multi_partitions,
)
def tag_distribution_asset(context) -> Output[dict]:
    log = context.log
    country, theme = context.partition_key.split("|")

    themes = THEME_EXPANSION.get(theme, [theme])

    boundary_path = f"data/{country}/boundary_ADM0.geojson"
    gdf = gpd.read_file(boundary_path).dissolve()
    simplified_gdf = gdf.simplify(tolerance=0.005, preserve_topology=True)
    geom_json = mapping(simplified_gdf.iloc[0])

    out_dir = Path("data") / country / "Output"
    out_dir.mkdir(parents=True, exist_ok=True)

    def call_ohsome(filter_expr, grouping_key, grouping_values, measure):
        params = {"bpolys": json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": geom_json}]}), "filter": filter_expr, "time": "2026-01-01"}
        url = OSMHISTORY_BASE[measure]
        if grouping_key:
            url = f"{url.rstrip('/')}/groupBy/tag"
            params["groupByKey"] = grouping_key
            if grouping_values:
                params["groupByValues"] = grouping_values
        try:
            r = requests.post(url, data=params, timeout=600)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 413:
                log.error(f"Payload still too large for {country}.")
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

    return Output({"files": results, "country": country, "theme": theme}, metadata={"country": country, "theme": theme, "files_written": len(results)})


@asset(deps=["build_outputs_asset"], partitions_def=multi_partitions)
def upload_s3_asset(context, build_outputs_asset):
    country, _ = context.partition_key.split("|")
    upload_to_s3(country=country, paths=build_outputs_asset, context=context.log)


@asset(deps=["tag_distribution_asset"], partitions_def=second_multi_partitions)
def upload_stats_s3_asset(context, tag_distribution_asset):
    log = context.log
    country, theme = context.partition_key.split("|")
    file_paths = tag_distribution_asset["files"]
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    for fp in file_paths:
        if not os.path.exists(fp):
            raise FileNotFoundError(fp)
        upload_stats_to_s3(country=country, file_path=fp, log=log)


@asset(deps=['upload_s3_asset'], partitions_def=country_partitions)
def upload_hdx_asset(context):
    country = context.partition_key.upper()
    hdx_country_datasets, links = upload_to_hdx(country, "configs/hdx_config.yaml", "configs/countries.yaml", context)
    return hdx_country_datasets, links


@asset(deps=["upload_hdx_asset", "upload_s3_asset", "ohsome_api_requests_asset", "build_outputs_asset"], partitions_def=country_partitions)
def verify_and_delete_asset(context):
    country = context.partition_key.upper()
    endpoint = f"https://{_s3_config['endpoint']}"
    fs = s3fs.S3FileSystem(anon=True, client_kwargs={'endpoint_url': endpoint})
    path_for_s3 = f"{_s3_config['bucket']}/{_s3_config['dest_prefix']}/downloads/{country.upper()}"
    files = fs.ls(path_for_s3)
    local_folder = f"data/{country}"
    time.sleep(5)

    failed_links, existing_topics = [], []
    for file_path in files:
        fname = os.path.basename(file_path)
        try:
            r = requests.head(f"{endpoint}/{file_path}", timeout=30)
            if r.status_code in [200, 301, 302]:
                topic = fname.rsplit(".", 1)[0].split("_")[1]
                if topic not in existing_topics:
                    existing_topics.append(topic)
            else:
                failed_links.append((fname, f"HTTP {r.status_code}"))
        except Exception as e:
            failed_links.append((fname, str(e)))

    if not all(t in existing_topics for t in must_have_topics):
        raise Exception(f"HDX verification failed: Missing required topics. Found: {existing_topics}")

    if not failed_links:
        if os.path.exists(local_folder):
            shutil.rmtree(local_folder)
        else:
            raise FileNotFoundError(f"Cleanup failed: {local_folder} not found.")
    else:
        raise Exception(f"Cleanup failed for {failed_links}")
