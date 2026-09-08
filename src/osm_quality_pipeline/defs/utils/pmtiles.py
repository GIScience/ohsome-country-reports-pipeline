import os
import subprocess
import tempfile

import dagster as dg
import geopandas as gpd

logger = dg.get_dagster_logger()


def geojson_to_multilayer_pmtiles(layers: dict, pmtiles_path: str, minzoom: int = 1, maxzoom: int = 14):
    """Combine one or more GeoJSON files into a single multi-layer PMTiles archive via tippecanoe."""
    layer_args = []

    for layer_name, geojson_path in layers.items():
        try:
            gdf = gpd.read_file(geojson_path)
        except Exception as e:
            logger.warning(f"Skipping layer '{layer_name}' (cannot read GeoJSON): {e}")
            continue

        if len(gdf) == 0:
            logger.warning(f"Skipping layer '{layer_name}' (0 features)")
            continue

        layer_args.extend(["-L", f"{layer_name}:{geojson_path}"])

    if not layer_args:
        logger.warning("No valid layers found for PMTiles generation")
        return

    if os.path.exists(pmtiles_path):
        os.remove(pmtiles_path)

    tippecanoe_cmd = [
        "tippecanoe",
        "-o", pmtiles_path,
        "--force",
        "-Z", str(minzoom),
        "-z", str(maxzoom),
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        *layer_args,
    ]

    try:
        subprocess.run(tippecanoe_cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"tippecanoe failed with exit code {e.returncode}: {e.stderr}")
        raise


def write_country_boundaries_pmtiles(layer_gpkg_paths: dict, pmtiles_path: str):
    """layer_gpkg_paths: {layer_name: path_to_gpkg}. Reprojects to EPSG:4326 and fixes
    invalid geometries before handing each layer to tippecanoe as a temp GeoJSON."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        clean_layers = {}
        for layer_name, gpkg_path in layer_gpkg_paths.items():
            gdf = gpd.read_file(gpkg_path)
            if len(gdf) == 0:
                continue

            if gdf.crs and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(4326)
            gdf["geometry"] = gdf.geometry.buffer(0)

            geojson_path = os.path.join(tmp_dir, f"{layer_name}.geojson")
            gdf.to_file(geojson_path, driver="GeoJSON")
            clean_layers[layer_name] = geojson_path

        geojson_to_multilayer_pmtiles(layers=clean_layers, pmtiles_path=pmtiles_path)
