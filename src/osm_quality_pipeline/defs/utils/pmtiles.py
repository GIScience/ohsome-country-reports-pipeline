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
        "-z", str(maxzoom),
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        # At low zoom, aggressive default simplification can carve spurious grey
        # notches into complex district borders that vanish again at high zoom.
        *layer_args,
        "-z12",
        "-B4",  # Base zoom: Keep all details visible from zoom 4 downwards
        # --- PREVENT DATA LOSS ---
        "--no-feature-limit",  # Never drop features because there are "too many"
        "--no-tile-size-limit",  # Allow tiles to be bigger than 500kb
        "--no-tiny-polygon-reduction",  # Don't delete small polygons
        "--no-line-simplification",
    ]

    try:
        subprocess.run(tippecanoe_cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"tippecanoe failed with exit code {e.returncode}: {e.stderr}")
        raise


def check_name_columns(gdf: gpd.GeoDataFrame, layer_name: str):
    """Warn if admin unit names are missing. tippecanoe drops null properties,
    so features without a value have no name in the tiles at all."""
    if "osm_id" in gdf.columns:
        name_cols = ["name", "name_en", "name_de"]
    elif layer_name.startswith("vg"):
        name_cols = ["name"]
    else:
        return

    for col in name_cols:
        if col not in gdf.columns:
            logger.warning(f"Layer '{layer_name}': name column '{col}' is missing")
            continue
        n_missing = gdf[col].isna().sum()
        if n_missing > 0:
            logger.warning(f"Layer '{layer_name}': {n_missing}/{len(gdf)} features have no '{col}'")


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
            if layer_name.startswith("vg"):
                # BKG: GEN in vg2500, GeografischerName_GEN in vg1000; rename to match the OSM `name` field
                gdf = gdf.rename(columns={"GEN": "name", "GeografischerName_GEN": "name"})
            check_name_columns(gdf, layer_name)

            geojson_path = os.path.join(tmp_dir, f"{layer_name}.geojson")
            gdf.to_file(geojson_path, driver="GeoJSON")
            clean_layers[layer_name] = geojson_path

        geojson_to_multilayer_pmtiles(layers=clean_layers, pmtiles_path=pmtiles_path)
