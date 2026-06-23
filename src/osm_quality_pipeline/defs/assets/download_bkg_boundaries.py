import os
import io
import dagster as dg
import requests
import sys
import zipfile
import geopandas as gpd


@dg.asset
def geoboundary_bkg() -> dg.Output[str]:
    out_dir = os.path.join("data/germany")
    os.makedirs(out_dir, exist_ok=True)

    list_url = "https://daten.gdz.bkg.bund.de/produkte/vg/vg25_ebenen/aktuell/vg25.utm32s.gpkg.zip"
    try:
        download_bkg_boundaries(list_url=list_url, level_val="vg25_sta", out_dir=out_dir)
    except SystemExit as e:
        print(f"BKG boundaries download failed: {e}")
        raise Exception(f"Failed to fetch boundaries for Germany from BKG.")
    return dg.Output(out_dir)

geoboundary_bkg()


def download_bkg_boundaries(list_url, level_val, out_dir):   #layer: vg25_sta, vg25_lan, vg25_gem
    try:
        resp = requests.get(list_url)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        zip_path = "daten/DE_VG25.gpkg"

    gpkg_path = os.path.join(out_dir, "DE_VG25.gpkg")

    with z.open(zip_path) as source:
        with open(gpkg_path, "wb") as target:
            target.write(source.read())


    out_path = os.path.join(out_dir, f"{level_val}.geojson")

    gdf = (
        gpd.read_file(gpkg_path, layer=level_val)
        .to_crs(4326)
    )

    gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]
    gdf.to_file(out_path, driver="GeoJSON")