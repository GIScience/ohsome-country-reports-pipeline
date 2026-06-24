import io
import dagster as dg
import requests
import sys
import zipfile
import geopandas as gpd
from osm_quality_pipeline.defs.constants import DATA_DIR


@dg.asset
def geoboundary_bkg() -> dg.Output[str]:
    list_url = "https://daten.gdz.bkg.bund.de/produkte/vg/vg25_ebenen/aktuell/vg25.utm32s.gpkg.zip"
    try:
        bkg_boundaries = download_bkg_boundaries(list_url=list_url, level_val="vg25_sta")
    except SystemExit as e:
        print(f"BKG boundaries download failed: {e}")
        raise Exception(f"Failed to fetch boundaries for Germany from BKG.")
    return dg.Output(str(bkg_boundaries))


def download_bkg_boundaries(list_url, level_val):   #layer: vg25_sta, vg25_lan, vg25_gem
    germany_dir = DATA_DIR / "germany"
    germany_dir.mkdir(parents=True, exist_ok=True)
    gpkg_path = germany_dir / "DE_VG25.gpkg"
    out_path = germany_dir / f"{level_val}.geojson"

    try:
        resp = requests.get(list_url)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        zip_path = "daten/DE_VG25.gpkg"
        with z.open(zip_path) as source:
            gpkg_path.write_bytes(source.read())

    gdf = (
        gpd.read_file(gpkg_path, layer=level_val)
        .to_crs(4326)
    )

    gdf = gdf[gdf.geometry.notnull() & gdf.is_valid]
    gdf.to_file(out_path, driver="GeoJSON")

    gpkg_path.unlink()

    return(out_path)
