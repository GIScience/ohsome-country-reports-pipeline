import os
import io
import dagster as dg
from utils import download_bkg_boundaries
import requests
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