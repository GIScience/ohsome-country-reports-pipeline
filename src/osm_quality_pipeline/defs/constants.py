import os

import dagster as dg
from pathlib import Path

H3_ZOOM_LEVEL = None

REQUIRED_TOPICS = [
    "roads-all-highways",
    "building-count",
    "schools",
    "hospitals",
    "healthcare-primary",
    "land-cover",
]

ADM_LEVELS = ["ADM0", "ADM1"]

BKG_BOUNDARY_URL = "https://daten.gdz.bkg.bund.de/produkte/vg/vg25_ebenen/aktuell/vg25.utm32s.gpkg.zip"


class S3Config(dg.Config):
    host: str = os.getenv("S3_HOST")
    key_id: str = os.getenv("S3_KEY_ID")
    secret: str = os.getenv("S3_SECRET")
    bucket: str = os.getenv("S3_BUCKET")


class BoundaryConfig(dg.Config):
    bkg_boundary_url: str = f"https://storage.heigit.org/heigit-hdx-public/oqapi_hdx/boundaries"
    bkg_boundary_levels: [str] = ["vg2500_sta", "vg2500_lan", "vg25_gem"]
    geoboundaries_levels: [str] = ["ADM0", "ADM1"]


class ApiRequestConfig(dg.Config):
    max_workers: int = 5
    handle_500_as_na: bool = False


REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPO_ROOT / "data"