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


class ApiRequestConfig(dg.Config):
    max_workers: int = 5
    handle_500_as_na: bool = False

REPO_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = REPO_ROOT / "data"