import dagster as dg

H3_ZOOM_LEVEL = None

REQUIRED_TOPICS = [
    "roads-all-highways",
    "building-count",
    "schools",
    "hospitals",
    "healthcare-primary",
]

ADM_LEVELS = ["ADM0", "ADM1"]


class ApiRequestConfig(dg.Config):
    max_workers: int = 5
    handle_500_as_na: bool = False
