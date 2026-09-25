import os
from pydantic_settings import BaseSettings
import dagster as dg
from hdx.api.configuration import Configuration
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


H3_ZOOM_LEVEL = None

REQUIRED_TOPICS = [
    "roads-all-highways",
    "buildings",
    "schools",
    "hospitals",
    "healthcare-primary",
    "land-cover",
]

ADM_LEVELS = ["ADM0", "ADM1"]


class S3Config(dg.Config):
    host: str = os.getenv("S3_HOST")
    key_id: str = os.getenv("S3_KEY_ID")
    secret: str = os.getenv("S3_SECRET")
    bucket: str = os.getenv("S3_BUCKET")
    prefix: str = "ohsome-quality-country-reports"


def get_hdx_config():
    hdx_config = Configuration.create(
        hdx_site=os.getenv("HDX_SITE"),  # either "prod" or "stage" in .env
        user_agent="HDXDataSeriesScript",
        hdx_key=os.getenv("HDX_KEY"),
        hdx_url="https://data.humdata.org/",
    )
    return hdx_config


class Config(BaseSettings):
    s3_config: S3Config = S3Config()


CONFIG = Config()

DATA_DIR = Path(os.getenv("DAGSTER_DATA_DIR"))
DATA_DIR.mkdir(parents=True, exist_ok=True)


OHSOME_QUALITY_API_URL = os.getenv("OHSOME_QUALITY_API_URL", "https://api.heigit.org/ohsome-quality-api-staging/v2")
HEIGIT_API_KEY = os.getenv("HEIGIT_API_KEY", "foo")
OHSOME_API_URL = os.getenv("OHSOME_API_URL", "https://api.heigit.org/ohsome-api-staging/v2")

# `or` fallback: empty values in .env (e.g. `OHSOME_API_TIMEOUT=`) use the default
OHSOME_QUALITY_API_TIMEOUT = int(os.getenv("OHSOME_QUALITY_API_TIMEOUT") or "120")
OHSOME_API_TIMEOUT = int(os.getenv("OHSOME_API_TIMEOUT") or "180")


def _int_or_none(value):
    return int(value) if value else None


# optional user-defined request caps, empty = unlimited
OHSOME_QUALITY_API_MAX_PER_MINUTE = _int_or_none(os.getenv("OHSOME_QUALITY_API_MAX_PER_MINUTE"))
OHSOME_QUALITY_API_MAX_PER_DAY = _int_or_none(os.getenv("OHSOME_QUALITY_API_MAX_PER_DAY"))
OHSOME_API_MAX_PER_MINUTE = _int_or_none(os.getenv("OHSOME_API_MAX_PER_MINUTE"))
OHSOME_API_MAX_PER_DAY = _int_or_none(os.getenv("OHSOME_API_MAX_PER_DAY"))

TOPICS_BY_INDICATOR = {
    "currentness": ["buildings", "roads", "land-cover", "schools", "hospitals"],
    "mapping-saturation": [
        "buildings",
        "roads",
        "land-cover",
        "schools",
        "hospitals",
    ],
    "user-activity": ["buildings", "roads", "land-cover", "schools", "hospitals"],
    "attribute-completeness": ["buildings", "roads", "schools", "hospitals"],
    "tag-distribution": ["buildings", "roads", "schools", "hospitals", "land-cover"],
}

TOPIC_ATTRIBUTES = {
    "buildings": ["height", "building-material"],
    "roads": ["name", "maxspeed", "surface"],
    "schools": ["name", "phone-number", "website"],
    "hospitals": ["emergency", "name", "opening-hours", "speciality"],
}

STATIC_TOPIC_ASSETS = {
    "buildings": ["building_comparison"],
    "land-cover": ["land_cover_completeness", "land_cover_thematic_accuracy"],
    "roads": ["roads_thematic_accuracy"],
}

ALL_TOPICS = sorted(
    set(topic for topics in TOPICS_BY_INDICATOR.values() for topic in topics)
)

API_TOPIC_NAMES = {
    "buildings": "Buildings",
    "hospitals": "Hospitals",
    "land-cover": "Land Use and Land Cover",
    "roads": "Roads (cars)",
    "schools": "Schools",
}
