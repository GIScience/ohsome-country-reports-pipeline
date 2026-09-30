import dagster as dg
from osm_quality_pipeline.defs.constants import HDX_PRIVATE, get_hdx_config
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from datetime import datetime, timezone
import re

logger = dg.get_dagster_logger()


def upload_to_hdx(country_code, links, context):
    """links: list of (file_name, url) tuples."""
    country_name = Country.get_country_name_from_iso3(country_code)
    return create_country_dataset(country_code, country_name, links, context)


def create_country_dataset(country_code: str, country_name: str, links, context):
    config = get_hdx_config()
    dataset_name = f"{country_name} OSM Data quality"
    title = f"{country_name} - OSM Data quality"

    dataset = Dataset()
    # HDX names allow only lowercase alphanumerics, "-" and "_"
    dataset["name"] = re.sub(r"[^a-z0-9_-]+", "-", dataset_name.lower())
    dataset["title"] = title
    dataset["owner_org"] = "heidelberg-institute-for-geoinformation-technology"
    dataset["groups"] = [{"name": "heidelberg-institute-for-geoinformation-technology"}]
    dataset["private"] = HDX_PRIVATE
    dataset.set_expected_update_frequency("Every six months")
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "HeiGIT"
    dataset["maintainer"] = "valentin-boehmer-8808"
    dataset["maintainer_email"] = "valentin.boehmer@heigit.org"
    dataset["methodology"] = "Quality analysis of OSM data using the ohsome quality API."
    dataset.set_custom_viz(
        f"https://giscience.github.io/osm-quality-country-reports/#/{country_code}/roads"
    )
    if country_code == "DEU":
        units = (
            "- **vg2500_sta**: country\n"
            "- **vg2500_lan**: federal states\n"
            "- **vg1000_krs**: districts\n\n"
            "The boundaries are taken from the [German Federal Agency for Cartography and Geodesy (BKG)](https://gdz.bkg.bund.de/)."
        )
    else:
        units = (
            "- **adm0**: country\n"
            "- **adm1**: first subnational administrative level\n"
            "- **h3**: H3 hexagons (only for some countries)\n\n"
            "The boundaries are taken from [OpenStreetMap](https://www.openstreetmap.org/) administrative boundaries."
        )
    dataset["notes"] = (
        f"This dataset provides insights into the data quality of [OpenStreetMap](https://www.openstreetmap.org/) (OSM) data in {country_name}."
        f" It has been created with the [ohsome quality API](https://github.com/GIScience/ohsome-quality-api) and covers the topics"
        f" buildings, roads, schools, hospitals and land cover. The results can also be explored in the"
        f" [interactive country report](https://giscience.github.io/osm-quality-country-reports/#/{country_code}/roads).\n\n"
        f"The analysis is available for the following units, each as one GeoPackage and one CSV file"
        f" (`{country_code}_<unit>_indicator_results`):\n\n"
        f"{units}\n\n"
        f"Indicators: currentness, mapping saturation, user activity, attribute completeness"
        f" and topic specific comparisons with reference data (building comparison, land cover completeness,"
        f" land cover thematic accuracy, roads thematic accuracy). Not every indicator is available for every topic or country."
        f" For how the indicators are calculated see the [ohsome quality API](https://github.com/GIScience/ohsome-quality-api) repository.\n\n"
        f"**GeoPackage**: one layer per topic, one row per unit.\n\n"
        f"- **id**: ID of the unit.\n"
        f"- **value_[indicator]**: Result of the indicator. Ranges between 0 and 1 for most indicators.\n"
        f"- **quality_class_[indicator]**: Quality rating from 1 (low) to 5 (high). Empty if the indicator has no rating.\n"
        f"- **description_[indicator]**: Text explaining the result.\n"
        f"- Attribute completeness is given per attribute, e.g. **value_attribute-completeness_name**.\n"
        f"- If an indicator is not available for this country, value and quality class are 0 and the description starts with \"skipped\".\n\n"
        f"**CSV**: one row per unit, topic and indicator, without geometry.\n\n"
        f"- **id**: ID of the unit (matches the GeoPackage).\n"
        f"- **topic**, **indicator**, **attribute**: What was analysed. The attribute is only set for attribute completeness.\n"
        f"- **value**, **quality_class**, **description**: As in the GeoPackage.\n"
        f"- **status_code**: 200 if the indicator was calculated, 0 if it is not available for this country.\n"
        f"- **osm_timestamp**: Date of the OSM data used.\n"
        f"- **figure**: Plotly chart of the result as JSON.\n\n"
        f"This dataset is one of many [HeiGIT exports on HDX](https://data.humdata.org/organization/heidelberg-institute-for-geoinformation-technology). See the [HeiGIT](https://heigit.org/) website for more information.\n\n"
        f"We are looking forward to hearing about your use-case! Feel free to reach out to us and tell us about your research at [communications@heigit.org](mailto:communications@heigit.org) – we would be happy to amplify your work.\n\n")

    tags = ["indicators", "openstreetmap"]
    if tags:
        dataset.add_tags(tags)

    try:
        dataset.add_country_location(country_code)
    except HDXError as e:
        context.log.info(f"Warning: {e}")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    dataset["dataset_date"] = f"[{today} TO {today}]"

    for fname, url in links:
        try:
            if fname.endswith(".gpkg"):
                fmt = "GeoPackage"
            elif fname.endswith(".csv"):
                fmt = "CSV"
            else:
                fmt = fname.rsplit(".", 1)[-1]
            resource = {
                "name": fname,
                "description": f"{fname} for {country_name}",
                "format": fmt,
                "url": url,
            }
            dataset.add_update_resource(resource)
            context.log.info(f"Resource added: {resource['name']} ({fmt})")
        except Exception as e:
            context.log.error(f"Error while trying to add {fname}: {e}")

    hdx_country_url = dataset.create_in_hdx()
    context.log.info(f"Data set created in hdx under the following url [{hdx_country_url}]")
    return hdx_country_url
