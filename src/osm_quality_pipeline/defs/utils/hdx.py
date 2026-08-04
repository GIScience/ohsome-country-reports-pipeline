import dagster as dg
import yaml
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from datetime import datetime, timezone
import os

logger = dg.get_dagster_logger()


def get_s3_links(config, country, s3):
    response = s3.get_client().list_objects_v2(
        Bucket=config.bucket,
        Prefix=f"oqapi_hdx/downloads/{country}/",
    )
    # links_list = get_s3_links(country)
    links_list = [
        (f"https://{config.host}/{config.bucket}/{obj['Key']}", obj["Key"].split("/")[-1],)
        for obj in response.get("Contents", [])
        if obj["Key"].lower().endswith(".zip") # later change this to get gpkgs and csvs
    ]
    logger.info(links_list)
    return links_list


def upload_to_hdx(country_code, links, context):

    Configuration.create(
        hdx_site="stage", # works on "prod", stage not tested yet but should also work
        user_agent="HDXDataSeriesScript",
        hdx_key=os.getenv("HDX_KEY"),
        hdx_url="https://data.humdata.org/",
    )
    with open("src/osm_quality_pipeline/configs/countries.yaml", "r") as f:
        countries = yaml.safe_load(f)
    hdx_country = countries[country_code]["slug"]
    country_name = hdx_country.replace("-", " ").title()
    return create_country_dataset(country_code, country_name, links, context), links

def create_country_dataset(country_code: str, country_name: str, links, context): 
    dataset_name = f"{country_name} OSM Data quality"
    title = f"{country_name} - OSM Data quality"

    dataset = Dataset()
    dataset["name"] = dataset_name.lower().replace(" ", "-")
    dataset["title"] = title
    dataset["owner_org"] = "heidelberg-institute-for-geoinformation-technology"
    dataset["groups"] = [{"name": "heidelberg-institute-for-geoinformation-technology"}]
    dataset["private"] = True # final wieder ändern wenn alles online gehen darf???
    dataset.set_expected_update_frequency("Every six months")
    dataset["license_id"] = "cc-by-sa"
    dataset["dataset_source"] = "HeiGIT"
    dataset["maintainer"] = "valentin-boehmer-8808"
    dataset["maintainer_email"] = "valentin.boehmer@heigit.org"
    dataset["methodology"] = " Quality analysis of OSM data unsing the ohsome dashboard."
    dataset.set_custom_viz(
        f"https://giscience.github.io/osm-quality-country-reports/#/{country_code}/roads-all-highways"
    )
    dataset["notes"] = (f"This dataset provides insights into the data quality of [OpenStreetMap](https://www.openstreetmap.org/) (OSM) data in {country_name}."
                        f" It has been created using the OSM data quality analysis of [ohsome](https://dashboard.ohsome.org/).\n\n"
                        f" Different indicators are used to asses the data quality depending on the selected topic, for further information regarding the calculation of the quality indicators see the [Github](https://github.com/GIScience/ohsome-quality-api) repository."
                        f" The OSM data quality analysis is available for different topics either as a CSV or as a Geopackage file."
                        f" The quality analysis is available in three units: admin level 0, admin level 1 and hexagons."
                        f" Each zip file contains all three units for the selected topic."
                        f" The unit of analysis is defined by [geoboundaries](https://www.geoboundaries.org/) country borders.\n\n"
                        f"Attributes of the CSV/ Geopackage file:\n\n"
                        f"- **[unit]_id**: Unit of quality analysis.\n\n"
                        f"- **ADM0_name**: Name of the country.\n\n"
                        f"- **ADM0_iso**: ISO3 country code.\n\n"
                        f"- **result_value_[indicator]**: Calculated result of OSM data quality for the respective indicator. Ranges between 0 and 1 for most indicators.\n\n"
                        f" Different indicators are available for each topic. Check out the [Topic Catalog](https://dashboard.ohsome.org/en/) to see which indicators are relevant for which topic.\n\n"
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
            if "_gpkg.zip" in fname:
                fmt = "zipped geopackage"
            elif "_csv.zip" in fname:
                fmt = "zipped csv"
            else:
                fmt = "zip"
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