import os
import dagster as dg
from refactored_pipeline.defs.assets.utils import download_from_geoboundaries
from refactored_pipeline.defs.partitions import country_partitions


@dg.asset(
    partitions_def=country_partitions,
)
def geoboundary_asset(context) -> dg.Output[str]:
    country = context.partition_key.upper()
    out_dir = os.path.join("data", country)
    os.makedirs(out_dir, exist_ok=True)

    list_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL"
    try:
        download_from_geoboundaries(list_url=list_url, country=country, level_val="boundaryType", url_val="gjDownloadURL", out_dir=out_dir)
    except SystemExit as e:
        context.log.warning(f"[{country}] geoBoundaries download failed: {e}")
        raise Exception(f"Failed to fetch boundaries for {country} from geoBoundaries.")

    return dg.Output(out_dir, metadata={"paths": out_dir, "count": len(out_dir), "source": "geoBoundaries"})
