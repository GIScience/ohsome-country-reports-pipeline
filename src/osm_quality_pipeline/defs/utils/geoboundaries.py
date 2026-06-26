import sys
import os
import requests as r
import geopandas as gpd


def download_from_geoboundaries(country, level_val, url_val, out_dir):
    list_url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL"
    print(f"Fetching available levels from {list_url}")
    try:
        resp = r.get(list_url)
        resp.raise_for_status()
        entries = resp.json()
    except r.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(entries, list) or not entries:
        print(f"No boundaries found for country '{country}'", file=sys.stderr)
        sys.exit(1)

    any_failures = False

    # 2) Iterate over every entry and download
    for entry in entries:
        level = entry.get(level_val)  # e.g. "ADM0", "ADM1", ...
        url = entry.get(url_val)
        if not level or not url:
            print(f"Skipping malformed entry: {entry}", file=sys.stderr)
            any_failures = True
            continue

        out_path = os.path.join(out_dir, f"{country}_{level.lower()}.gpkg")

        print(f"[{level}] Downloading from {url}")
        try:
            download = r.get(url)
            download.raise_for_status()
        except r.RequestException as e:
            print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
            any_failures = True
            continue

        gdf = gpd.read_file(io.BytesIO(download.content))
        if "id" not in gdf.columns:
            adm_level = (
                os.path.basename(out_path)
                .split("_")[1]
                .replace("adm", "")
                .replace(".gpkg", "")
            )
            gdf = gdf.reset_index(drop=True)
            gdf["id"] = [
                f"{country}_adm{adm_level}_{str(i + 1).zfill(2)}"
                for i in range(len(gdf))
            ]
            gdf.to_file(out_path, driver="GPKG")
        print(f"[{level}] Saved to {out_path}")

    if any_failures:
        print("One or more boundaries failed to download.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All available boundaries downloaded successfully.")
