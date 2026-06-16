import requests
import os
import sys

def download_from_geoboundaries(list_url, country, level_val, url_val, out_dir):
    print(f"Fetching available levels from {list_url}")
    try:
        resp = requests.get(list_url)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"ERROR fetching boundary list: {e}", file=sys.stderr)
        sys.exit(1)
    entries = resp.json()
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

        out_path = os.path.join(out_dir, f"boundary_{level}.geojson")

        print(f"[{level}] Downloading from {url}")
        try:
            download = requests.get(url)
            download.raise_for_status()
        except requests.RequestException as e:
            print(f"[{level}] ERROR downloading: {e}", file=sys.stderr)
            any_failures = True
            continue

        with open(out_path, "wb") as fp:
            fp.write(download.content)
        print(f"[{level}] Saved to {out_path}")

    if any_failures:
        print("One or more boundaries failed to download.", file=sys.stderr)
        sys.exit(1)
    else:
        print("All available boundaries downloaded successfully.")

