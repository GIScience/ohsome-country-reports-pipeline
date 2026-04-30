# OQAPI HDX PIPELINE
## Setup
**Python env**

Make sure you use python 3.12.10

```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**S3 config**

To enable the S3 upload asset, create a YAML config (e.g. `s3_config.yaml`):

```yaml
s3_asset:
    endpoint: warm.storage.heigit.org
    bucket: heigit-hdx-public
    access_key: <ACESS_KEY>
    secret_key: <SECRET_KEY>
    dest_prefix: oqapi_hdx
    secure: true
```

**HDX config**

To enable the HDX upload asset, create a YAML config (e.g. `hdx_config.yaml`) with:

```yaml
hdx:
  site: "prod"
  api_key: <YOUR API KEY>
  owner_org: <YOUR OWNER_ORG> #e.g"heidelberg-institute-for-geoinformation-technology"
  data_update_frequency: "Every six months"
  maintainer: <MAINTAINER NAME>
  maintainer_email: <MAINTAINER E-MAIL>
  private: true
  url: "https://data.humdata.org/"
  tags:
    - <INDICATOR>
    - <INDICATOR> #e.g "indicators","openstreetmap" find more here: https://docs.google.com/spreadsheets/d/1fTO8T8ZVXU9eoh3EIrw490Z2pX7E59MhHmCvT_cXmNs/edit?gid=1261258630#gid=1261258630

```

## Configure and start up dagster interactively

```sh
export DAGSTER_HOME="$PWD/.dagster"
dagster dev  -w workspace.yml -p 4444
```
