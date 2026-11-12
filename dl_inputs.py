"""Download the three input parquet tables from the R2 bucket.

Usage:  python dl_inputs.py [clob_markets|markets_v2|ctf_v2_conditions ...]
With no arguments, downloads all three. Files already present in DATA_DIR
are skipped, so re-running is cheap.
"""
import os
import sys

import boto3

ENDPOINT = os.environ["R2_ENDPOINT"]
AK = os.environ["R2_ACCESS_KEY_ID"]
SK = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET = "polymarket-dataset"
DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

# object key in the bucket -> local file name
FILES = {
    "clob_markets": ("data/parquet/clob_markets/tokens.parquet", "clob_markets.parquet"),
    "markets_v2": ("data/parquet/gamma_v2/markets_v2.parquet", "markets_v2.parquet"),
    "ctf_v2_conditions": ("data/parquet/ctf_v2_conditions.parquet", "ctf_v2_conditions.parquet"),
}

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=boto3.session.Config(signature_version="s3v4",
                                              s3={"addressing_style": "path"}))

targets = sys.argv[1:] or list(FILES)
for name in targets:
    key, local = FILES[name]
    dest = os.path.join(DATA_DIR, local)
    if os.path.exists(dest):
        print(f"[skip] {local} already in {DATA_DIR}")
        continue
    size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    print(f"[get ] {key} -> {dest} ({size/1e9:.2f} GB)")
    s3.download_file(BUCKET, key, dest)
print("done")
