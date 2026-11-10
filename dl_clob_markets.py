# -*- coding: utf-8 -*-
"""Download clob_markets.parquet and inspect its structure."""
import os
import boto3
import pandas as pd

ENDPOINT = os.environ["R2_ENDPOINT"]
AK = os.environ["R2_ACCESS_KEY_ID"]
SK = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET = "polymarket-dataset"
KEY = "data/parquet/clob_markets/part.parquet"
DEST = os.path.join(os.environ.get("DATA_DIR", "./data"), "clob_markets.parquet")
os.makedirs(os.path.dirname(DEST) or ".", exist_ok=True)

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=boto3.session.Config(signature_version="s3v4",
                                              s3={"addressing_style": "path"}))

# locate the actual object key
r = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/parquet/clob_markets/")
for obj in r.get("Contents", []):
    print("found:", obj["Key"], f"{obj['Size']/1e9:.2f} GB")
    KEY = obj["Key"]

if not os.path.exists(DEST):
    print("downloading...")
    s3.download_file(BUCKET, KEY, DEST)
print("size:", os.path.getsize(DEST) / 1e9, "GB")

pf_cols = None
import pyarrow.parquet as pq
pf = pq.ParquetFile(DEST)
print("rows:", pf.metadata.num_rows)
print("cols:", pf.schema_arrow.names)
df = pf.read_row_group(0).to_pandas()
print(df.head(3).to_string())
