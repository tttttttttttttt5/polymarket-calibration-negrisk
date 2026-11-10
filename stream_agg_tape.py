# -*- coding: utf-8 -*-
r"""Step 3: stream all tape_v2 files and aggregate into a token x hour price table (resumable).

Output: <CACHE_DIR>/tape_agg/<key_sanitized>.parquet
  columns: token_id, hour_utc, n_trades, vwap, last_price, last_ts
"""
import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd

DL = os.environ.get("DATA_DIR", "./data")
CACHE_DIR = os.environ.get("CACHE_DIR", "./cache")
AGG_DIR = os.path.join(CACHE_DIR, "tape_agg")
PLAN_LOG = os.path.join(CACHE_DIR, "tape_plan.json")
BUCKET = "polymarket-dataset"
ENDPOINT = os.environ["R2_ENDPOINT"]
AK = os.environ["R2_ACCESS_KEY_ID"]
SK = os.environ["R2_SECRET_ACCESS_KEY"]
WORKERS = 24

os.makedirs(AGG_DIR, exist_ok=True)

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=boto3.session.Config(signature_version="s3v4",
                                              s3={"addressing_style": "path"}))
paginator = s3.get_paginator("list_objects_v2")

# ---------- file plan: cached to disk to avoid re-listing the bucket ----------
if os.path.exists(PLAN_LOG):
    plan_raw = json.load(open(PLAN_LOG))
    plan = [(k, tuple(b) if b else None) for k, b in plan_raw]
    print(f"plan cached: {len(plan)} items")
else:
    span_map = {}
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/parquet/tape_v2/"):
        for o in page.get("Contents", []):
            mm = re.search(r"blocks=(\d+)-(\d+)/part\.parquet$", o["Key"])
            if mm:
                span_map.setdefault(int(mm.group(2)) - int(mm.group(1)) + 1,
                                    []).append(o["Key"])

    fine = sorted(span_map.get(500, []))
    def parse_span(key):
        mm = re.search(r"blocks=(\d+)-(\d+)/", key)
        return int(mm.group(1)), int(mm.group(2))

    plan = [(k, None) for k in fine]          # full files
    gaps = []
    if fine:
        prev = parse_span(fine[0])[1]
        for k in fine[1:]:
            b1, b2 = parse_span(k)
            if b1 > prev + 1:
                gaps.append((prev + 1, b1 - 1))
            prev = max(prev, b2)

    all_other = []
    for span, keys in span_map.items():
        if span != 500:
            all_other.extend((parse_span(k), k) for k in keys)
    all_other.sort()

    # fill gaps between 500-block files with larger (2000-block) files
    for g1, g2 in gaps:
        filler = None
        for (f1, f2), kk in all_other:
            if f1 <= g1 and f2 >= g2:
                filler = (kk, (g1, g2))
                break
        if filler:
            plan.append(filler)
            continue
        used = 0
        for (f1, f2), kk in all_other:
            if f2 < g1 or f1 > g2:
                continue
            plan.append((kk, (max(f1, g1), min(f2, g2))))
            used += 1
        if used == 0:
            print("WARN: no filler covers gap", g1, g2)
    json.dump([[k, list(b) if b else None] for k, b in plan],
              open(PLAN_LOG, "w"))
    print(f"plan built and cached: {len(plan)} items")

# ---------- universe YES tokens ----------
uni = pd.read_parquet(os.path.join(DL, "universe_v2.parquet"),
                      columns=["condition_id", "yes_token_id"],
                      dtype_backend="numpy_nullable")
YES = set(uni["yes_token_id"].astype(str))
print("universe YES tokens:", len(YES))

def norm_tokens(s):
    """token_id may arrive as decimal/int/str -> normalize to decimal string
    (str() of a Decimal object is its exact decimal representation)."""
    return s.astype(str)


def part_path(key):
    return os.path.join(AGG_DIR,
                        re.sub(r"[^A-Za-z0-9=._-]", "_", key.split("/")[-2])
                        + ".parquet")


def process(key, only_blocks):
    last_err = None
    for attempt in range(3):
        try:
            buf = io.BytesIO()
            s3.download_fileobj(BUCKET, key, buf)
            break
        except Exception as e:  # retry on network errors
            last_err = e
            time.sleep(2 * (attempt + 1))
    else:
        return key, False, str(last_err)
    buf.seek(0)
    try:
        df = pd.read_parquet(buf, columns=["token_id", "ts_utc", "price",
                                           "shares", "block_number"])
    finally:
        del buf
    if only_blocks is not None:
        df = df[df["block_number"].between(only_blocks[0], only_blocks[1])]
    if not df.empty:
        df["token_id"] = norm_tokens(df["token_id"])
        df = df[df["token_id"].isin(YES)]
    if df.empty:
        pd.DataFrame({"token_id": pd.Series(dtype=str)}).to_parquet(
            part_path(key), index=False)
        return key, True, None
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts_utc", "price"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
    df = df.dropna(subset=["price"])
    if df.empty:
        pd.DataFrame({"token_id": pd.Series(dtype=str)}).to_parquet(
            part_path(key), index=False)
        return key, True, None
    df["hour_utc"] = df["ts_utc"].dt.floor("h")
    df = df.sort_values("ts_utc")
    g = df.groupby(["token_id", "hour_utc"])
    agg = g.agg(n_trades=("price", "size"),
                last_price=("price", "last"),
                last_ts=("ts_utc", "last")).reset_index()
    pv = df.assign(pv=df["price"] * df["shares"]).groupby(
        ["token_id", "hour_utc"])["pv"].sum()
    sh = df.groupby(["token_id", "hour_utc"])["shares"].sum()
    agg = agg.merge(pv.rename("pv"), on=["token_id", "hour_utc"], how="left")
    agg = agg.merge(sh.rename("sh"), on=["token_id", "hour_utc"], how="left")
    agg["vwap"] = (agg["pv"] / agg["sh"].replace(0, pd.NA)).astype("float64")
    agg = agg.drop(columns=["pv", "sh"])
    agg.to_parquet(part_path(key), index=False)
    return key, True, None


todo = [(k, b) for k, b in plan if not os.path.exists(part_path(k))]
print(f"plan {len(plan)}, existing parts {len(os.listdir(AGG_DIR))}, todo {len(todo)}")
_T0 = time.time()
_TIME_BUDGET = 5 * 60  # seconds: exit cleanly when the time box expires (checkpoint saved)
failed = []
with ThreadPoolExecutor(WORKERS) as ex:
    futs = {ex.submit(process, k, b): (k, b) for k, b in todo}
    for i, fut in enumerate(as_completed(futs), 1):
        kb = futs[fut]
        try:
            key, ok, err = fut.result()
            if not ok:
                failed.append(kb)
        except Exception as e:  # a single-file failure does not stop the batch
            failed.append(kb)
            print(f"ERR {kb[0].split('/')[-2]}: {e}", flush=True)
        if i % 25 == 0:
            print(f"progress {i}/{len(todo)}", flush=True)
        if time.time() - _T0 > _TIME_BUDGET:
            print("time budget reached", flush=True)
            break

print(f"failed (retry later): {len(failed)}")
parts = len(os.listdir(AGG_DIR))
left = sum(1 for k, b in plan if not os.path.exists(part_path(k)))
print(f"round finished: parts {parts}, remaining {left}")
