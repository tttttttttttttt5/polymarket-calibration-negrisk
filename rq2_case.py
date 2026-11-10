# -*- coding: utf-8 -*-
r"""RQ2 case study: extract trades from the 1 hour around spread convergence and aggregate participating wallets.

Usage: python rq2_case.py <case_json>
case_json fields: token_a, token_b, t_start, t_end (UTC ISO strings), out_csv
time -> block: block 86,126,978 = 2026-04-28 11:00 UTC, 2s/block
"""
import io
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

ENDPOINT = os.environ["R2_ENDPOINT"]
AK = os.environ["R2_ACCESS_KEY_ID"]
SK = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET = "polymarket-dataset"
PLAN = os.environ.get("R2_TAPE_PLAN", "tape_plan.json")
TMP = os.environ.get("TAPE_CACHE_DIR", "./cache/tape_parts")
os.makedirs(TMP, exist_ok=True)

EPOCH_BLOCK = 86_126_978
EPOCH_TS = datetime(2026, 4, 28, 11, 0, 0, tzinfo=timezone.utc)
BLOCK_SEC = 2.0

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=AK,
                  aws_secret_access_key=SK,
                  config=boto3.session.Config(signature_version="s3v4",
                                              s3={"addressing_style": "path"}))


def ts2block(t):
    return int(EPOCH_BLOCK + (t - EPOCH_TS).total_seconds() / BLOCK_SEC)


def main(cfg_path):
    cfg = json.load(open(cfg_path))
    t0 = datetime.fromisoformat(cfg["t_start"])
    t1 = datetime.fromisoformat(cfg["t_end"])
    b0, b1 = ts2block(t0), ts2block(t1)
    print(f"window {t0} -> {t1}  block {b0}..{b1}  ({b1-b0} blocks)")

    plan = json.load(open(PLAN))
    hits = []
    for key, only in plan:
        mm = key.split("/")[-2]  # blocks=a-b
        a, b = map(int, mm.split("=")[1].split("-"))
        if b < b0 or a > b1:
            continue
        hits.append((key, (a, b), only))
    print(f"files covering the block range: {len(hits)}")

    frames = []
    for key, span, only in hits:
        local = os.path.join(TMP, str(span[0]) + "_" + os.path.basename(key))
        if not os.path.exists(local):
            for attempt in range(3):
                try:
                    buf = io.BytesIO()
                    s3.download_fileobj(BUCKET, key, buf)
                    buf.seek(0)
                    with open(local, "wb") as f:
                        f.write(buf.getvalue())
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    print("retry:", e)
        df = pd.read_parquet(local,
                             columns=["ts_utc", "block_number", "maker", "taker",
                                      "side", "token_id", "usdc", "shares",
                                      "price", "tx_hash"])
        if only is not None:
            df = df[df["block_number"].between(only[0], only[1])]
        df = df[df["block_number"].between(b0, b1)]
        df["token_id"] = df["token_id"].astype(str)
        df = df[df["token_id"].isin({cfg["token_a"], cfg["token_b"]})]
        if not df.empty:
            frames.append(df)
        print(f"  {span}: +{len(df)} rows (total so far {sum(len(x) for x in frames)})")

    if not frames:
        print("no trades in the window!")
        return
    d = pd.concat(frames, ignore_index=True)
    d["ts_utc"] = pd.to_datetime(d["ts_utc"], utc=True)
    d["usdc"] = pd.to_numeric(d["usdc"], errors="coerce").fillna(0)
    d["shares"] = pd.to_numeric(d["shares"], errors="coerce").fillna(0)
    d = d.sort_values("ts_utc")

    # ---- wallet summary: count maker and taker separately, volume = usdc ----
    rows = []
    for who in ("taker", "maker"):
        g = (d.groupby(who)
             .agg(n_fills=("usdc", "size"), usdc=("usdc", "sum"),
                  shares=("shares", "sum"), n_tx=("tx_hash", "nunique"))
             .reset_index().rename(columns={who: "wallet"}))
        g["role"] = who
        rows.append(g)
    wallets = (pd.concat(rows)
               .groupby(["wallet"], as_index=False)
               .agg(n_fills=("n_fills", "sum"), usdc=("usdc", "sum"),
                    shares=("shares", "sum"), n_tx=("n_tx", "sum"),
                    taker_fills=("role", lambda r: (r == "taker").sum())))
    wallets = wallets.sort_values("usdc", ascending=False)
    wallets.to_csv(cfg["out_csv"], index=False)
    print("\n=== top 15 wallets (by usdc volume) ===")
    print(wallets.head(15).to_string(index=False))
    print("\ntotal wallets:", len(wallets))

    # ---- 5-minute price path ----
    d["min5"] = d["ts_utc"].dt.floor("5min")
    for tk, name in ((cfg["token_a"], "A"), (cfg["token_b"], "B")):
        s = d[d["token_id"] == tk].groupby("min5")["price"].last()
        print(f"\nprice path {name} (5min last):")
        print(s.to_string())

if __name__ == "__main__":
    main(sys.argv[1])
