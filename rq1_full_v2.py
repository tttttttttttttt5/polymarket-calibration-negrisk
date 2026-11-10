# -*- coding: utf-8 -*-
r"""Step 4: compute RQ1 calibration errors from the token x hour aggregate table (all v2 markets).

Method (following paper 7540819):
  - T = resolved_ts_utc (ctf_v2_conditions, authoritative v2 resolution time)
  - T-7: nearest hourly price to T-7, tolerance +/- 2 days
  - T-1: nearest hourly price to T-1, tolerance +/- 1 day
  - calibration error = |price - y|, y = 1 - winning_outcome_index
  - category: first tag in the clob tokens.parquet tags -> crypto/sports/politics/other
Outputs:
  - calibration_full_results.csv        (category x era summary)
  - calibration_market_level_full.csv   (market-level detail)
"""
import glob
import os

import numpy as np
import pandas as pd

DL = os.environ.get("DATA_DIR", "./data")
AGG_DIR = os.path.join(os.environ.get("CACHE_DIR", "./cache"), "tape_agg")
OUT_DIR = os.environ.get("OUT_DIR", "./out")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_SUMMARY = os.path.join(OUT_DIR, "calibration_full_results.csv")
OUT_DETAIL = os.path.join(OUT_DIR, "calibration_market_level_full.csv")

CAT_MAP = {"crypto": "crypto", "sports": "sports", "politics": "politics"}


def tags_category(t) -> str:
    # tags may be a numpy array / list / string / NaN
    if t is None:
        return "other"
    try:
        if pd.isna(t):
            return "other"
    except (TypeError, ValueError):
        pass
    if isinstance(t, (list, np.ndarray, pd.Series)):
        if len(t) == 0:
            return "other"
        first = str(t[0])
    else:
        s = str(t).strip("[]")
        first = s.split("'")[1] if "'" in s else s.split(",")[0]
    return CAT_MAP.get(first.strip().strip("'\" ").lower(), "other")


# ---------- 1. load the hourly aggregate table (cached) ----------
CACHE = os.path.join(AGG_DIR, "hourly_cache.parquet")
if os.path.exists(CACHE):
    hourly = pd.read_parquet(CACHE)
    print("cache loaded:", f"{len(hourly):,} rows")
else:
    parts = glob.glob(os.path.join(AGG_DIR, "*.parquet"))
    print("aggregate parts:", len(parts))
    frames = []
    for p in parts:
        t = pd.read_parquet(p)  # empty marker files only have token_id, read all and filter
        if "hour_utc" not in t.columns or t.empty:
            continue
        frames.append(t[["token_id", "hour_utc", "last_price", "vwap"]])
    hourly = pd.concat(frames, ignore_index=True)
    del frames
    hourly = hourly.drop_duplicates(["token_id", "hour_utc"], keep="last")
    hourly["token_id"] = hourly["token_id"].astype(str)
    hourly.to_parquet(CACHE, index=False)
hourly["hour_utc"] = hourly["hour_utc"].dt.tz_convert("UTC").dt.as_unit("ns")
hourly = hourly.sort_values(["hour_utc", "token_id"]).reset_index(drop=True)
print("hourly rows:", f"{len(hourly):,}", " tokens:", hourly["token_id"].nunique())

# ---------- 2. universe ----------
uni = pd.read_parquet(os.path.join(DL, "universe_v2.parquet"),
                      columns=["condition_id", "yes_token_id", "y",
                               "resolved_ts", "tags", "market_slug"],
                      dtype_backend="numpy_nullable")
uni["yes_token_id"] = uni["yes_token_id"].astype(str)
uni["resolved_ts"] = pd.to_datetime(uni["resolved_ts"], utc=True)
uni["category"] = uni["tags"].map(tags_category)
print("universe:", len(uni), " categories:", uni["category"].value_counts().to_dict())

# ---------- 3. merge_asof to find the nearest hourly price at T-7 / T-1 ----------
uni = uni.sort_values("resolved_ts").reset_index(drop=True)
hourly_sorted = hourly.sort_values("hour_utc")

for label, offset, tol in (("7d", pd.Timedelta(days=7), pd.Timedelta(days=2)),
                           ("1d", pd.Timedelta(days=1), pd.Timedelta(days=1))):
    tgt = uni[["yes_token_id", "resolved_ts"]].copy()
    tgt["target"] = tgt["resolved_ts"] - offset
    tgt = tgt.rename(columns={"yes_token_id": "token_id"})
    tgt = tgt.sort_values("target")
    m = pd.merge_asof(
        tgt, hourly_sorted[["token_id", "hour_utc", "last_price", "vwap"]],
        left_on="target", right_on="hour_utc", by="token_id",
        tolerance=tol, direction="nearest")
    uni[f"price_{label}"] = m["last_price"].to_numpy()
    uni[f"vwap_{label}"] = m["vwap"].to_numpy()
    uni[f"gap_{label}"] = (m["hour_utc"].to_numpy() - m["target"].to_numpy())
    uni[f"error_{label}"] = (uni[f"price_{label}"] - uni["y"]).abs()
    print(f"T-{label}: matched {uni[f'error_{label}'].notna().sum()} / {len(uni)}")

# ---------- 4. summary ----------
uni["era"] = "v2"  # tape_v2 only covers the v2 era
detail = uni[["condition_id", "market_slug", "category", "era", "y",
              "resolved_ts", "price_7d", "error_7d", "price_1d", "error_1d",
              "gap_7d", "gap_1d", "vwap_7d", "vwap_1d"]]
detail.to_csv(OUT_DETAIL, index=False)

summary = (detail.groupby(["category", "era"], as_index=False)
           .agg(n_markets=("condition_id", "count"),
                n_with_7d=("error_7d", "count"),
                n_with_1d=("error_1d", "count"),
                avg_error_7d=("error_7d", "mean"),
                avg_error_1d=("error_1d", "mean"))
           .sort_values(["category", "era"]).round(4))
summary.to_csv(OUT_SUMMARY, index=False)
import shutil
_dest = os.environ.get("CALIB_FINAL_COPY")  # optional convenience copy target
if _dest:
    shutil.copyfile(OUT_SUMMARY, _dest)
    print("copied to:", _dest)
print("\n=== calibration_final.csv ===")
print(summary.to_string(index=False))
