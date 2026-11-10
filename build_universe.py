# -*- coding: utf-8 -*-
"""Step 1: build the market universe (binary / closed / YES token / y / closed_time / tags category)."""
import os
import numpy as np
import pandas as pd

DL = os.environ.get("DATA_DIR", "./data")
OUT = os.path.join(DL, "universe_markets.parquet")

# --- 1. clob tokens: keep closed markets with Yes/No tokens ---
toks = pd.read_parquet(
    os.path.join(DL, "clob_markets.parquet"),
    columns=["condition_id", "closed", "token_id", "outcome", "tags",
             "market_slug", "end_date_iso"],
    dtype_backend="numpy_nullable")
toks = toks[toks["closed"] == True]  # noqa: E712
toks = toks[toks["token_id"].notna() & toks["outcome"].notna()]
toks["condition_id"] = toks["condition_id"].astype(str)
toks["token_id"] = toks["token_id"].astype(str)
toks["outcome"] = toks["outcome"].str.strip().str.lower()
toks = toks[toks["outcome"].isin(["yes", "no"])]
print("closed Yes/No token rows:", len(toks))

# each condition must have exactly one YES and one NO token
g = toks.groupby("condition_id")["outcome"].agg(["nunique", "count"])
good_cids = set(g[(g["nunique"] == 2) & (g["count"] == 2)].index)
toks = toks[toks["condition_id"].isin(good_cids)]
yes = toks[toks["outcome"] == "yes"].drop_duplicates("condition_id")
print("binary closed markets:", len(yes))

uni = yes[["condition_id", "token_id", "tags", "market_slug",
           "end_date_iso"]].rename(columns={"token_id": "yes_token_id"}).copy()
uni["end_date_iso"] = pd.to_datetime(uni["end_date_iso"], utc=True,
                                     errors="coerce")

# --- 2. y = 1 - winning_outcome_index (woi=0 -> YES wins) ---
cond = pd.read_parquet(os.path.join(DL, "ctf_v2_conditions.parquet"),
                       columns=["condition_id", "winning_outcome_index",
                                "outcome_slot_count"],
                       dtype_backend="numpy_nullable")
cond["condition_id"] = cond["condition_id"].astype(str)
cond = cond[(cond["outcome_slot_count"] == 2)
            & cond["winning_outcome_index"].notna()]
woi = cond.drop_duplicates("condition_id").set_index("condition_id")[
    "winning_outcome_index"]
uni["y"] = 1 - uni["condition_id"].map(woi).astype("float64")
n_resolved = uni["y"].notna().sum()
print("markets with outcome y:", n_resolved)
uni = uni[uni["y"].notna()]

# --- 3. closed_time: prefer markets_v2, fall back to end_date_iso ---
m2 = pd.read_parquet(os.path.join(DL, "markets_v2.parquet"),
                     columns=["condition_id", "closed_time"],
                     dtype_backend="numpy_nullable")
m2["condition_id"] = m2["condition_id"].astype(str)
m2["closed_time"] = pd.to_datetime(m2["closed_time"], utc=True, errors="coerce")
ct = m2.dropna(subset=["closed_time"]).drop_duplicates("condition_id")[
    "closed_time"]
uni["closed_time_v2"] = uni["condition_id"].map(ct)
uni["closed_time"] = uni["closed_time_v2"].fillna(uni["end_date_iso"])
print("closed_time coverage:", uni["closed_time"].notna().sum(), "/",
      len(uni), " (true closed_time:",
      uni["closed_time_v2"].notna().sum(), ")")

# time range
ct_min, ct_max = uni["closed_time"].min(), uni["closed_time"].max()
print("closed_time range:", ct_min, "->", ct_max)

# --- 4. tags category distribution (first tag) ---
uni["tags_str"] = uni["tags"].astype(str)
first_tag = uni["tags_str"].str.strip("[]").str.split(",").str[0].str.strip(" '\"")
print("\ntop 15 first tags:")
print(first_tag.value_counts().head(15).to_string())

uni.to_parquet(OUT, index=False)
print("\nsaved:", OUT, len(uni), "markets")
