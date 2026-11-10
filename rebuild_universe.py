# -*- coding: utf-8 -*-
"""Step 1b: rebuild the universe using ctf resolved_ts_utc (authoritative v2 resolution time)."""
import os
import pandas as pd

DL = os.environ.get("DATA_DIR", "./data")
OUT = os.path.join(DL, "universe_v2.parquet")
TAPE_START = pd.Timestamp("2026-04-28", tz="UTC")

uni = pd.read_parquet(os.path.join(DL, "universe_markets.parquet"),
                      dtype_backend="numpy_nullable")
uni["condition_id"] = uni["condition_id"].astype(str)
print("universe loaded:", len(uni))

# --- ctf: y + resolved_ts_utc ---
cond = pd.read_parquet(os.path.join(DL, "ctf_v2_conditions.parquet"),
                       columns=["condition_id", "outcome_slot_count",
                                "winning_outcome_index", "resolved_ts_utc"],
                       dtype_backend="numpy_nullable")
cond["condition_id"] = cond["condition_id"].astype(str)
cond = cond[(cond["outcome_slot_count"] == 2)
            & cond["winning_outcome_index"].notna()]
cond = cond.drop_duplicates("condition_id")
cond["resolved_ts_utc"] = pd.to_datetime(cond["resolved_ts_utc"], utc=True,
                                         errors="coerce")
m = cond.set_index("condition_id")
uni["y"] = (1 - uni["condition_id"].map(m["winning_outcome_index"])
            .astype("float64"))
uni["resolved_ts"] = uni["condition_id"].map(m["resolved_ts_utc"])

print("with y:", uni["y"].notna().sum())
print("with resolved_ts:", uni["resolved_ts"].notna().sum())
v2 = uni[uni["resolved_ts"].notna()].copy()
v2 = v2[v2["resolved_ts"] >= TAPE_START]
print("resolved_ts >= 2026-04-28:", len(v2))
print("resolved_ts range:", v2["resolved_ts"].min(), "->", v2["resolved_ts"].max())

v2.to_parquet(OUT, index=False)
print("saved:", OUT)
