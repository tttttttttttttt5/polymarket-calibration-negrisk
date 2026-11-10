# -*- coding: utf-8 -*-
r"""RQ2 feasibility: scan negRisk families for price-spread events between mutually exclusive markets.

Family = neg_risk_market_id (clob_markets tokens table).
For each condition use the Yes token's hourly last_price (hourly_cache).
For every pair (A,B) in the same family, check |pA+pB-1| > 0.1 at hours
before min(T_A,T_B), and locate the convergence time (spread <= 0.05).

Output: rq2_spread_candidates.csv (sorted by peak spread)
"""
import os
import numpy as np
import pandas as pd

DL = os.environ.get("DATA_DIR", "./data")
PROJ = os.environ.get("OUT_DIR", "./out")
os.makedirs(PROJ, exist_ok=True)
AGG = os.path.join(os.environ.get("CACHE_DIR", "./cache"), "tape_agg")
TAPE_START = pd.Timestamp("2026-04-28", tz="UTC")
TAPE_END = pd.Timestamp("2026-09-01", tz="UTC")

print("loading tokens table ...")
tok = pd.read_parquet(os.path.join(DL, "clob_markets.parquet"),
                      columns=["condition_id", "token_id", "outcome",
                               "market_slug", "question", "neg_risk",
                               "neg_risk_market_id"])
tok["token_id"] = tok["token_id"].astype(str)
yes = tok[(tok["neg_risk"]) & (tok["outcome"] == "Yes")].copy()
yes = yes.drop_duplicates("condition_id")
print("negRisk conditions (Yes token):", len(yes))

print("loading ctf resolution times ...")
ctf = pd.read_parquet(os.path.join(DL, "ctf_v2_conditions.parquet"),
                      columns=["condition_id", "resolved_ts_utc"])
ctf["condition_id"] = ctf["condition_id"].astype(str)
yes = yes.merge(ctf, on="condition_id", how="inner")
yes["T"] = pd.to_datetime(yes["resolved_ts_utc"], utc=True)
yes = yes.drop(columns=["resolved_ts_utc"])
yes = yes[yes["T"].between(TAPE_START, TAPE_END)]
print("negRisk conditions within tape coverage:", len(yes))

print("loading hourly_cache ...")
hourly = pd.read_parquet(os.path.join(AGG, "hourly_cache.parquet"),
                         columns=["token_id", "hour_utc", "last_price"])
hourly["token_id"] = hourly["token_id"].astype(str)
have = set(hourly["token_id"].unique())
yes = yes[yes["token_id"].isin(have)]
print("negRisk conditions with hourly prices:", len(yes))

# keep only tokens of interest, build {token: price series indexed by hour}
need_tokens = set(yes["token_id"])
hourly = hourly[hourly["token_id"].isin(need_tokens)]
price_map = {
    t: g.sort_values("hour_utc").set_index("hour_utc")["last_price"].dropna()
    for t, g in hourly.groupby("token_id", sort=False)
}
del hourly
print("price_map tokens:", len(price_map))

fam_counts = yes.groupby("neg_risk_market_id")["condition_id"].nunique()
fams = fam_counts[fam_counts >= 2].index
yes = yes[yes["neg_risk_market_id"].isin(fams)]
print("families with >= 2 conditions:", len(fams))

fam_groups = {k: v for k, v in yes.groupby("neg_risk_market_id")}
print("scanning families ...")

rows = []
import time as _t
_T0 = _t.time()
for i, (fid, g) in enumerate(fam_groups.items()):
    if i % 500 == 0:
        print(f"  family {i}/{len(fam_groups)}  events {len(rows)}  "
              f"{_t.time()-_T0:.0f}s", flush=True)
    if i % 5000 == 0 and rows and i > 0:
        pd.DataFrame(rows).to_csv(os.path.join(PROJ, "rq2_spread_partial.csv"),
                                  index=False)
    g = g.sort_values("T")
    # at most 10 conditions per family to avoid combinatorial blowup
    if len(g) > 10:
        g = g.head(10)
    conds = g.to_dict("records")
    price_s = {}
    for c in conds:
        s = price_map.get(c["token_id"])
        if s is None or len(s) < 24:
            continue
        price_s[c["condition_id"]] = s

    n = len(conds)
    for a in range(n):
        for b in range(a + 1, n):
            ca, cb = conds[a], conds[b]
            if ca["condition_id"] not in price_s or cb["condition_id"] not in price_s:
                continue
            pa = price_s[ca["condition_id"]]
            pb = price_s[cb["condition_id"]]
            Tm = min(ca["T"], cb["T"])
            j = pa.index.intersection(pb.index)
            if len(j) == 0:
                continue
            # only consider hours before both settlements
            j = j[j < Tm]
            if len(j) < 12:
                continue
            sa = pa.loc[j].to_numpy()
            sb = pb.loc[j].to_numpy()
            spread = np.abs(sa + sb - 1.0)
            imax = int(spread.argmax())
            if spread[imax] <= 0.10:
                continue
            # convergence: first hour after the event with spread <= 0.05 (within 12h)
            conv_h = pd.NaT
            after = np.where(spread[imax:] <= 0.05)[0]
            if len(after) > 0 and after[0] > 0:
                conv_h = j[imax + after[0]]
            elif len(after) > 0:
                continue  # already converged before the event (anomaly)
            vol_a = float(pa.loc[j[imax:]].sum()) if len(j) > imax else 0
            rows.append({
                "family_id": fid,
                "slug_a": ca["market_slug"], "slug_b": cb["market_slug"],
                "q_a": ca["question"], "q_b": cb["question"],
                "cond_a": ca["condition_id"], "cond_b": cb["condition_id"],
                "token_a": ca["token_id"], "token_b": cb["token_id"],
                "T_a": ca["T"], "T_b": cb["T"],
                "hour_peak": j[imax], "spread_peak": round(float(spread[imax]), 3),
                "p_a": round(float(sa[imax]), 3), "p_b": round(float(sb[imax]), 3),
                "hour_conv": conv_h,
                "hours_to_conv": (conv_h - j[imax]).total_seconds() / 3600
                    if not pd.isna(conv_h) else np.nan,
                "n_hours_overlap": len(j),
            })

ev = pd.DataFrame(rows)
if len(ev):
    ev = ev.sort_values("spread_peak", ascending=False)
ev.to_csv(os.path.join(PROJ, "rq2_spread_candidates.csv"), index=False)
print("\ntotal events:", len(ev))
if len(ev):
    print(ev[["slug_a", "slug_b", "hour_peak", "spread_peak", "p_a", "p_b",
              "hour_conv", "hours_to_conv"]].head(30).to_string(index=False))
