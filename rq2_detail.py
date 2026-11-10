# -*- coding: utf-8 -*-
"""Case study fill detail: print every fill in the window (who bought / who sold) to identify the price correctors."""
import glob
import json
import os
import sys

import pandas as pd

TMP = os.environ.get("TAPE_CACHE_DIR", "./cache/tape_parts")


def detail(cfg_path):
    cfg = json.load(open(cfg_path))
    t0 = pd.Timestamp(cfg["t_start"])
    t1 = pd.Timestamp(cfg["t_end"])
    toks = {"A": cfg["token_a"], "B": cfg["token_b"]}
    frames = []
    for f in glob.glob(os.path.join(TMP, "*.parquet")):
        d = pd.read_parquet(f, columns=["ts_utc", "block_number", "maker",
                                        "taker", "side", "token_id", "usdc",
                                        "shares", "price", "tx_hash"])
        d["token_id"] = d["token_id"].astype(str)
        d = d[d["token_id"].isin(toks.values())]
        frames.append(d)
    d = pd.concat(frames, ignore_index=True)
    d["ts_utc"] = pd.to_datetime(d["ts_utc"], utc=True)
    d = d[(d["ts_utc"] >= t0) & (d["ts_utc"] < t1)].sort_values("ts_utc")
    inv = {v: k for k, v in toks.items()}
    d["mkt"] = d["token_id"].map(inv)
    for c in ("usdc", "shares", "price"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    print(cfg_path, " fills:", len(d))
    print(d[["ts_utc", "mkt", "side", "price", "shares", "usdc",
             "maker", "taker"]].to_string(index=False))
    print("\n--- summary by market x direction (taker = aggressive side) ---")
    for m in ("A", "B"):
        s = d[d["mkt"] == m]
        for side in ("BUY", "SELL"):
            x = s[s["side"] == side]
            if len(x) == 0:
                continue
            print(f"\n[{m} {side}] {len(x)} fills, usdc={x['usdc'].sum():.0f}, "
                  f"shares={x['shares'].sum():.0f}")
            for w, col in (("taker", "taker"), ("maker", "maker")):
                g = x.groupby(col)["usdc"].agg(["size", "sum"]).sort_values(
                    "sum", ascending=False)
                print(f"  as {w}: " + "; ".join(
                    f"{i[:10]}..({r['size']} fills / ${r['sum']:.0f})"
                    for i, r in g.iterrows()))


if __name__ == "__main__":
    detail(sys.argv[1])
