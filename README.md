# Polymarket Calibration & negRisk Spread Analysis

Replication code for two research questions on the Polymarket dataset
(stored on Cloudflare R2, bucket `polymarket-dataset`):

- **RQ1**: Calibration error of market prices at T-7 and T-1 days before
  resolution, by category (sports / other / crypto / politics).
- **RQ2**: Feasibility study — who corrects price inconsistencies between
  mutually exclusive markets in the same negRisk family?

## Setup

Python 3.12 with `pandas`, `numpy`, `pyarrow`, `boto3`, `matplotlib`.

Credentials and endpoints are read from environment variables (never hardcode them):

```
R2_ENDPOINT            # Cloudflare R2 S3 endpoint URL
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
```

Optional directory overrides (defaults keep everything inside the repo folder):

```
DATA_DIR               # input parquet files        (default ./data)
OUT_DIR                # result outputs             (default ./out)
CACHE_DIR              # hourly aggregates, plan    (default ./cache)
TAPE_CACHE_DIR         # downloaded raw tape files  (default ./cache/tape_parts)
R2_TAPE_PLAN           # tape file index JSON       (default ./tape_plan.json)
CALIB_FINAL_COPY       # optional: extra copy of the RQ1 summary CSV
```

## Pipeline (run in order)

| Step | Script | Description |
|------|--------|-------------|
| 0 | `dl_clob_markets.py` | Download the clob tokens table (~5.7M rows) -> `data/clob_markets.parquet`. |
| 1 | `build_universe.py` | Build the market universe (binary, closed, with outcome `y`) -> `data/universe_markets.parquet`. |
| 1b | `rebuild_universe.py` | Rebuild using the authoritative `resolved_ts_utc` from `ctf_v2_conditions` -> `data/universe_v2.parquet` (the final universe). |
| 2 | — | Also download `ctf_v2_conditions.parquet` and `markets_v2.parquet` from the bucket into `DATA_DIR` (same R2 layout). |
| 3 | `stream_agg_tape.py` | Stream all ~8,884 `tape_v2` files and aggregate trades into a token x hour price table (resumable; caches per-file parts under `CACHE_DIR/tape_agg/`). |
| 4 | `rq1_full_v2.py` | Compute T-7 / T-1 calibration errors -> `out/calibration_full_results.csv` + `out/calibration_market_level_full.csv`. Optionally set `CALIB_FINAL_COPY` to also save a copy named `calibration_final.csv`. |
| 5 | `rq1_plot.py` | Grouped bar chart. Reads `calibration_final.csv` from `DATA_DIR` — either set `CALIB_FINAL_COPY=<DATA_DIR>/calibration_final.csv` in step 4 or rename `calibration_full_results.csv`. Output -> `out/rq1_error_by_category.png`. |
| 6 | `rq2_scan.py` | Scan negRisk families for mutually exclusive pairs with \|P_A + P_B - 1\| > 0.1 before settlement -> `out/rq2_spread_candidates.csv`. |
| 7 | `rq2_case.py` | For a chosen case (JSON config: `token_a`, `token_b`, `t_start`, `t_end`, `out_csv`), pull raw fills from the cloud by block range and aggregate wallets. Needs the tape plan generated in step 3: set `R2_TAPE_PLAN=<CACHE_DIR>/tape_plan.json`. |
| 8 | `rq2_detail.py` | Per-fill view of a case window (who bought / who sold, aggregation by side). Reads the raw tape parts cached by step 7. |

## Key facts

- Time -> block conversion: block `86,126,978` = 2026-04-28 11:00 UTC,
  ~2 s/block. `tape_v2` covers blocks 86,126,978-92,994,671.
- Resolution semantics: `winning_outcome_index = 0` -> YES wins,
  so `y = 1 - winning_outcome_index`.
- negRisk mutually exclusive families are grouped by `neg_risk_market_id`
  in the clob tokens table.
- `sum > 1` between two mutually exclusive YES prices is a guaranteed
  inconsistency (arbitrage); `sum < 1` is usually legitimate because other
  outcomes in the family absorb the remaining probability.

## Results summary

See `../rq2_feasibility.txt` (Chinese) and `../rq2_feasibility_en.txt`
(English) for the full RQ1 chart summary and two RQ2 case studies.
