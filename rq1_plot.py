# -*- coding: utf-8 -*-
"""RQ1 chart: grouped bar chart of calibration error by category (T-7 vs T-1)."""
import os

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

df = pd.read_csv(os.path.join(os.environ.get("DATA_DIR", "./data"),
                              "calibration_final.csv"))
cats = ["sports", "other", "crypto", "politics"]
df = df.set_index("category").loc[cats]

x = range(len(cats))
w = 0.36
fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
b1 = ax.bar([i - w/2 for i in x], df["avg_error_7d"], w,
            label="T-7", color="#4C72B0")
b2 = ax.bar([i + w/2 for i in x], df["avg_error_1d"], w,
            label="T-1", color="#DD8452")

for bars in (b1, b2):
    for b in bars:
        ax.text(b.get_x() + b.get_width()/2, b.get_height() + 0.004,
                f"{b.get_height():.3f}", ha="center", va="bottom", fontsize=9)

ax.set_xticks(list(x))
ax.set_xticklabels([c.capitalize() for c in cats])
ax.set_ylabel("Mean calibration error  |price − y|")
ax.set_xlabel("Category")
ax.set_title("RQ1: Calibration error by category (Polymarket, v2 era)")
ax.set_ylim(0, max(df["avg_error_7d"].max(), df["avg_error_1d"].max()) * 1.18)
ax.legend()
ax.grid(axis="y", alpha=0.3, linestyle="--")
fig.tight_layout()
out = os.path.join(os.environ.get("OUT_DIR", "./out"),
                   "rq1_error_by_category.png")
os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
fig.savefig(out)
print("saved:", out)
