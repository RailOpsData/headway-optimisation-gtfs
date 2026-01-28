# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
from pathlib import Path
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = ['Noto Sans CJK JP', 'IPAPGothic', 'sans-serif']

p = Path("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stop_times.txt")
st = pl.scan_csv(str(p), has_header=True)

# Safe conversion of departure_time (use str.slice to avoid producing List-typed columns).
departure_seconds_expr = (
	pl.col("departure_time").str.slice(0, 2).cast(pl.Int64) * 3600
	+ pl.col("departure_time").str.slice(3, 2).cast(pl.Int64) * 60
	+ pl.col("departure_time").str.slice(6, 2).cast(pl.Int64)
).alias("departure_time_seconds")

# when trip_id follows the format weekday_startstation_routeid_...,
# extract the 3rd underscore-delimited field with a regex and store it as route_id_extracted.
route_extracted = pl.col("trip_id").str.extract(r'^[^_]+_[^_]+_([^_]+)').alias("route_id_extracted")


df_lazy = (
	st.with_columns([departure_seconds_expr, route_extracted])
	  .sort(["route_id_extracted", "stop_id", "departure_time_seconds"])
	  .with_columns([
		  pl.col("departure_time_seconds").diff().over(["route_id_extracted", "stop_id"]).alias("headway_s")
	  ])
)

stats = (
	df_lazy.group_by(["route_id_extracted", "stop_id"]).agg([
		pl.col("headway_s").mean().alias("mean_headway_s"),
		pl.col("headway_s").median().alias("median_headway_s"),
		pl.col("headway_s").std().alias("std_headway_s"),
		(pl.col("headway_s").std() / pl.col("headway_s").mean()).alias("cv_headway")
	])
).collect()

# %%
from matplotlib import font_manager as fm, rcParams

font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"  # fc-list 出力に合わせる

fm.fontManager.addfont(font_path)
fm.fontManager = fm.FontManager()
rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Noto Sans CJK JP', 'IPAPGothic', 'DejaVu Sans']

# %%
stats_pd = stats.to_pandas()

# Compute per-route averages (mean of per-stop means) and plot as a bar chart.
route_mean = stats_pd.groupby("route_id_extracted", as_index=False)["mean_headway_s"].mean().sort_values("mean_headway_s", ascending=False)

import seaborn as sns, matplotlib.pyplot as plt
sns.set_theme(style="whitegrid")
plt.figure(figsize=(10,6))
sns.barplot(data=route_mean, x="route_id_extracted", y="mean_headway_s", palette="viridis")
plt.xticks(rotation=45, ha="right")
plt.ylabel("Mean headway (s)")
plt.tight_layout()
plt.savefig("/app/results/GTFS_jp_mean_headway_per_route.png", dpi=200)

# %%
plt.figure(figsize=(10,6))
sns.barplot(data=route_mean, x="route_id_extracted", y="mean_headway_s", palette="viridis")
plt.xticks(rotation=45, ha="right")
plt.ylabel("Mean headway (s)")
plt.tight_layout()
