# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (gtfs-sumo-rl-2026-1)
#     language: python
#     name: gtfs-sumo-rl
# ---

# %% [markdown]
# # 0. config

# %%
#1
import polars as pl
import plotly.express as px
import pandas as pd

#1.3 
# import polars as pl
from pathlib import Path

#2
# import polars as pl
import folium
from folium.plugins import HeatMap

import plotly.express as px
import plotly.graph_objects as go

import plotly.io as pio
print(pio.renderers.default)
pio.renderers.default = "iframe"
print(pio.renderers.default)

# from datetime import datetime, timedelta

pl.Config.set_tbl_rows(-1)
# pl.Config.set_tbl_cols(-1)

#3 
import datetime as dt
import matplotlib.pyplot as plt

#4 
from keplergl import KeplerGl



# %% [markdown]
# # 1. vehicle_id categorisation 
#
#

# %% [markdown]
# ## 1.1. singl file (1day)

# %% [markdown]
# ### 1.1.1. tram_vehicle_position 

# %%

p_tram_vp_04 = "~/adaptive-signal-open-data/data/bronze/chitetsu_tram/vehicle_positions_/20251104.parquet"

df = pl.read_parquet(p_tram_vp_04)
print(df.schema)
# print(df.shape)
# display(df[1500:2000].to_pandas())
display(df.head(3).to_pandas())




# %% [markdown]
# ```text
# unique_values = df[col_name].unique() 
# print(unique_values)
# ```

# %%

display(df.head(3))
unique_values = df["vehicle_id"].unique()
print(unique_values)


# %% [markdown]
# ### 1.1.2. tram_trip_update

# %%
p_tram_tu_14 = "~/adaptive-signal-open-data/data/bronze/chitetsu_tram/trip_updates/20251114.parquet"
df = pl.read_parquet(p_tram_tu_14)
display(df.head(3))
unique_values = df["vehicle_id"].unique()
print(unique_values)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 1.2 multipul files (all days)

# %% [markdown]
# ### 1.2.1 tram/trip_updates

# %%

df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/trip_updates/*.parquet")
print(df.shape)
display(df[110000:110005].to_pandas())
print(df.schema)



# %% [markdown]
# ### 1.2.2 tram/vehicle_positions

# %%

# df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/vehicle_positions/*.parquet")
df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/trip_updates/*.parquet")
print(df.shape)
display(df[110000:110005].to_pandas())
print(df.schema)


# %% [markdown]
# ### 1.2.3 bus/vehicle_position

# %%

# df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/vehicle_positions/*.parquet")
# df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/trip_updates/*.parquet")
df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_bus/vehicle_positions/*.parquet")
print(df.shape)
display(df[110000:110005].to_pandas())
print(df.schema)


# %% [markdown]
# ### 1.2.4 bus/trip_updates

# %%

df1 = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/vehicle_positions/*.parquet")
df2 = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/trip_updates/*.parquet")
df3 = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_bus/vehicle_positions/*.parquet")
df4 = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_bus/trip_updates/*.parquet")

print(df4.shape)
display(df4[110000:110005].to_pandas())
print(df4.schema)


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 1.3 download combined files

# %% [markdown]
# ### 1.3.1 tram

# %%
target_dir = Path.home() / "adaptive-signal-open-data" / "data" / "bronze" / "chitetsu_tram" / "combined_tram_vp"
target_dir.mkdir(parents=True, exist_ok=True)
# constructs the path object in memory.
output_path = target_dir / "combined_tram_vp.parquet"

df1.write_parquet(output_path)

display(df1.head(3))

# %% [markdown]
# ### 1.3.2 bus

# %%
target_dir = Path.home() / "adaptive-signal-open-data" / "data" / "bronze" / "chitetsu_bus" / "combined_bus_vp"
target_dir.mkdir(parents=True, exist_ok=True)
# constructs the path object in memory.
output_path = target_dir / "combined_bus_vp.parquet"

df3.write_parquet(output_path)

display(df3.head(3))
display(df3.schema)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # 2. Map visualization (failed)

# %%
df = pl.read_parquet("~/adaptive-signal-open-data/data/bronze/chitetsu_tram/combined_tram_vp/combined_tram_vp.parquet")

df_plot = df.with_columns([
    pl.col("lat").alias("latitude"),
    pl.col("lon").alias("longitude")
]).drop_nulls(["latitude", "longitude"])

# time-series map animation.
pdf = df_plot.sort("snapshot_ts").head(1000).to_pandas()
fig = px.scatter_geo(
    pdf,
    lat="latitude", lon="longitude",
    color="vehicle_id",
    animation_frame="snapshot_ts",
    animation_group="vehicle_id",
    hover_data=["route_id", "speed"],
    projection="natural earth",
    title="Vehicles (sample)"
)
fig.update_geos(lataxis_range=[36.4, 37.1], lonaxis_range=[136.9, 137.7])
fig.update_layout(height=700)

fig.show()
# 再生ボタン/スライダーで時間進行確認可能。​

# 軌跡線付き（過去位置保持）
# python
# fig = px.line_mapbox(
#     df_plot.to_pandas().sort(["vehicle_id", "snapshot_ts"]),
#     lat="latitude", lon="longitude",
#     color="vehicle_id",
#     animation_frame="snapshot_ts",
#     title="Bus Trajectories Over Time"
# fig.show()
# 大規模データ最適化

# python
# # 1秒間隔リサンプリング（10万行→数千行）
# df_sampled = df_plot.group_by_dynamic(
#     "snapshot_ts", every="1s", by="vehicle_id"
# ).agg([
#     pl.col("latitude").first(),
#     pl.col("longitude").first(),
#     pl.col("speed").mean()
# ])

# px.scatter_mapbox(df_sampled.to_pandas(), 
#                   lat="latitude", lon="longitude",
#                   animation_frame="snapshot_ts",
#                   color="vehicle_id")

# %% [markdown]
# <b>bash</b>
# ````
# explorer.exe .
# ````
# でフォルダを開く

# %% [markdown]
# # 3 Filter dataset to tram service hours (05:30–23:59)
#

# %% [markdown]
# ## 3.1 read filtered dataset
#

# %%
# parquet読み込み → snapshot_ts の型確認・変換・05:30-23:59でフィルタ → 可視化準備

p = Path.home() / 'adaptive-signal-open-data' / 'data' / 'bronze' / 'chitetsu_tram' / 'combined_tram_vp' / 'combined_tram_vp.parquet'
df = pl.read_parquet(str(p))
print('total rows:', df.height)
print('snapshot_ts schema:', df.schema.get('snapshot_ts'))

# 場合分けで安全に Datetime に変換: まず速い str.strptime を試し、ダメなら pandas.to_datetime を使う
t = df.schema.get('snapshot_ts')
if t != pl.Datetime:
    try:
        df = df.with_columns(pl.col('snapshot_ts').str.strptime(pl.Datetime, strict=False).alias('snapshot_ts'))
        print('parsed snapshot_ts with pl.str.strptime')
    except Exception as e:
        print('str.strptime failed, falling back to pandas parser:', e)
        df = df.with_columns(pl.col('snapshot_ts').apply(lambda x: pd.to_datetime(x, errors='coerce'), return_dtype=pl.Datetime).alias('snapshot_ts'))
        print('parsed snapshot_ts with pandas.to_datetime via apply')
else:
    print('snapshot_ts already Datetime')

# parsing に失敗した行を削除しておく
n_before = df.height
df = df.filter(pl.col('snapshot_ts').is_not_null())
n_after = df.height
print(f'rows after dropping unparsable snapshot_ts: {n_after} (removed {n_before-n_after})')

# 時刻フィルタ（各日の 05:30〜23:59）
hour = pl.col('snapshot_ts').dt.hour()
minute = pl.col('snapshot_ts').dt.minute()
start_cond = (hour > 5) | ((hour == 5) & (minute >= 30))
end_cond = (hour <= 23)
df_filtered = df.filter(start_cond & end_cond)
print('rows after time filter:', df_filtered.height)

# 緯度経度整備と表示サンプル
df_plot = df_filtered.with_columns([pl.col('lat').alias('latitude'), pl.col('lon').alias('longitude')]).drop_nulls(['latitude','longitude'])
display(df_plot.head(5).to_pandas())
# df_plot を使って可視化や保存に進めます

display(df_plot.schema)


# %%
# data visualization

pdf = df_plot.to_pandas()

# snapshot_ts を pandas の datetime に（念のため）
pdf["snapshot_ts"] = pd.to_datetime(pdf["snapshot_ts"])

# 1分ごとの件数を集計
counts = (
    pdf.set_index("snapshot_ts")
       .resample("1min")
       .size()
)

# プロット
plt.figure(figsize=(10, 4))
counts.plot()
plt.xlabel("time")
plt.ylabel("count per minute")
plt.title("Number of records per minute")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 3.2. Map visualization (kepler gl)

# %% [markdown]
# ### 3.2.1. All date

# %%
df_plot = df_filtered.select([
    pl.col('lat'),
    pl.col('lon'),
    pl.col('snapshot_ts'),
    pl.col('vehicle_id'),
    pl.col('route_id'),
]).drop_nulls(['lat', 'lon'])

pdf = df_plot.to_pandas()
pdf = pdf.sample(n=20000, random_state=0)
# Unix timestamp (ミリ秒) を別列として追加
pdf['timestamp'] = pd.to_datetime(pdf['snapshot_ts']).astype('int64') // 10**6

# 元の snapshot_ts は文字列に変換（HTML保存用）
pdf['snapshot_ts'] = pdf['snapshot_ts'].astype(str)


m = KeplerGl(height=600)
m.add_data(data=pdf, name="vehicles")
m.save_to_html(file_name='kepler_map.html')
print("Saved to kepler_map.html")

# %% [markdown]
# <b>bash</b>
# ````
# explorer.exe .
# ````
# でフォルダを開く

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 3.2.2. 1day (Config後単独実行可)

# %%
# combined parquet から読み込み
p = Path.home() / 'adaptive-signal-open-data' / 'data' / 'bronze' / 'chitetsu_tram' / 'combined_tram_vp' / 'combined_tram_vp.parquet'
df = pl.read_parquet(str(p))
print('total rows:', df.height)

# snapshot_ts を Datetime に変換
t = df.schema.get('snapshot_ts')
if t != pl.Datetime:
    df = df.with_columns(pl.col('snapshot_ts').str.strptime(pl.Datetime, strict=False).alias('snapshot_ts'))

df = df.filter(pl.col('snapshot_ts').is_not_null())

# 11/10のデータのみに絞り込み
df = df.filter(
    (pl.col('snapshot_ts').dt.year() == 2025) &
    (pl.col('snapshot_ts').dt.month() == 11) &
    (pl.col('snapshot_ts').dt.day() == 10)
)
print('rows after date filter (11/10):', df.height)

# 時刻フィルタ（05:30〜23:59）
hour = pl.col('snapshot_ts').dt.hour()
minute = pl.col('snapshot_ts').dt.minute()
start_cond = (hour > 5) | ((hour == 5) & (minute >= 30))
end_cond = (hour <= 23)
df_filtered = df.filter(start_cond & end_cond)
print('rows after time filter:', df_filtered.height)

# Kepler.gl用に準備
df_plot = df_filtered.select([
    pl.col('lat'),
    pl.col('lon'),
    pl.col('snapshot_ts'),
    pl.col('vehicle_id'),
    pl.col('route_id'),
]).drop_nulls(['lat', 'lon'])

pdf = df_plot.to_pandas()

# Unix timestamp (ミリ秒) を別列として追加
pdf['timestamp'] = pd.to_datetime(pdf['snapshot_ts']).astype('int64') // 10**6
# snapshot_ts を文字列に変換（JSON serializable にする）
pdf['snapshot_ts'] = pdf['snapshot_ts'].astype(str)


m = KeplerGl(height=600)
m.add_data(data=pdf, name="vehicles_20251110")
m.save_to_html(file_name='kepler_map_20251110_ver2.html')
print(f"Saved to kepler_map_20251110_ver2.html ({len(pdf)} records)")

# %% [markdown]
# <b>bash</b>
# ````
# explorer.exe .
# ````
# でフォルダを開く

# %% [markdown]
# # 4. All dataset (1month)

# %% [markdown]
# ## 4.1. parquet(before canonicalize)

# %%
p = Path.home() / 'adaptive-signal-open-data' / 'data' / 'raw' / 'parquet' / 'prod-gtfs-latest_raw_gtfs-rt_data_9.parquet'
df = pl.read_parquet(str(p))
print('total rows:', df.height)


# %% [markdown]
# <b> Python </b>
# ```
# tar2parquet.py
# ```
# が正規化する前のjsonファイルだったためその修正が必要

# %%
p_tram_vp_1201 = "/home/koki_deutsch/adaptive-signal-open-data/data/bronze/prod_tram/vehicle_positions/20251201.parquet"
df = pl.read_parquet(p_tram_vp_1201)
print(df.schema)
print(df.shape)

pdf = df.to_pandas()

# snapshot_ts を pandas の datetime
pdf["snapshot_ts"] = pd.to_datetime(pdf["snapshot_ts"])

# 1分ごとの件数を集計
counts = (
    pdf.set_index("snapshot_ts")
       .resample("1min")
       .size()
)

# plot
plt.figure(figsize=(10, 4))
counts.plot()
plt.xlabel("time")
plt.ylabel("count per minute")
plt.title("Number of records per minute")
plt.tight_layout()
plt.show()

sorted_df = df.sort("snapshot_ts")
display(sliced_df.head(100).to_pandas())


# %%
