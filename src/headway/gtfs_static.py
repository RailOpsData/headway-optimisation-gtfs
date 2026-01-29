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

# %% jupyter={"source_hidden": true}
# !pip install fonttools
from matplotlib import font_manager
from pathlib import Path
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns

# # DejaVu Sans のファイルパス
# print(font_manager.findfont("DejaVu Sans"))

# インストール済みフォント名の例表示（フィルタ）
# print([f.name for f in font_manager.fontManager.ttflist if "Noto" in f.name or "IPA" in f.name][:20])



# # 再構築（必要なら）
# import matplotlib.font_manager as fm
# print(fm.findfont("Noto Sans CJK JP"))

# fm.fontManager.addfont("/opt/conda/lib/python3.12/site-packages/matplotlib/mpl-data/fonts/ttf/DejaVuSans.ttf")
# plt.rcParams["font.family"] = "IPAexGothic"   # ttf 内のフォント名
# plt.rcParams["axes.unicode_minus"] = False
# 簡易テスト
# plt.text(0.5,0.5,"系統3003-5-1", ha="center", fontsize=24)
# plt.axis("off")
# plt.savefig("test_font.png")

# # 「系統」に原因がないかチェック
# from fontTools.ttLib import TTFont
# path = "/opt/conda/lib/python3.12/site-packages/matplotlib/mpl-data/fonts/ttf/DejaVuSans.ttf"
# font = TTFont(path)
# cmap = font["cmap"].getBestCmap()
# for ch in "系統":
#     print(ch, hex(ord(ch)), ord(ch) in cmap)

# def has_chars(fontpath, chars):
#     cmap = TTFont(fontpath)["cmap"].getBestCmap()
#     return all(ord(c) in cmap for c in chars)

# # 系統が見つかるフォント検索
chars = "系統"
for fp in fm.findSystemFonts():
    try:
        if has_chars(fp, chars):
            print("FOUND", fp)
            break
    except Exception:
        continue

# 再度テスト       
from matplotlib.font_manager import FontProperties
plt.rcParams["font.family"] = FontProperties(fname="/usr/share/fonts/opentype/ipafont-mincho/ipam.ttf").get_name()
plt.text(0.5,0.5,"系統3003-5-1", ha="center", fontsize=24)
plt.axis("off")
plt.savefig("test_font.png")

plt.rcParams["axes.unicode_minus"] = False

# fontの.ttfファイルを設定
fp = FontProperties(fname="/usr/share/fonts/opentype/ipafont-mincho/ipam.ttf")
font_name = fp.get_name()

# 全体設定（seaborn + matplotlib）
plt.rcParams["font.family"] = font_name
plt.rcParams["axes.unicode_minus"] = False
sns.set_theme(rc={"font.family": font_name})

# %% [markdown]
# # 1. GTFS-JP_stop-times.txtからheadway計算(1日)

# %% [markdown]
# ## 1.1. 平均、中央値、標準偏差、CV（変動係数＝標準偏差/平均値）の算出

# %%
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

# %% [markdown]
# ## 1.2. 可視化と保存

# %%

stats_pd = stats.to_pandas()
metrics = ["mean_headway_s", "median_headway_s", "std_headway_s", "cv_headway"]

for m in metrics:
    
    fig, ax = plt.subplots(figsize=(10,6))
    sns.barplot(data=stats_pd, x="route_id_extracted", y=m, palette="viridis", ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), fontproperties=fp, rotation=45, ha="right")
    ax.set_ylabel(m.replace("_", " "))
    plt.tight_layout()
    plt.savefig(f"/app/results/GTFS_jp_{m}_by_route.png", dpi=200)
    plt.close(fig)

# %% [markdown]
# # 2. GTFS-JP_stop-times.txtからheadway計算(1か月・2025年11月11日スタート)

# %%
import datetime, calendar

jst = datetime.timezone(datetime.timedelta(hours=9))
dt = datetime.datetime(2025, 11, 11, 0, 0, 0, tzinfo=jst)   # JST midnight
unix_s = int(dt.timestamp())   # 秒
unix_ms = unix_s * 1000
unix_us = unix_s * 1_000_000

print(dt)

# %% editable=true slideshow={"slide_type": ""} jupyter={"outputs_hidden": true}
from pathlib import Path
from datetime import date, timedelta
import polars as pl

# 入力CSV
INPUT = Path("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stop_times.txt")
OUT_DIR = Path("/app/data/bronze/chitetsu_tram/gtfs_jp_tram/")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MICROS_PER_SECOND = 1_000_000
MICROS_PER_DAY = 86_400 * MICROS_PER_SECOND # 86_400_000_000

# 日付範囲（例: 2025-11-11 .. 2025-12-19）
START = date(2025, 11, 11)
END = date(2025, 12, 19)
n_days = (END - START).days + 1
dates = [(START + timedelta(days=i)).isoformat() for i in range(n_days)]

# Lazy scan
st = pl.scan_csv(str(INPUT), has_header=True)

# parse h,m,s and compute day_offset and seconds modulo 24h
h = pl.col("departure_time").str.slice(0,2).cast(pl.Int64)
m = pl.col("departure_time").str.slice(3,2).cast(pl.Int64)
s = pl.col("departure_time").str.slice(6,2).cast(pl.Int64)
day_offset = (h >= 24).cast(pl.Int64)
secs_mod = ((h % 24) * 3600) + (m * 60) + s

# DataFrame of dates to cross-join
dates_ldf = pl.DataFrame({"file_date": dates}).lazy()

expanded = (
    st.with_columns([
        h.alias("_h"),
        m.alias("_m"),
        s.alias("_s"),
        day_offset.alias("day_offset"),            # 0 or 1 (days)
        secs_mod.alias("departure_seconds_mod")   # seconds modulo 24h (int)
    ])
    .join(dates_ldf, how="cross")
    # 1) file_date -> epoch microseconds (create alias first)
      .with_columns([
          (pl.col("file_date").str.strptime(pl.Date, "%Y-%m-%d").cast(pl.Datetime("us")).cast(pl.Int64) + pl.col("day_offset") * MICROS_PER_DAY).alias("service_date_epoch_us"),
          (pl.col("file_date").str.strptime(pl.Date, "%Y-%m-%d").cast(pl.Datetime("us")).cast(pl.Int64) + pl.col("day_offset") * MICROS_PER_DAY + pl.col("departure_seconds_mod") * MICROS_PER_SECOND).alias("departure_epoch_us")
      ])
      .with_columns([
          # service_date string (YYYYMMDD) based on service_date_epoch_us
          (pl.col("service_date_epoch_us").cast(pl.Datetime("us")).dt.strftime("%Y%m%d").alias("service_date_ymd")),
          (pl.col("departure_epoch_us").cast(pl.Datetime("us")).alias("departure_ts"))
        # departure timestamp as Datetime("us")
      ])
)

# drop helper columns if desired
ex = expanded.select([
        pl.exclude(["_h","_m","_s","departure_seconds_mod","file_date_epoch_us","service_date_epoch_us"]),
    ])

# Collect (note: may be large for very big schedules; adjust memory strategy if needed)
df = ex.collect()

# iterate groups by service_date_ymd and write per-day parquet files
for (service_date,), group in df.sort("service_date_ymd").group_by("service_date_ymd", maintain_order=True):
    out_file = OUT_DIR / f"stop_{service_date}.parquet"
    group.drop("service_date_ymd").write_parquet(out_file, compression="snappy")
    print("wrote", out_file)




# %%
df_all_read = pl.read_parquet(OUT_DIR / "stop_*.parquet")
print(df_all_read.head(5))

df_all = pl.scan_parquet(OUT_DIR / "stop_*.parquet")



# # 運行日と出発時刻でソート
# st = df_all.sort(["file_date", "departure_ts"])
# # print(df_sorted.tail(5))

# # Safe conversion of departure_time (use str.slice to avoid producing List-typed columns).
# departure_seconds_expr = (
# 	pl.col("departure_time").str.slice(0, 2).cast(pl.Int64) * 3600
# 	+ pl.col("departure_time").str.slice(3, 2).cast(pl.Int64) * 60
# 	+ pl.col("departure_time").str.slice(6, 2).cast(pl.Int64)
# ).alias("departure_time_seconds")

# # when trip_id follows the format weekday_startstation_routeid_...,
# # extract the 3rd underscore-delimited field with a regex and store it as route_id_extracted.
# route_extracted = pl.col("trip_id").str.extract(r'^[^_]+_[^_]+_([^_]+)').alias("route_id_extracted")


# df_lazy = (
# 	st.with_columns([departure_seconds_expr, route_extracted])
# 	  .sort(["route_id_extracted", "stop_id", "departure_time_seconds"])
# 	  .with_columns([
# 		  pl.col("departure_time_seconds").diff().over(["route_id_extracted", "stop_id"]).alias("headway_s")
# 	  ])
# )

# print(df_lazy.slice(10,5).collect())

# stats = (
# 	df_lazy.group_by(["route_id_extracted", "stop_id"]).agg([
# 		pl.col("headway_s").mean().alias("mean_headway_s"),
# 		pl.col("headway_s").median().alias("median_headway_s"),
# 		pl.col("headway_s").std().alias("std_headway_s"),
# 		(pl.col("headway_s").std() / pl.col("headway_s").mean()).alias("cv_headway")
# 	])
# ).collect()

# %%
stats_pd = stats.to_pandas()
metrics = ["mean_headway_s", "median_headway_s", "std_headway_s", "cv_headway"]

for m in metrics:
    
    fig, ax = plt.subplots(figsize=(10,6))
    sns.barplot(data=stats_pd, x="route_id_extracted", y=m, palette="viridis", ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), fontproperties=fp, rotation=45, ha="right")
    ax.set_ylabel(m.replace("_", " "))
    plt.tight_layout()
    plt.savefig(f"/app/results/GTFS_jp_40days_{m}_by_route.png", dpi=200)
    plt.close(fig)

# %% [markdown]
# # 3. GTFS-rtの開始時刻をそろえてsnapshot_tsとvehicle_idをキーにしやすくする

# %%
import polars as pl
from pathlib import Path



# %%

p = Path("/app/data/raw/chitetsu_tram/trip_updates/20251111.parquet")
# cheap way: read only the schema / small sample
df_sample = pl.read_parquet(str(p), columns=None)   # reads metadata + columns

# print(df_sample.columns)
# print(df_sample.dtypes)


# # sliceで概略を表示
# lf = pl.scan_parquet(str(p))
# window = lf.sort(["snapshot_ts", "vehicle_id"]).slice(22000, 30).collect()
# print(window.tail(30)["snapshot_ts","vehicle_id"])


# 時刻を秒にして比較 (05:25 = 5*3600 + 25*60)
threshold_seconds = 5 * 3600 + 25 * 60
# sec_of_day_expr = (
#     pl.col("snapshot_ts").dt.hour() * 3600
#     + pl.col("snapshot_ts").dt.minute() * 60
#     + pl.col("snapshot_ts").dt.second()
# ).alias("sec_of_day")


sec_of_day_expr = (
    ((pl.col("snapshot_ts").cast(pl.Int64) // 1_000_000) % 86400)
    .cast(pl.Int32)   # or Int64
).alias("sec_of_day")

# フィルタしてソート、必要なら slice を追加してウィンドウを取る
res = (
    lf.with_columns(sec_of_day_expr)
      .filter(pl.col("sec_of_day") >= threshold_seconds)
      .sort(["snapshot_ts", "vehicle_id"])
      # .slice(0, 10)   # 必要ならここで部分抽出
      .collect()
)




# print(res.dtypes)
window = res.sort(["snapshot_ts", "vehicle_id"]).head(30)
print(window.slice(10,5)["snapshot_ts","vehicle_id"])

# %%

p = Path("/app/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet")
# cheap way: read only the schema / small sample
df_sample = pl.read_parquet(str(p), columns=None)   # reads metadata + columns

# print(df_sample.columns)
# print(df_sample.dtypes)


# # sliceで概略を表示
# lf = pl.scan_parquet(str(p))
# window = lf.sort(["snapshot_ts", "vehicle_id"]).slice(22000, 30).collect()
# print(window.tail(30)["snapshot_ts","vehicle_id"])


# 時刻を秒にして比較 (05:25 = 5*3600 + 25*60)
threshold_seconds = 5 * 3600 + 25 * 60
# sec_of_day_expr = (
#     pl.col("snapshot_ts").dt.hour() * 3600
#     + pl.col("snapshot_ts").dt.minute() * 60
#     + pl.col("snapshot_ts").dt.second()
# ).alias("sec_of_day")


sec_of_day_expr = (
    ((pl.col("snapshot_ts").cast(pl.Int64) // 1_000_000) % 86400)
    .cast(pl.Int32)   # or Int64
).alias("sec_of_day")

# フィルタしてソート、必要なら slice を追加してウィンドウを取る
res = (
    lf.with_columns(sec_of_day_expr)
      .filter(pl.col("sec_of_day") >= threshold_seconds)
      .sort(["snapshot_ts", "vehicle_id"])
      # .slice(0, 10)   # 必要ならここで部分抽出
      .collect()
)




# print(res.dtypes)
window = res.sort(["snapshot_ts", "vehicle_id"]).head(30)
print(window.slice(10,5)["snapshot_ts","vehicle_id"])

# %%
