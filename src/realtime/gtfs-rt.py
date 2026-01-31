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
from datetime import date as dt
import polars as pl
pl.Config.set_tbl_rows(1000)

# %% [markdown]
# # 1. vehicle_positionsの読み込み

# %% [markdown]
# ## 1.1. データ前処理
#    - 車両idと取得時間で並べ替え
#    - 取得時間6～9時台

# %%

df = pl.read_parquet("/app/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet")

df = df.select(
    pl.all().exclude([s.name for s in df if s.null_count() == len(df)]))
df = df.select(
    pl.exclude(["bearing","speed"]))
df = df.sort(df["vehicle_id","snapshot_ts"]).filter(pl.col("snapshot_ts").dt.hour().is_between(6, 9))\
    # .head(1000)
df


# %% [markdown]
# ## 1.2. vehicle_idの種類と個数確認

# %%
df.shape
print(df["vehicle_id"].value_counts())

# %% [markdown]
# # 2. trip_updates読み込み

# %% [markdown]
# ## 2.0. （GTFS-JP）route_idから富山大学前にくるものを抽出

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.1. 静的時刻表データ(stop_times.txt)からtrip_idとstop_idを抽出

# %%
df_static = pl.read_csv("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stop_times.txt").select(["trip_id","stop_headsign","stop_id","pickup_type","drop_off_type","timepoint","arrival_time","departure_time","stop_sequence"])
df_static

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.2. 駅データ(stops.txt)から駅ナンバー(stop_id)と駅名(stop_name)を抽出

# %%
df_static_route = pl.read_csv("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt").select(["stop_id","stop_name"])
print(df_static_route)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.3. 駅データ（stops.txt）の駅ナンバー(stop_id)と駅名(stop_name)が重複していないか確認

# %%
print(df_static_route.group_by(["stop_id","stop_name"]).len())

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.4. 静的時刻表データ(stop_times.txt)と駅データ(stops.txt)の結合操作(2.0.1更新時必ず実行)
# - 静的時刻表データ(stops_times.txt)に
# - 駅データ(stops.txt)を追加するために
# - 停留所ID(stop_id)をキーとして結合<br><br>
# **（静的時刻表データにある停留所を名前で表示）**

# %%
df_stops = df_static.join(
    df_static_route,
    on = "stop_id",
    how = "left"
)
df_stops

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.5. 結合した静的時刻表データ（駅名付き）(df_stop)の区間を駅名で絞り込み（駅名が諏訪川原-富山大学前間)
# 方針：**合流のない区間を選択（諏訪川原-富山大学前）し、そこを通るroute_idを選定し、合流ふくむ区間も含めて全体（富山駅～富山大学前）で調査する**<br>
# 意図：合流区間を含むとその区間を通過する別ルート(route_id)も含まれてしまう

# %%
df_stops_daigakumae = df_stops.filter(
    pl.col("stop_name").is_in([
        # "富山駅",
        # "新富町",
        # "県庁前",
        # "丸の内",
        "諏訪川原",
        "安野屋",
        "トヨタモビリティ富山Gスクエア五福前",
        "富山大学前"
    ])
)
df_stops_daigakumae

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.6. 結合された静的時刻表データ（駅名付きかつ、対象区間絞り込み済）(df_stop)のtrip_idから運行曜日ならびにroute_idを抽出
# #### 運行曜日
# ラッシュ時間を検討するためには平日のデータだけが必要
# #### route_id
# 富山地鉄軌道線・富山港線においてはtrip_idにroute_idが含まれた形式になっているため：2.0.7.でその組み合わせと行先を抽出

# %%
df_extracted = df_stops_daigakumae.with_columns([
    pl.col("trip_id").str.extract(r"^([^_%]+)", 1).alias("service_day"),
    pl.col("trip_id").str.extract(r"系統(.*)$", 1).alias("route_id")
])
df_extracted

# %%
print(df_extracted["route_id"].unique())

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.7. route_idごとの行先表示と個数を確認

# %%
df_counts = df_extracted.group_by(["route_id", "stop_headsign"]).len()
df_counts

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.8. (Appendix)駅名、出発時間のこの順番でソートし、各駅の時刻表を作成

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 2.1. （GTFS-rt,trip_updates）データ前処理
#    - 車両idと取得時間で並べ替え
#    - 取得時間6～9時台
#    - **route_idで富山大学前発着のみ絞り込み <- trip_updatesのみに含まれる**
#    - 取得時間、route_id、車両IDのみ表示

# %%
df_t = pl.read_parquet("/app/data/raw/chitetsu_tram/trip_updates/20251111.parquet")
df_t = df_t.select(
    pl.all().exclude([s.name for s in df_t if s.null_count() == len(df_t)]))

df_t = df_t.select(
    pl.exclude(["start_time","start_date"]))
df_t = df_t.sort(df_t["vehicle_id","snapshot_ts"])\
    .filter(pl.col("snapshot_ts").dt.hour().is_between(6, 9))\
    .filter(pl.col("route_id").is_in(["市内軌道線(3001-2-1)","市内軌道線(3001-2-2)" #南富山発着
                                      ,"富山港線（富山大学前）(3003-5-2)","富山港線（富山大学前）(3003-5-1)" #岩瀬浜発着
                                      ,"市内軌道線(3001-8-1)","市内軌道線(3001-8-2)" #富山駅発着
                                      ,"富山港線（富山大学前）(3003-5-2-1)","富山港線（富山大学前）(3003-5-1-1)" #越中中島発着
                                                   ]))\
    .select(pl.col(["snapshot_ts","route_id","vehicle_id"]))
# df_t.filter(pl.col("vehicle_id")=="chitetsu_tram_4983")
df_t #(9806,3)件

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 2.2. 確認　
# - value_countsメソッドでroute_idと車両IDの種類確認
# - (**df_t**:trip_updates,**df_filtered**:vehicle_positions)

# %%
print(df_t["route_id"].value_counts())
print(df_t["vehicle_id"].value_counts().sort("vehicle_id"))
print(df_filtered["vehicle_id"].value_counts().sort("vehicle_id")) #(3.1.実行後に比較のため追加)
# print(df_t["snapshot_ts"].value_counts())

# %% [markdown]
# # 3. vehicle_id追加処理および重複カウントと削除

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.1. "2.2."で確認した車両IDのみにfilter:

# %%
target_vehicles = [
    "chitetsu_tram_4980", "chitetsu_tram_4983", "chitetsu_tram_4984", 
    "chitetsu_tram_4985", "chitetsu_tram_4986", "chitetsu_tram_4987", 
    "chitetsu_tram_4989", "chitetsu_tram_4990", "chitetsu_tram_4991", 
    "chitetsu_tram_4992", "chitetsu_tram_4993", "chitetsu_tram_4995", 
    "chitetsu_tram_5001", "chitetsu_tram_5004", "chitetsu_tram_5005", 
    "chitetsu_tram_5006", "chitetsu_tram_5011"
]

df_filtered = df.filter(
    pl.col("vehicle_id").is_in(target_vehicles)
)

df_filtered 

# %% [markdown] jp-MarkdownHeadingCollapsed=true
#

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.2. 重複している行をカウント・削除（vehicle_positions）
# - 取得時間(snapshot_ts)と車両ID(vehicle_id)でグループ化、各グループの数が1より大きいものを代入
#   - GTFSサーバー側への記録の問題で2種類記録されているが、そこまでは踏み込めないので省略
# - uniqueメソッドとサブセットで取得時間と車両IDが一致するものを削除

# %%
duplicate_count = (
    df_filtered.group_by(["snapshot_ts", "vehicle_id"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count)

# %%
df_v_cleaned = df_filtered.unique(subset=["snapshot_ts", "vehicle_id"],keep="first")
df_v_cleaned #(8532,10)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.3. 重複している行をカウント・削除（trip_updates）

# %%
duplicate_count_t = (
    df_t.group_by(["snapshot_ts", "vehicle_id"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count_t)

# %%
df_t_cleaned = df_t.unique(subset=["snapshot_ts", "vehicle_id"],keep="first")
df_t_cleaned #(7238,3)

# %% [markdown]
# # 4. vehicle_positionsにtrip_updateに含まれていたroute_id情報を結合

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 4.1. 結合処理
# - キーは取得時間と車両ID＝**同一時間の車両IDは一意**
# - この時点でroute_id=nullのもの
#   - 南富山方向の運用（route_id）
#   - trip_updateの取得失敗に起因するもの：車両IDと時系列の並べ替え後のデータにおいて、前後が一致すればそのroute_idで補完（4.3.）

# %%
df_vt = df_v_cleaned.join(
    df_t_cleaned,
    on=["snapshot_ts", "vehicle_id"],
    how="left"                           #ここでhow="inner"にしてしまうとtrip_updatesが記録できなかった時間が削除されてしまう
)
df_vt.sort(["vehicle_id","snapshot_ts"]) #(8532,11)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 4.2. route_id欠損値カウント・欠損率・前後の内容確認

# %%
print(df_vt["route_id"].null_count()) #1880件


# %%
rate = df_vt["route_id"].null_count() / len(df_vt)
print(f"route_idの欠損率: {rate:.2%}") #22.03%

# %% [markdown]
# ## 4.3. ルート情報の欠損値を補完or削除

# %% [markdown]
# ### 補完の前に車両ID、取得時間の順番でソート

# %%
df_vt_sorted = df_vt.sort(["vehicle_id","snapshot_ts"]) #sortしないと前後の行で時系列バラバラになる

# %% [markdown]
# route_idがnullの当該行とその前後1行に該当するものにfilter

# %%
df_check = df_vt_sorted.filter(
    pl.col("route_id").is_null() |           # 当該行がnull
    pl.col("route_id").shift(-1).over("vehicle_id").is_null() | # 次の行がnull（＝自分はnullの直前）
    pl.col("route_id").shift(1).over("vehicle_id").is_null()   # 前の行がnull（＝自分はnullの直後）
)
df_check.select(["current_stop_sequence","vehicle_id","route_id"]) #(2550,3)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 前後でroute_idが一致すればその値で補完：補完済データは**df_imputed**に代入される

# %%
df_imputed = df_vt_sorted.with_columns([
    pl.when(
        pl.col("route_id").is_null() & 
        (pl.col("route_id").forward_fill().over("vehicle_id") == 
         pl.col("route_id").backward_fill().over("vehicle_id"))
    )
    .then(pl.col("route_id").forward_fill().over("vehicle_id")) # 一致すればその前の値（＝後ろの値）で埋める
    .otherwise(pl.col("route_id"))                             # 一致しなければ元のまま
    .alias("route_id")
])
df_imputed #(8532,11)

# %% [markdown]
# route_idの欠損率を再計算

# %%
rate = df_imputed["route_id"].null_count() / len(df_imputed)
print(f"route_idの欠損率: {rate:.2%}") #(16.63%)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### ここまでで補完しきれなかったデータ
# 1. 各車両IDの一番初めの行と一番最後の行
# 2. 南富山方面への運用に入るもの（trip_updateの時点で除外しているため）
# <br>のどちらか。<br>
# df_chには最終的にnullの当該行とその前後1行が代入される

# %%
df_check2 = df_imputed.filter(
    pl.col("route_id").is_null() |           # 当該行がnull
    pl.col("route_id").shift(-1).over("vehicle_id").is_null() | # 次の行がnull（＝自分はnullの直前）
    pl.col("route_id").shift(1).over("vehicle_id").is_null()   # 前の行がnull（＝自分はnullの直後）
)
df_ch2 = df_check2.select(["current_stop_sequence","lat","lon","snapshot_ts","vehicle_id","route_id"])\
    # .filter(
    # ~pl.col("vehicle_id").eq("chitetsu_tram_4980"))
df_ch2.sort(pl.col(["vehicle_id","snapshot_ts"])) #(1496,6)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### ルート情報（route_id）が欠損しているまたはその前後1行の車両ID(vehicle_id)の数
# - 数十件あり：南富山方面を含む車両ID
# - 数件のみ：運用の折り返しの都合で前後ルート情報(route_id)が欠損している

# %%
df_ch2["vehicle_id"].value_counts().sort("vehicle_id") #(17,2)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### ルート情報（route_id）が欠損している行の車両ID(vehicle_id)の数ならびに該当する行

# %%
df_ch3 = df_imputed.filter(
    pl.col("route_id").is_null()).sort(pl.col(["vehicle_id","snapshot_ts"]))
df_ch3 #(1419,11)

# %%
df_ch3["vehicle_id"].value_counts().sort("vehicle_id") #(13,2)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### ルート情報(route_id)が欠損している行を削除

# %%
print(df_check2.schema)
df_clean = df_imputed.drop_nulls("route_id")
df_clean.select(["snapshot_ts","vehicle_id","lat","lon","current_stop_sequence","route_id"]) #(7113,6)

# %% [markdown]
# ## 4.4. 行先ふくむルート情報(route_id)をroute_id_nameに変更

# %%
df_clean_rn = df_clean.rename({"route_id":"route_id_name"})

# %% [markdown]
# ## 4.5. 行先ふくむルート情報(route_id_name)から数字のみのルート情報(route_id)を正規表現で抽出

# %% jupyter={"outputs_hidden": true, "source_hidden": true}
pattern = r"\(([\d\-]+)\)"

df_rt = df_clean_rn.with_columns(
    pl.col("route_id_name")
    .str.extract(pattern, 1)
    .alias("route_id")
)
df_rt.select(["snapshot_ts","vehicle_id","lat","lon","current_stop_sequence","route_id_name","route_id"]) #(7113,7)

# %% [markdown]
# # 5. GTFS-JPと結合(2.0.6.のroute_idが抽出された静的時刻表データを利用)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 5.1. (GTFS-JP)route_idの抽出・
# ルート情報(route_id)内、ルートにおける幾つめの停車駅か？(stop_sequence)、電停名(stop_name)の組み合わせの数

# %%
df_st =df_stops.with_columns([
    pl.col("trip_id").str.extract(r"^([^_%]+)", 1).alias("service_day"),
    pl.col("trip_id").str.extract(r"系統(.*)$", 1).alias("route_id")
])
df_st =df_st.unique(subset=["route_id","stop_sequence","stop_name"],keep="first")\
    .select(pl.col(["route_id","stop_sequence","stop_name"])).sort(pl.col(["route_id","stop_sequence"]))
df_st #(349,3)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 5.2. （GTFS-JP）富山駅から大学前までに絞り込み
# ルート(route_id)、ルートにおける幾つめの駅か？(stop_sequence)、駅名(stop_name)
# ここには別系統の環状線も含まれる

# %%
df_st_uni = df_st.filter(
    pl.col("stop_name").is_in([
        "富山駅",
        "新富町",
        "県庁前",
        "丸の内",
        "諏訪川原",
        "安野屋",
        "トヨタモビリティ富山Gスクエア五福前",
        "富山大学前"
    ])
)
df_st_uni #(85,3)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 5.3. GTFS-rt(convined)とGTFS-JPの結合(df_merged):"6.2."でも利用

# %%
df_merged = df_rt.join(
    df_st_uni,
    left_on=["route_id", "current_stop_sequence"],
    right_on=["route_id", "stop_sequence"],
    how="inner"
)
df_merged #(2954,13)

# %%
null_count=df_merged["stop_name"].null_count()
null_count #0

# %% [markdown]
# # 6. 緯度経度で最も近い距離を現在の距離として計算

# %% [markdown]
# ## 6.1. GTFS-JPから富山駅～富山大学前の緯度経度を抽出

# %%
uni = [
        "富山駅",
        "新富町",
        "県庁前",
        "丸の内",
        "諏訪川原",
        "安野屋",
        "トヨタモビリティ富山Gスクエア五福前",
        "富山大学前"
    ]

df_lat_lon = pl.read_csv("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt")\
    .select(pl.col(["stop_name","stop_lat","stop_lon"]))\
    .filter(pl.col("stop_name").is_in(uni))
df_lat_lon #(8,3)

# %% [markdown]
# ## 6.2. GTFS-rt("5.3.で前処理済のもの")から位置情報（緯度経度の組み合わせ）が一意なものを抽出

# %% jupyter={"outputs_hidden": true}
df_coords = df_merged.select(["lat", "lon"]).unique()
df_coords #(2477,2)

# %% [markdown]
# ## 6.3. realtimeデータの取得位置情報と各駅の位置情報のすべての組み合わせを代入

# %% jupyter={"outputs_hidden": true}
df_calc_base = df_coords.join(df_lat_lon, how="cross")
df_calc_base.select(pl.col(["lat","stop_lat","lon","stop_lon","stop_name"])) #(19816,5)
