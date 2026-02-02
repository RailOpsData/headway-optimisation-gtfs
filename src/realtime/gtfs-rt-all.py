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
from datetime import time
import polars as pl
import numpy as np
pl.Config.set_tbl_rows(30)

# %% [markdown]
# # 1. vehicle_positionsの読み込み

# %% [markdown]
# ## 1.1. データ前処理
#    - 車両idと取得時間で並べ替え
#    - 00:20-23:59
#    

# %%

df = pl.read_parquet("/app/data/raw/chitetsu_tram/vehicle_positions/*.parquet")

df = df.select(
    pl.all().exclude([s.name for s in df if s.null_count() == len(df)]))
df = df.select(
    pl.exclude(["bearing","speed"]))
df = df.sort(df["vehicle_id","snapshot_ts"]).filter(
    (pl.col("snapshot_ts").dt.time() >= time(0, 20)) &
    (pl.col("snapshot_ts").dt.time() <= time(23, 59))).sort(pl.col(["snapshot_ts","vehicle_id"]))
    
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

# %% [markdown]
# ### 2.0.1. 静的時刻表データ(stop_times.txt)からtrip_idとstop_idを抽出

# %%
df_static = pl.read_csv("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stop_times.txt").select(["trip_id","stop_headsign","stop_id","pickup_type","drop_off_type","timepoint","arrival_time","departure_time","stop_sequence"])
df_static

# %% [markdown]
# ### 2.0.2. 駅データ(stops.txt)から駅ナンバー(stop_id)と駅名(stop_name)を抽出

# %%
df_static_route = pl.read_csv("/app/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt").select(["stop_id","stop_name"])
print(df_static_route)

# %% [markdown]
# ### 2.0.3. 駅データ（stops.txt）の駅ナンバー(stop_id)と駅名(stop_name)が重複していないか確認

# %%
print(df_static_route.group_by(["stop_id","stop_name"]).len())

# %% [markdown]
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

# %% [markdown]
# ### 2.0.5. 結合した静的時刻表データ（駅名付き）(df_stop)の区間を駅名で絞り込み（駅名が諏訪川原-富山大学前間)
# 方針：**合流のない区間を選択（諏訪川原-富山大学前）し、そこを通るroute_idを選定し、合流ふくむ区間も含めて全体（富山駅～富山大学前）で調査する**<br>
# 意図：合流区間を含むとその区間を通過する別ルート(route_id)も含まれてしまう

# %%
df_stops_all = df_stops
# df_stops.filter(
#     pl.col("stop_name").is_in([
#         # "富山駅",
#         # "新富町",
#         # "県庁前",
#         # "丸の内",
#         "諏訪川原",
#         "安野屋",
#         "トヨタモビリティ富山Gスクエア五福前",
#         "富山大学前"
#     ])
# )
df_stops_all

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.6. 結合された静的時刻表データ（駅名付きかつ、対象区間絞り込み済）(df_stop)のtrip_idから運行曜日ならびにroute_idを抽出
# #### 運行曜日
# ラッシュ時間を検討するためには平日のデータだけが必要
# #### route_id
# 富山地鉄軌道線・富山港線においてはtrip_idにroute_idが含まれた形式になっているため：2.0.7.でその組み合わせと行先を抽出

# %%
df_extracted = df_stops_all.with_columns([
    pl.col("trip_id").str.extract(r"^([^_%]+)", 1).alias("service_day"),
    pl.col("trip_id").str.extract(r"系統(.*)$", 1).alias("route_id")
])
df_extracted

# %%
df_extracted["route_id"].unique()

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### 2.0.7. route_idごとの行先表示と個数を確認

# %%
df_counts = df_extracted.group_by(["service_day","route_id", "stop_headsign"]).len().sort(pl.col(["service_day","route_id"]))
df_counts

# %% [markdown]
# ### 2.0.8. (Appendix)駅名、出発時間のこの順番でソートし、各駅の時刻表を作成

# %%
df_extracted.sort()

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 2.1. （GTFS-rt,trip_updates）データ前処理
#    - 車両idと取得時間で並べ替え
#    - 取得時間6～9時台
#    - **route_idで富山大学前発着のみ絞り込み <- trip_updatesのみに含まれる**
#    - 取得時間、route_id、車両IDのみ表示

# %%
df_t = pl.read_parquet("/app/data/raw/chitetsu_tram/trip_updates/*.parquet")
# df_t = df_t.select(
#     pl.all().exclude([s.name for s in df_t if s.null_count() == len(df_t)]))

df_t = df_t.select(
    pl.exclude(["start_time","start_date"]))
df_t = df_t.sort(df_t["vehicle_id","snapshot_ts"])\
    .filter(
    (pl.col("snapshot_ts").dt.time() >= time(5, 20)) &
    (pl.col("snapshot_ts").dt.time() <= time(23, 59)))\
    # .filter(
    #     pl.col("route_id").is_in(["市内軌道線(3001-2-1)","市内軌道線(3001-2-2)" #南富山発着
    #                                   ,"富山港線（富山大学前）(3003-5-2)","富山港線（富山大学前）(3003-5-1)" #岩瀬浜発着
    #                                   ,"市内軌道線(3001-8-1)","市内軌道線(3001-8-2)" #富山駅発着
    #                                   ,"富山港線（富山大学前）(3003-5-2-1)","富山港線（富山大学前）(3003-5-1-1)" #越中中島発着
    #                                                ]))\
# df_t.filter(pl.col("vehicle_id")=="chitetsu_tram_4983")
# df_t.describe()
df_t = df_t.select(pl.exclude("entity_id","tu_timestamp","delay"))
df_t

# %% [markdown]
# ## 2.2. 確認　
# - value_countsメソッドでroute_idと車両IDの種類確認
# - (**df_t**:trip_updates,**df_filtered**:vehicle_positions)

# %%
print(df_t["route_id"].value_counts())
print(df_t["vehicle_id"].value_counts().sort("vehicle_id"))
# print(df_filtered["vehicle_id"].value_counts().sort("vehicle_id")) #(3.1.実行後に比較のため追加)
# print(df_t["snapshot_ts"].value_counts())

# %% [markdown]
# # 3. vehicle_positionsの追加処理および重複カウントと削除

# %% [markdown]
# ## 3.1. "2.2."で確認した車両IDのみVehicle_positionsをfilter：全路線対応ならスキップ

# %%
# target_vehicles = [
#     "chitetsu_tram_4980", "chitetsu_tram_4981","chitetsu_tram_4983", "chitetsu_tram_4984", 
#     "chitetsu_tram_4985", "chitetsu_tram_4986", "chitetsu_tram_4987", 
#     "chitetsu_tram_4989", "chitetsu_tram_4990", "chitetsu_tram_4991", 
#     "chitetsu_tram_4992", "chitetsu_tram_4993", "chitetsu_tram_4994","chitetsu_tram_4995", 
#     "chitetsu_tram_5001", "chitetsu_tram_5004", "chitetsu_tram_5005", 
#     "chitetsu_tram_5006", "chitetsu_tram_5007", "chitetsu_tram_5008", "chitetsu_tram_5010","chitetsu_tram_5011", "chitetsu_tram_5012"
# ]

# df_filtered = df.filter(
#     pl.col("vehicle_id").is_in(target_vehicles)
# )

# df_filtered 

# %% [markdown]
# ## 3.2. 重複している行をカウント・削除（vehicle_positions）
# - 取得時間(snapshot_ts)と車両ID(vehicle_id)でグループ化、各グループの数が1より大きいものを代入
#   - GTFSサーバー側への記録の問題で2種類記録されているが、そこまでは踏み込めないので省略
# - uniqueメソッドとサブセットで取得時間と車両IDが一致するものを削除

# %%
duplicate_count = (
    df.group_by(["snapshot_ts", "vehicle_id","lat","lon"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count)

# %%
df_v_cleaned = df.unique(subset=["snapshot_ts", "vehicle_id"],keep="first").sort(pl.col(["vehicle_id","snapshot_ts"]))
df_v_cleaned #(8532,10)/(25171,10)

# %% [markdown]
# ## 3.3. 重複している行をカウント・削除（trip_updates）

# %%
duplicate_count_t = (
    df_t.group_by(["snapshot_ts", "vehicle_id"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count_t) #(2488,3)/(6448,3)

# %%
df_t_cleaned = df_t.unique(subset=["snapshot_ts", "vehicle_id"],keep="first")
df_t_cleaned.sort("snapshot_ts") #(7238,3)/(18287,3)

# %% [markdown]
# # 4. vehicle_positionsにtrip_updateに含まれていたroute_id情報を結合

# %% [markdown]
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
df_vt.sort(["vehicle_id","snapshot_ts"]) #(8532,11)/(25171,11)

# %% [markdown]
# ## 4.2. route_id欠損値カウント・欠損率・前後の内容確認

# %%
print(df_vt["route_id"].null_count()) #1880件/8685件


# %%
rate = df_vt["route_id"].null_count() / len(df_vt)
print(f"route_idの欠損率: {rate:.2%}") #22.03%/34.50%

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

# %% [markdown]
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
df_imputed #(8532,11)/(25171,11)

# %% [markdown]
# route_idの欠損率を再計算

# %%
rate = df_imputed["route_id"].null_count() / len(df_imputed)
print(f"route_idの欠損率: {rate:.2%}") #(16.63%)/(28.85%)

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
df_ch2.sort(pl.col(["vehicle_id","snapshot_ts"])) #(1496,6)/(7367,6)

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
df_ch3 #(1419,11)/(7263,11)

# %%
df_ch3["vehicle_id"].value_counts().sort("vehicle_id") #(13,2)/(21,2)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ### ルート情報(route_id)が欠損している行を削除

# %%
print(df_check2.schema)
df_clean = df_imputed.drop_nulls("route_id")
df_clean.select(["snapshot_ts","vehicle_id","lat","lon","current_stop_sequence","route_id"]) #(7113,6)/(17908,6)

# %% [markdown]
# ## 4.4. 行先ふくむルート情報(route_id)をroute_id_nameに変更

# %%
df_clean_rn = df_clean.rename({"route_id":"route_id_name"})

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 4.5. 行先ふくむルート情報(route_id_name)から数字のみのルート情報(route_id)を正規表現で抽出

# %% jupyter={"outputs_hidden": true}
pattern = r"\(([\d\-]+)\)"

df_rt = df_clean_rn.with_columns(
    pl.col("route_id_name")
    .str.extract(pattern, 1)
    .alias("route_id")
)
df_rt.select(["snapshot_ts","vehicle_id","lat","lon","current_stop_sequence","route_id_name","route_id"]) #(7113,7)/(17908,7)

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
# ## 5.2. （GTFS-JP）富山駅から大学前までに絞り込み(stop_sequence)(df_st_uni)
# ルート(route_id)、ルートにおける幾つめの駅か？(stop_sequence)、駅名(stop_name)
# ここには別系統の環状線も含まれる
# #### "6.7."でも位置情報から求めた最寄駅のルートにおける幾つめの停車駅か？(current_stop_sequence_right)を生成するために利用

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
# ## 5.3. GTFS-rt(df_rt)とGTFS-JP(df_st_uni)の結合(df_merged):"6.2."でも利用

# %%
df_merged = df_rt.join(
    df_st_uni,
    left_on=["route_id", "current_stop_sequence"],
    right_on=["route_id", "stop_sequence"],
    how="inner"
)
df_merged #(2954,13)/(7499,13)

# %%
null_count=df_merged["stop_name"].null_count()
null_count #0

# %% [markdown]
# # 6. realtime位置情報（緯度経度)と駅の位置情報(GTFS-JP緯度経度)から最寄り駅を計算

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.1. GTFS-JPから富山駅～富山大学前の位置情報（緯度経度）を抽出

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

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.2. GTFS-rt("5.3.で前処理済のもの")から位置情報（緯度経度の組み合わせ）が一意なものを抽出

# %% jupyter={"outputs_hidden": true}
df_coords = df_merged.select(["lat", "lon"]).unique()
df_coords #(2477,2)/(6077,2)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.3. realtimeデータの取得位置情報と各駅の位置情報のすべての組み合わせを代入

# %% jupyter={"outputs_hidden": true}
df_calc_base = df_coords.join(df_lat_lon, how="cross")
df_calc_base = df_calc_base.select(pl.col(["lat","stop_lat","lon","stop_lon","stop_name"])) #並べ替え
df_calc_base #(19816,5)/(48616,5)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.4. 各駅との距離を計算 

# %%
EQ_LON_DIST = 111320
LAT_DIST = 111111

df_with_dist = df_calc_base.with_columns(
    # 1. 平均緯度の算出
    ((pl.col("lat") + pl.col("stop_lat")) / 2).alias("lat_avg")
).with_columns(
    # 2. NumPyを使用して経度係数を算出
    # np.deg2rad() で度からラジアンへ変換し、np.cos() を求めます
    (np.cos(np.deg2rad(pl.col("lat_avg"))) * EQ_LON_DIST).alias("lon_factor")
).with_columns(
    # 3. 最終的な距離計算
    (
        ((pl.col("lat") - pl.col("stop_lat")) * LAT_DIST).pow(2) + 
        ((pl.col("lon") - pl.col("stop_lon")) * pl.col("lon_factor")).pow(2)
    ).sqrt().alias("distance_m")
)

# df_with_dist = df_calc_base.with_columns(
#     (
#         ((pl.col("lat") - pl.col("stop_lat")) * 111000).pow(2) + 
#         ((pl.col("lon") - pl.col("stop_lon")) * 91000).pow(2)
#     ).sqrt().alias("distance_m") # メートル単位の直線距離
# )
df_with_dist #(19816,8)/(48616,8)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.5. それぞれの位置情報データから最寄駅算出

# %%
df_nearest_stop = (
    df_with_dist.sort("distance_m")
    .group_by(["lat", "lon"])
    .first()
)

# 結果の表示
df_nearest_stop = df_nearest_stop.select(pl.col(["lat","lon","stop_name","distance_m"]))
df_nearest_stop = df_nearest_stop.rename({"stop_name":"nearest_stop"})
df_nearest_stop

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.6. 計算された最寄り駅を位置情報をキーにして元のデータに結合

# %%
df_merged_neareststop = df_merged.join(
    df_nearest_stop,
    on = ["lat","lon"],
    how = "inner"
)
df_merged_neareststop #(2954,15)/(7499,15)になれば"5.3."との整合性がとれていることになる

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.7. 位置情報ベースのcurrent_stop_sequenceに変更するためにGTFS-JPのstop_sequenceを結合

# %%
df_merged_neareststop_stopsequence_right = df_merged_neareststop.join(
    df_st_uni,
    left_on = ["route_id","nearest_stop"],
    right_on = ["route_id","stop_name"],
    how = "left"
)
df_result = df_merged_neareststop_stopsequence_right.select(
    pl.exclude(["current_stop_sequence", "stop_name"])
)
df_result = df_result.rename({"stop_sequence":"current_stop_sequence_loc"})
df_result #(2954,45)/(7499,14)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.8. 時間順に並べて新しい便になったタイミングを特定
# - 条件A：シーケンスが若返った＝そのルートの終着駅に到着して、次の運用に就いた
# - 条件B：違う運用として車両が戻ってきて、数え始める駅が異なった

# %%

df_with_trip_count = df_result.sort(["vehicle_id", "snapshot_ts"]).with_columns(
    (
        # 条件A: シーケンスが若返った
        (pl.col("current_stop_sequence_loc") < pl.col("current_stop_sequence_loc").shift(1)) |
        # 条件B: シーケンスが大きく飛んだ
        ((pl.col("current_stop_sequence_loc") - pl.col("current_stop_sequence_loc").shift(1)).abs() > 5)
    )  
    .over("vehicle_id")
    .fill_null(True)
    .cast(pl.Int32)
    .cum_sum()
    .over("vehicle_id")
    .alias("trip_count")
)
df_with_trip_count #(2954,15)/(7499,15)

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 6.9. 「車両」「便番号」「シーケンス番号」の3つでグループ化
# #### 「〇便目の〇番目の駅」ごとに、最も近づいた瞬間を取得
# - 車両が混ざらないように車両IDを並べ替え第一優先にする
# - 同じ運用内での最寄り駅との距離を見るために便番号を第二優先にする
# - 停車駅の順番は上記の中であれば一意に昇順であるはずなので、時間的なソートにもなる

# %%
df_final_approach = (
    df_with_trip_count.sort("distance_m")
    .group_by(["vehicle_id", "trip_count", "current_stop_sequence_loc"])
    .first()
    .sort(pl.col(["vehicle_id","snapshot_ts"]))
    .select(pl.col(["trip_count","vehicle_id","snapshot_ts","lat","lon","route_id","nearest_stop","current_stop_sequence_loc","distance_m"]))
)
df_final_approach #(429,9)

# %% [markdown]
# # 7. 各駅の時刻表作成

# %% [markdown]
# ## 7.1. route_idからdirection_idを正規表現で抽出
# 違う方向は当然別でカウントする必要があるため

# %% jupyter={"outputs_hidden": true}
df_with_direction = df_final_approach.with_columns(
    pl.col("route_id")
    .str.extract(r"(\d)$")  # 末尾の数字(\d)を抽出
    .cast(pl.Int32)         # 数値として扱いたい場合はキャスト
    .alias("direction_id")
)
df_with_direction 

# %% [markdown]
# ## 7.2. 補完しなくてはいけない(駅から50m離れているものが最寄となっている）行

# %% [markdown]
# ### 7.2.1. 要補完の行をboolean型で整理

# %%
df_with_context = df_with_direction.with_columns(
    (
        (pl.col("distance_m") >= 50)  # 本来の条件
        # |                      
        # (pl.col("distance_m") >= 50).shift(1).fill_null(False) |  # 1行後ろも含める
        # (pl.col("distance_m") >= 50).shift(-1).fill_null(False)   # 1行前も含める
    ).alias("required_compensation") # ← 条件全体のカッコを閉じてから alias を指定
)

# df_with_context
df_with_context["required_compensation"].value_counts()

# %% [markdown]
# ### 7.2.2. 要補完の行を並べ替えて出力

# %% jupyter={"outputs_hidden": true}
df_with_context.filter(pl.col(["required_compensation"])).sort(pl.col(["vehicle_id","snapshot_ts"]))

# %% [markdown]
# ## 7.3. distanceが50m以上のものとその前後の行

# %% [markdown]
# ### 7.3.1. 要補完の前後の行のboolean型も追加

# %%
df_with_context_adjacent = df_with_context.with_columns(
    (
        (pl.col("distance_m") >= 50)  # 本来の条件
        |                      
        (pl.col("distance_m") >= 50).shift(1).over("vehicle_id").fill_null(False) |  # 1行後ろも含める
        (pl.col("distance_m") >= 50).shift(-1).over("vehicle_id").fill_null(False)   # 1行前も含める
    ).alias("is_target_context") # ← 条件全体のカッコを閉じてから alias を指定
)

# df_with_context
df_with_context_adjacent["is_target_context"].value_counts()

# %% [markdown]
# ### 7.3.2. 要補完の前後の行を並べ替えて出力

# %% jupyter={"outputs_hidden": true}
df_with_context_adjacent.filter(pl.col(["is_target_context"])).sort(pl.col(["vehicle_id","snapshot_ts"]))

# %% [markdown]
# ## 7.4. distance_mが50m以内のものを選定

# %% jupyter={"outputs_hidden": true}
df_with_direction.filter(pl.col("distance_m") < 50)

# %% [markdown]
# ## 7.5. 時刻表示用に変更、方向ID(direction_id)で位置情報データを分類

# %%
# df_with_direction = df_with_direction.with_columns(
#     pl.col("snapshot_ts").dt.strftime("%H:%M:%S").alias("time_str")
# )

# direction_map = {1: "daigakumae", 2: "toyamaSta"}
# dfs = {
#     name: df_with_direction.filter(pl.col("direction_id") == id)
#     for id, name in direction_map.items()
# }


# %%

# 1. 各便の「基準駅（ここでは丸の内）」の通過時刻を抽出
departure_times = (
    dfs["daigakumae"]
    .filter(pl.col("nearest_stop") == "丸の内")
    .select(["vehicle_id", "trip_count", "snapshot_ts"])
    .rename({"snapshot_ts": "standard_departure_time"})
)

# 2. 元のデータに基準時刻を紐付ける
dfs_with_standard_time = dfs["daigakumae"].join(
    departure_times, 
    on=["vehicle_id", "trip_count"], 
    how="left"
)

# # 3. 基準時刻で全体をソート
# # これにより、ピボットした際に左から右へ時間が流れるようになります
# df_sorted = df_with_standard_time.sort("standard_departure_time")

# 4. df_sorted を使ってピボットを実行
timetable = df_with_direction.pivot(
    values="time_str",
    index=["vehicle_id", "trip_count","direction_id"],
    on="nearest_stop",
    aggregate_function="first"
)

# # 5. 正しい駅順（マスタ）のリストでソート
# stop_order = [
#     "富山駅", "新富町", "県庁前", "丸の内", "諏訪川原", "安野屋", 
#     "トヨタモビリティ富山Gスクエア五福前", "富山大学前"
# ]

# order_map = {s: i for i, s in enumerate(stop_order)}

# timetable = timetable.with_columns(
#     pl.col("nearest_stop").replace(order_map).alias("_order")
# ).sort("_order").drop("_order")

# 結果表示
timetable = timetable.filter(pl.col("direction_id")==1).sort(pl.col("新富町"))
timetable

# %%
timetable.write_csv("timetable_daigakumae.csv")

# %%
