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

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 1.1. データ前処理
#    - 車両idと取得時間で並べ替え
#    - 取得時間6～9時台

# %% jupyter={"source_hidden": true, "outputs_hidden": true}

df = pl.read_parquet("/app/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet")

df = df.select(
    pl.all().exclude([s.name for s in df if s.null_count() == len(df)]))
df = df.select(
    pl.exclude(["bearing","speed"]))
df = df.sort(df["vehicle_id","snapshot_ts"]).filter(pl.col("snapshot_ts").dt.hour().is_between(6, 9))\
    # .head(1000)
df


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 1.2. vehicle_idの種類と個数確認

# %% jupyter={"outputs_hidden": true}
df.shape
print(df["vehicle_id"].value_counts())

# %% [markdown]
# # 2. trip_updates読み込み

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 2.1. データ前処理
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
    .filter(pl.col("route_id").is_in(["市内軌道線(3001-2-1)","市内軌道線(3001-2-2)"\
                                      # ,"富山港線（富山大学前）(3003-5-2)","富山港線（富山大学前）(3003-5-1)"
                                                   ]))\
    .select(pl.col(["snapshot_ts","route_id","vehicle_id"]))
# df_t.filter(pl.col("vehicle_id")=="chitetsu_tram_4983")
df_t #8131件

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 2.2. 確認　
# - value_countsメソッドでroute_idと車両IDの種類確認
# - (**df_t**:trip_updates,**df_filtered**:vehicle_positions)

# %% jupyter={"source_hidden": true}
print(df_t["route_id"].value_counts())
print(df_t["vehicle_id"].value_counts().sort("vehicle_id"))
print(df_filtered["vehicle_id"].value_counts().sort("vehicle_id")) #(3.1.実行後に比較のため追加)
print(df_t["snapshot_ts"].value_counts())

# %% [markdown]
# # 3. vehicle_id追加処理および重複カウントと削除

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.1. 2.2.で確認した車両IDのみにfilter:

# %%
target_vehicles = [
    "chitetsu_tram_4984", "chitetsu_tram_4985", "chitetsu_tram_4992",
    "chitetsu_tram_4993", "chitetsu_tram_4989", "chitetsu_tram_4991",
    "chitetsu_tram_4986", "chitetsu_tram_4990", "chitetsu_tram_4995",
    "chitetsu_tram_5001", "chitetsu_tram_5011", "chitetsu_tram_4987",
    "chitetsu_tram_4983"
]

df_filtered = df.filter(
    pl.col("vehicle_id").is_in(target_vehicles)
)

df_filtered #8658件

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.2. 重複している行をカウント・削除（vehicle_positions）
# - snapshot_tsとvehicle_idでグループ化、各グループの数が1より大きいものを代入
# - uniqueメソッドとサブセットで取得時間と車両IDが一致するものを削除

# %% jupyter={"outputs_hidden": true}
duplicate_count = (
    df_filtered.group_by(["snapshot_ts", "vehicle_id"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count)

# %% jupyter={"outputs_hidden": true}
df_v_cleaned = df_filtered.unique(subset=["snapshot_ts", "vehicle_id"],keep="first")
df_v_cleaned

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 3.3. 重複している行をカウント・削除（trip_updates）

# %% jupyter={"outputs_hidden": true}
duplicate_count_t = (
    df_t.group_by(["snapshot_ts", "vehicle_id"])
    .len() # 各グループの行数を数える
    .filter(pl.col("len") > 1) # 1行より多い（重複している）ものだけ抽出
)

print(duplicate_count_t)

# %% jupyter={"outputs_hidden": true}
df_t_cleaned = df_t.unique(subset=["snapshot_ts", "vehicle_id"],keep="first")
df_t_cleaned

# %% [markdown]
# # 4. vehicle_positionsにtrip_updateに含まれていたroute_id情報を結合

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## 4.1. 結合処理
# - キーは取得時間と車両ID＝同一時間の車両IDは一意

# %% jupyter={"outputs_hidden": true}
df_vt = df_v_cleaned.join(
    df_t_cleaned,
    on=["snapshot_ts", "vehicle_id"],
    how="left"
)
df_vt

# %% [markdown]
# ## 4.2. route_id欠損値カウント・欠損率・前後の内容確認

# %%
print(df_vt["route_id"].null_count())


# %%
rate = df_vt["route_id"].null_count() / len(df_vt)
print(f"route_idの欠損率: {rate:.2%}")

# %% [markdown]
# ## 4.3. 欠損値補完

# %% [markdown]
# 補完の前に車両ID、取得時間の順番でソート

# %%
df_vt_sorted = df_vt.sort(["vehicle_id","snapshot_ts"])

# %% [markdown]
# route_idがnullの当該行とその前後1行に該当するものにfilter

# %% jupyter={"outputs_hidden": true}
df_check = df_vt_sorted.filter(
    pl.col("route_id").is_null() |           # 当該行がnull
    pl.col("route_id").shift(-1).over("vehicle_id").is_null() | # 次の行がnull（＝自分はnullの直前）
    pl.col("route_id").shift(1).over("vehicle_id").is_null()   # 前の行がnull（＝自分はnullの直後）
)
df_check.select(["current_stop_sequence","vehicle_id","route_id"])

# %% [markdown]
# 前後でroute_idが一致すればその値で補完

# %% jupyter={"source_hidden": true}
df_imputed = df_vt_sorted.with_columns([
    pl.when(
        pl.col("route_id").is_null() & 
        (pl.col("route_id").forward_fill().over("vehicle_id") == 
         pl.col("route_id").backward_fill().over("vehicle_id"))
    )
    .then(pl.col("route_id").forward_fill().over("vehicle_id")) # 一致すればその値で埋める
    .otherwise(pl.col("route_id"))                             # 一致しなければ元のまま
    .alias("route_id")
])
df_imputed

# %% [markdown]
# route_idの欠損率を再計算

# %% jupyter={"outputs_hidden": true}
rate = df_imputed["route_id"].null_count() / len(df_imputed)
print(f"route_idの欠損率: {rate:.2%}")

# %% [markdown]
# 上記で補完しきれなかったデータを確認

# %%
df_check2 = df_imputed.filter(
    pl.col("route_id").is_null() |           # 当該行がnull
    pl.col("route_id").shift(-1).over("vehicle_id").is_null() | # 次の行がnull（＝自分はnullの直前）
    pl.col("route_id").shift(1).over("vehicle_id").is_null()   # 前の行がnull（＝自分はnullの直後）
)
df_ch = df_check2.select(["current_stop_sequence","snapshot_ts","vehicle_id","route_id"]).slice(20,100)
df_ch

# %%
df_ch["vehicle_id"].value_counts()

# %%
