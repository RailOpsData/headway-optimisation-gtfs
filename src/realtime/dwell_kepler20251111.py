import json
from pathlib import Path
import pandas as pd
from keplergl import KeplerGl


DATA_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet"
SIGNALS_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/signals/signals.txt"

# 信号の位置情報レイヤー用
def load_signals(path: Path):
    if not path.exists():
        return None
    # Read .txt as CSV (skipinitialspace handles spaces after commas)
    try:
        df = pd.read_csv(path, encoding="utf-8", skipinitialspace=True)
    except Exception:
        df = pd.read_csv(path, encoding="latin-1", skipinitialspace=True)

    cols = df.columns.tolist()
    canonical = ["lat", "lng", "name", "description"]
    rename_map = {}
    for i, col in enumerate(cols[:4]):
        target = canonical[i]
        if col != target:
            # prefer existing canonical column if present
            if target in df.columns:
                continue
            rename_map[col] = target

    if rename_map:
        df = df.rename(columns=rename_map)

    # Ensure numeric lat/lng and cast text columns to string dtype
    if "lat" in df.columns and "lng" in df.columns:
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
        for txtcol in ("name", "description"):
            if txtcol in df.columns:
                df[txtcol] = df[txtcol].astype("string")
        df = df.dropna(subset=["lat", "lng"])
        return df

    return None


def main():
    # データ読み込み（dwell.py と同じファイルを使用）
    parq_path = DATA_PATH.expanduser()
    df = pd.read_parquet(str(parq_path), engine="pyarrow")

    # normalize common coordinate column names and types
    # prefer `lat` / `lng` column names; coerce to numeric
    if "lat" in df.columns:
        df["latitude_tram"] = pd.to_numeric(df["lat"], errors="coerce")

    if "lon" in df.columns:
        df["longitude_tram"] = pd.to_numeric(df["lon"], errors="coerce")

    # 必要なカラムだけ残す + 特定車両に絞り込み
    df = df.replace("None", pd.NA).dropna(axis=1, how="all")
    df = df.drop(columns=[c for c in ["bearing", "speed"] if c in df.columns], errors="ignore")
    # ensure vehicle_id is string and filter by prefix
    if "vehicle_id" in df.columns:
        df["vehicle_id"] = df["vehicle_id"].astype("string")
        df_temp = df[df["vehicle_id"].str.startswith("chitetsu_tram_")].copy()
    else:
        df_temp = df.copy()

    # ensure vehicle longitude column named `lng` for Kepler
    if "lon" in df_temp.columns and "lng" not in df_temp.columns:
        df_temp = df_temp.rename(columns={"lon": "lng"})

    # Kepler 用にカラム名を調整（Kepler は lng/lat を期待する）
    # normalize vehicle coordinate columns to `lat` and `lng` and coerce to numeric
    if "lat" in df_temp.columns:
        df_temp["lat"] = pd.to_numeric(df_temp["lat"], errors="coerce")
    elif "latitude" in df_temp.columns:
        df_temp["lat"] = pd.to_numeric(df_temp["latitude"], errors="coerce")

    if "lng" in df_temp.columns:
        df_temp["lng"] = pd.to_numeric(df_temp["lng"], errors="coerce")
    elif "lon" in df_temp.columns:
        df_temp["lng"] = pd.to_numeric(df_temp["lon"], errors="coerce")
    elif "longitude" in df_temp.columns:
        df_temp["lng"] = pd.to_numeric(df_temp["longitude"], errors="coerce")

    # Prepare Kepler config to show signal counts as point sizes (if counts available)
    sig_df = load_signals(SIGNALS_PATH)
    # try to read precomputed counts and merge by lat/lon if available
    counts_path = Path.home() / "BachelorThesis/headway-optimisation-gtfs/src/realtime/results/signal_100m_counts.csv"
    if counts_path.exists() and sig_df is not None and not sig_df.empty:
        try:
            counts_df = pd.read_csv(counts_path)
            # ensure numeric coords in counts file (may be strings)
            counts_df['lat'] = pd.to_numeric(counts_df['lat'], errors='coerce')
            counts_df['lon'] = pd.to_numeric(counts_df['lon'], errors='coerce')
            # merge on rounded coordinates to avoid tiny float mismatches
            sig_df['lat'] = pd.to_numeric(sig_df['lat'], errors='coerce')
            sig_df['lng'] = pd.to_numeric(sig_df['lng'], errors='coerce')
            # 緯度経度の丸め込みによるマージ失敗を防止
            sig_df['__lat_r'] = sig_df['lat'].round(6)
            sig_df['__lng_r'] = sig_df['lng'].round(6)
            counts_df['__lat_r'] = counts_df['lat'].round(6)
            counts_df['__lng_r'] = counts_df['lon'].round(6)
            merged = sig_df.merge(counts_df[['__lat_r', '__lng_r', 'count']], on=['__lat_r', '__lng_r'], how='left')
            merged['count'] = merged['count'].fillna(0).astype(int)
            sig_df = merged.drop(columns=['__lat_r', '__lng_r'])
        except Exception:
            # fallback: leave sig_df as-is
            pass

    # scale counts to a `size` column for visualization (min size 5, max 40)
    if sig_df is not None and 'count' in sig_df.columns:
        cmin = sig_df['count'].min()
        cmax = sig_df['count'].max()
        if cmax == cmin:
            sig_df['size'] = 10
        else:
            sig_df['size'] = ((sig_df['count'] - cmin) / (cmax - cmin)) * 35 + 5

    # Kepler インスタンスを作成してデータを追加
    # provide a simple config that maps `size` to point size when present
    kepler_config = None
    if sig_df is not None and 'size' in sig_df.columns:
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        {
                            "id": "signals_layer",
                            "type": "point",
                            "config": {
                                "dataId": "signals",
                                "label": "signals",
                                "columns": {"lat": "lat", "lng": "lng"},
                                "isVisible": True,
                                "visConfig": {
                                    "radius": 10,
                                    "sizeField": {"name": "size", "type": "real"},
                                    "sizeScale": "linear",
                                    "radiusRange": [1, 50],
                                    "opacity": 0.8
                                }
                            }
                        },
                        {
                            "id": "vehicle_positions_layer",
                            "type": "grid",
                            "config": {
                                "dataId": "toyama_vehicle_positions",
                                "label": "toyama_vehicle_positions",
                                "columns": {"lat": "lat", "lng": "lng"},
                                "isVisible": True,
                                # ★ config直下に visualChannels を配置します
                                "visualChannels": {
                                    "colorField": {
                                        "name": "Point Count",
                                        "type": "integer"
                                    },
                                    "colorScale": "log" 
                                },
                                "visConfig": {
                                    "opacity": 0.9,
                                    "worldUnitSize": 0.02, # 20m
                                    "unit": "kilometers",
                                    # ★ 最小値を少し上げると、動いている車（低密度）が消えて
                                    # 止まっている場所（高密度）だけが浮かび上がります
                                    "colorDomain": [10, 1039], 
                                    "colorRange": {
                                        "name": "Global Warming",
                                        "type": "sequential",
                                        "category": "Uber",
                                        "colors": ["#5A1846", "#900C3F", "#C70039", "#E3611C", "#F1920E", "#FFC300"]
                                    },
                                    "coverage": 1,
                                    "enable3d": True, # 高さを出すと「止まっている場所」が物理的に突き抜けます
                                    "elevationScale": 2,
                                    "elevationField": {
                                        "name": "Point Count",
                                        "type": "integer"
                                    }
                                }
                            }
                        }
                    ]
                },
                "mapState": {
                    "bearing": 24, # 少し斜めから見ると高さがわかって「止まっている場所」が見やすいです
                    "pitch": 45
                }
            }
        }

    # If we have coordinates, set map center to the mean of available points
    try:
        center_lat = None
        center_lng = None
        if 'lat' in df_temp.columns and 'lng' in df_temp.columns and not df_temp.empty:
            center_lat = float(df_temp['lat'].mean())
            center_lng = float(df_temp['lng'].mean())
        elif sig_df is not None and 'lat' in sig_df.columns and 'lng' in sig_df.columns and not sig_df.empty:
            center_lat = float(sig_df['lat'].mean())
            center_lng = float(sig_df['lng'].mean())
        if center_lat is not None and center_lng is not None:
            if kepler_config is None:
                kepler_config = {"version": "v1", "config": {"visState": {"layers": []}, "mapState": {}, "mapStyle": {}}}
            kepler_config['config'].setdefault('mapState', {})
            kepler_config['config']['mapState'].update({
                'latitude': center_lat,
                'longitude': center_lng,
                'zoom': 13
            })
    except Exception:
        pass

    if kepler_config is not None:
        map_ = KeplerGl(height=800, config=kepler_config)
    else:
        map_ = KeplerGl(height=800)
    # debug: print dtypes to help track if lat/lng are strings
    # try:
    #     print("df_temp dtypes:", dict(df_temp.dtypes))
    # except Exception:
    #     pass
    # if sig_df is not None:
    #     try:
    #         print("sig_df dtypes:", dict(sig_df.dtypes))
    #     except Exception:
    #         pass

    map_.add_data(data=df_temp, name="toyama_vehicle_positions")
    if sig_df is not None and not sig_df.empty:
        map_.add_data(data=sig_df, name="signals")

    # 保存（HTML） — Kepler の GUI でレイヤーを heatmap/point に変更可能
    out = "toyama_tram_gtfs_rt_kepler_20251111.html"
    map_.save_to_html(file_name=out)
    print(f"Saved Kepler HTML to {out}")


if __name__ == "__main__":
    main()
