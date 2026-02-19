import json
from pathlib import Path
import csv
import numpy as np
import pandas as pd
from keplergl import KeplerGl


DATA_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/20251201.parquet"
SIGNALS_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/signals/signals.txt"
STOPS_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt"

# 信号の位置情報レイヤー用
def load_signals(path: Path):
    if not path.exists():
        return None
    # Attempt to detect delimiter and header using csv.Sniffer for robustness
    text = None
    for enc in ("utf-8", "cp932", "latin-1"):
        try:
            text = path.read_text(encoding=enc)
            encoding_used = enc
            break
        except Exception:
            text = None
    if text is None:
        # last resort: let pandas try with defaults
        try:
            df = pd.read_csv(path, engine='python', skipinitialspace=True)
        except Exception:
            return None
    else:
        sample = "\n".join(text.splitlines()[:10])
        try:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(sample)
            try:
                dialect = sniffer.sniff(sample, delimiters=[',', '\t', ';', '|'])
                delim = dialect.delimiter
            except Exception:
                delim = None

            if delim:
                if has_header:
                    df = pd.read_csv(path, sep=delim, engine='python', encoding=encoding_used, skipinitialspace=True)
                else:
                    df = pd.read_csv(path, sep=delim, header=None, engine='python', encoding=encoding_used, skipinitialspace=True)
            else:
                # fallback to pandas auto-detection
                if has_header:
                    df = pd.read_csv(path, engine='python', encoding=encoding_used, skipinitialspace=True)
                else:
                    df = pd.read_csv(path, header=None, engine='python', encoding=encoding_used, skipinitialspace=True)
        except Exception:
            # fallback: try plain read
            try:
                df = pd.read_csv(path, engine='python', skipinitialspace=True)
            except Exception:
                return None

    cols = df.columns.tolist()
    canonical = ["lat", "lng", "name", "description", "count", "signal_idx"]
    rename_map = {}
    # Map by position for the first six columns if needed (handles headerless files)
    for i in range(min(6, len(cols))):
        col = cols[i]
        target = canonical[i]
        if col != target and target not in df.columns:
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
    counts_path = Path.home() / "BachelorThesis/headway-optimisation-gtfs/results/ver6/signal_50m_counts_withStations.csv"
    if counts_path.exists() and sig_df is not None and not sig_df.empty:
        try:
            counts_df = pd.read_csv(counts_path)
            # normalize coordinate column names in counts_df (accept lon or lng)
            if 'lon' not in counts_df.columns and 'lng' in counts_df.columns:
                counts_df['lon'] = counts_df['lng']
            if 'lat' not in counts_df.columns and 'latitude' in counts_df.columns:
                counts_df['lat'] = counts_df['latitude']

            # ensure numeric coords in counts file (may be strings)
            counts_df['lat'] = pd.to_numeric(counts_df['lat'], errors='coerce')
            counts_df['lon'] = pd.to_numeric(counts_df['lon'], errors='coerce')

            # normalize sig_df lng column if needed
            if 'lng' not in sig_df.columns and 'lon' in sig_df.columns:
                sig_df = sig_df.rename(columns={'lon': 'lng'})

            # ensure numeric coords in sig_df
            sig_df['lat'] = pd.to_numeric(sig_df['lat'], errors='coerce')
            sig_df['lng'] = pd.to_numeric(sig_df['lng'], errors='coerce')

            # create rounded keys for robust merging
            sig_df['__lat_r'] = sig_df['lat'].round(6)
            sig_df['__lng_r'] = sig_df['lng'].round(6)
            counts_df['__lat_r'] = counts_df['lat'].round(6)
            counts_df['__lng_r'] = counts_df['lon'].round(6)

            merged = sig_df.merge(
                counts_df[['__lat_r', '__lng_r', 'count']],
                on=['__lat_r', '__lng_r'],
                how='left'
            )
            merged['count'] = merged['count'].fillna(0).astype(int)
            sig_df = merged.drop(columns=['__lat_r', '__lng_r'])
        except Exception as e:
            print(f"Warning: failed to merge counts: {e}")
            # ensure a count column exists so downstream size calc runs
            if 'count' not in sig_df.columns:
                sig_df['count'] = 0

        # If merge produced no non-zero counts, attempt spatial nearest-neighbour
        # fallback to match signals to counts by distance (tolerance 50 m).
        try:
            total_counts = int(sig_df['count'].sum()) if 'count' in sig_df.columns else 0
        except Exception:
            total_counts = 0

        if total_counts == 0:
            # ensure counts_df is available
            if 'counts_df' in locals() and not counts_df.empty:
                try:
                    print("Debug: counts_df cols:", counts_df.columns.tolist())
                    # prepare arrays
                    c_lat = counts_df['lat'].to_numpy(dtype=float)
                    c_lon = counts_df['lon'].to_numpy(dtype=float)
                    s_lat = sig_df['lat'].to_numpy(dtype=float)
                    s_lon = sig_df['lng'].to_numpy(dtype=float)

                    def haversine_m(lat1, lon1, lat2_arr, lon2_arr):
                        R = 6371000.0
                        phi1 = np.radians(lat1)
                        phi2 = np.radians(lat2_arr)
                        dphi = phi2 - phi1
                        dlambda = np.radians(lon2_arr - lon1)
                        a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
                        return 2 * R * np.arcsin(np.sqrt(a))

                    assigned = []
                    for lat, lon in zip(s_lat, s_lon):
                        dists = haversine_m(lat, lon, c_lat, c_lon)
                        idx = int(np.nanargmin(dists))
                        min_d = float(dists[idx])
                        if min_d <= 50.0:
                            assigned.append(int(counts_df.iloc[idx]['count']))
                        else:
                            assigned.append(0)

                    sig_df['count'] = assigned
                    print("Info: applied spatial fallback matching for counts (<=50m)")
                except Exception as e:
                    print(f"Warning: spatial fallback failed: {e}")
                    if 'count' not in sig_df.columns:
                        sig_df['count'] = 0
            else:
                print("Debug: counts_df not available for spatial fallback; setting count=0 for all signals")
                sig_df['count'] = 0

    # scale counts to a `size` column for visualization (min size 5, max 40)
    if sig_df is not None and 'count' in sig_df.columns:
        cmin = sig_df['count'].min()
        cmax = sig_df['count'].max()
        if cmax == cmin:
            sig_df['size'] = 10
        else:
            sig_df['size'] = ((sig_df['count'] - cmin) / (cmax - cmin)) * 35 + 5

    # Load station/stop locations for separate Kepler layer
    stops_df = None
    try:
        try:
            stops_df = pd.read_csv(STOPS_PATH, encoding='utf-8', engine='python')
        except Exception:
            stops_df = pd.read_csv(STOPS_PATH, encoding='cp932', engine='python')
        if 'stop_lat' in stops_df.columns and 'stop_lon' in stops_df.columns:
            stops_df['lat'] = pd.to_numeric(stops_df['stop_lat'], errors='coerce')
            stops_df['lng'] = pd.to_numeric(stops_df['stop_lon'], errors='coerce')
            if 'stop_name' in stops_df.columns:
                stops_df['name'] = stops_df['stop_name'].astype('string')
            stops_df = stops_df.dropna(subset=['lat', 'lng'])
        else:
            stops_df = None
    except Exception:
        stops_df = None
    # Debug output for stops dataset
    if stops_df is None:
        print(f"Debug: stops_df not loaded from {STOPS_PATH}")
    else:
        print("Debug: stops_df shape:", stops_df.shape)
        print("Debug: stops_df cols:", stops_df.columns.tolist())
        try:
            print("Debug: stops_df sample:\n", stops_df[['lat','lng'] + ([ 'name'] if 'name' in stops_df.columns else [])].head())
        except Exception:
            pass

    # Kepler インスタンスを作成してデータを追加
    # provide a simple config that maps `size` to point size when present
    kepler_config = None
    if sig_df is not None and 'size' in sig_df.columns:
        kepler_config = {
            "version": "v1",
            "config": {
                "visState": {
                    "layers": [
                        # {
                        #     "id": "signals_layer",
                        #     "type": "point",
                        #     "config": {
                        #         "dataId": "signals",
                        #         "label": "signals",
                        #         "columns": {"lat": "lat", "lng": "lng"},
                        #         "isVisible": True,
                        #         "visConfig": {
                        #             "radius": 10,
                        #             "sizeField": {"name": "size", "type": "real"},
                        #             "sizeScale": "linear",
                        #             "radiusRange": [1, 50],
                        #             "opacity": 0.8
                        #         }
                        #     }
                        # },
                        {
                            "id": "stations_layer",
                            "type": "point",
                            "config": {
                                "dataId": "stations",
                                "label": "stations",
                                "columns": {"lat": "lat", "lng": "lng"},
                                "isVisible": True,
                                "visConfig": {
                                    "radius": 6,
                                    "fixedRadius": False,
                                        "opacity": 0.95,
                                    "outline": False,
                                    "thickness": 0,
                                    "colorRange": {
                                        "name": "Station Colors",
                                        "type": "qualitative",
                                        "category": "Custom",
                                        "colors": ["#006400"]
                                    }
                                },
                                "color": [0, 100, 0],
                                "textLabel": [
                                    {
                                        "field": {"name": "name", "type": "string"},
                                        "color": [0, 0, 0],
                                        "size": 12,
                                        "fontWeight": "bold",
                                        "offset": [0, 0],
                                        "anchor": "middle",
                                        "alignment": "center"
                                    }
                                ]
                            }
                        },
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
                },
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
    if stops_df is not None and not stops_df.empty:
        map_.add_data(data=stops_df, name="stations")

    # 保存（HTML） — Kepler の GUI でレイヤーを heatmap/point に変更可能
    out = "toyama_tram_gtfs_rt_kepler_20251201.html"
    map_.save_to_html(file_name=out)
    print(f"Saved Kepler HTML to {out}")

    print("df_temp shape:", df_temp.shape)
    print("df_temp cols:", df_temp.columns.tolist())
    print("df_temp dtypes:\n", df_temp.dtypes)
    print("sig_df shape:", None if sig_df is None else sig_df.shape)
    print("sig_df cols:", None if sig_df is None else sig_df.columns.tolist())
    if sig_df is not None:
        print("sig_df sample:\n", sig_df.head())


if __name__ == "__main__":
    main()

