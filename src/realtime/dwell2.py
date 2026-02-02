import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import font_manager
from pathlib import Path

# --- フォント・ライブラリ設定 ---
try:
    import japanize_matplotlib
    japanize_matplotlib.japanize()
except Exception:
    pass

try:
    from adjust_text import adjustText as adjust_text
except Exception:
    adjust_text = None

def main():

    # 1. データ読み込み
    df = pd.read_parquet("~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet", engine="pyarrow")

    # 前処理
    df = df.replace("None", np.nan).dropna(axis=1, how="all")
    df_temp = df[df["vehicle_id"].notna()].copy()
    
    # 2. 物理距離の計算 (20mメッシュ)
    m_per_deg_lat = 111111
    m_per_deg_lon = 111111 * np.cos(np.radians(36.7))
    d_lat_20m = 20 / m_per_deg_lat
    d_lon_20m = 20 / m_per_deg_lon

    # 3. 表示範囲の設定
    center_lon, center_lat = 137.213, 36.701
    zoom_range = 0.025
    xmin, xmax = center_lon - zoom_range, center_lon + zoom_range
    ymin, ymax = center_lat - zoom_range, center_lat + zoom_range

    # 4. グリッド定義
    grid_x = np.arange(xmin, xmax + d_lon_20m, d_lon_20m)
    grid_y = np.arange(ymin, ymax + d_lat_20m, d_lat_20m)

    # 5. 描画
    fig = plt.figure(figsize=(14, 12))
    ax = fig.add_subplot(1, 1, 1)

    counts, xedges, yedges, im = ax.hist2d(
        df_temp["lon"], df_temp["lat"], 
        bins=[grid_x, grid_y], cmap='YlOrRd', cmin=1
    )
    ax.set_aspect(1.0 / np.cos(np.radians(36.7))) 

    # --- 6. 停留所・信号プロット ---
    ## 6.1. 駅名辞書の構築
    name_map = {}
    map_path = Path("data/raw/stations/stops_chitetsu.csv")
    if map_path.exists():
        df_map = pd.read_csv(map_path)
        name_map = dict(zip(df_map["jp_name"].str.strip(), df_map["en_name"]))

    all_texts = [] # adjust_text一括処理用

    ## 6.2. 停留所プロット 
    df_stops = pd.read_csv("~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt")
    for i, row in df_stops.iterrows():
        lon, lat, jp_name = row["stop_lon"], row["stop_lat"], row["stop_name"]
        if xmin <= lon <= xmax and ymin <= lat <= ymax:
            ax.scatter(lon, lat, color='blue', marker='o', s=25, edgecolors='white', zorder=5)
            display_name = name_map.get(jp_name.strip(), jp_name)
            offset = 0.0004 if i % 2 == 0 else -0.0004
            t = ax.text(lon + 0.0002, lat + offset, display_name, 
                        fontsize=8, color='blue', fontweight='bold',
                        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=0.8), zorder=6)
            all_texts.append(t)

    ## 6.3. 信号機プロット (queryで最短化)
    sig_path = Path("data/raw/signals/signals.csv")
    if sig_path.exists():
        # read with common encodings and normalize column names
        try:
            df_sig = pd.read_csv(sig_path, encoding='utf-8-sig', engine='python')
        except Exception:
            df_sig = pd.read_csv(sig_path, encoding='cp932', engine='python')

        cols_map = {c.strip().lower(): c for c in df_sig.columns}
        lat_key = cols_map.get('lat') or cols_map.get('latitude') or cols_map.get('stop_lat')
        lon_key = cols_map.get('lng') or cols_map.get('lon') or cols_map.get('longitude') or cols_map.get('stop_lon')

        if lat_key and lon_key:
            # coerce to numeric (remove stray characters) and drop invalid rows
            df_sig['lat'] = pd.to_numeric(df_sig[lat_key].astype(str).str.replace('[^0-9.+-]', '', regex=True), errors='coerce')
            df_sig['lng'] = pd.to_numeric(df_sig[lon_key].astype(str).str.replace('[^0-9.+-]', '', regex=True), errors='coerce')
            df_sig = df_sig.dropna(subset=['lat', 'lng']).reset_index(drop=True)

            # filter to current view bounds
            mask = (df_sig['lng'] >= xmin) & (df_sig['lng'] <= xmax) & (df_sig['lat'] >= ymin) & (df_sig['lat'] <= ymax)
            df_sig_in = df_sig[mask]
            if not df_sig_in.empty:
                # plot points only (no text labels)
                ax.scatter(df_sig_in['lng'], df_sig_in['lat'], color='green', marker='D', s=60, edgecolors='white', zorder=7)

    # ラベル重なり自動調整 (停留所と信号機を一括で)
    if adjust_text and all_texts:
        adjust_text(all_texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))

    # 7. 装飾
    ax.set_title("Toyama Tram Vehicle Density Analysis", fontsize=16, pad=20)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linestyle='--', alpha=0.3)
    cb = fig.colorbar(im, ax=ax)
    cb.set_label('Point Count (Stay Duration)')

    plt.savefig("toyama_tram_full.png", dpi=300, bbox_inches='tight')


if __name__ == "__main__":
    main()