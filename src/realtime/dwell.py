import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import font_manager
from pathlib import Path

# If available, use japanize-matplotlib to configure Japanese fonts automatically.
try:
    import japanize_matplotlib
    japanize_matplotlib.japanize()
except Exception:
    pass

# adjust_textがインストールされている場合のみインポート
try:
    from adjust_text import adjust_text
except Exception:
    try:
        import adjustText
        adjust_text = getattr(adjustText, "adjust_text", None)
    except Exception:
        adjust_text = None

def main():
    # Ensure minus sign and prefer Japanese fonts if available
    matplotlib.rcParams['axes.unicode_minus'] = False
    jp_candidates = [
        "Noto Sans CJK JP",
        "Noto Sans JP",
        "IPAPGothic",
        "IPAexGothic",
        "TakaoPGothic",
        "DejaVu Sans",
    ]
    # Hardcode preferred Japanese fonts to avoid mojibake (will fall back if not installed)
    matplotlib.rcParams['axes.unicode_minus'] = False
    matplotlib.rcParams['font.family'] = 'Noto Sans CJK JP'
    matplotlib.rcParams['font.sans-serif'] = [
        'Noto Sans CJK JP',
        'Noto Sans JP',
        'IPAPGothic',
        'IPAexGothic',
        'TakaoPGothic',
        'DejaVu Sans',
    ]

    # 1. データ読み込み
    # 車両位置データの読み込み
    df = pd.read_parquet("~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet", engine="pyarrow")

    # 前処理
    df = df.replace("None", np.nan).dropna(axis=1, how="all")
    cols_to_drop = [c for c in ["bearing", "speed"] if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    # 特定の車両に絞り込み
    df_temp = df[df["vehicle_id"].notna()].copy()
    
    # 2. 物理距離の計算 (富山市付近)
    # meters per degree (approx). Use ~111111 m/deg for latitude
    m_per_deg_lat = 111111
    m_per_deg_lon = 111111 * np.cos(np.radians(36.7))
    
    # 20mに相当する「°」の幅
    d_lat_20m = 20 / m_per_deg_lat
    d_lon_20m = 20 / m_per_deg_lon

    # 3. 表示範囲の設定
    center_lon, center_lat = 137.213, 36.701
    zoom_range = 0.025  # 緯度経度での度数 
    xmin, xmax = center_lon - zoom_range, center_lon + zoom_range
    ymin, ymax = center_lat - zoom_range, center_lat + zoom_range

    # 4. グリッド定義 (np.arangeで20m間隔の目盛りを作成)
    grid_x = np.arange(xmin, xmax + d_lon_20m, d_lon_20m)
    grid_y = np.arange(ymin, ymax + d_lat_20m, d_lat_20m)

    # 5. 描画準備
    fig = plt.figure(figsize=(14, 12))
    ax = fig.add_subplot(1, 1, 1)

    # 2次元ヒストグラムの描画
    counts, xedges, yedges, im = ax.hist2d(
        df_temp["lon"], df_temp["lat"], 
        bins=[grid_x, grid_y], cmap='YlOrRd', cmin=1
    )

    # アスペクト比を地図の歪みに合わせて補正
    ax.set_aspect(1.0 / np.cos(np.radians(36.7))) 

    # --- 6. 停留所マッピング ---
    # Load name mapping from CSV (preferred) or fall back to empty dict
    try:
        map_path = Path("data/raw/stations/stops_chitetsu.csv")
        if map_path.exists():
            df_map = pd.read_csv(map_path)
            # normalize and build dict
            df_map["jp_name"] = df_map["jp_name"].astype(str).str.strip()
            df_map["en_name"] = df_map["en_name"].astype(str)
            name_map = dict(zip(df_map["jp_name"], df_map["en_name"]))
        else:
            name_map = {}
    except Exception:
        name_map = {}

    # 停留所・信号データのプロット
    try:
        df_stops = pd.read_csv("~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/gtfs_jp_tram/stops.txt")
        texts = [] # ラベル重なり調整用のリスト
        
        # 停留所ごとにループ
        for i, row in df_stops.iterrows():
            lon, lat, jp_name = row["stop_lon"], row["stop_lat"], row["stop_name"]
            
            # 画面内に収まっている場合のみ描画
            if xmin <= lon <= xmax and ymin <= lat <= ymax:
                # 停留所の地点をプロット
                ax.scatter(lon, lat, color='blue', marker='o', s=25, edgecolors='white', zorder=5)
                
                # 名前を英語に変換 (一致しない場合は日本語のまま)
                display_name = name_map.get(jp_name.strip(), jp_name)
                
                # 重なりを避けるための交互オフセット
                offset = 0.0004 if i % 2 == 0 else -0.0004
                
                # テキストラベルの配置
                t = ax.text(lon + 0.0002, lat + offset, display_name, 
                            fontsize=8, color='blue', fontweight='bold',
                            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=0.8),
                            zorder=6)
                texts.append(t)

        # adjust_textが利用可能な場合は実行
        if adjust_text and texts:
            adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))

        # --- 信号データの読み込みとプロット ---
        try:
            # Minimal inline CSV parsing (auto-detect numeric lat/lon columns)
            csv_path = Path("data/raw/signals/signals.csv")
            df_signals = csv_utils.load_signals(str(csv_path))

            if not df_signals.empty:
                # filter to view window
                df_sig_in = df_signals[(df_signals['lng'] >= xmin) & (df_signals['lng'] <= xmax) & (df_signals['lat'] >= ymin) & (df_signals['lat'] <= ymax)]
                if not df_sig_in.empty:
                    ax.scatter(df_sig_in['lng'], df_sig_in['lat'], color='red', marker='^', s=60, edgecolors='white', zorder=7)
                    sig_texts = []
                    for i, r in df_sig_in.iterrows():
                        label = r.get('name') if pd.notna(r.get('name')) and r.get('name') != 'nan' else (r.get('desc') if pd.notna(r.get('desc')) else 'signal')
                        t = ax.text(r['lng'] + 0.00025, r['lat'] + 0.00025, label,
                                    fontsize=8, color='red', fontweight='bold',
                                    bbox=dict(facecolor='white', alpha=0.8, edgecolor='none', pad=0.6), zorder=8)
                        sig_texts.append(t)
                    if adjust_text and sig_texts:
                        adjust_text(sig_texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))
        except Exception as _e:
            print(f"Warning: failed to plot signals: {_e}")
            
    except FileNotFoundError:
        print("Error: stops.txt not found.")

    # 7. 装飾
    ax.set_title("Toyama Tram Vehicle Density Analysis ", fontsize=16, pad=20)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linestyle='--', alpha=0.3)

    # カラーバー
    cb = fig.colorbar(im, ax=ax)
    cb.set_label('Point Count (Stay Duration)')

    # 保存と表示
    plt.savefig("toyama_tram_full_analysis.png", dpi=300, bbox_inches='tight')
    

if __name__ == "__main__":
    main()