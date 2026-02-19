import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import japanize_matplotlib
import matplotlib.pyplot as plt

import matplotlib.font_manager as fm
import logging
# 複数ファイルのParquet読み込み用
import pyarrow.dataset as ds


def haversine_vectorized(lat1, lon1, lat2, lon2):
    # all inputs in degrees; returns meters
    R = 6371000.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    # 緯度の差分を計算
    dphi = phi2 - phi1
    # 経度の差分を計算
    dlambda = np.radians(lon2 - lon1)
    # harversineの距離公式
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def load_signals(path: Path):
    path = path.expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Signals file not found: {path}")
    try:
        df = pd.read_csv(path, encoding='utf-8-sig', engine='python')
    except Exception:
        df = pd.read_csv(path, encoding='cp932', engine='python')
    cols = list(df.columns)
    if len(cols) < 2:
        raise ValueError("Signals file must have at least two columns (lat, lon)")
    lat_col, lon_col = cols[0], cols[1]
    df['lat'] = pd.to_numeric(df[lat_col], errors='coerce')
    df['lon'] = pd.to_numeric(df[lon_col], errors='coerce')
    df = df.dropna(subset=['lat', 'lon']).reset_index(drop=True)
    return df


def main():
    p = argparse.ArgumentParser(description='Count points within 75m of each signal and plot')
    # parquetのパスは単一ファイル用
    p.add_argument('--vehicle', default='~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/output.parquet')
    
    p.add_argument('--signals', default='~/BachelorThesis/headway-optimisation-gtfs/data/raw/signals/signals.txt')
    p.add_argument('--out-csv', default='~/BachelorThesis/headway-optimisation-gtfs/results/ver7/signal_75m_counts_withStationsAndName_all.csv')
    p.add_argument('--out-png', default='~/BachelorThesis/headway-optimisation-gtfs/results/ver7/signal_75m_counts_withStationsAndName_all.png')
    p.add_argument('--font-path', default=None, help='Path to a .ttf/.otf font file to use for Japanese labels')
    # # parquet のパス（ディレクトリ / 単一ファイル / ワイルドカード を受け取れる）
    # p.add_argument('--vehicle', default='~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions')

    args = p.parse_args()

    # vehicle_path = Path(args.vehicle).expanduser()
    signals_path = Path(args.signals).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_png = Path(args.out_png).expanduser()
    # vehicle path handling: directory | single parquet file | glob pattern
    vehicle_path = Path(args.vehicle).expanduser()
    if vehicle_path.is_dir():
        ds_ = ds.dataset(str(vehicle_path), format='parquet')
        table = ds_.to_table()
        df = table.to_pandas()
    elif vehicle_path.exists() and vehicle_path.suffix == '.parquet':
        df = pd.read_parquet(str(vehicle_path), engine='pyarrow')
    else:
        # try glob/wildcard
        from glob import glob
        files = glob(str(vehicle_path))
        if not files:
            raise FileNotFoundError(f"No parquet files found at: {vehicle_path}")
        ds_ = ds.dataset([str(f) for f in files], format='parquet')
        table = ds_.to_table()
        df = table.to_pandas()

    # 単一ファイル読み込み時のエラー処理
    if not vehicle_path.exists():
        raise FileNotFoundError(f"Vehicle parquet not found: {vehicle_path}")


    # 単一ファイル読み込み時のコード
    df = pd.read_parquet(str(vehicle_path), engine='pyarrow')
    df = df.replace('None', np.nan).dropna(axis=1, how='all')
    df_temp = df[df["vehicle_id"].notna()].copy()
    if df_temp.empty:
        raise ValueError('No vehicle position records found (vehicle_id missing or filtered out)')

    df_sig = load_signals(signals_path)

    counts = []
    # vectorized per-signal (computes distance array per signal)
    lat_arr = df_temp['lat'].to_numpy()
    lon_arr = df_temp['lon'].to_numpy()
    for i, row in df_sig.iterrows():
        sig_lat = float(row['lat'])
        sig_lon = float(row['lon'])
        # harversineで距離計算（関数呼び出し）
        dists = haversine_vectorized(sig_lat, sig_lon, lat_arr, lon_arr)
        c = int(np.sum(dists <= 75.0))
        name = row.iloc[2] if len(row) > 2 else ''
        description = row.iloc[3] if len(row) > 3 else ''
        counts.append({'signal_idx': i, 'name': name, 'description': description, 'lat': sig_lat, 'lon': sig_lon, 'count': c})
        # counts.append({'signal_idx': i, 'name': name, 'lat': sig_lat, 'lon': sig_lon, 'count': c})
        

    df_out = pd.DataFrame(counts).sort_values('count', ascending=False).reset_index(drop=True)
    # Optional: limit to top N for testing
    df_out = df_out.head(12)

    # Ensure descriptions are strings and not null
    df_out['description'] = df_out['description'].fillna('').astype(str)

    # Save CSV from the DataFrame
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False)

    # Prepare order for plotting (preserve sorted order)
    order = df_out['description'].tolist()

    # Ensure Japanese fonts are available for labels (try several common candidates)
    def set_japanese_font(font_path=None):
        candidates = [
            'Noto Sans CJK JP', 'NotoSansCJKJP', 'IPAPGothic', 'TakaoPGothic',
            'Yu Gothic', 'Hiragino Maru Gothic Pro', 'Meiryo', 'Arial Unicode MS'
        ]
        # If user provided a font file, try to register and use it
        if font_path:
            try:
                fp = Path(font_path).expanduser()
                if not fp.exists():
                    logging.warning('Provided font-path does not exist: %s', font_path)
                else:
                    fm.fontManager.addfont(str(fp))
                    prop = fm.FontProperties(fname=str(fp))
                    name = prop.get_name()
                    plt.rcParams['font.family'] = name
                    logging.info('Using font from --font-path: %s (font name: %s)', font_path, name)
                    plt.rcParams['axes.unicode_minus'] = False
                    return
            except Exception as e:
                logging.warning('Failed to load font from %s: %s', font_path, e)

        available = {f.name for f in fm.fontManager.ttflist}
        for name in candidates:
            if name in available:
                plt.rcParams['font.family'] = name
                logging.info('Using detected font: %s', name)
                break
        else:
            # fallback: keep default but ensure unicode minus is handled
            plt.rcParams['font.family'] = 'sans-serif'
            logging.warning('No preferred Japanese font found. Available sample: %s', ', '.join(sorted(list(available))[:8]))
        plt.rcParams['axes.unicode_minus'] = False

    # configure logging and set font (allows manual font file via CLI)
    logging.basicConfig(level=logging.INFO)
    font_path = args.font_path
    set_japanese_font(font_path)
    # Plot with matplotlib (replace seaborn barplot)
    # Presentation-friendly, subdued palette and subtle grid
    plt.rcParams.update({
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.labelsize': 11,
        'xtick.labelsize': 10,
        'ytick.labelsize': 10,
        'figure.facecolor': 'white'
    })
    plt.figure(figsize=(10, max(4, len(df_out) * 0.15)))
    x = list(range(len(df_out)))
    labels = df_out['description'].tolist()
    counts_vals = df_out['count'].tolist()
    # Single subdued color for publication (consistent, non-colorful)
    single_color = '#4F81BD'  # calm blue suitable for figures
    edge_color = '#2F4F4F'
    colors = [single_color for _ in range(len(df_out))]
    bars = plt.bar(x, counts_vals, color=colors, edgecolor=edge_color, linewidth=0.6, alpha=0.95)
    plt.xticks(x, labels, rotation=45, ha='right')
    plt.ylabel('Number of points within 75 m')
    plt.xlabel('Signal description')
    plt.title('Counts within 75 m of each signal with Stations')
    ax = plt.gca()
    ax.set_facecolor('white')
    ax.grid(axis='y', color='#e6e6e6', linewidth=0.8)
    # tidy layout and save
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=200)
    print(f'Wrote CSV: {out_csv} and PNG: {out_png}')


if __name__ == '__main__':
    main()
