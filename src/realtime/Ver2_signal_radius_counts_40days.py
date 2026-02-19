import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
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
    p = argparse.ArgumentParser(description='Count points within 100m of each signal and plot')
    # parquetのパスは単一ファイル用
    p.add_argument('--vehicle', default='~/BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/output.parquet')
    
    p.add_argument('--signals', default='~/BachelorThesis/headway-optimisation-gtfs/data/raw/signals/signals.txt')
    p.add_argument('--out-csv', default='~/BachelorThesis/headway-optimisation-gtfs/results/signal_100m_counts_all.csv')
    p.add_argument('--out-png', default='~/BachelorThesis/headway-optimisation-gtfs/results/signal_100m_counts_all.png')
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
        c = int(np.sum(dists <= 100.0))
        name = row.iloc[2] if len(row) > 2 else ''
        description = row.iloc[3] if len(row) > 3 else ''
        counts.append({'signal_idx': i, 'name': name, 'description': description, 'lat': sig_lat, 'lon': sig_lon, 'count': c})
        # counts.append({'signal_idx': i, 'name': name, 'lat': sig_lat, 'lon': sig_lon, 'count': c})
        

    df_out = pd.DataFrame(counts).sort_values('count', ascending=False).reset_index(drop=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False)

    # Plot with seaborn
    sns.set(style='whitegrid')
    plt.figure(figsize=(10, max(4, len(df_out) * 0.15)))
    ax = sns.barplot(data=df_out, x='signal_idx', y='count', order=df_out['signal_idx'], palette='viridis')
    ax.set_ylabel('Signal index')
    ax.set_xlabel('Count within 100 m')
    plt.title('Counts within 100 m of each signal')
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=200)
    print(f'Wrote CSV: {out_csv} and PNG: {out_png}')


if __name__ == '__main__':
    main()
