import json
from pathlib import Path
import pandas as pd
from keplergl import KeplerGl


DATA_PATH = Path.home() / "BachelorThesis/headway-optimisation-gtfs/data/raw/chitetsu_tram/vehicle_positions/20251111.parquet"
SIGNALS_PATH = Path("data/raw/signals/signals_chitetsu_tram.json")


def load_signals(path: Path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    # ensure lat/lng columns present
    if "lat" in df.columns and "lng" in df.columns:
        return df
    if "lat" in df.columns and "lon" in df.columns:
        df = df.rename(columns={"lon": "lng"})
        return df
    return None


def main():
    # データ読み込み（dwell.py と同じファイルを使用）
    df = pd.read_parquet(DATA_PATH.expanduser(), engine="pyarrow")

    # 必要なカラムだけ残す + 特定車両に絞り込み
    df = df.replace("None", pd.NA).dropna(axis=1, how="all")
    df = df.drop(columns=[c for c in ["bearing", "speed"] if c in df.columns], errors="ignore")
    df_temp = df[df.get("vehicle_id") == "chitetsu_tram_4976"].copy()

    # Kepler 用にカラム名を調整（Kepler は lng/lat を期待することが多い）
    if "lon" in df_temp.columns and "lat" in df_temp.columns:
        df_temp = df_temp.rename(columns={"lon": "lng"})

    # Kepler インスタンスを作成してデータを追加
    map_ = KeplerGl(height=800)
    map_.add_data(data=df_temp, name="toyama_vehicle_positions")

    # 信号データがあれば読み込んで追加
    sig_df = load_signals(SIGNALS_PATH)
    if sig_df is not None and not sig_df.empty:
        map_.add_data(data=sig_df, name="signals")

    # 保存（HTML） — Kepler の GUI でレイヤーを heatmap/point に変更可能
    out = "toyama_tram_kepler.html"
    map_.save_to_html(file_name=out)
    print(f"Saved Kepler HTML to {out}")


if __name__ == "__main__":
    main()
