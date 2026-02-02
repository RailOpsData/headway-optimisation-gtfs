"""
Clean, modular version of the GTFS-RT -> timetable pipeline.

This script implements the core steps from src/realtime/gtfs-rt.py (sections 1–7)
but organizes them into reusable functions and removes redundant reads/operations.

Usage: adjust path constants below or call functions from another script.
"""
from typing import Optional, Sequence
from pathlib import Path
import numpy as np
import polars as pl

pl.Config.set_tbl_rows(100)


def _drop_all_null_columns(df: pl.DataFrame) -> pl.DataFrame:
    cols_to_drop = [c for c in df.columns if df[c].null_count() == df.height]
    if not cols_to_drop:
        return df
    return df.select(pl.all().exclude(cols_to_drop))


def load_vehicle_positions(path: str, hour_min: int = 6, hour_max: int = 9, keep_cols: Optional[Sequence[str]] = None) -> pl.DataFrame:
    df = pl.read_parquet(path)
    df = _drop_all_null_columns(df)
    cols_to_remove = [c for c in ['bearing', 'speed'] if c in df.columns]
    if cols_to_remove:
        df = df.drop(cols_to_remove)
    df = df.sort(['vehicle_id', 'snapshot_ts']).filter(pl.col('snapshot_ts').dt.hour().is_between(hour_min, hour_max))
    if keep_cols is not None:
        df = df.select(keep_cols)
    return df


def load_trip_updates(path: str, hour_min: int = 6, hour_max: int = 9, route_filter: Optional[Sequence[str]] = None) -> pl.DataFrame:
    df = pl.read_parquet(path)
    df = _drop_all_null_columns(df)
    cols_to_remove = [c for c in ['start_time', 'start_date'] if c in df.columns]
    if cols_to_remove:
        df = df.drop(cols_to_remove)
    df = df.sort(['vehicle_id', 'snapshot_ts']).filter(pl.col('snapshot_ts').dt.hour().is_between(hour_min, hour_max))
    if route_filter is not None:
        df = df.filter(pl.col('route_id').is_in(route_filter))
    return df.select(['snapshot_ts', 'route_id', 'vehicle_id'])


def remove_duplicate_observations(df: pl.DataFrame, subset: Sequence[str]) -> pl.DataFrame:
    return df.unique(subset=list(subset), keep='first')


def impute_route_from_neighbors(df: pl.DataFrame) -> pl.DataFrame:
    # forward/backward fill per vehicle and fill when they agree
    df_sorted = df.sort(['vehicle_id', 'snapshot_ts'])
    route_ff = pl.col('route_id').forward_fill().over('vehicle_id')
    route_bf = pl.col('route_id').backward_fill().over('vehicle_id')
    route = pl.when(pl.col('route_id').is_null() & (route_ff == route_bf)).then(route_ff).otherwise(pl.col('route_id')).alias('route_id')
    return df_sorted.with_columns([route])


def extract_route_number(route_name_col: pl.Expr = pl.col('route_id_name')) -> pl.Expr:
    return route_name_col.str.extract(r"\(([-\d]+)\)", 1).alias('route_id')


def load_gtfs_stops(stops_csv: str, interesting_names: Optional[Sequence[str]] = None) -> pl.DataFrame:
    df = pl.read_csv(stops_csv).select(['stop_name', 'stop_lat', 'stop_lon'])
    if interesting_names is not None:
        df = df.filter(pl.col('stop_name').is_in(interesting_names))
    return df


def compute_nearest_stop(df_coords: pl.DataFrame, df_stops: pl.DataFrame) -> pl.DataFrame:
    # df_coords: unique (lat, lon) from realtime data
    # df_stops: stop_name, stop_lat, stop_lon
    df_calc = df_coords.join(df_stops, how='cross')
    EQ_LON_DIST = 111320
    LAT_DIST = 111111
    df_with_dist = df_calc.with_columns(
        (((pl.col('lat') + pl.col('stop_lat')) / 2).alias('lat_avg'))
    ).with_columns(
        (np.cos(np.deg2rad(pl.col('lat_avg'))) * EQ_LON_DIST).alias('lon_factor')
    ).with_columns(
        (
            ((pl.col('lat') - pl.col('stop_lat')) * LAT_DIST).pow(2) +
            ((pl.col('lon') - pl.col('stop_lon')) * pl.col('lon_factor')).pow(2)
        ).sqrt().alias('distance_m')
    )
    nearest = (
        df_with_dist.sort('distance_m').group_by(['lat', 'lon']).first().select(['lat', 'lon', 'stop_name', 'distance_m'])
    )
    return nearest.rename({'stop_name': 'nearest_stop'})


def derive_trip_counts(df: pl.DataFrame, seq_col: str = 'current_stop_sequence_loc') -> pl.DataFrame:
    cond = (
        (pl.col(seq_col) < pl.col(seq_col).shift(1)) |
        ((pl.col(seq_col) - pl.col(seq_col).shift(1)).abs() > 5)
    )
    trip_count = (cond.over('vehicle_id').fill_null(True).cast(pl.Int32).cum_sum().over('vehicle_id').alias('trip_count'))
    return df.sort(['vehicle_id', 'snapshot_ts']).with_columns([trip_count])


def select_nearest_approach(df: pl.DataFrame) -> pl.DataFrame:
    # group by vehicle/trip/sequence and take the row with smallest distance
    return (
        df.sort('distance_m')
        .group_by(['vehicle_id', 'trip_count', 'current_stop_sequence_loc'])
        .first()
    )


def extract_timetable(df: pl.DataFrame, direction_val: int = 1, pivot_index: Sequence[str] = ['vehicle_id', 'trip_count', 'direction_id']) -> pl.DataFrame:
    df_time = df.with_columns(pl.col('snapshot_ts').dt.strftime('%H:%M:%S').alias('time_str'))
    timetable = df_time.pivot(values='time_str', index=pivot_index, on='nearest_stop', aggregate_function='first')
    return timetable.filter(pl.col('direction_id') == direction_val)


def main():
    # Paths (adjust as needed)
    base = Path('/app/data/raw/chitetsu_tram')
    vp_path = base / 'vehicle_positions' / '20251111.parquet'
    tu_path = base / 'trip_updates' / '20251111.parquet'
    stops_csv = base / 'gtfs_jp_tram' / 'stops.txt'

    # 1. load and prefilter
    df_v = load_vehicle_positions(str(vp_path))

    # 2. trip updates (filtered to relevant route ids if needed)
    route_filter = [
        '市内軌道線(3001-2-1)', '市内軌道線(3001-2-2)',
        '富山港線（富山大学前）(3003-5-2)', '富山港線（富山大学前）(3003-5-1)',
        '市内軌道線(3001-8-1)', '市内軌道線(3001-8-2)',
        '富山港線（富山大学前）(3003-5-2-1)', '富山港線（富山大学前）(3003-5-1-1)'
    ]
    df_t = load_trip_updates(str(tu_path), route_filter=route_filter)

    # 3. filter vehicle set (keep as in original)
    target_vehicles = [
        'chitetsu_tram_4980', 'chitetsu_tram_4983', 'chitetsu_tram_4984',
        'chitetsu_tram_4985', 'chitetsu_tram_4986', 'chitetsu_tram_4987',
        'chitetsu_tram_4989', 'chitetsu_tram_4990', 'chitetsu_tram_4991',
        'chitetsu_tram_4992', 'chitetsu_tram_4993', 'chitetsu_tram_4995',
        'chitetsu_tram_5001', 'chitetsu_tram_5004', 'chitetsu_tram_5005',
        'chitetsu_tram_5006', 'chitetsu_tram_5011'
    ]
    df_v = df_v.filter(pl.col('vehicle_id').is_in(target_vehicles))

    # 4. remove duplicates
    df_v = remove_duplicate_observations(df_v, ['snapshot_ts', 'vehicle_id'])
    df_t = remove_duplicate_observations(df_t, ['snapshot_ts', 'vehicle_id'])

    # 5. join trip updates (route info) and impute where possible
    df_vt = df_v.join(df_t, on=['snapshot_ts', 'vehicle_id'], how='left')
    df_vt = impute_route_from_neighbors(df_vt)
    df_vt = df_vt.drop_nulls('route_id')

    # 6. extract numeric route id
    df_vt = df_vt.rename({'route_id': 'route_id_name'}).with_columns(extract_route_number(pl.col('route_id_name')))

    # 7. prepare GTFS static stop mapping
    uni = [
        '富山駅', '新富町', '県庁前', '丸の内', '諏訪川原', '安野屋', 'トヨタモビリティ富山Gスクエア五福前', '富山大学前'
    ]
    df_stops = load_gtfs_stops(str(stops_csv), uni)

    # 8. join static stop_sequence (only compute once)
    # load stop_times and create route-stop mapping
    df_stop_times = pl.read_csv(str(base / 'gtfs_jp_tram' / 'stop_times.txt')).select(['trip_id', 'stop_name', 'stop_sequence']).with_columns([
        pl.col('trip_id').str.extract(r'^([^_%]+)', 1).alias('service_day'),
        pl.col('trip_id').str.extract(r'系統(.*)$', 1).alias('route_id')
    ])
    df_route_stops = df_stop_times.unique(subset=['route_id', 'stop_sequence', 'stop_name']).select(['route_id', 'stop_sequence', 'stop_name'])
    df_route_stops = df_route_stops.filter(pl.col('stop_name').is_in(uni))

    # 9. join route+current_stop_sequence from original (if exists) to route_stops
    # attempt to map current_stop_sequence -> stop_name when possible
    df_rt = df_vt.join(df_route_stops, left_on=['route_id', 'current_stop_sequence'], right_on=['route_id', 'stop_sequence'], how='inner')

    # 10. compute nearest stop using coords from df_rt
    df_coords = df_rt.select(['lat', 'lon']).unique()
    nearest = compute_nearest_stop(df_coords, df_stops)
    df_merge = df_rt.join(nearest, on=['lat', 'lon'], how='inner')

    # 11. derive location-based stop_sequence
    df_merge = df_merge.join(df_route_stops, left_on=['route_id', 'nearest_stop'], right_on=['route_id', 'stop_name'], how='left')
    df_merge = df_merge.rename({'stop_sequence': 'current_stop_sequence_loc'})

    # 12. trip segmentation and nearest-approach selection
    df_with_trip = derive_trip_counts(df_merge, 'current_stop_sequence_loc')
    df_final = select_nearest_approach(df_with_trip).sort(['vehicle_id', 'snapshot_ts'])

    # 13. direction extraction
    df_final = df_final.with_columns(pl.col('route_id').str.extract(r'(\d)$').cast(pl.Int32).alias('direction_id'))

    # 14. pivot to timetable and write
    timetable = extract_timetable(df_final, direction_val=1)
    timetable.write_csv('timetable_daigakumae_clean.csv')


if __name__ == '__main__':
    main()
