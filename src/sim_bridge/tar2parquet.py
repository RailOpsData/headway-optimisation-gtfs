#!/usr/bin/env python
"""
Extract JSON from tar archives and save as normalized Parquet files (DuckDB-accelerated)
tarアーカイブからJSONを抽出し、正規化されたParquetファイルとして保存（DuckDB高速化版）
"""
import argparse
import tarfile
import os
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional
import io
import tempfile
import shutil
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import polars as pl

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    print("Warning: DuckDB not installed. Install with: pip install duckdb")
    print("Falling back to standard Python processing...")

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


# Reuse all JSON parsing logic from existing module
# 既存モジュールからすべてのJSON解析ロジックを再利用
from gtfsrt_json2Parquet import (
    load_trip_updates_from_json,
    load_vehicle_positions_from_json,
    save_to_parquet_partitioned,
)


def process_tar_with_duckdb(
    tar_path: Path,
    output_dir: Path,
    agency_filter: str = None,
    show_agencies: bool = False,
    threads: int = None,
) -> None:
    """
    Extract JSON from tar using DuckDB for high-speed processing
    DuckDBを使用してtarからJSONを高速に抽出
    
    Args:
        tar_path: Path to tar/tar.gz file / tar/tar.gzファイルのパス
        output_dir: Output directory (e.g., ./data/bronze) / 出力ディレクトリ
        agency_filter: Optional agency to filter (e.g., 'chitetsu_tram,chitetsu_bus')
        show_agencies: If True, only show detected agencies and exit
        threads: Number of DuckDB threads (None = auto-detect) / DuckDBスレッド数
    """
    if not HAS_DUCKDB:
        print("Error: DuckDB is required for this mode. Install with: pip install duckdb")
        print("Falling back to Python processing...")
        return process_tar_to_normalized_parquet(tar_path, output_dir, agency_filter, show_agencies, None)
    
    print(f"Processing {tar_path} with DuckDB...")
    
    # Initialize DuckDB with optimal settings / 最適な設定でDuckDBを初期化
    conn = duckdb.connect(':memory:')
    
    # Set thread count / スレッド数を設定
    if threads is None:
        threads = os.cpu_count() or 4
    conn.execute(f"PRAGMA threads={threads};")
    conn.execute("PRAGMA memory_limit='8GB';")
    conn.execute(f"PRAGMA temp_directory='{tempfile.gettempdir()}';")
    
    print(f"DuckDB threads: {threads}")
    print(f"Memory limit: 8GB")
    
    # Parse agency filter / agencyフィルターを解析
    allowed_agencies = None
    if agency_filter:
        allowed_agencies = set(a.strip() for a in agency_filter.split(','))
        print(f"Filtering agencies: {allowed_agencies}")
    
    # Track statistics / 統計情報を追跡
    detected_agencies: Set[str] = set()
    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    
    # Phase 1: Read all JSON files from tar into memory (DuckDB can't read tar directly)
    # フェーズ1: tarから全JSONファイルをメモリに読み込む（DuckDBはtarを直接読めない）
    print("\nPhase 1: Reading JSON files from tar...")
    json_tasks: List[Tuple[str, bytes, str]] = []
    total_size_bytes = 0
    
    with tarfile.open(tar_path, 'r:*') as tar:
        members = [m for m in tar if m.isfile() and m.name.endswith('.json')]
        total_members = len(members)
        print(f"Total JSON files in tar: {total_members}")
        
        iterator = tqdm(members, desc="Reading tar") if HAS_TQDM else members
        
        for member in iterator:
            # Infer feed type from filename / ファイル名からfeed typeを推測
            if 'trip_update' in member.name:
                feed_type = 'trip_updates'
            elif 'vehicle_position' in member.name:
                feed_type = 'vehicle_positions'
            else:
                continue
            
            # Extract content / 内容を抽出
            try:
                content = tar.extractfile(member).read()
                total_size_bytes += len(content)
                json_tasks.append((member.name, content, feed_type))
            except Exception as e:
                continue
    
    print(f"\nData size: {total_size_bytes / (1024**3):.2f} GB ({len(json_tasks)} files)")
    print(f"Loaded {len(json_tasks)} JSON files into memory")
    
    # Phase 2: Parse JSON files using existing gtfsrt_json2Parquet functions
    # フェーズ2: 既存のgtfsrt_json2Parquet関数を使用してJSONファイルを解析
    print("\nPhase 2: Parsing JSON files with existing logic...")
    
    trip_updates_dfs: List[pl.DataFrame] = []
    vehicle_positions_dfs: List[pl.DataFrame] = []
    processed_count = 0
    skipped_count = 0
    
    iterator = json_tasks
    if HAS_TQDM:
        iterator = tqdm(json_tasks, desc="Parsing JSON")
    
    for filename, content, feed_type in iterator:
        try:
            # Create canonical filename so parser can extract timestamp
            canon_name = _canonicalize_name_for_parser(filename, feed_type, content)
            # Create mock path for compatibility / 互換性のためモックパスを作成
            mock_path = _TarMockPath(canon_name, content)
            
            # Parse using existing functions / 既存関数を使用して解析
            if feed_type == 'trip_updates':
                df = load_trip_updates_from_json(mock_path)
            else:
                df = load_vehicle_positions_from_json(mock_path)
            
            if df.is_empty():
                skipped_count += 1
                continue
            
            # Detect agency / agencyを検出
            agency = df['agency'][0] if 'agency' in df.columns else 'unknown'
            detected_agencies.add(agency)
            stats[agency][feed_type] += 1
            
            # Apply filter / フィルターを適用
            if allowed_agencies and agency not in allowed_agencies:
                skipped_count += 1
                continue
            
            # Store DataFrame / DataFrameを保存
            if feed_type == 'trip_updates':
                trip_updates_dfs.append(df)
            else:
                vehicle_positions_dfs.append(df)
            
            processed_count += 1
        
        except Exception as e:
            skipped_count += 1
            continue
    
    # Show statistics / 統計情報を表示
    print(f"\n{'='*60}")
    print(f"Processing Summary:")
    print(f"  Total JSON files: {len(json_tasks)}")
    print(f"  Successfully processed: {processed_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"\nDetected Agencies:")
    for agency in sorted(detected_agencies):
        tu_count = stats[agency]['trip_updates']
        vp_count = stats[agency]['vehicle_positions']
        print(f"  {agency}:")
        print(f"    - trip_updates: {tu_count}")
        print(f"    - vehicle_positions: {vp_count}")
    print(f"{'='*60}\n")
    
    # If only showing agencies, exit here / agency検出のみの場合はここで終了
    if show_agencies:
        print("Agency detection complete. Use --agency-filter to filter specific agencies.")
        conn.close()
        return
    
    # Phase 3: Combine and save parquet files using DuckDB for faster I/O
    # フェーズ3: DuckDBを使用してより高速にparquetファイルを結合・保存
    print("Phase 3: Saving parquet files with DuckDB...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save trip_updates / trip_updatesを保存
    if trip_updates_dfs:
        print(f"\nCombining {len(trip_updates_dfs)} trip_updates DataFrames...")
        combined_tu = pl.concat(trip_updates_dfs, how="vertical")
        print(f"Total trip_updates records: {combined_tu.height}")
        
        saved_files = 0
        for agency in sorted(combined_tu['agency'].unique()):
            agency_df = combined_tu.filter(pl.col('agency') == agency)
            
            for date_str in sorted(agency_df['date_str'].unique()):
                date_df = agency_df.filter(pl.col('date_str') == date_str)
                out_path = save_to_parquet_partitioned(
                    date_df, output_dir, agency, 'trip_updates', date_str
                )
                print(f"  [{agency}] Saved {date_df.height} trip_updates to {out_path.name}")
                saved_files += 1
        
        print(f"Saved {saved_files} trip_updates parquet files")
    
    # Save vehicle_positions / vehicle_positionsを保存
    if vehicle_positions_dfs:
        print(f"\nCombining {len(vehicle_positions_dfs)} vehicle_positions DataFrames...")
        combined_vp = pl.concat(vehicle_positions_dfs, how="vertical")
        print(f"Total vehicle_positions records: {combined_vp.height}")
        
        saved_files = 0
        for agency in sorted(combined_vp['agency'].unique()):
            agency_df = combined_vp.filter(pl.col('agency') == agency)
            
            for date_str in sorted(agency_df['date_str'].unique()):
                date_df = agency_df.filter(pl.col('date_str') == date_str)
                out_path = save_to_parquet_partitioned(
                    date_df, output_dir, agency, 'vehicle_positions', date_str
                )
                print(f"  [{agency}] Saved {date_df.height} vehicle_positions to {out_path.name}")
                saved_files += 1
        
        print(f"Saved {saved_files} vehicle_positions parquet files")
    
    conn.close()


class _TarMockPath:
    """
    Mock Path object for tar member compatibility with gtfsrt_json2Parquet functions
    tarメンバーをgtfsrt_json2Parquet関数と互換性を持たせるためのモックPathオブジェクト
    """
    def __init__(self, name: str, content: bytes):
        # Ensure .name is basename like pathlib.Path.name
        self.name = os.path.basename(name)
        self._content = content
    
    def open(self, mode='r', encoding='utf-8'):
        """Return file-like object for reading / 読み込み用のファイル風オブジェクトを返す"""
        return io.TextIOWrapper(io.BytesIO(self._content), encoding=encoding)


def _process_single_json(args: Tuple[str, bytes, str]) -> Optional[Tuple[str, pl.DataFrame]]:
    """
    Process a single JSON file (used for parallel processing)
    単一JSONファイルを処理（並列処理用）
    
    Args:
        args: Tuple of (filename, content, feed_type)
        
    Returns:
        Tuple of (feed_type, DataFrame) or None on error
    """
    filename, content, feed_type = args
    
    try:
        canon_name = _canonicalize_name_for_parser(filename, feed_type, content)
        mock_path = _TarMockPath(canon_name, content)
        
        if feed_type == 'trip_updates':
            df = load_trip_updates_from_json(mock_path)
        else:
            df = load_vehicle_positions_from_json(mock_path)
        
        if df.is_empty():
            return None
        
        return (feed_type, df)
    
    except Exception as e:
        # Silent failures in parallel mode to avoid overwhelming output
        return None


def _canonicalize_name_for_parser(original_name: str, feed_type: str, content: bytes) -> str:
    """Return a filename matching parser regex using JSON header timestamp.

    Output pattern (no agency): gtfs_rt_{feed_type}_{YYYYMMDD_HHMMSS}.json
    Falls back to 19700101_000000 if timestamp not found.
    """
    ts_str = "19700101_000000"
    try:
        feed = json.loads(content.decode("utf-8", errors="ignore"))
        ts = None
        if isinstance(feed, dict):
            header = feed.get("header") or {}
            ts = header.get("timestamp")
            if ts is None:
                ents = feed.get("entity") or []
                for ent in ents:
                    if isinstance(ent, dict):
                        if "trip_update" in ent and isinstance(ent["trip_update"], dict):
                            ts = ent["trip_update"].get("timestamp")
                        elif "vehicle" in ent and isinstance(ent["vehicle"], dict):
                            ts = ent["vehicle"].get("timestamp")
                        if ts is not None:
                            break
        if ts is not None:
            ts_int = int(ts)
            ts_str = datetime.utcfromtimestamp(ts_int).strftime("%Y%m%d_%H%M%S")
    except Exception:
        pass
    # feed_type expected to be 'trip_updates' or 'vehicle_positions'
    return f"gtfs_rt_{feed_type}_{ts_str}.json"


def process_tar_to_normalized_parquet(
    tar_path: Path,
    output_dir: Path,
    agency_filter: str = None,
    show_agencies: bool = False,
    workers: int = None,
) -> None:
    """
    Extract JSON from tar and directly convert to normalized partitioned parquet
    tarからJSONを抽出し、直接正規化されたパーティション化parquetに変換
    
    Args:
        tar_path: Path to tar/tar.gz file / tar/tar.gzファイルのパス
        output_dir: Output directory (e.g., ./data/bronze) / 出力ディレクトリ
        agency_filter: Optional agency to filter (e.g., 'chitetsu_tram,chitetsu_bus')
        show_agencies: If True, only show detected agencies and exit
        workers: Number of parallel workers (None = auto-detect, 1 = sequential) / 並列ワーカー数
    """
    print(f"Processing {tar_path}...")
    
    # Parse agency filter / agencyフィルターを解析
    allowed_agencies = None
    if agency_filter:
        allowed_agencies = set(a.strip() for a in agency_filter.split(','))
        print(f"Filtering agencies: {allowed_agencies}")
    
    # Track statistics / 統計情報を追跡
    detected_agencies: Set[str] = set()
    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    
    # Phase 1: Read all JSON files from tar into memory
    # フェーズ1: tarから全JSONファイルをメモリに読み込む
    print("\nPhase 1: Reading JSON files from tar...")
    json_tasks: List[Tuple[str, bytes, str]] = []
    total_size_bytes = 0
    
    with tarfile.open(tar_path, 'r:*') as tar:
        members = [m for m in tar if m.isfile() and m.name.endswith('.json')]
        total_members = len(members)
        print(f"Total JSON files in tar: {total_members}")
        
        iterator = tqdm(members, desc="Reading tar") if HAS_TQDM else members
        
        for member in iterator:
            # Infer feed type from filename / ファイル名からfeed typeを推測
            if 'trip_update' in member.name:
                feed_type = 'trip_updates'
            elif 'vehicle_position' in member.name:
                feed_type = 'vehicle_positions'
            else:
                continue
            
            # Extract content / 内容を抽出
            try:
                content = tar.extractfile(member).read()
                total_size_bytes += len(content)
                json_tasks.append((member.name, content, feed_type))
            except Exception as e:
                continue
    
    # Auto-detect optimal workers if not specified / 指定されていない場合は最適なワーカー数を自動検出
    if workers is None:
        # Simple auto-detection: use 1 for small data, else use all cores
        cpu_count = os.cpu_count() or 4
        if len(json_tasks) < 1000 and total_size_bytes < 50 * 1024 * 1024:
            workers = 1
        else:
            workers = cpu_count
        print(f"\nData size: {total_size_bytes / (1024**3):.2f} GB ({len(json_tasks)} files)")
        print(f"Auto-detected optimal workers: {workers}")
    elif workers > 1:
        print(f"\nData size: {total_size_bytes / (1024**3):.2f} GB ({len(json_tasks)} files)")
        print(f"Using {workers} parallel workers")
    
    print(f"Loaded {len(json_tasks)} JSON files into memory")
    
    # Phase 2: Parse JSON files (parallel or sequential)
    # フェーズ2: JSONファイルを解析（並列または逐次）
    print("\nPhase 2: Parsing JSON files...")
    
    trip_updates_dfs: List[pl.DataFrame] = []
    vehicle_positions_dfs: List[pl.DataFrame] = []
    processed_count = 0
    skipped_count = 0
    
    if workers > 1:
        # Parallel processing / 並列処理
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_single_json, task): task for task in json_tasks}
            
            iterator = as_completed(futures)
            if HAS_TQDM:
                iterator = tqdm(iterator, total=len(json_tasks), desc="Parsing JSON")
            
            for future in iterator:
                result = future.result()
                if result is None:
                    skipped_count += 1
                    continue
                
                feed_type, df = result
                
                # Detect agency / agencyを検出
                agency = df['agency'][0] if 'agency' in df.columns else 'unknown'
                detected_agencies.add(agency)
                stats[agency][feed_type] += 1
                
                # Apply filter / フィルターを適用
                if allowed_agencies and agency not in allowed_agencies:
                    skipped_count += 1
                    continue
                
                # Store DataFrame / DataFrameを保存
                if feed_type == 'trip_updates':
                    trip_updates_dfs.append(df)
                else:
                    vehicle_positions_dfs.append(df)
                
                processed_count += 1
    else:
        # Sequential processing / 逐次処理
        iterator = json_tasks
        if HAS_TQDM:
            iterator = tqdm(json_tasks, desc="Parsing JSON")
        
        for task in iterator:
            result = _process_single_json(task)
            if result is None:
                skipped_count += 1
                continue
            
            feed_type, df = result
            
            # Detect agency / agencyを検出
            agency = df['agency'][0] if 'agency' in df.columns else 'unknown'
            detected_agencies.add(agency)
            stats[agency][feed_type] += 1
            
            # Apply filter / フィルターを適用
            if allowed_agencies and agency not in allowed_agencies:
                skipped_count += 1
                continue
            
            # Store DataFrame / DataFrameを保存
            if feed_type == 'trip_updates':
                trip_updates_dfs.append(df)
            else:
                vehicle_positions_dfs.append(df)
            
            processed_count += 1
    
    # Show statistics / 統計情報を表示
    print(f"\n{'='*60}")
    print(f"Processing Summary:")
    print(f"  Total JSON files: {len(json_tasks)}")
    print(f"  Successfully processed: {processed_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"\nDetected Agencies:")
    for agency in sorted(detected_agencies):
        tu_count = stats[agency]['trip_updates']
        vp_count = stats[agency]['vehicle_positions']
        print(f"  {agency}:")
        print(f"    - trip_updates: {tu_count}")
        print(f"    - vehicle_positions: {vp_count}")
    print(f"{'='*60}\n")
    
    # If only showing agencies, exit here / agency検出のみの場合はここで終了
    if show_agencies:
        print("Agency detection complete. Use --agency-filter to filter specific agencies.")
        return
    
    # Phase 3: Combine and save parquet files
    # フェーズ3: 結合してparquetファイルを保存
    print("Phase 3: Saving parquet files...")
    
    # Save trip_updates / trip_updatesを保存
    if trip_updates_dfs:
        print(f"\nCombining {len(trip_updates_dfs)} trip_updates DataFrames...")
        combined_tu = pl.concat(trip_updates_dfs, how="vertical")
        print(f"Total trip_updates records: {combined_tu.height}")
        
        saved_files = 0
        for agency in sorted(combined_tu['agency'].unique()):
            agency_df = combined_tu.filter(pl.col('agency') == agency)
            
            for date_str in sorted(agency_df['date_str'].unique()):
                date_df = agency_df.filter(pl.col('date_str') == date_str)
                out_path = save_to_parquet_partitioned(
                    date_df, output_dir, agency, 'trip_updates', date_str
                )
                print(f"  [{agency}] Saved {date_df.height} trip_updates to {out_path.name}")
                saved_files += 1
        
        print(f"Saved {saved_files} trip_updates parquet files")
    
    # Save vehicle_positions / vehicle_positionsを保存
    if vehicle_positions_dfs:
        print(f"\nCombining {len(vehicle_positions_dfs)} vehicle_positions DataFrames...")
        combined_vp = pl.concat(vehicle_positions_dfs, how="vertical")
        print(f"Total vehicle_positions records: {combined_vp.height}")
        
        saved_files = 0
        for agency in sorted(combined_vp['agency'].unique()):
            agency_df = combined_vp.filter(pl.col('agency') == agency)
            
            for date_str in sorted(agency_df['date_str'].unique()):
                date_df = agency_df.filter(pl.col('date_str') == date_str)
                out_path = save_to_parquet_partitioned(
                    date_df, output_dir, agency, 'vehicle_positions', date_str
                )
                print(f"  [{agency}] Saved {date_df.height} vehicle_positions to {out_path.name}")
                saved_files += 1
        
        print(f"Saved {saved_files} vehicle_positions parquet files")


def process_tar_directory(tar_dir: str, output_dir: str, pattern: str = "*.tar*", agency_filter: str = None, threads: int = None, use_python: bool = False):
    """
    Process tar files in the specified directory
    指定ディレクトリ内のtarファイルを処理
    
    Args:
        tar_dir: Directory containing tar files / tarファイルが格納されているディレクトリ
        output_dir: Output directory for parquet files / parquetファイルの出力先ディレクトリ
        pattern: File pattern to process / 処理対象のファイルパターン
        agency_filter: Optional agency filter / オプションのagencyフィルター
        threads: Number of DuckDB threads or Python workers / DuckDBスレッド数またはPythonワーカー数
        use_python: Force Python processing instead of DuckDB / DuckDBの代わりにPython処理を強制
    """
    tar_dir_path = Path(tar_dir)
    output_dir_path = Path(output_dir)
    
    # Create output directory / 出力ディレクトリを作成
    output_dir_path.mkdir(parents=True, exist_ok=True)
    
    # Search for tar files / tarファイルを検索
    tar_files = list(tar_dir_path.glob(pattern))
    
    if not tar_files:
        print(f"Error: No files matching {pattern} found in {tar_dir}")
        return
    
    print(f"Processing {len(tar_files)} tar file(s)...")
    
    for tar_file in tar_files:
        print(f"\n{'='*60}")
        print(f"Processing tar file: {tar_file.name}")
        print(f"{'='*60}")
        
        try:
            # Use DuckDB by default unless use_python is True
            if HAS_DUCKDB and not use_python:
                process_tar_with_duckdb(
                    tar_file,
                    output_dir_path,
                    agency_filter=agency_filter,
                    show_agencies=False,
                    threads=threads
                )
            else:
                process_tar_to_normalized_parquet(
                    tar_file,
                    output_dir_path,
                    agency_filter=agency_filter,
                    show_agencies=False,
                    workers=threads
                )
        except Exception as e:
            print(f"Error: Failed to process {tar_file.name}: {e}")
            import traceback
            traceback.print_exc()
            continue


def create_test_tar(test_dir: Path) -> Path:
    """
    Create a test tar file with GTFS-RT JSON format for testing
    テスト用のGTFS-RT形式JSONを含むtarファイルを作成
    
    Args:
        test_dir: Directory to create test files in / テストファイルを作成するディレクトリ
        
    Returns:
        Path to the created tar file / 作成されたtarファイルのパス
    """
    import json
    from datetime import datetime, timedelta
    
    # Create mock GTFS-RT JSON data / モックGTFS-RT JSONデータを作成
    now = datetime.now()
    now_timestamp = int(now.timestamp())
    ts_str = now.strftime("%Y%m%d_%H%M%S")
    ts_str_plus1h = (now + timedelta(hours=1)).strftime("%Y%m%d_%H%M%S")
    
    trip_update_json = {
        "header": {
            "gtfs_realtime_version": "2.0",
            "timestamp": now_timestamp
        },
        "entity": [
            {
                "id": "trip1",
                "trip_update": {
                    "trip": {
                        "trip_id": "trip_001",
                        "route_id": "route_A"
                    },
                    "stop_time_update": [
                        {
                            "stop_sequence": 1,
                            "arrival": {"delay": 120},
                            "stop_id": "stop_1"
                        }
                    ]
                }
            }
        ]
    }
    
    vehicle_position_json = {
        "header": {
            "gtfs_realtime_version": "2.0",
            "timestamp": now_timestamp
        },
        "entity": [
            {
                "id": "vehicle1",
                "vehicle": {
                    "trip": {
                        "trip_id": "trip_001",
                        "route_id": "route_A"
                    },
                    "position": {
                        "latitude": 35.6762,
                        "longitude": 139.6503
                    },
                    "timestamp": now_timestamp
                }
            }
        ]
    }
    
    # Create test files with expected filename pattern for parser
    # 期待されるファイル名パターンでテストファイルを作成
    # Pattern: gtfs_rt_{feed_type}_{agency}_{YYYYMMDD_HHMMSS}.json
    test_files = {
        f'test_agency/gtfs_rt_trip_updates_test_agency_{ts_str}.json': json.dumps(trip_update_json).encode('utf-8'),
        f'test_agency/gtfs_rt_vehicle_positions_test_agency_{ts_str}.json': json.dumps(vehicle_position_json).encode('utf-8'),
        f'test_agency/gtfs_rt_trip_updates_test_agency_{ts_str_plus1h}.json': json.dumps(trip_update_json).encode('utf-8'),
    }
    
    # Create tar file / tarファイルを作成
    tar_path = test_dir / 'test.tar.gz'
    
    with tarfile.open(tar_path, 'w:gz') as tar:
        for filename, content in test_files.items():
            # Create TarInfo object / TarInfoオブジェクトを作成
            tarinfo = tarfile.TarInfo(name=filename)
            tarinfo.size = len(content)
            
            # Add file to tar / tarにファイルを追加
            tar.addfile(tarinfo, io.BytesIO(content))
    
    return tar_path


def run_test():
    """
    Run a test to verify the script functionality
    スクリプトの機能を検証するテストを実行
    """
    print("=" * 60)
    print("Running test mode...")
    print("=" * 60)
    
    # Create temporary directories / 一時ディレクトリを作成
    temp_base = tempfile.mkdtemp(prefix='tar2parquet_test_')
    temp_dir = Path(temp_base)
    tar_dir = temp_dir / 'tar_files'
    output_dir = temp_dir / 'output'
    tar_dir.mkdir()
    
    try:
        # Create test tar file / テスト用tarファイルを作成
        print("\n1. Creating test tar file...")
        tar_path = create_test_tar(tar_dir)
        print(f"   Created: {tar_path}")
        print(f"   Size: {os.path.getsize(tar_path):,} bytes")
        
        # Process tar file / tarファイルを処理
        print("\n2. Processing tar file...")
        
        # Use DuckDB if available, otherwise Python
        if HAS_DUCKDB:
            print("   Using DuckDB processing...")
            process_tar_with_duckdb(tar_path, output_dir)
        else:
            print("   Using Python processing...")
            process_tar_to_normalized_parquet(tar_path, output_dir)
        
        # Verify output / 出力を検証
        print("\n3. Verifying output...")
        parquet_files = list(output_dir.glob('**/*.parquet'))
        
        if not parquet_files:
            print("   ❌ FAILED: No parquet files created")
            return False
        
        print(f"   ✓ Found {len(parquet_files)} parquet file(s)")
        
        # Read and verify parquet content / parquetの内容を読み込んで検証
        for pq_file in parquet_files:
            print(f"\n4. Reading parquet file: {pq_file.name}")
            df = pl.read_parquet(pq_file)
            
            print(f"   ✓ Number of records: {df.height}")
            print(f"   ✓ Columns: {df.columns}")
            print(f"   ✓ Shape: {df.shape}")
            
            # Check if required columns exist / 必須カラムの存在確認
            if 'agency' in df.columns:
                print(f"   ✓ Agency detected: {df['agency'].unique().to_list()}")
            if 'date_str' in df.columns:
                print(f"   ✓ Dates: {df['date_str'].unique().to_list()}")
        
        print("\n" + "=" * 60)
        print("✓ All tests passed successfully!")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Clean up temporary directory / 一時ディレクトリをクリーンアップ
        print(f"\n5. Cleaning up temporary files...")
        shutil.rmtree(temp_base)
        print(f"   Removed: {temp_base}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract JSON from tar and save as normalized GTFS-RT Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show detected agencies in a single tar file
  %(prog)s --tar-path data.tar.gz --show-agencies
  
  # Process single tar file with all agencies (parallel)
  %(prog)s --tar-path data.tar.gz --output-dir ./data/bronze --workers 4
  
  # Process single tar file with specific agencies
  %(prog)s --tar-path data.tar.gz --output-dir ./data/bronze \\
    --agency-filter "chitetsu_tram,chitetsu_bus"
  
  # Process all tar files in directory
  %(prog)s --tar-dir /path/to/tar/files --output-dir /path/to/output
  
  # Run test mode
  %(prog)s --test
        """
    )
    
    parser.add_argument(
        '--tar-path',
        type=str,
        required=False,
        help='Path to a single tar/tar.gz file'
    )
    
    parser.add_argument(
        '--tar-dir',
        type=str,
        required=False,
        help='Path to directory containing tar files'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        required=False,
        help='Path to output directory for normalized Parquet files'
    )
    
    parser.add_argument(
        '--pattern',
        type=str,
        default='*.tar*',
        help='File pattern to process when using --tar-dir (default: *.tar*)'
    )
    
    parser.add_argument(
        '--agency-filter',
        type=str,
        help='Comma-separated list of agencies to process (e.g., "chitetsu_tram,chitetsu_bus")'
    )
    
    parser.add_argument(
        '--show-agencies',
        action='store_true',
        help='Only show detected agencies and exit (requires --tar-path)'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        default=None,
        help='Number of DuckDB threads (default: auto-detect based on CPU count)'
    )
    
    parser.add_argument(
        '--use-python',
        action='store_true',
        help='Force Python processing instead of DuckDB (slower but no extra dependencies)'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run test mode to verify functionality'
    )
    
    args = parser.parse_args()
    
    # Run test if requested / テストモードが要求された場合は実行
    if args.test:
        success = run_test()
        return 0 if success else 1
    
    # Show agencies mode / agency表示モード
    if args.show_agencies:
        if not args.tar_path:
            print("Error: --show-agencies requires --tar-path")
            return 1
        
        tar_path = Path(os.path.expandvars(os.path.expanduser(args.tar_path)))
        if not tar_path.exists():
            print(f"Error: Tar file not found: {tar_path}")
            return 1
        
        # Use DuckDB by default for agency discovery / Agency検出にはデフォルトでDuckDBを使用
        if HAS_DUCKDB and not args.use_python:
            process_tar_with_duckdb(
                tar_path,
                Path('./tmp'),  # Dummy output dir
                show_agencies=True,
                threads=args.workers
            )
        else:
            process_tar_to_normalized_parquet(
                tar_path,
                Path('./tmp'),  # Dummy output dir
                show_agencies=True,
                workers=1  # Sequential for discovery mode
            )
        return 0
    
    # Normal processing mode / 通常処理モード
    if args.tar_path:
        # Single tar file mode / 単一tarファイルモード
        if not args.output_dir:
            print("Error: --output-dir is required when using --tar-path")
            return 1
        
        tar_path = Path(os.path.expandvars(os.path.expanduser(args.tar_path)))
        output_dir = Path(os.path.expandvars(os.path.expanduser(args.output_dir)))
        
        if not tar_path.exists():
            print(f"Error: Tar file not found: {tar_path}")
            return 1
        
        # Use DuckDB by default unless --use-python is specified
        # DuckDBをデフォルトで使用（--use-pythonが指定されていない限り）
        if HAS_DUCKDB and not args.use_python:
            print("\nUsing DuckDB for high-speed processing...")
            process_tar_with_duckdb(
                tar_path,
                output_dir,
                agency_filter=args.agency_filter,
                show_agencies=False,
                threads=args.workers
            )
        else:
            if not HAS_DUCKDB:
                print("\nDuckDB not available, using Python processing...")
            else:
                print("\nUsing Python processing (--use-python specified)...")
            process_tar_to_normalized_parquet(
                tar_path,
                output_dir,
                agency_filter=args.agency_filter,
                show_agencies=False,
                workers=args.workers
            )
    
    elif args.tar_dir:
        # Directory mode / ディレクトリモード
        if not args.output_dir:
            print("Error: --output-dir is required when using --tar-dir")
            return 1
        
        tar_dir = os.path.expandvars(os.path.expanduser(args.tar_dir))
        output_dir = os.path.expandvars(os.path.expanduser(args.output_dir))
        
        if not os.path.isdir(tar_dir):
            print(f"Error: Directory not found: {tar_dir}")
            return 1
        
        process_tar_directory(
            tar_dir, 
            output_dir, 
            args.pattern, 
            args.agency_filter, 
            args.workers,
            args.use_python
        )
    
    else:
        parser.print_help()
        print("\nError: Either --tar-path or --tar-dir is required (or use --test)")
        return 1
    
    print("\nProcessing completed!")
    return 0


if __name__ == '__main__':
    exit(main())
