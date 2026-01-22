#!/bin/bash
set -e

# Run GTFS-RT -> parquet conversion relative to repository root
DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$DIR/.." && pwd)"

python "$PROJECT_ROOT/src/sim_bridge/gtfsrt_dual_processor.py" \
    --input-dir "$PROJECT_ROOT/data/raw" \
    --output-dir "$PROJECT_ROOT/data/bronze"
 python /home/koki_deutsch/adaptive-signal-open-data/src/sim_bridge/gtfsrt_dual_processor.py  \
    --input-dir /home/koki_deutsch/adaptive-signal-open-data/data/raw   \
    --output-dir /home/koki_deutsch/adaptive-signal-open-data/data/bronze

