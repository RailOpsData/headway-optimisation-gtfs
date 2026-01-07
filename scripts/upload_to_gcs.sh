#!/bin/bash
# Upload parquet files to GCS with WSL stability improvements
# Parquetファイルをスタビリティ改善でGCSにアップロード

# Load environment variables from .env.local / .env.local から環境変数を読み込む
set -a
source "$(dirname "$0")/../.env.local"
set +a

# Parquet directory check / Parquetディレクトリチェック
if [ -z "$1" ]; then
    PARQUET_DIR="${PARQUET_OUTPUT_DIR}"
else
    PARQUET_DIR="$1"
fi

# Find parquet files / Parquetファイルを検索
PARQUET_FILE=$(ls ${PARQUET_DIR}/*.parquet 2>/dev/null | head -1)

if [ -z "$PARQUET_FILE" ]; then
    echo "Error: No parquet files found in ${PARQUET_DIR}"
    exit 1
fi

FILE_SIZE=$(du -h "$PARQUET_FILE" | cut -f1)
echo "File to upload: $(basename "$PARQUET_FILE") (Size: $FILE_SIZE)"
echo "Destination: ${GCS_BUCKET}/${GCS_PREFIX_LATEST}/"
echo "Starting upload to GCS (this may take a while)..."
echo ""

# Background process with logging / ログ出力付きバックグラウンド処理
nohup gsutil -o GSUtil:parallel_process_count=1 cp "$PARQUET_FILE" "${GCS_BUCKET}/${GCS_PREFIX_LATEST}/" > /tmp/gsutil_upload.log 2>&1 &
UPLOAD_PID=$!

echo "Upload process started (PID: $UPLOAD_PID)"
echo "Log file: /tmp/gsutil_upload.log"
echo "To check status: tail -f /tmp/gsutil_upload.log"
echo "Process will continue even if terminal closes"
echo ""

# Wait for process with timeout (2 hours) / プロセス終了を待機（タイムアウト2時間）
sleep 2
ps -p $UPLOAD_PID > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "Process is running in background, waiting for completion..."
    # Check every 30 seconds / 30秒ごとにチェック
    ELAPSED=0
    MAX_TIME=7200
    
    while ps -p $UPLOAD_PID > /dev/null 2>&1; do
        ELAPSED=$((ELAPSED + 30))
        if [ $ELAPSED -gt $MAX_TIME ]; then
            echo "Timeout reached (2 hours). Process still running:"
            ps -p $UPLOAD_PID
            break
        fi
        echo "Still uploading... (${ELAPSED}s elapsed)"
        sleep 30
    done
fi

wait $UPLOAD_PID 2>/dev/null
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=============================="
    echo "✓ Upload completed successfully!"
    echo "=============================="
    
    # Verify uploaded files / アップロードされたファイルを確認
    echo ""
    echo "Verifying uploaded files..."
    gsutil ls "${GCS_BUCKET}/${GCS_PREFIX_LATEST}"
    
    exit 0
elif [ $EXIT_CODE -eq 124 ]; then
    echo ""
    echo "=============================="
    echo "⚠ Upload timed out after 2 hours"
    echo "=============================="
    echo "Process is still running in background (PID: $UPLOAD_PID)"
    echo "Check status: tail -f /tmp/gsutil_upload.log"
    echo ""
    tail -20 /tmp/gsutil_upload.log
    
    exit 1
else
    echo ""
    echo "=============================="
    echo "✗ Upload failed"
    echo "=============================="
    echo "Check /tmp/gsutil_upload.log for details:"
    echo ""
    tail -30 /tmp/gsutil_upload.log
    
    exit 1
fi
