#!/bin/bash
# Load environment variables from .env.local / .env.local から環境変数を読み込む
set -a
source "$(dirname "$0")/../.env.local"
set +a

# scpは時間がかかるため非推奨
# GCSを使う（大容量・再利用向け）
# ローカル実行：バケット名確認＆出力を環境変数に登録してから実行すること



# バケット一覧を確認
gsutil ls 
# バケット内のディレクトリを確認
gsutil ls "$GCS_BUCKET"
# バケットの中身をすべて確認
gsutil ls "${GCS_BUCKET}/"
# prefix付きで確認
gsutil ls "${GCS_BUCKET}/${OLD_GCS_PREFIX}" 
# 新しいprefixにデータが転送できたか確認
gsutil ls "${GCS_BUCKET}/${NEW_GCS_PREFIX}"



# VM instance上での実行：tar archive作成とGCS bucketへのアップロード
# データディレクトリへ移動
cd ~/adaptive-signal-open-data/data/raw
# tar + pigz で圧縮（並列・長期保存用）
tar cf - . | pigz -9 > /tmp/gtfs-rt_data_9.tar.gz



# ローカル/VM instance実行どちらでも可：GCSへアップロード
gsutil cp /tmp/gtfs-rt_data_9.tar.gz "${GCS_BUCKET}/${GCS_PREFIX_LATEST}/gtfs-rt_data_9.tar.gz"
# GCSからローカルにダウンロードすると、自動的に解凍される
# ローカル実行：GCSからダウンロードして解凍されているtar archiveを展開
tar xf prod-gtfs-latest_raw_gtfs-rt_data_9.tar

# tar archiveをparquet形式に変換
python tar2parquet.py --tar-dir ${TAR_DIR} --output-dir ${PARQUET_OUTPUT_DIR} 



# GCSへアップロードされたものがあるか確認
gsutil ls "${GCS_BUCKET}/${GCS_PREFIX_LATEST}"


# バケット一覧を確認
gsutil ls 
# バケット内のディレクトリを確認
gsutil ls "$GCS_BUCKET"
# バケットの中身をすべて確認
gsutil ls "${GCS_BUCKET}/"
# prefix付きで確認
gsutil ls "${GCS_BUCKET}/${OLD_GCS_PREFIX}" 
# 新しいprefixにデータが転送できたか確認
gsutil ls "${GCS_BUCKET}/${NEW_GCS_PREFIX}"
# GCS_PREFIX_LATESTの中身を確認
gsutil ls "${GCS_BUCKET}/${GCS_PREFIX_LATEST}"



# VM instance上での実行：tar archive作成とGCS bucketへのアップロード
# データディレクトリへ移動
cd ~/adaptive-signal-open-data/data/raw
# tar + pigz で圧縮（並列・長期保存用）
tar cf - . | pigz -9 > /tmp/gtfs-rt_data_9.tar.gz



# ローカル/VM instance実行どちらでも可：GCSへアップロード
gsutil cp /tmp/gtfs-rt_data_9.tar.gz "${GCS_BUCKET}/${GCS_PREFIX_LATEST}/gtfs-rt_data_9.tar.gz"
# GCSからローカルにダウンロードすると、自動的に解凍される
# ローカル実行：GCSからダウンロードして解凍されているtar archiveを展開
tar xf prod-gtfs-latest_raw_gtfs-rt_data_9.tar



# # 仮想環境activate必須：tar archiveを正規化されたparquet形式に変換
# python tar2parquet.py \
#     --tar-dir ${LOCAL_SSD_DIR} \
#     --output-dir ${PARQUET_OUTPUT_DIR} 

# バックグラウンドで実行する場合
nohup python tar2parquet.py \
    --tar-path ${LOCAL_SSD_DIR} \
    --output-dir ${LOCAL_SSD_DIR} > tar2parquet.log 2>&1 &

echo ""
echo "Parquet conversion completed!"
echo "To upload to GCS, run: bash scripts/upload_to_gcs.sh ${PARQUET_OUTPUT_DIR}"
echo ""


