#!/bin/bash
set -euo pipefail

# この .command ファイル自身の場所（excel_templates/bin）
DIR="$(cd "$(dirname "$0")" && pwd)"

# excel_templates に移動
cd "$DIR/.."

# export_csv が無ければ作る
mkdir -p export_csv

# optimizer 実行（ログ出力）
./bin/posms_optimizer --csvdir ./export_csv > ./export_csv/optimizer.log 2>&1
