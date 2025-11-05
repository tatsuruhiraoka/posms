#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# 班ごとの統合Excelを一括出力
# 例:
#   bash scripts/export_all_teams.sh
#   bash scripts/export_all_teams.sh -d "第二集配営業部" -s 1 -e 9 -o excel_out -t excel_templates/shift_template.xlsm
# -------------------------------

# 既定値（SQLiteデモDB前提）
DEPT="第一集配営業部"
START=1
END=9
OUT_DIR="excel_out"
TEMPLATE="excel_templates/shift_template.xlsm"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]
  -d  部署（department_name）既定: "${DEPT}"
  -s  開始班番号（整数）既定: ${START}
  -e  終了班番号（整数）既定: ${END}
  -o  出力ディレクトリ 既定: ${OUT_DIR}
  -t  テンプレート .xls/.xlsx/.xlsm 既定: ${TEMPLATE}
  -h  このヘルプ

例:
  $(basename "$0") -d "第一集配営業部" -s 1 -e 9
USAGE
}

while getopts "d:s:e:o:t:h" opt; do
  case "$opt" in
    d) DEPT="$OPTARG" ;;
    s) START="$OPTARG" ;;
    e) END="$OPTARG" ;;
    o) OUT_DIR="$OPTARG" ;;
    t) TEMPLATE="$OPTARG" ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done

# 日本語ファイル名の安全運転（必要なら）
export LANG=ja_JP.UTF-8
export LC_ALL=ja_JP.UTF-8 || true

# 出力先
mkdir -p "${OUT_DIR}"

echo "=== 全班出力開始 ==="
echo "部署: ${DEPT}"
echo "班: ${START}〜${END}"
echo "出力: ${OUT_DIR}"
echo "テンプレ: ${TEMPLATE}"
echo

# 1..N をループ
for ((i=START; i<=END; i++)); do
  TEAM="${i}班"
  OUT_PATH="${OUT_DIR}/${TEAM}データ.xlsx"

  echo "▶ ${TEAM} を出力中…"
  # ローカル（SQLite）の場合はそのまま。Docker内で走らせたい場合は
  #   docker compose exec app python -m posms ...
  # に置き換え可。
  python -m posms export-team-workbook \
    --department-code "${DEPT}" \
    --team "${TEAM}" \
    --out "${OUT_PATH}" \
    --template "${TEMPLATE}" \
    || { echo "❌ ${TEAM} 失敗"; exit 1; }

  echo "✅ 完了: ${OUT_PATH}"
done

echo
echo "=== 全班出力完了 ==="

