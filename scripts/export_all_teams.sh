#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# 班ごとの統合Excelを一括出力
# -------------------------------

DEPT=""                     # 未指定なら自動切替
START=1
END=9
OUT_DIR="excel_templates"
TEMPLATE="excel_templates/分担予定表(案).xlsm"
PY_CMD="python -m posms.cli"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]
  -d  部署（department_name）。指定時は全班で固定。未指定なら 1-5=第一 / 6-9=第二 に自動切替
  -s  開始班番号（整数）既定: ${START}
  -e  終了班番号（整数）既定: ${END}
  -o  出力ディレクトリ 既定: ${OUT_DIR}
  -t  テンプレート .xls/.xlsx/.xlsm 既定: ${TEMPLATE}
  -h  このヘルプ
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

export LANG=ja_JP.UTF-8
export LC_ALL=ja_JP.UTF-8 || true

mkdir -p "${OUT_DIR}"

echo "=== 全班出力開始 ==="
if [[ -n "${DEPT}" ]]; then
  echo "部署(固定): ${DEPT}"
else
  echo "部署(自動): 1〜5=第一集配営業部 / 6〜9=第二集配営業部"
fi
echo "班: ${START}〜${END}"
echo "出力: ${OUT_DIR}"
echo "テンプレ: ${TEMPLATE}"
echo

for ((i=START; i<=END; i++)); do
  TEAM="${i}班"
  OUT_PATH="${OUT_DIR}/${TEAM}データ.xlsm"

  # 部署の自動/固定切替
  if [[ -n "${DEPT}" ]]; then
    DEPT_EACH="${DEPT}"
  else
    if (( i <= 5 )); then
      DEPT_EACH="第一集配営業部"
    else
      DEPT_EACH="第二集配営業部"
    fi
  fi

  echo "--------------------------------------------"
  echo "▶ ${TEAM} を出力中…（部署: ${DEPT_EACH}）"

  # 🔧 ここが重複対策の「決め手」：毎回“クリーンなブック”から開始
  # 既存出力を削除 → テンプレを物理コピー（.xlsm の VBA/定義を確実に温存）
  if [[ -e "${OUT_PATH}" ]]; then
    rm -f "${OUT_PATH}"
  fi
  cp -f "${TEMPLATE}" "${OUT_PATH}"

  # 🔧 以降、Python 側は既存ブック(${OUT_PATH})に各シートを書き込むだけ
  #    （ブックが毎回リセットされるので、以前の行が残りません）
  ${PY_CMD} export-team-workbook \
    --department-code "${DEPT_EACH}" \
    --team "${TEAM}" \
    --out "${OUT_PATH}" \
    --template "${TEMPLATE}" \
    --sqlite "excel_templates/posms_demo.db" \
    || { echo "❌ ${TEAM} 失敗"; exit 1; }

  echo "✅ 完了: ${OUT_PATH}"
done

echo
echo "=== 全班出力完了 ==="
