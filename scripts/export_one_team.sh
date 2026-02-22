#!/usr/bin/env bash
set -euo pipefail

# -------------------------------
# 1 班だけ出力（テンプレの分担予定表(案)を取り込んで .xlsm で出力）
# 使い方:
#   bash scripts/export_one_team.sh "1班"
#   bash scripts/export_one_team.sh "6班" -d "DPT-A" -o excel_templates -T excel_templates/分担予定表(案).xlsm
#   bash scripts/export_one_team.sh "6班" --sqlite excel_templates/posms_demo.db
# -------------------------------

TEAM="${1:-1班}"
DEPT=""
OUT_DIR="excel_templates"
TEMPLATE="excel_templates/分担予定表(案).xlsm"
SQLITE_DB=""

# 追加オプションのパース
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--department|--department-code) DEPT="$2"; shift 2;;
    -o|--out-dir) OUT_DIR="$2"; shift 2;;
    -T|--template) TEMPLATE="$2"; shift 2;;
    --sqlite) SQLITE_DB="$2"; shift 2;;
    -h|--help)
      cat <<USAGE
Usage: $(basename "$0") <班名> [options]
  <班名>                例: "1班"（既定）
  -d, --department      部署（department_code/name）既定: ${DEPT}
  -o, --out-dir         出力ディレクトリ（既定: ${OUT_DIR}）
  -T, --template        テンプレ .xlsm（既定: ${TEMPLATE}）
  --sqlite              SQLite .db パス（指定時はそれを使用）
  -h, --help            このヘルプ
USAGE
      exit 0;;
    *) echo "未知の引数: $1"; exit 1;;
  esac
done

# --- Python 実行ファイルをポータブルに探す -------------------------------
find_python() {
  # Windows venv (Git Bash / MSYS)
  if [[ -x "./.venv/Scripts/python.exe" ]]; then echo "./.venv/Scripts/python.exe"; return; fi
  if [[ -x "./.venv/Scripts/python" ]]; then echo "./.venv/Scripts/python"; return; fi

  # Linux/Mac venv
  if [[ -x "./.venv/bin/python" ]]; then echo "./.venv/bin/python"; return; fi

  if command -v python3 >/dev/null 2>&1; then echo "python3"; return; fi
  if command -v python  >/dev/null 2>&1; then echo "python";  return; fi
  echo "python"
}
PY="$(find_python)"

# --- ルート検出（スクリプト場所からプロジェクト直下に移動） --------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# --- 事前チェック ---------------------------------------------------------
if [[ ! -f "${TEMPLATE}" ]]; then
  echo "ERROR: テンプレートが見つかりません: ${TEMPLATE}" >&2
  exit 1
fi
mkdir -p "${OUT_DIR}"

OUT_PATH="${OUT_DIR}/${TEAM}データ.xlsm"   # マクロ保持のため .xlsm で出力

# --- 班番号から自動判定（-d 未指定のときだけ） -------------------------
# "1班" → 1 を取り出す
team_num="$(echo "${TEAM}" | sed -E 's/[^0-9]//g')"
if [[ -z "${DEPT}" && -n "${team_num}" ]]; then
  if (( team_num <= 5 )); then
    DEPT="DPT-A"        # ← 部署コード（名前を使うなら "第一集配営業部"）
  else
    DEPT="DPT-B"
  fi
fi

# --- DB オプション --------------------------------------------------------
EXTRA_DB_OPTS=()
if [[ -n "${SQLITE_DB}" ]]; then
  EXTRA_DB_OPTS+=( --sqlite "${SQLITE_DB}" )
fi

echo "▶ 班 ${TEAM} を出力中…"
echo "  部署: ${DEPT}"
echo "  出力: ${OUT_PATH}"
echo "  テンプレ: ${TEMPLATE}"
echo

# 既存の同名ファイル群を事前に削除（…データ.xlsm, …データ(1).xlsm, …データ2.xlsm 等）
find "${OUT_DIR}" -maxdepth 1 -type f -name "${TEAM}データ*.xlsm" -print -delete

# 既存出力を削除 → テンプレを物理コピー（.xlsm の VBA/定義を確実に温存）
if [[ -e "${OUT_PATH}" ]]; then
  rm -f "${OUT_PATH}"
fi
cp -f "${TEMPLATE}" "${OUT_PATH}"

# 🔧 以降、Python は既存ブック(${OUT_PATH})に上書き出力するだけ
"${PY}" -m posms.cli export-team-workbook \
  --department-code "${DEPT}" \
  --team "${TEAM}" \
  --out "${OUT_PATH}" \
  --template "${TEMPLATE}" \
  ${EXTRA_DB_OPTS+"${EXTRA_DB_OPTS[@]}"}

echo
echo "✅ ${TEAM} のファイル出力完了 → ${OUT_PATH}"
