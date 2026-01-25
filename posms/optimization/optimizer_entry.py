from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
# from posms.optimization.shift_builder_grid_solver import solve

import pandas as pd


def _cbc_path_for_frozen() -> str | None:
    # PyInstaller onedir: 実行ファイルのあるフォルダに同梱される
    if not getattr(sys, "frozen", False):
        return None

    base = os.path.dirname(sys.executable)
    cand = os.path.join(base, "cbc")
    cand_win = os.path.join(base, "cbc.exe")

    if os.path.exists(cand):
        try:
            os.chmod(cand, 0o755)
        except Exception:
            pass
        return cand

    if os.path.exists(cand_win):
        return cand_win

    return None

def _write_solution_csv(out_path: Path, rows: list[dict]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    # 最低限の列順（無ければある分だけ出す）
    preferred = [
        "team_name",
        "date",
        "employee_code",
        "employee_name",
        "zone_name",
        "job_name",
        "job_code",
        "leave_code",
        "special_code",
        "note",
    ]
    cols = [c for c in preferred if c in df.columns] + [
        c for c in df.columns if c not in preferred
    ]
    df = df[cols]
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"OK: wrote solution -> {out_path}")


def _load_inputs(csvdir: Path) -> dict:
    """
    Excel側が export_csv に吐く想定の入力を読み込む。
    まずは最低限として inputs.json があれば読む、無ければ空。
    """
    inputs = {}
    p = csvdir / "inputs.json"
    if p.exists():
        inputs = json.loads(p.read_text(encoding="utf-8"))
    return inputs


def main() -> int:
    ap = argparse.ArgumentParser(description="POSMS optimizer entry (exe target)")
    ap.add_argument("--csvdir", required=True, help="export_csv directory from Excel")
    ap.add_argument("--team", default="", help="team name (optional)")
    ap.add_argument("--start", default="", help="start date (optional, YYYY-MM-DD)")
    ap.add_argument(
        "--days", type=int, default=28, help="planning horizon days (default: 28)"
    )
    ap.add_argument("--sqlite", default="", help="SQLite db path (optional)")
    ap.add_argument(
        "--dry-run", action="store_true", help="load inputs only, do not solve"
    )
    args = ap.parse_args()

    csvdir = Path(args.csvdir)
    outdir = csvdir
    out_csv = outdir / "solution.csv"

    if not csvdir.exists():
        print(f"ERROR: csvdir not found: {csvdir}", file=sys.stderr)
        return 2

    # 1) 入力を読む（将来ここで fixed_assignments.csv 等も読む）
    inputs = _load_inputs(csvdir)

    # 2) dry-run なら入力確認だけして終了
    if args.dry_run:
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "inputs_echo.json").write_text(
            json.dumps(inputs, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"OK: dry-run. wrote inputs_echo.json -> {outdir}")
        return 0

    # 3) solver 実行
    try:
        from posms.optimization.shift_builder_grid_solver import ShiftBuilderGrid
    except Exception as e:
        print("ERROR: cannot import ShiftBuilderGrid.", file=sys.stderr)
        print(str(e), file=sys.stderr)
        return 3

    alpha = float(inputs.get("alpha", 0.1))
    msg = bool(inputs.get("msg", True))

    cbc_path = _cbc_path_for_frozen()

    sb = ShiftBuilderGrid(csv_dir=csvdir)
    sb.build()
    sb.solve(alpha=alpha, msg=msg, cbc_path=cbc_path)

    # summary.json（デバッグ用）
    summary = sb.summary()
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ★最適化結果を明示的に export（自動検出はしない）
    out_csv = outdir / "solution.csv"
    sb.export_solution_csv(out_csv)
    print(f"OK: wrote solution.csv -> {out_csv}")
    print(f"OK: wrote summary.json -> {outdir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
