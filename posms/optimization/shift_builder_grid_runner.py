# posms/optimization/shift_builder_grid_runner.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import datetime as dt

from .shift_builder_grid_solver import ShiftBuilderGrid
from .shift_builder_grid import GridLayout, apply_assignments_to_grid_xlsm


def _as_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def build_assignments(
    sb: ShiftBuilderGrid,
) -> Dict[Tuple[str, dt.date], Tuple[Optional[str], Optional[str]]]:
    """
    solver結果 → grid書き込み用 assignments を作る

    assignments[(emp_key, date)] = (upper_text, lower_text)

    upper_text:
      - pre_dict_time があればそれ
      - なければ zone_to_shift[job]
      - なければ None

    lower_text:
      - 休暇があれば休暇
      - なければ specialWork があればそれ（廃休/マル超）
      - なければ job（区名）
      - なければ None
    """
    assignments: Dict[Tuple[str, dt.date], Tuple[Optional[str], Optional[str]]] = {}

    for i in sb.employees:
        # Excel側の emp_key 揺れ対策（"12" / "12.0" / "山田" のどれでも当てる）
        name = str(sb.emp_dict.get(i, {}).get("氏名", "")).strip()
        emp_keys = {str(i), str(float(i))}
        if name:
            emp_keys.add(name)

        for d in sb.days:
            # 仕事かどうか（WorkOrRest で 0/1）
            yv = sb.y[(i, d)].varValue
            is_workday = (yv is not None and yv > 0.5)

            # --- rest（休暇）---
            rest_type: Optional[str] = None
            if not is_workday:
                for r in sb.rest_types:
                    vv = sb.rest[(i, d, r)].varValue
                    if vv is not None and vv > 0.5:
                        rest_type = r
                        break

            # --- job（区名）---
            job: Optional[str] = None
            if is_workday:
                for k in sb.jobs:
                    vv = sb.x[(i, d, k)].varValue
                    if vv is not None and vv > 0.5:
                        job = k
                        break

            # --- special（廃休/マル超）---
            special: Optional[str] = None
            for s in sb.special_attendance:
                vv = sb.specialWork[(i, d, s)].varValue
                if vv is not None and vv > 0.5:
                    special = s
                    break

            # --- upper（上段）---
            upper: Optional[str] = sb.pre_dict_time.get((i, d))
            if upper is None and job is not None:
                upper = sb.zone_to_shift.get(job)

            # --- lower（下段）---
            lower: Optional[str] = rest_type or special or job

            for ek in emp_keys:
                assignments[(ek, d)] = (upper, lower)

    return assignments


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    runner入口（dict in / dict out）

    必須:
      - csv_dir: str | Path
      - in_xlsm: str | Path

    任意:
      - out_xlsm: str | Path（未指定なら in_xlsm と同じ場所に *_out.xlsm）
      - alpha: float（default 0.1）
      - msg: bool（default False）
      - clear_before_write: bool（default True）
      - layout: dict（GridLayout のフィールド上書き）

    戻り:
      {
        "ok": bool,
        "status": str,
        "out_xlsm": str | None,
        "summary": dict,
        "error": str | None,
      }
    """
    try:
        csv_dir = Path(params["csv_dir"])
        in_xlsm = Path(params["in_xlsm"])

        out_xlsm_param = params.get("out_xlsm")
        out_xlsm = Path(out_xlsm_param) if out_xlsm_param else in_xlsm.with_name(
            f"{in_xlsm.stem}_out{in_xlsm.suffix}"
        )

        alpha = float(params.get("alpha", 0.1))
        msg = _as_bool(params.get("msg"), default=False)
        clear_before_write = _as_bool(params.get("clear_before_write"), default=True)

        layout_overrides = params.get("layout") or {}
        if not isinstance(layout_overrides, dict):
            raise TypeError("params['layout'] は dict である必要があります。")
        layout = GridLayout(**layout_overrides)

        sb = ShiftBuilderGrid(csv_dir=csv_dir)
        sb.build()
        sb.solve(alpha=alpha, msg=msg)

        status = getattr(sb, "status_name", None) or "Unknown"

        if status != "Optimal":
            return {
                "ok": False,
                "status": status,
                "out_xlsm": None,
                "summary": sb.summary(),
                "error": None,
            }

        assignments = build_assignments(sb)

        out_path = apply_assignments_to_grid_xlsm(
            in_xlsm=in_xlsm,
            out_xlsm=out_xlsm,
            assignments=assignments,
            layout=layout,
            clear_before_write=clear_before_write,
        )

        return {
            "ok": True,
            "status": status,
            "out_xlsm": str(out_path),
            "summary": sb.summary(),
            "error": None,
        }

    except Exception as e:
        return {
            "ok": False,
            "status": "Error",
            "out_xlsm": None,
            "summary": {},
            "error": f"{type(e).__name__}: {e}",
        }
