# bridge.py
# 指定日から4週間(28日)のカレンダーを生成し、祝日を反映してExcelに出力
# - Win/Mac 共通
# - オフライン可（jpholiday はローカル判定）
# 使い方:
#   1) Excelで開始日セルを選択（複数セル選択可：左上セルを起点）
#   2) 図形ボタンに xlwings_run_fill_calendar_4w を割り当てて実行

import datetime as dt
from typing import Any, Dict, List, Optional

import jpholiday
import xlwings as xw

WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]
OUTPUT_HEADERS = [
    "date",
    "weekday",
    "is_weekend",
    "is_holiday",
    "holiday_name",
    "is_weekend_or_holiday",
]

# 必要なら True に変更（Pythonの weekday(): 月=0..日=6）
ENFORCE_SUNDAY_START = False


# -------- Excel 日付シリアル対応（1900/1904システム） -----------------
def _is_date1904(book: Optional[xw.Book]) -> bool:
    """ブックが1904日付システムかどうか（取れなければ False 扱い）"""
    if book is None:
        return False
    try:
        return bool(book.api.Date1904)  # WindowsのWorkbookオブジェクト
    except Exception:
        try:
            return bool(book.api.date_1904)  # macOS(appscript) での属性名
        except Exception:
            return False


def _from_excel_serial(n: float, book: Optional[xw.Book]) -> dt.date:
    """Excel日付シリアルを date に変換（1900/1904の両方に対応）"""
    if _is_date1904(book):
        base = dt.datetime(1904, 1, 1)
    else:
        # 1900システム（Excelの1900年うるう年バグ補正込み）
        base = dt.datetime(1899, 12, 30)
    return (base + dt.timedelta(days=int(n))).date()


# -------- 開始日の正規化 --------------------------------------------
def _to_date(v: Any, book: Optional[xw.Book]) -> dt.date:
    """Excelセルなどの値を日付に正規化（datetime, date, 数値, 文字列に対応）"""
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    if isinstance(v, (int, float)):
        return _from_excel_serial(float(v), book)
    if isinstance(v, str):
        s = v.strip()
        # 代表的な形式に対応
        fmts = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y.%m.%d",
            "%Y%m%d",
            "%m/%d/%Y",
            "%m-%d-%Y",
            "%Y年%m月%d日",
        ]
        for f in fmts:
            try:
                return dt.datetime.strptime(s, f).date()
            except Exception:
                pass
        # ISO形式の最後の砦
        try:
            return dt.date.fromisoformat(s)
        except Exception:
            pass
    raise ValueError(f"開始日を解釈できません: {v!r}")


# -------- 4週間分のカレンダー生成 -----------------------------------
def make_calendar_4w(start: dt.date) -> List[Dict[str, Any]]:
    """start を含めて 28 日分の行を返す"""
    rows: List[Dict[str, Any]] = []
    for i in range(28):
        d = start + dt.timedelta(days=i)
        dow = d.weekday()  # 0=Mon .. 6=Sun
        is_weekend = dow >= 5
        hol_name = jpholiday.is_holiday_name(d) or ""
        is_holiday = bool(hol_name)
        rows.append(
            {
                "date": d,
                "weekday": WEEKDAY_JP[dow],
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
                "holiday_name": hol_name,
                "is_weekend_or_holiday": (is_weekend or is_holiday),
            }
        )
    return rows


# -------- Excel から使うエントリ（ボタンで貼り付け） -------------------
@xw.sub
def xlwings_run_fill_calendar_4w():
    """
    選択セルの左上を起点として、そこから28日分の表を貼り付け（ヘッダー付き）。
    列: date, weekday, is_weekend, is_holiday, holiday_name, is_weekend_or_holiday
    """
    book = xw.Book.caller()
    app = book.app

    # アンカー取得（複数セル選択なら左上セル／うまく取れない場合はA1）
    try:
        sel = app.selection
        try:
            # 2D Range を想定：左上セル
            anchor = sel[0, 0]
        except Exception:
            anchor = sel
    except Exception:
        anchor = book.sheets.active.range("A1")

    try:
        start = _to_date(anchor.value, book)
    except Exception as e:
        app.api.StatusBar = f"開始日セルの選択＋有効な日付を入力してください: {e}"
        return

    if ENFORCE_SUNDAY_START and start.weekday() != 6:
        app.api.StatusBar = "開始日は日曜日にしてください（設定: ENFORCE_SUNDAY_START=True）"
        return

    rows = make_calendar_4w(start)
    data = [OUTPUT_HEADERS] + [[r[c] for c in OUTPUT_HEADERS] for r in rows]

    # 出力範囲を 29行×6列に限定してクリア → 書き込み
    target = anchor.resize(len(data), len(OUTPUT_HEADERS))
    target.clear_contents()
    target.value = data

    # 体裁
    target.columns.autofit()
    target.rows.autofit()

    app.api.StatusBar = f"{start} から 4週間分を貼り付けました"

HOLIDAY_WEEKEND_FILL_RGB = (255, 220, 230)  # 行4〜22の背景
DATE_FONT_RED_RGB = (255, 0, 0)             # 行5・行22の文字色（純赤）

@xw.sub
def color_holidays_28d_jp():
    import datetime as dt
    book = xw.Book.caller()
    ws = book.sheets["Sheet1"]

    base_col = int(ws.range("C5").column)
    s = ws.range("V1").value
    e = ws.range("AA1").value

    if s is None:
        book.app.api.StatusBar = "開始日(V1)が未設定です。"
        return
    if isinstance(s, dt.datetime): s = s.date()
    if e is None: e = s + dt.timedelta(days=27)
    if isinstance(e, dt.datetime): e = e.date()

    # 祝日取得（1回でセット化）＋ 週末判定
    try:
        import jpholiday
        hol_set = {d for d, _ in jpholiday.between(s, e)}
    except Exception:
        hol_set = set()

    is_off = []
    cur = s
    one = dt.timedelta(days=1)
    for _ in range(28):
        dow = cur.weekday()             # 0=Mon .. 6=Sun
        weekend = (dow >= 5)            # 土日
        holiday = (cur in hol_set)      # 祝日
        is_off.append(weekend or holiday)
        cur += one

    # 1) 背景を一括クリア（行4〜22、28列分）※行4の文字は残る
    ws.range((4, base_col), (22, base_col + 27)).color = None
    # 文字色：日付行をいったん黒に（再実行時の戻し）
    ws.range((5, base_col), (5, base_col + 27)).font.color = (0, 0, 0)
    ws.range((22, base_col), (22, base_col + 27)).font.color = (0, 0, 0)

    # 2) 連続した休み列をまとめて塗る（高速化）
    i = 0
    while i < 28:
        if is_off[i]:
            j = i
            while j + 1 < 28 and is_off[j + 1]:
                j += 1
            # 背景：行4〜22をブロックで塗る
            ws.range((4, base_col + i), (22, base_col + j)).color = HOLIDAY_WEEKEND_FILL_RGB
            # 日付文字色（行5・22）もブロックで赤に
            try:
                ws.range((5, base_col + i), (5, base_col + j)).font.color = DATE_FONT_RED_RGB
                ws.range((22, base_col + i), (22, base_col + j)).font.color = DATE_FONT_RED_RGB
            except Exception:
                # 互換フォールバック（環境によっては tuple 指定が効かない場合）
                ws.range((5, base_col + i), (5, base_col + j)).api.Font.Color = 0x0000FF  # BGRの赤=0x0000FF
                ws.range((22, base_col + i), (22, base_col + j)).api.Font.Color = 0x0000FF
            i = j + 1
        else:
            i += 1

    book.app.api.StatusBar = "休列（週末+祝日）の背景/文字色を適用しました"