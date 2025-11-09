Attribute VB_Name = "Module9"
Option Explicit

'================= 先頭：ユーティリティ（依存を先に定義）=================

' 縦方向クリア（Rangeを使わず Cells/Resize のみ）
Private Sub ClearDownCells(ByVal ws As Worksheet, ByVal startRow As Long, ByVal startCol As Long, _
                           ByVal maxRows As Long, ByVal width As Long)
    If maxRows <= 0 Or width <= 0 Then Exit Sub
    On Error Resume Next
    ws.Cells(startRow, startCol).Resize(maxRows, width).ClearContents
    Err.Clear
    On Error GoTo 0
End Sub

' 行の一部を安全にクリア（結合対応、1セルずつ）
Private Sub SafeClearRowSpan(ByVal ws As Worksheet, ByVal row As Long, ByVal colStart As Long, _
                             ByVal colEnd As Long)
    Dim c As Long
    For c = colStart To colEnd
        Call SafeClearCell(ws, row, c)
    Next
End Sub

' 見出し行(1行目)のヘッダー名で列番号（Cellsのみ）
Private Function FindColByHeader(ByVal ws As Worksheet, ByVal headerText As String) As Long
    Dim lastCol As Long, c As Long
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    For c = 1 To lastCol
        If Trim$(CStr(ws.Cells(1, c).Value)) = Trim$(headerText) Then
            FindColByHeader = c
            Exit Function
        End If
    Next c
    FindColByHeader = 0
End Function

' 曜日キー（先頭1文字: 日/月/火/水/木/金/土/祝）→ 列番号（Cellsのみ）
Private Function WeekdayKeyToCol(ByVal ws As Worksheet, ByVal key As String) As Long
    If key = "" Then WeekdayKeyToCol = 0: Exit Function
    Dim lastCol As Long, c As Long, h As String
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    For c = 1 To lastCol
        h = Trim$(CStr(ws.Cells(1, c).Value))
        If h <> "" Then
            If Left$(h, 1) = key Then
                WeekdayKeyToCol = c
                Exit Function
            End If
        End If
    Next c
    WeekdayKeyToCol = 0
End Function

' ===== 祝日判定（色優先 + 日付ロジック） =====
' 祝日シート(A列)に一致する日付があれば True
Private Function IsHolidayInSheet(ByVal d As Date) As Boolean
    On Error GoTo done
    Dim wb As Workbook: Set wb = ThisWorkbook
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = wb.Worksheets("祝日")
    On Error GoTo 0
    If ws Is Nothing Then GoTo done

    Dim last As Long: last = ws.Cells(ws.Rows.Count, 1).End(xlUp).row
    If last < 1 Then GoTo done

    Dim i As Long
    For i = 1 To last
        If IsDate(ws.Cells(i, 1).Value) Then
            If CLng(CDate(ws.Cells(i, 1).Value)) = CLng(d) Then
                IsHolidayInSheet = True
                Exit Function
            End If
        End If
    Next i
done:
End Function

' N番目の特定曜日（例：第2月曜）
Private Function NthWeekdayOfMonth(ByVal y As Long, ByVal m As Long, ByVal vbWDay As VbDayOfWeek, ByVal n As Long) As Date
    Dim first As Date, delta As Long
    first = DateSerial(y, m, 1)
    delta = (vbWDay - Weekday(first, vbSunday) + 7) Mod 7
    NthWeekdayOfMonth = first + delta + 7 * (n - 1)
End Function

' 春分/秋分（1980-2099近似式）
Private Function VernalEquinoxDay(ByVal y As Long) As Date
    Dim d As Long
    d = Int(20.8431 + 0.242194 * (y - 1980) - Int((y - 1980) / 4))
    VernalEquinoxDay = DateSerial(y, 3, d)
End Function
Private Function AutumnalEquinoxDay(ByVal y As Long) As Date
    Dim d As Long
    d = Int(23.2488 + 0.242194 * (y - 1980) - Int((y - 1980) / 4))
    AutumnalEquinoxDay = DateSerial(y, 9, d)
End Function

' 日本の主な祝日（固定+ハッピーマンデー+分点）
Private Function IsJPNationalHolidayCore(ByVal d As Date) As Boolean
    Dim y As Long: y = Year(d)
    Dim m As Long: m = Month(d)
    Dim dd As Long: dd = Day(d)

    If (m = 1 And dd = 1) Or _
       (m = 2 And dd = 11) Or _
       (m = 2 And dd = 23) Or _
       (m = 4 And dd = 29) Or _
       (m = 5 And dd = 3) Or _
       (m = 5 And dd = 4) Or _
       (m = 5 And dd = 5) Or _
       (m = 8 And dd = 11) Or _
       (m = 11 And dd = 3) Or _
       (m = 11 And dd = 23) Then
        IsJPNationalHolidayCore = True
        Exit Function
    End If

    If d = NthWeekdayOfMonth(y, 1, vbMonday, 2) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 7, vbMonday, 3) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 9, vbMonday, 3) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 10, vbMonday, 2) Then IsJPNationalHolidayCore = True: Exit Function

    If d = VernalEquinoxDay(y) Then IsJPNationalHolidayCore = True: Exit Function
    If d = AutumnalEquinoxDay(y) Then IsJPNationalHolidayCore = True: Exit Function
End Function

' 振替休日（簡易）
Private Function IsSubstituteHoliday(ByVal d As Date) As Boolean
    If Weekday(d, vbSunday) = vbSunday Then Exit Function
    Dim prev As Date: prev = DateAdd("d", -1, d)
    If Weekday(prev, vbSunday) = vbSunday Then
        If IsJPNationalHolidayCore(prev) Then IsSubstituteHoliday = True
    End If
End Function

' 総合：祝日シート or 日本の祝日 + 振替
Private Function IsHolidayDate(ByVal v As Variant) As Boolean
    If Not IsDate(v) Then Exit Function
    Dim d As Date: d = CDate(v)
    If IsHolidayInSheet(d) Then IsHolidayDate = True: Exit Function
    If IsJPNationalHolidayCore(d) Then IsHolidayDate = True: Exit Function
    If IsSubstituteHoliday(d) Then IsHolidayDate = True: Exit Function
End Function

' 行5(C5:AD5)のベース色（最頻色）
Private Function Row5BaselineColor(ByVal ws As Worksheet, ByVal colStart As Long, ByVal colEnd As Long) As Long
    Dim colors() As Long, counts() As Long
    Dim n As Long: n = colEnd - colStart + 1
    Dim i As Long, c As Long, cur As Long, idx As Long, bestCnt As Long, bestColor As Long
    ReDim colors(1 To n): ReDim counts(1 To n)
    For c = colStart To colEnd
        If Len(Trim$(CStr(ws.Cells(5, c).Value))) > 0 Then
            cur = ws.Cells(5, c).Interior.Color
            idx = 0
            For i = 1 To n
                If colors(i) = cur Then idx = i: Exit For
                If colors(i) = 0 And idx = 0 Then idx = i
            Next i
            colors(idx) = cur
            counts(idx) = counts(idx) + 1
            If counts(idx) > bestCnt Then bestCnt = counts(idx): bestColor = colors(idx)
        End If
    Next c
    Row5BaselineColor = bestColor
End Function

' 行5のセル色が「ベース色と違う」= 祝日等の特別色
Private Function IsHolidayByColor(ByVal ws As Worksheet, ByVal col As Long, ByVal baseColor As Long) As Boolean
    IsHolidayByColor = (ws.Cells(5, col).Interior.Color <> baseColor)
End Function

' ---- 「通配」含有のゆるい判定（空白/全角を除去）----
Private Function NormJP(ByVal s As String) As String
    Dim t As String
    t = Trim$(s)
    t = Replace$(t, " ", "")
    t = Replace$(t, "　", "")
    NormJP = t
End Function
Private Function ContainsTsuhai(ByVal s As Variant) As Boolean
    ContainsTsuhai = (InStr(1, NormJP(CStr(s)), "通配", vbTextCompare) > 0)
End Function

' 1/0 のみを真偽として扱う
Public Function IsTruthy(ByVal v As Variant) As Boolean
    If IsNumeric(v) Then
        IsTruthy = (v = 1)
    Else
        IsTruthy = False
    End If
End Function

' 役職判定
Private Function BuildRole(ByVal vLeader As Variant, ByVal vSub As Variant, _
                           ByVal vType As Variant, ByVal tempName As String) As String
    If IsTruthy(vLeader) Then
        BuildRole = "班長"
    ElseIf IsTruthy(vSub) Then
        BuildRole = "副班長"
    Else
        Dim s As String: s = Trim$(CStr(vType))
        If s = tempName Then
            BuildRole = "ゆ"
        Else
            BuildRole = ""
        End If
    End If
End Function

' 2D配列の安全アクセス（列1固定）
Private Function SafeIdx(ByVal arr As Variant, ByVal idx As Long) As Variant
    If IsEmpty(arr) Then Exit Function
    On Error Resume Next
    SafeIdx = arr(idx, 1)
    Err.Clear
    On Error GoTo 0
End Function

' 最終データ行（指定列）
Private Function LastDataRow(ByVal ws As Worksheet, ByVal col As Long) As Long
    If col <= 0 Then LastDataRow = 0: Exit Function
    LastDataRow = ws.Cells(ws.Rows.Count, col).End(xlUp).row
End Function

' 範囲クリップ
Private Function Bound(ByVal v As Long, ByVal lo As Long, ByVal hi As Long) As Long
    If v < lo Then
        Bound = lo
    ElseIf v > hi Then
        Bound = hi
    Else
        Bound = v
    End If
End Function

' シート取得（存在しなければ Nothing）
Private Function SheetByName(ByVal wb As Workbook, ByVal nm As String) As Worksheet
    On Error Resume Next
    Set SheetByName = wb.Worksheets(nm)
    Err.Clear
    On Error GoTo 0
End Function

' バッチ切替
Private Sub BeginBatch()
    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual
End Sub
Private Sub EndBatch()
    Application.Calculation = xlCalculationAutomatic
    Application.EnableEvents = True
    Application.ScreenUpdating = True
End Sub

'=== 安全書き込み/クリア（結合セル対応、失敗は握りつぶし） ===
Private Sub SafeSetCellValue(ByVal ws As Worksheet, ByVal row As Long, ByVal col As Long, ByVal v As Variant)
    On Error Resume Next
    If ws.Cells(row, col).MergeCells Then
        ws.Cells(row, col).MergeArea.Value = v
    Else
        ws.Cells(row, col).Value = v
    End If
    Err.Clear
    On Error GoTo 0
End Sub

Private Sub SafeClearCell(ByVal ws As Worksheet, ByVal row As Long, ByVal col As Long)
    On Error Resume Next
    If ws.Cells(row, col).MergeCells Then
        ws.Cells(row, col).MergeArea.ClearContents
    Else
        ws.Cells(row, col).ClearContents
    End If
    Err.Clear
    On Error GoTo 0
End Sub

'=== 保護の一時解除/復帰（パスワード無し） ===
Private Function WasProtected(ByVal ws As Worksheet) As Boolean
    On Error Resume Next
    WasProtected = (ws.ProtectContents Or ws.ProtectDrawingObjects Or ws.ProtectScenarios)
    Err.Clear
    On Error GoTo 0
End Function
Private Sub TryUnprotect(ByVal ws As Worksheet)
    On Error Resume Next
    ws.Unprotect
    Err.Clear
    On Error GoTo 0
End Sub
Private Sub TryReprotect(ByVal ws As Worksheet)
    On Error Resume Next
    ws.Protect
    Err.Clear
    On Error GoTo 0
End Sub

'================= 公開エントリ（シンプル名） =================
Public Sub ImportEmployeeNames()
    ' ログなし・完成版
    Call Posms_Import_Core(False)
    MsgBox "取り込み完了しました。", vbInformation
End Sub

'================= 本体（ログ無し版） =================
Private Function Posms_Import_Core(ByVal enableLog As Boolean) As Boolean
    On Error GoTo ErrHandler

    ' ---- 固定情報 ----
    Const SRC_SHEET$ = "社員"
    Const DST_SHEET$ = "分担予定表(案)"
    Const KU_SHEET$ = "区情報"

    ' 列
    Const COL_A& = 1, COL_B& = 2, COL_C& = 3, COL_G& = 7, COL_K& = 11, COL_AD& = 30
    ' 行（★通配は6行目に出力）
    Const ROW_ROLE_START& = 23, ROW_NAME_START& = 23, ROW_ZONE_START& = 7, ZONE_ROWS& = 8
    Const ROW_WDAY_LABEL& = 4
    Const ROW_DATE& = 5
    Const ROW_DEMAND& = 6   ' ★ C6:AD6 に出力

    ' 社員ヘッダ
    Const H_NAME$ = "氏名", H_DEPT$ = "部", H_TEAM$ = "班"
    Const H_LEADER$ = "班長", H_SUB$ = "副班長", H_TYPE$ = "社員タイプ"
    Const EMP_TYPE_TEMP$ = "期間雇用社員"
    ' 区情報ヘッダ
    Const H_KU_NAME$ = "区名", H_KU_STATUS$ = "稼働", H_KU_TYPE$ = "種別"

    Dim wb As Workbook: Set wb = ThisWorkbook
    Dim wsS As Worksheet, wsD As Worksheet, wsK As Worksheet
    Set wsS = SheetByName(wb, SRC_SHEET)
    Set wsD = SheetByName(wb, DST_SHEET)
    Set wsK = SheetByName(wb, KU_SHEET)
    If wsS Is Nothing Or wsD Is Nothing Or wsK Is Nothing Then GoTo Cleanup

    BeginBatch

    '--- 社員ヘッダ列 ---
    Dim nameCol&, deptCol&, teamCol&, leaderCol&, subCol&, empTypeCol&
    nameCol = FindColByHeader(wsS, H_NAME)
    If nameCol = 0 Then GoTo Cleanup
    deptCol = FindColByHeader(wsS, H_DEPT)
    teamCol = FindColByHeader(wsS, H_TEAM)
    leaderCol = FindColByHeader(wsS, H_LEADER)
    subCol = FindColByHeader(wsS, H_SUB)
    empTypeCol = FindColByHeader(wsS, H_TYPE)

    '--- 部/班 → G1/K1 ---
    If deptCol > 0 Then wsD.Cells(1, COL_G).Value = Trim$(CStr(wsS.Cells(2, deptCol).Value))
    If teamCol > 0 Then wsD.Cells(1, COL_K).Value = Trim$(CStr(wsS.Cells(2, teamCol).Value))

    '--- 氏名/役職 クリア＆取り込み ---
    ClearDownCells wsD, ROW_ROLE_START, COL_A, 300, 1
    ClearDownCells wsD, ROW_NAME_START, COL_B, 300, 1

    Dim lastRow&, rowsN&, outCount&
    lastRow = LastDataRow(wsS, nameCol)
    If lastRow >= 2 Then
        rowsN = lastRow - 1
        Dim arrName As Variant: arrName = wsS.Cells(2, nameCol).Resize(rowsN, 1).Value
        Dim arrLeader As Variant, arrSub As Variant, arrType As Variant
        If leaderCol > 0 Then arrLeader = wsS.Cells(2, leaderCol).Resize(rowsN, 1).Value
        If subCol > 0 Then arrSub = wsS.Cells(2, subCol).Resize(rowsN, 1).Value
        If empTypeCol > 0 Then arrType = wsS.Cells(2, empTypeCol).Resize(rowsN, 1).Value

        Dim i&
        For i = 1 To rowsN
            Dim nm$: nm = Trim$(CStr(arrName(i, 1)))
            If nm <> "" Then
                outCount = outCount + 1
                wsD.Cells(ROW_NAME_START + outCount - 1, COL_B).Value = nm
                wsD.Cells(ROW_ROLE_START + outCount - 1, COL_A).Value = _
                    BuildRole(SafeIdx(arrLeader, i), SafeIdx(arrSub, i), SafeIdx(arrType, i), EMP_TYPE_TEMP)
            End If
        Next i
    End If

    '--- 区情報ヘッダ ---
    Dim kuNameCol&, kuStatusCol&, kuTypeCol&
    kuNameCol = FindColByHeader(wsK, H_KU_NAME)
    kuStatusCol = FindColByHeader(wsK, H_KU_STATUS)
    kuTypeCol = FindColByHeader(wsK, H_KU_TYPE)
    If kuNameCol = 0 Or kuStatusCol = 0 Then GoTo AfterZones

    '--- 区名 B7:B14 クリア＆混合抽出 ---
    wsD.Cells(ROW_ZONE_START, COL_B).Resize(ZONE_ROWS, 1).ClearContents

    Dim lastRowK&, rowsK&, zCount&
    lastRowK = LastDataRow(wsK, kuNameCol)
    rowsK = IIf(lastRowK >= 2, lastRowK - 1, 0)

    If rowsK > 0 Then
        Dim arrZN As Variant: arrZN = wsK.Cells(2, kuNameCol).Resize(rowsK, 1).Value
        Dim arrST As Variant: arrST = wsK.Cells(2, kuStatusCol).Resize(rowsK, 1).Value

        Dim j&
        For j = 1 To rowsK
            If InStr(1, Trim$(CStr(arrST(j, 1))), "混合", vbTextCompare) > 0 Then
                Dim kn$: kn = Trim$(CStr(arrZN(j, 1)))
                If kn <> "" Then
                    zCount = zCount + 1
                    If zCount <= ZONE_ROWS Then
                        wsD.Cells(ROW_ZONE_START + zCount - 1, COL_B).Value = kn
                    End If
                End If
            End If
        Next j
    End If

AfterZones:
    '--- 曜日列 → 固定C..AD ---
    Dim colStart&: colStart = COL_C
    Dim colEnd&:   colEnd = COL_AD

    '=== 行5(C5:AD5)のベース色（最頻色）取得 ===
    Static baseColor As Long, baseColor2 As Long
    baseColor = Row5BaselineColor(wsD, colStart, colEnd)
    baseColor2 = baseColor

    '=== 保護一時解除 ===
    Dim restoreProtect As Boolean
    restoreProtect = WasProtected(wsD)
    TryUnprotect wsD

    '==========================================================
    ' 通配件数（祝日=色 or 日付 / 稼働に「通配」を含む行の件数）→ C6:AD6
    '==========================================================
    Dim col&, label As String, key As String, needCol&, theDate As Variant
    Dim r&, cnt As Long

    For col = colStart To colEnd
        On Error Resume Next

        label = Trim$(CStr(wsD.Cells(ROW_WDAY_LABEL, col).Value)) ' C4:AD4
        theDate = wsD.Cells(ROW_DATE, col).Value                  ' C5:AD5

        ' 祝日キー決定：色優先 → 日付 → ラベル
        If IsHolidayByColor(wsD, col, baseColor) Then
            If Len(label) > 0 And (Left$(label, 1) <> "土") And (Left$(label, 1) <> "日") Then
                key = "祝"
            Else
                key = IIf(Len(label) > 0, Left$(label, 1), "")
            End If
        ElseIf IsHolidayDate(theDate) Then
            key = "祝"
        Else
            key = IIf(Len(label) > 0, Left$(label, 1), "")
        End If

        needCol = IIf(key <> "", WeekdayKeyToCol(wsK, key), 0)

        cnt = 0
        If needCol > 0 And lastRowK >= 2 Then
            For r = 2 To lastRowK
                Dim kStatus As String: kStatus = CStr(wsK.Cells(r, kuStatusCol).Value)
                If ContainsTsuhai(kStatus) Then
                    Dim v As Variant: v = wsK.Cells(r, needCol).Value
                    If IsNumeric(v) Then If CDbl(v) >= 1 Then cnt = cnt + 1
                End If
            Next r
        End If

        If cnt > 0 Then
            Call SafeSetCellValue(wsD, ROW_DEMAND, col, cnt)
        Else
            Call SafeClearCell(wsD, ROW_DEMAND, col)
        End If

        Err.Clear
        On Error GoTo 0
    Next col

    '==========================================================
    ' 混合区×曜日 1入れ（祝日=色優先）→ B7:B14 × C..AD
    '==========================================================
    Dim rowZone&, wLbl As String, keyW As String, nCol&, rr&, putOne As Boolean, zName$
    For rowZone = ROW_ZONE_START To ROW_ZONE_START + ZONE_ROWS - 1
        zName = Trim$(CStr(wsD.Cells(rowZone, COL_B).Value))
        If zName <> "" Then
            For col = colStart To colEnd
                On Error Resume Next
                wLbl = Trim$(CStr(wsD.Cells(ROW_WDAY_LABEL, col).Value))
                If IsHolidayByColor(wsD, col, baseColor2) Then
                    If Len(wLbl) > 0 And (Left$(wLbl, 1) <> "土") And (Left$(wLbl, 1) <> "日") Then
                        keyW = "祝"
                    Else
                        keyW = IIf(Len(wLbl) > 0, Left$(wLbl, 1), "")
                    End If
                ElseIf IsHolidayDate(wsD.Cells(ROW_DATE, col).Value) Then
                    keyW = "祝"
                Else
                    keyW = IIf(Len(wLbl) > 0, Left$(wLbl, 1), "")
                End If

                nCol = IIf(keyW <> "", WeekdayKeyToCol(wsK, keyW), 0)
                putOne = False
                If nCol > 0 And rowsK > 0 Then
                    For rr = 2 To lastRowK
                        If InStr(1, Trim$(CStr(wsK.Cells(rr, kuStatusCol).Value)), "混合", vbTextCompare) > 0 Then
                            If Trim$(CStr(wsK.Cells(rr, kuNameCol).Value)) = zName Then
                                Dim vv As Variant: vv = wsK.Cells(rr, nCol).Value
                                If IsNumeric(vv) Then If CDbl(vv) >= 1 Then putOne = True: Exit For
                            End If
                        End If
                    Next rr
                End If

                If putOne Then
                    Call SafeSetCellValue(wsD, rowZone, col, 1)
                Else
                    Call SafeClearCell(wsD, rowZone, col)
                End If

                Err.Clear
                On Error GoTo 0
            Next col
        Else
            Call SafeClearRowSpan(wsD, rowZone, colStart, colEnd)
        End If
    Next rowZone

    '--- 書込後：保護の復帰 ---
    If restoreProtect Then TryReprotect wsD

    Posms_Import_Core = True
    GoTo Cleanup

ErrHandler:
    Posms_Import_Core = False
Cleanup:
    EndBatch
End Function


