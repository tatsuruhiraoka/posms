Attribute VB_Name = "ImportSchedule"
Option Explicit

'================= レイアウト/配置設定 =================
Private Const START_ROW As Long = 23                 ' 1人の上段
Private Const END_ROW   As Long = 122                ' 最終行（含む）→ 23～122 で 100 行 = 50 名
Private Const COL_FIRST As Long = 3                  ' C 列（ドロップダウン開始）
Private Const COL_LAST  As Long = 30                 ' AD 列（ドロップダウン終了）
Private Const DST_SHEET As String = "分担予定表(案)" ' 宛先シート名
Private Const COL_EMPNO As Long = 31   ' AE列：社員番号（上段）
'======================================================

'================= 参照元シート名（同一ブック） =========
Private Const SHEET_EMP     As String = "社員"               ' 氏名/社員タイプ/役職/班長/副班長
Private Const SHEET_FULL    As String = "正社員服務表"       ' 勤務名
Private Const SHEET_TEMP_1  As String = "期間雇用社員服務表" ' 勤務名（優先）
Private Const SHEET_TEMP_2  As String = "期間雇用服務表"     ' 勤務名（代替）
Private Const SHEET_ZONES   As String = "区情報"             ' 区名・需要・稼働
Private Const SHEET_LEAVE   As String = "休暇種類"           ' 休暇（列名は柔軟に）
Private Const SHEET_SPECIAL As String = "特殊区分"           ' 特別（列名は柔軟に）
'======================================================

'================= 基本ヘッダー名 =======================
Private Const HDR_EMP_NAME   As String = "氏名"
Private Const HDR_EMP_TYPE   As String = "社員タイプ"
Private Const HDR_EMP_ROLE   As String = "役職"
Private Const HDR_LEADER     As String = "班長"
Private Const HDR_VICE       As String = "副班長"

Private Const HDR_JOB_NAME   As String = "勤務名"
Private Const HDR_ZONE_NAME  As String = "区名"
Private Const HDR_ZONE_STAT  As String = "稼働"     ' 「混合」抽出に使用
'======================================================

'================= 名前定義名 ===========================
Private Const NM_REG_JOBS      As String = "RegJobs"
Private Const NM_TEMP_JOBS     As String = "TempJobs"
Private Const NM_LOWER_CHOICES As String = "LowerChoices"
'======================================================


'================= 汎用ユーティリティ ===================
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

Private Function SheetByName(ByVal wb As Workbook, ByVal nm As String) As Worksheet
    On Error Resume Next
    Set SheetByName = wb.Worksheets(nm)
    On Error GoTo 0
End Function

Private Sub ClearDownCells(ByVal ws As Worksheet, ByVal startRow As Long, ByVal startCol As Long, ByVal maxRows As Long, ByVal width As Long)
    If maxRows <= 0 Or width <= 0 Then Exit Sub
    On Error Resume Next
    ws.Cells(startRow, startCol).Resize(maxRows, width).ClearContents
    On Error GoTo 0
End Sub

Private Sub SafeSetCellValue(ByVal ws As Worksheet, ByVal row As Long, ByVal col As Long, ByVal v As Variant)
    On Error Resume Next
    If ws.Cells(row, col).MergeCells Then
        ws.Cells(row, col).MergeArea.Value = v
    Else
        ws.Cells(row, col).Value = v
    End If
    On Error GoTo 0
End Sub

Private Sub SafeClearCell(ByVal ws As Worksheet, ByVal row As Long, ByVal col As Long)
    On Error Resume Next
    If ws.Cells(row, col).MergeCells Then
        ws.Cells(row, col).MergeArea.ClearContents
    Else
        ws.Cells(row, col).ClearContents
    End If
    On Error GoTo 0
End Sub

Private Sub SafeClearRowSpan(ByVal ws As Worksheet, ByVal row As Long, ByVal colStart As Long, ByVal colEnd As Long)
    Dim c As Long
    For c = colStart To colEnd
        SafeClearCell ws, row, c
    Next
End Sub

Private Function FindHeaderColStrict(ws As Worksheet, headerText As String) As Long
    Dim lastCol As Long, c As Long, v As String
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    For c = 1 To lastCol
        v = Trim$(CStr(ws.Cells(1, c).Value))
        If v = Trim$(headerText) Then
            FindHeaderColStrict = c
            Exit Function
        End If
    Next c
End Function

Private Function FindHeaderColAny(ws As Worksheet, ParamArray candidates()) As Long
    Dim i As Long, col As Long
    For i = LBound(candidates) To UBound(candidates)
        If Len(Trim$(CStr(candidates(i)))) > 0 Then
            col = FindHeaderColStrict(ws, CStr(candidates(i)))
            If col > 0 Then
                FindHeaderColAny = col
                Exit Function
            End If
        End If
    Next i
    FindHeaderColAny = 0
End Function

Private Function LastDataRow(ws As Worksheet, col As Long) As Long
    If col <= 0 Then
        LastDataRow = 1
    Else
        LastDataRow = ws.Cells(ws.Rows.Count, col).End(xlUp).row
    End If
End Function

Private Function WeekdayKeyToCol(ByVal ws As Worksheet, ByVal key As String) As Long
    If key = "" Then Exit Function
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
End Function

' ---- 祝日系（色 or 日付） ----
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

Private Function IsHolidayByColor(ByVal ws As Worksheet, ByVal col As Long, ByVal baseColor As Long) As Boolean
    IsHolidayByColor = (ws.Cells(5, col).Interior.Color <> baseColor)
End Function

Private Function IsHolidayInSheet(ByVal d As Date) As Boolean
    On Error GoTo done
    Dim wb As Workbook: Set wb = ThisWorkbook
    Dim ws As Worksheet: Set ws = SheetByName(wb, "祝日")
    If ws Is Nothing Then GoTo done
    Dim last As Long: last = ws.Cells(ws.Rows.Count, 1).End(xlUp).row
    If last < 1 Then GoTo done
    Dim i As Long
    For i = 1 To last
        If IsDate(ws.Cells(i, 1).Value) Then
            If CLng(CDate(ws.Cells(i, 1).Value)) = CLng(d) Then IsHolidayInSheet = True: Exit Function
        End If
    Next i
done:
End Function

Private Function NthWeekdayOfMonth(ByVal y As Long, ByVal m As Long, ByVal vbWDay As VbDayOfWeek, ByVal n As Long) As Date
    Dim first As Date, delta As Long
    first = DateSerial(y, m, 1)
    delta = (vbWDay - Weekday(first, vbSunday) + 7) Mod 7
    NthWeekdayOfMonth = first + delta + 7 * (n - 1)
End Function

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
        IsJPNationalHolidayCore = True: Exit Function
    End If
    If d = NthWeekdayOfMonth(y, 1, vbMonday, 2) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 7, vbMonday, 3) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 9, vbMonday, 3) Then IsJPNationalHolidayCore = True: Exit Function
    If d = NthWeekdayOfMonth(y, 10, vbMonday, 2) Then IsJPNationalHolidayCore = True: Exit Function
    If d = VernalEquinoxDay(y) Then IsJPNationalHolidayCore = True: Exit Function
    If d = AutumnalEquinoxDay(y) Then IsJPNationalHolidayCore = True: Exit Function
End Function

Private Function IsSubstituteHoliday(ByVal d As Date) As Boolean
    If Weekday(d, vbSunday) = vbSunday Then Exit Function
    Dim prev As Date: prev = DateAdd("d", -1, d)
    If Weekday(prev, vbSunday) = vbSunday Then
        If IsJPNationalHolidayCore(prev) Then IsSubstituteHoliday = True
    End If
End Function

Private Function IsHolidayDate(ByVal v As Variant) As Boolean
    If Not IsDate(v) Then Exit Function
    Dim d As Date: d = CDate(v)
    If IsHolidayInSheet(d) Then IsHolidayDate = True: Exit Function
    If IsJPNationalHolidayCore(d) Then IsHolidayDate = True: Exit Function
    If IsSubstituteHoliday(d) Then IsHolidayDate = True: Exit Function
End Function

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

Public Function IsTruthy(ByVal v As Variant) As Boolean
    If IsNumeric(v) Then
        IsTruthy = (v = 1)
    Else
        IsTruthy = False
    End If
End Function

Private Function BuildRole(ByVal vLeader As Variant, ByVal vSub As Variant, ByVal vType As Variant, ByVal tempName As String) As String
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

Private Function SafeIdx(ByVal arr As Variant, ByVal idx As Long) As Variant
    If IsEmpty(arr) Then Exit Function
    On Error Resume Next
    SafeIdx = arr(idx, 1)
    On Error GoTo 0
End Function

Private Sub DefineNameIfRange(ByVal nm As String, rng As Range)
    On Error Resume Next
    ThisWorkbook.Names(nm).Delete
    On Error GoTo 0
    If Not rng Is Nothing Then
        If Application.WorksheetFunction.CountA(rng) > 0 Then
            ThisWorkbook.Names.Add Name:=nm, RefersTo:="=" & rng.Address(True, True, , True)
        End If
    End If
End Sub

Private Function IsTempType(ByVal s As String) As Boolean
    Dim t As String: t = Trim$(CStr(s))
    Select Case t
        Case "期間雇用社員", "期間雇用", "期間雇用外務", "期間雇用内務", "ゆうメイト", "アソシエイト"
            IsTempType = True
        Case Else
            IsTempType = False
    End Select
End Function
'======================================================


'======================================================
' メイン：名簿配置（2行間隔）＋混合区/需要＋ドロップダウン
'======================================================
Public Sub ImportScheduleAndSetupLists()
    Dim wb As Workbook: Set wb = ThisWorkbook
    Dim wsD As Worksheet, wsS As Worksheet, wsK As Worksheet
    Dim wsFull As Worksheet, wsTemp As Worksheet, wsLeave As Worksheet, wsSp As Worksheet, wsLists As Worksheet

    Set wsD = SheetByName(wb, DST_SHEET)
    Set wsS = SheetByName(wb, SHEET_EMP)
    Set wsK = SheetByName(wb, SHEET_ZONES)
    Set wsFull = SheetByName(wb, SHEET_FULL)
    Set wsTemp = SheetByName(wb, SHEET_TEMP_1)
    If wsTemp Is Nothing Then Set wsTemp = SheetByName(wb, SHEET_TEMP_2)
    Set wsLeave = SheetByName(wb, SHEET_LEAVE)
    Set wsSp = SheetByName(wb, SHEET_SPECIAL)

    If wsD Is Nothing Then MsgBox "'" & DST_SHEET & "' がありません。", vbExclamation: Exit Sub
    If wsS Is Nothing Then MsgBox "'" & SHEET_EMP & "' がありません。", vbExclamation: Exit Sub
    If wsK Is Nothing Then MsgBox "'" & SHEET_ZONES & "' がありません。", vbExclamation: Exit Sub
    If wsFull Is Nothing Then MsgBox "'" & SHEET_FULL & "' がありません。", vbExclamation: Exit Sub
    If wsTemp Is Nothing Then MsgBox "'" & SHEET_TEMP_1 & "／" & SHEET_TEMP_2 & "' がありません。", vbExclamation: Exit Sub

    BeginBatch
    On Error GoTo TIDY

    '--- 社員ヘッダ列 ---
    Dim nameCol As Long, deptCol As Long, teamCol As Long, leaderCol As Long, subCol As Long, empTypeCol As Long, roleCol As Long
    nameCol = FindHeaderColStrict(wsS, HDR_EMP_NAME)
    deptCol = FindHeaderColStrict(wsS, "部")
    teamCol = FindHeaderColStrict(wsS, "班")
    leaderCol = FindHeaderColStrict(wsS, HDR_LEADER)
    subCol = FindHeaderColStrict(wsS, HDR_VICE)
    empTypeCol = FindHeaderColStrict(wsS, HDR_EMP_TYPE)
    roleCol = FindHeaderColStrict(wsS, HDR_EMP_ROLE) ' 任意

    If nameCol = 0 Or leaderCol = 0 Or subCol = 0 Then
        MsgBox "『社員』に '" & HDR_EMP_NAME & "', '" & HDR_LEADER & "', '" & HDR_VICE & "' 列が必要です。", vbExclamation
        GoTo TIDY
    End If

    '--- 部/班 → G1/K1 ---
    Const COL_G As Long = 7, COL_K As Long = 11
    If deptCol > 0 Then wsD.Cells(1, COL_G).Value = Trim$(CStr(wsS.Cells(2, deptCol).Value))
    If teamCol > 0 Then wsD.Cells(1, COL_K).Value = Trim$(CStr(wsS.Cells(2, teamCol).Value))

    '--- 氏名/役職 クリア＆取り込み（2行間隔：上段のみ書き込み） ---
    ClearDownCells wsD, START_ROW, 1, 300, 2  ' A/B
    ClearDownCells wsD, START_ROW, 31, 300, 1 ' AEだけ

    Dim lastRowS As Long, rowsN As Long, outCount As Long
    lastRowS = LastDataRow(wsS, nameCol)
    If lastRowS >= 2 Then
        rowsN = lastRowS - 1
        Dim arrName As Variant:  arrName = wsS.Cells(2, nameCol).Resize(rowsN, 1).Value
        Dim arrEmpNo As Variant
        arrEmpNo = wsS.Cells(2, 1).Resize(rowsN, 1).Value  ' 社員シート A列（社員番号）
        Dim arrLeader As Variant, arrSub As Variant, arrType As Variant, arrRole As Variant
        If leaderCol > 0 Then arrLeader = wsS.Cells(2, leaderCol).Resize(rowsN, 1).Value
        If subCol > 0 Then arrSub = wsS.Cells(2, subCol).Resize(rowsN, 1).Value
        If empTypeCol > 0 Then arrType = wsS.Cells(2, empTypeCol).Resize(rowsN, 1).Value
        If roleCol > 0 Then arrRole = wsS.Cells(2, roleCol).Resize(rowsN, 1).Value

        Dim i As Long
        For i = 1 To rowsN
            Dim nm As String: nm = Trim$(CStr(arrName(i, 1)))
            If nm <> "" Then
                outCount = outCount + 1
                SafeSetCellValue wsD, START_ROW + (outCount - 1) * 2, 2, nm ' B(上段)
                Dim roleText As String: roleText = BuildRole(SafeIdx(arrLeader, i), SafeIdx(arrSub, i), SafeIdx(arrType, i), "期間雇用社員")
                If roleText = "" And roleCol > 0 Then roleText = Trim$(CStr(arrRole(i, 1))) ' 役職を補助的に
                If IsTempType(CStr(SafeIdx(arrType, i))) Then roleText = "ゆ"
                SafeSetCellValue wsD, START_ROW + (outCount - 1) * 2, 1, roleText ' A(上段)
                ' 社員番号(AE上段)
                Dim empNo As String
                empNo = Trim$(CStr(arrEmpNo(i, 1)))
                SafeSetCellValue wsD, START_ROW + (outCount - 1) * 2, COL_EMPNO, empNo
            End If
        Next i
    End If

    '--- 区情報：列取得 ---
    Dim kuNameCol As Long, kuStatusCol As Long
    kuNameCol = FindHeaderColStrict(wsK, HDR_ZONE_NAME)
    kuStatusCol = FindHeaderColStrict(wsK, HDR_ZONE_STAT)
    If kuNameCol = 0 Or kuStatusCol = 0 Then GoTo AfterZones

    '--- 混合の区名を B7:B14 に抽出 ---
    Const ROW_ZONE_START As Long = 7, ZONE_ROWS As Long = 8
    wsD.Cells(ROW_ZONE_START, 2).Resize(ZONE_ROWS, 1).ClearContents

    Dim lastRowK As Long, rowsK As Long, zCount As Long
    lastRowK = LastDataRow(wsK, kuNameCol)
    rowsK = IIf(lastRowK >= 2, lastRowK - 1, 0)
    If rowsK > 0 Then
        Dim arrZN As Variant: arrZN = wsK.Cells(2, kuNameCol).Resize(rowsK, 1).Value
        Dim arrST As Variant: arrST = wsK.Cells(2, kuStatusCol).Resize(rowsK, 1).Value
        Dim j As Long
        For j = 1 To rowsK
            If InStr(1, Trim$(CStr(arrST(j, 1))), "混合", vbTextCompare) > 0 Then
                Dim kn As String: kn = Trim$(CStr(arrZN(j, 1)))
                If kn <> "" Then
                    zCount = zCount + 1
                    If zCount <= ZONE_ROWS Then
                        SafeSetCellValue wsD, ROW_ZONE_START + zCount - 1, 2, kn ' B7..B14
                    End If
                End If
            End If
        Next j
    End If

AfterZones:
    '--- 需要（C6:AD6）算出：稼働に「通配」を含む行の、曜日列の値>=1 をカウント ---
    Const ROW_WDAY_LABEL As Long = 4, ROW_DATE As Long = 5, ROW_DEMAND As Long = 6
    Dim colStart As Long: colStart = COL_FIRST
    Dim colEnd   As Long: colEnd = COL_LAST

    Dim baseColor As Long
    baseColor = Row5BaselineColor(wsD, colStart, colEnd)

    Dim r As Long, key As String, needCol As Long, rr As Long, cnt As Long
    For r = colStart To colEnd
        Dim wlbl As String: wlbl = Trim$(CStr(wsD.Cells(ROW_WDAY_LABEL, r).Value))
        Dim theDate As Variant: theDate = wsD.Cells(ROW_DATE, r).Value

        If IsHolidayByColor(wsD, r, baseColor) Then
            If Len(wlbl) > 0 And (Left$(wlbl, 1) <> "土") And (Left$(wlbl, 1) <> "日") Then
                key = "祝"
            Else
                key = IIf(Len(wlbl) > 0, Left$(wlbl, 1), "")
            End If
        ElseIf IsHolidayDate(theDate) Then
            key = "祝"
        Else
            key = IIf(Len(wlbl) > 0, Left$(wlbl, 1), "")
        End If

        needCol = IIf(key <> "", WeekdayKeyToCol(wsK, key), 0)
        cnt = 0
        If needCol > 0 And lastRowK >= 2 Then
            For rr = 2 To lastRowK
                If ContainsTsuhai(wsK.Cells(rr, kuStatusCol).Value) Then
                    Dim v As Variant: v = wsK.Cells(rr, needCol).Value
                    If IsNumeric(v) Then If CDbl(v) >= 1 Then cnt = cnt + 1
                End If
            Next rr
        End If

        If cnt > 0 Then
            SafeSetCellValue wsD, ROW_DEMAND, r, cnt
        Else
            SafeClearCell wsD, ROW_DEMAND, r
        End If
    Next r

    '--- 混合×曜日 → B7:B14 × C..AD の 1 入れ ---
    Dim rowZone As Long, keyW As String, nCol As Long, putOne As Boolean, zName As String
    For rowZone = ROW_ZONE_START To ROW_ZONE_START + ZONE_ROWS - 1
        zName = Trim$(CStr(wsD.Cells(rowZone, 2).Value))
        If zName <> "" Then
            For r = colStart To colEnd
                wlbl = Trim$(CStr(wsD.Cells(ROW_WDAY_LABEL, r).Value))
                If IsHolidayByColor(wsD, r, baseColor) Then
                    If Len(wlbl) > 0 And (Left$(wlbl, 1) <> "土") And (Left$(wlbl, 1) <> "日") Then
                        keyW = "祝"
                    Else
                        keyW = IIf(Len(wlbl) > 0, Left$(wlbl, 1), "")
                    End If
                ElseIf IsHolidayDate(wsD.Cells(ROW_DATE, r).Value) Then
                    keyW = "祝"
                Else
                    keyW = IIf(Len(wlbl) > 0, Left$(wlbl, 1), "")
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
                    SafeSetCellValue wsD, rowZone, r, 1
                Else
                    SafeClearCell wsD, rowZone, r
                End If
            Next r
        Else
            SafeClearRowSpan wsD, rowZone, colStart, colEnd
        End If
    Next rowZone

    '================= ドロップダウン・リスト作成 =================
    On Error Resume Next
    Set wsLists = wb.Worksheets("Lists")
    On Error GoTo 0
    If wsLists Is Nothing Then
        Set wsLists = wb.Worksheets.Add(After:=wb.Sheets(wb.Sheets.Count))
        wsLists.Name = "Lists"
    Else
        wsLists.Cells.Clear
    End If

    ' 正社員勤務名（A）／ 期間雇用勤務名（B）
    Dim jobColFull As Long, jobColTemp As Long, lastR As Long, regCnt As Long, tempCnt As Long
    jobColFull = FindHeaderColStrict(wsFull, HDR_JOB_NAME)
    jobColTemp = FindHeaderColStrict(wsTemp, HDR_JOB_NAME)

    If jobColFull > 0 Then
        lastR = LastDataRow(wsFull, jobColFull)
        regCnt = Application.Max(0, lastR - 1)
        If regCnt > 0 Then
            wsFull.Range(wsFull.Cells(2, jobColFull), wsFull.Cells(lastR, jobColFull)).Copy wsLists.Range("A1")
        End If
        DefineNameIfRange NM_REG_JOBS, IIf(regCnt > 0, wsLists.Range("A1").Resize(regCnt, 1), Nothing)
    End If

    If jobColTemp > 0 Then
        lastR = LastDataRow(wsTemp, jobColTemp)
        tempCnt = Application.Max(0, lastR - 1)
        If tempCnt > 0 Then
            wsTemp.Range(wsTemp.Cells(2, jobColTemp), wsTemp.Cells(lastR, jobColTemp)).Copy wsLists.Range("B1")
        End If
        DefineNameIfRange NM_TEMP_JOBS, IIf(tempCnt > 0, wsLists.Range("B1").Resize(tempCnt, 1), Nothing)
    End If

    ' 下段リスト（C）：区名 + 休暇 + 特別
    Dim zoneCol As Long, leaveCol As Long, spCol As Long, dstRow As Long
    dstRow = 1

    zoneCol = FindHeaderColStrict(wsK, HDR_ZONE_NAME)
    If zoneCol > 0 Then
        lastR = LastDataRow(wsK, zoneCol)
        If lastR > 1 Then
            wsK.Range(wsK.Cells(2, zoneCol), wsK.Cells(lastR, zoneCol)).Copy wsLists.Cells(dstRow, "C")
            dstRow = dstRow + (lastR - 1)
        End If
    End If

    ' 休暇：柔軟（休暇種類名 / 休暇名 / leave_name） ※シートが無い/空ならフォールバック
    Dim added As Long, k As Long
    Dim leaveDefaults As Variant
    leaveDefaults = Array("非番", "週休", "祝休", "計年", "年休", "夏期", "冬期", "代休", "承欠", "産休", "育休", "介護", "病休", "休職", "その他")

    added = 0
    If Not wsLeave Is Nothing Then
        leaveCol = FindHeaderColAny(wsLeave, "休暇種類名", "休暇名", "leave_name")
        If leaveCol > 0 Then
            lastR = LastDataRow(wsLeave, leaveCol)
            If lastR > 1 Then
                wsLeave.Range(wsLeave.Cells(2, leaveCol), wsLeave.Cells(lastR, leaveCol)).Copy wsLists.Cells(dstRow, "C")
                added = lastR - 1
                dstRow = dstRow + added
            End If
        End If
    End If
    If added = 0 Then
        For k = LBound(leaveDefaults) To UBound(leaveDefaults)
            wsLists.Cells(dstRow, "C").Value = leaveDefaults(k)
            dstRow = dstRow + 1
        Next k
    End If

    ' 特別：柔軟（特別区分名 / 区分名 / attendance_name） ※シートが無い/空ならフォールバック
    Dim specialDefaults As Variant
    specialDefaults = Array("廃休", "マル超")

    added = 0
    If Not wsSp Is Nothing Then
        spCol = FindHeaderColAny(wsSp, "特別区分名", "区分名", "attendance_name")
        If spCol > 0 Then
            lastR = LastDataRow(wsSp, spCol)
            If lastR > 1 Then
                wsSp.Range(wsSp.Cells(2, spCol), wsSp.Cells(lastR, spCol)).Copy wsLists.Cells(dstRow, "C")
                added = lastR - 1
                dstRow = dstRow + added
            End If
        End If
    End If
    If added = 0 Then
        For k = LBound(specialDefaults) To UBound(specialDefaults)
            wsLists.Cells(dstRow, "C").Value = specialDefaults(k)
            dstRow = dstRow + 1
        Next k
    End If

    DefineNameIfRange NM_LOWER_CHOICES, IIf(dstRow > 1, wsLists.Range("C1").Resize(dstRow - 1, 1), Nothing)
    wsLists.Visible = xlSheetHidden

    '================= 検証割当（上段/下段） =================
    Dim empLast As Long, empCount As Long, usedEmp As Long, cap As Long
    cap = Int((END_ROW - START_ROW + 1) / 2)
    empLast = LastDataRow(wsS, nameCol)
    If empLast < 2 Then GoTo TIDY
    empCount = empLast - 1
    usedEmp = IIf(empCount > cap, cap, empCount)

    Dim rowTop As Long, rowBot As Long, rng As Range, empType As String, isTemp As Boolean, topFormula As String
    Dim iEmp As Long
    For iEmp = 1 To usedEmp
        rowTop = START_ROW + (iEmp - 1) * 2
        rowBot = rowTop + 1

        ' 上段：勤務名（正/期で切替）
        empType = IIf(empTypeCol > 0, CStr(wsS.Cells(iEmp + 1, empTypeCol).Value), "")
        isTemp = (empTypeCol > 0 And IsTempType(empType))
        topFormula = IIf(isTemp, "=" & NM_TEMP_JOBS, "=" & NM_REG_JOBS)

        Set rng = wsD.Range(wsD.Cells(rowTop, COL_FIRST), wsD.Cells(rowTop, COL_LAST))
        On Error Resume Next: rng.Validation.Delete: On Error GoTo 0
        If (Not ThisWorkbook.Names(NM_REG_JOBS) Is Nothing) Or (Not ThisWorkbook.Names(NM_TEMP_JOBS) Is Nothing) Then
            With rng.Validation
                .Add Type:=xlValidateList, AlertStyle:=xlValidAlertStop, _
                     Operator:=xlBetween, Formula1:=topFormula
                .IgnoreBlank = True
                .InCellDropdown = True
            End With
        End If

        ' 下段：区名 + 休暇 + 特別
        Set rng = wsD.Range(wsD.Cells(rowBot, COL_FIRST), wsD.Cells(rowBot, COL_LAST))
        On Error Resume Next: rng.Validation.Delete: On Error GoTo 0
        If Not ThisWorkbook.Names(NM_LOWER_CHOICES) Is Nothing Then
            With rng.Validation
                .Add Type:=xlValidateList, AlertStyle:=xlValidAlertStop, _
                     Operator:=xlBetween, Formula1:="=" & NM_LOWER_CHOICES
                .IgnoreBlank = True
                .InCellDropdown = True
            End With
        End If
    Next iEmp
    ' 社員番号非表示化
    wsD.Columns(COL_EMPNO).Hidden = True

    MsgBox "名簿配置（2行間隔）＋ 混合区/需要の反映 ＋ ドロップダウン設定を完了しました。", vbInformation

TIDY:
    EndBatch
End Sub






