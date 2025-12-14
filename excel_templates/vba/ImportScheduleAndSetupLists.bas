Attribute VB_Name = "Module9"
Option Explicit

'================= レイアウト/配置設定 =================
Private Const START_ROW As Long = 23                 ' 1人の上段
Private Const END_ROW   As Long = 122                ' 最終行（含む）→ 23～122 で 100 行 = 50 名
Private Const COL_FIRST As Long = 3                  ' C 列（ドロップダウン開始）
Private Const COL_LAST  As Long = 30                 ' AD 列（ドロップダウン終了）
Private Const DST_SHEET As String = "分担予定表(案)" ' 宛先シート名
'======================================================

'================= 参照元シート名（同一ブック） =========
Private Const SHEET_EMP     As String = "社員"               ' 氏名/社員タイプ/役職/班長/副班長
Private Const SHEET_FULL    As String = "正社員服務表"       ' 勤務名
Private Const SHEET_TEMP_1  As String = "期間雇用社員服務表" ' 勤務名（優先）
Private Const SHEET_TEMP_2  As String = "期間雇用服務表"     ' 勤務名（代替）
Private Const SHEET_ZONES   As String = "区情報"             ' 区名
Private Const SHEET_LEAVE   As String = "休暇種類"           ' 休暇（列名は柔軟に）
Private Const SHEET_SPECIAL As String = "特殊区分"           ' 特別（列名は柔軟に）
'======================================================

'================= 厳格ヘッダー名（既定） ================
Private Const HDR_EMP_NAME   As String = "氏名"
Private Const HDR_EMP_TYPE   As String = "社員タイプ"
Private Const HDR_EMP_ROLE   As String = "役職"
Private Const HDR_LEADER     As String = "班長"
Private Const HDR_VICE       As String = "副班長"

Private Const HDR_JOB_NAME   As String = "勤務名"
Private Const HDR_ZONE_NAME  As String = "区名"

' --- 休暇/特別の見出しは“非厳格”に変更（複数候補から自動検出） ---
Private Const HDR_LEAVE_NAME_STRICT As String = "休暇種類名"   ' 従来の厳格名（あってもOK）
Private Const HDR_SP_NAME_STRICT    As String = "特別区分名"   ' 従来の厳格名（あってもOK）
'======================================================

'================= 作成する名前定義 =====================
Private Const NM_REG_JOBS      As String = "RegJobs"
Private Const NM_TEMP_JOBS     As String = "TempJobs"
Private Const NM_LOWER_CHOICES As String = "LowerChoices"
'======================================================


'================= ヘルパ ===============================
Private Function FindHeaderColStrict(ws As Worksheet, headerText As String) As Long
    Dim lastCol As Long, c As Long
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column
    For c = 1 To lastCol
        If CStr(ws.Cells(1, c).Value) = headerText Then
            FindHeaderColStrict = c
            Exit Function
        End If
    Next c
End Function

' **追加：候補配列から最初に見つかった列を返す（厳格一致）**
Private Function FindHeaderColAny(ws As Worksheet, ParamArray candidates()) As Long
    Dim i As Long, col As Long
    For i = LBound(candidates) To UBound(candidates)
        If LenB(CStr(candidates(i))) > 0 Then
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

Private Function ValToBool(v As Variant) As Boolean
    Dim s As String
    If IsNumeric(v) Then
        ValToBool = (CLng(v) <> 0)
        Exit Function
    End If
    s = Trim$(CStr(v))
    ValToBool = (UCase$(s) = "TRUE") Or (s = "○") Or (s = "◯") Or (s = "1") Or (s = "はい") Or (UCase$(s) = "YES")
End Function

Private Function IsTempType(ByVal s As String) As Boolean
    Dim t As String: t = Trim$(CStr(s))
    Select Case t
        Case "期間雇用社員", "期間雇用", "期間雇用外務", "期間雇用内務", "ゆうメイト", "アソシエイト"
            IsTempType = True
        Case Else
            IsTempType = False
    End Select
End Function

Private Sub DefineNameIfRange(ByVal nm As String, rng As Range)
    On Error Resume Next
    ThisWorkbook.names(nm).Delete
    On Error GoTo 0
    If Not rng Is Nothing Then
        If Application.WorksheetFunction.CountA(rng) > 0 Then
            ThisWorkbook.names.Add Name:=nm, RefersTo:="=" & rng.Address(True, True, , True)
        End If
    End If
End Sub
'======================================================


'======================================================
' メイン：名簿配置（2行間隔）＋ドロップダウン設定（上段/下段）
'======================================================
Public Sub ImportScheduleAndSetupLists()
    Dim appCalc As XlCalculation
    Dim wsDst As Worksheet, wsEmp As Worksheet, wsFull As Worksheet, wsTemp As Worksheet
    Dim wsZones As Worksheet, wsLeave As Worksheet, wsSp As Worksheet, wsLists As Worksheet

    Dim nameCol As Long, typeCol As Long, roleCol As Long, leaderCol As Long, viceCol As Long
    Dim jobColFull As Long, jobColTemp As Long, zoneCol As Long, leaveCol As Long, spCol As Long

    Dim empLast As Long, empCount As Long, cap As Long, usedEmp As Long
    Dim i As Long, rowTop As Long, rowBot As Long
    Dim empName As String, empType As String, empRole As String
    Dim isLeader As Boolean, isVice As Boolean, isTemp As Boolean
    Dim aVal As String
    Dim lastR As Long, regCnt As Long, tempCnt As Long, lowerCnt As Long, dstRow As Long
    Dim topFormula As String, botFormula As String

    cap = Int((END_ROW - START_ROW + 1) / 2)   ' 2行/人 → 最大50人

    ' 参照取得（同一ブック）
    On Error Resume Next
    Set wsDst = ThisWorkbook.Worksheets(DST_SHEET)
    Set wsEmp = ThisWorkbook.Worksheets(SHEET_EMP)
    Set wsFull = ThisWorkbook.Worksheets(SHEET_FULL)
    Set wsTemp = ThisWorkbook.Worksheets(SHEET_TEMP_1)
    If wsTemp Is Nothing Then Set wsTemp = ThisWorkbook.Worksheets(SHEET_TEMP_2)
    Set wsZones = ThisWorkbook.Worksheets(SHEET_ZONES)
    Set wsLeave = ThisWorkbook.Worksheets(SHEET_LEAVE)
    Set wsSp = ThisWorkbook.Worksheets(SHEET_SPECIAL)
    On Error GoTo 0

    If wsDst Is Nothing Then MsgBox "宛先シート '" & DST_SHEET & "' がありません。", vbExclamation: Exit Sub
    If wsEmp Is Nothing Then MsgBox "『" & SHEET_EMP & "』がありません。", vbExclamation: Exit Sub
    If wsFull Is Nothing Then MsgBox "『" & SHEET_FULL & "』がありません。", vbExclamation: Exit Sub
    If wsTemp Is Nothing Then MsgBox "『" & SHEET_TEMP_1 & "／" & SHEET_TEMP_2 & "』がありません。", vbExclamation: Exit Sub
    If wsZones Is Nothing Then MsgBox "『" & SHEET_ZONES & "』がありません。", vbExclamation: Exit Sub
    ' 休暇/特別は非必須（存在すれば読む）

    ' 見出し列（厳格：社員/勤務/区）
    nameCol = FindHeaderColStrict(wsEmp, HDR_EMP_NAME)
    typeCol = FindHeaderColStrict(wsEmp, HDR_EMP_TYPE)
    roleCol = FindHeaderColStrict(wsEmp, HDR_EMP_ROLE)         ' 任意
    leaderCol = FindHeaderColStrict(wsEmp, HDR_LEADER)
    viceCol = FindHeaderColStrict(wsEmp, HDR_VICE)
    If nameCol = 0 Or leaderCol = 0 Or viceCol = 0 Then
        MsgBox "『社員』に '" & HDR_EMP_NAME & "', '" & HDR_LEADER & "', '" & HDR_VICE & "' 列が必要です。", vbExclamation
        Exit Sub
    End If

    jobColFull = FindHeaderColStrict(wsFull, HDR_JOB_NAME)
    jobColTemp = FindHeaderColStrict(wsTemp, HDR_JOB_NAME)
    If jobColFull = 0 Or jobColTemp = 0 Then
        MsgBox "『" & SHEET_FULL & "／" & wsTemp.Name & "』に '" & HDR_JOB_NAME & "' 列が必要です。", vbExclamation
        Exit Sub
    End If

    zoneCol = FindHeaderColStrict(wsZones, HDR_ZONE_NAME)
    If zoneCol = 0 Then
        MsgBox "『区情報』に '" & HDR_ZONE_NAME & "' 列が必要です。", vbExclamation
        Exit Sub
    End If

    ' **非厳格**：休暇/特別は DB 由来の列名にも追従（英語・日本語どちらも許容）
    If Not wsLeave Is Nothing Then
        leaveCol = FindHeaderColAny(wsLeave, _
                    HDR_LEAVE_NAME_STRICT, "休暇名", "leave_name")
    End If
    If Not wsSp Is Nothing Then
        spCol = FindHeaderColAny(wsSp, _
                    HDR_SP_NAME_STRICT, "区分名", "attendance_name")
    End If

    ' パフォーマンス
    With Application
        appCalc = .Calculation
        .ScreenUpdating = False
        .EnableEvents = False
        .Calculation = xlCalculationManual
        .DisplayAlerts = False
    End With
    On Error GoTo TIDY

    ' 宛先クリア（名簿＆検証）
    wsDst.Range("A" & START_ROW & ":AD" & END_ROW).ClearContents
    On Error Resume Next
    wsDst.Range(wsDst.Cells(START_ROW, COL_FIRST), wsDst.Cells(END_ROW, COL_LAST)).Validation.Delete
    On Error GoTo 0

    ' 社員件数 → 上限決定
    empLast = LastDataRow(wsEmp, nameCol)
    If empLast < 2 Then
        MsgBox "『社員』のデータがありません。", vbExclamation
        GoTo TIDY
    End If
    empCount = empLast - 1
    usedEmp = IIf(empCount > cap, cap, empCount)
    If empCount > cap Then
        MsgBox "表示可能な上限（" & cap & "名）まで取り込みます。", vbInformation
    End If

    ' ========== A/B の配置（2行間隔：上段のみ書き込み） ==========
    For i = 1 To usedEmp
        rowTop = START_ROW + (i - 1) * 2
        rowBot = rowTop + 1

        empName = CStr(wsEmp.Cells(i + 1, nameCol).Value)
        empType = IIf(typeCol > 0, CStr(wsEmp.Cells(i + 1, typeCol).Value), "")
        empRole = IIf(roleCol > 0, CStr(wsEmp.Cells(i + 1, roleCol).Value), "")
        isLeader = ValToBool(wsEmp.Cells(i + 1, leaderCol).Value)
        isVice = ValToBool(wsEmp.Cells(i + 1, viceCol).Value)
        isTemp = (typeCol > 0 And IsTempType(empType))

        ' A列の優先順位：班長 > 副班長 > 期間雇用("ゆ") > 役職
        If isLeader Then
            aVal = "班長"
        ElseIf isVice Then
            aVal = "副班長"
        ElseIf isTemp Then
            aVal = "ゆ"
        Else
            aVal = empRole
        End If

        wsDst.Cells(rowTop, "A").Value = aVal     ' 上段のみ
        wsDst.Cells(rowTop, "B").Value = empName  ' 上段のみ
    Next i

    ' ========== ドロップダウン用リスト（Lists）作成 ==========
    On Error Resume Next
    Set wsLists = ThisWorkbook.Worksheets("Lists")
    On Error GoTo 0
    If wsLists Is Nothing Then
        Set wsLists = ThisWorkbook.Worksheets.Add(After:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count))
        wsLists.Name = "Lists"
    Else
        wsLists.Cells.Clear
    End If

    ' A: 正社員勤務名
    lastR = LastDataRow(wsFull, jobColFull)
    regCnt = Application.Max(0, lastR - 1) ' 2行目以降がデータ
    If regCnt > 0 Then
        wsFull.Range(wsFull.Cells(2, jobColFull), wsFull.Cells(lastR, jobColFull)).Copy wsLists.Range("A1")
    End If
    Call DefineNameIfRange(NM_REG_JOBS, IIf(regCnt > 0, wsLists.Range("A1").Resize(regCnt, 1), Nothing))

    ' B: 期間雇用勤務名
    lastR = LastDataRow(wsTemp, jobColTemp)
    tempCnt = Application.Max(0, lastR - 1)
    If tempCnt > 0 Then
        wsTemp.Range(wsTemp.Cells(2, jobColTemp), wsTemp.Cells(lastR, jobColTemp)).Copy wsLists.Range("B1")
    End If
    Call DefineNameIfRange(NM_TEMP_JOBS, IIf(tempCnt > 0, wsLists.Range("B1").Resize(tempCnt, 1), Nothing))

    ' C: 下段候補（区名 + 休暇 + 特別）
    dstRow = 1

    ' 1) 区名
    lastR = LastDataRow(wsZones, zoneCol)
    If lastR > 1 Then
        wsZones.Range(wsZones.Cells(2, zoneCol), wsZones.Cells(lastR, zoneCol)).Copy wsLists.Cells(dstRow, "C")
        dstRow = dstRow + (lastR - 1)
    End If

    ' 2) 休暇（列名は柔軟）
    If Not wsLeave Is Nothing And leaveCol > 0 Then
        lastR = LastDataRow(wsLeave, leaveCol)
        If lastR > 1 Then
            wsLeave.Range(wsLeave.Cells(2, leaveCol), wsLeave.Cells(lastR, leaveCol)).Copy wsLists.Cells(dstRow, "C")
            dstRow = dstRow + (lastR - 1)
        End If
    End If

    ' 3) 特別（列名は柔軟）
    If Not wsSp Is Nothing And spCol > 0 Then
        lastR = LastDataRow(wsSp, spCol)
        If lastR > 1 Then
            wsSp.Range(wsSp.Cells(2, spCol), wsSp.Cells(lastR, spCol)).Copy wsLists.Cells(dstRow, "C")
            dstRow = dstRow + (lastR - 1)
        End If
    End If

    lowerCnt = dstRow - 1
    Call DefineNameIfRange(NM_LOWER_CHOICES, IIf(lowerCnt > 0, wsLists.Range("C1").Resize(lowerCnt, 1), Nothing))
    wsLists.Visible = xlSheetHidden

    ' ========== ドロップダウン割当（上段/下段） ==========
    Dim rng As Range
    For i = 1 To usedEmp
        rowTop = START_ROW + (i - 1) * 2
        rowBot = rowTop + 1

        empName = Trim$(CStr(wsDst.Cells(rowTop, 2).Value))
        If empName = "" Then GoTo NEXT_I

        ' 上段：勤務名（正/期で切替）
        empType = IIf(typeCol > 0, CStr(wsEmp.Cells(i + 1, typeCol).Value), "")
        isTemp = (typeCol > 0 And IsTempType(empType))
        topFormula = IIf(isTemp, "=" & NM_TEMP_JOBS, "=" & NM_REG_JOBS)

        Set rng = wsDst.Range(wsDst.Cells(rowTop, COL_FIRST), wsDst.Cells(rowTop, COL_LAST))
        On Error Resume Next: rng.Validation.Delete: On Error GoTo 0
        If topFormula <> "=" Then
            With rng.Validation
                .Add Type:=xlValidateList, AlertStyle:=xlValidAlertStop, _
                     Operator:=xlBetween, Formula1:=topFormula
                .IgnoreBlank = True
                .InCellDropdown = True
            End With
        End If

        ' 下段：区名 + 休暇 + 特別
        Set rng = wsDst.Range(wsDst.Cells(rowBot, COL_FIRST), wsDst.Cells(rowBot, COL_LAST))
        On Error Resume Next: rng.Validation.Delete: On Error GoTo 0
        If lowerCnt > 0 Then
            With rng.Validation
                .Add Type:=xlValidateList, AlertStyle:=xlValidAlertStop, _
                     Operator:=xlBetween, Formula1:="=" & NM_LOWER_CHOICES
                .IgnoreBlank = True
                .InCellDropdown = True
            End With
        End If

NEXT_I:
    Next i

    MsgBox "名簿配置（2行間隔）とドロップダウン設定（上段=勤務名／下段=区+休暇+特別）を完了しました。", vbInformation

TIDY:
    With Application
        .DisplayAlerts = True
        .Calculation = appCalc
        .EnableEvents = True
        .ScreenUpdating = True
    End With
End Sub




