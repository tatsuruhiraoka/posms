Attribute VB_Name = "Get28Days"
Option Explicit

' ====== 設定（必要に応じて変更） ==========================================
Private Const POSMS_CSV_DIR As String = "db/init/csv"
Private Const HOLIDAY_CSV_FILE As String = "holidays_jp_2020_2050.csv"
Private Const PROMPT_IF_NOT_FOUND As Boolean = True
Private Const MAX_PARENT_HOPS As Long = 8
Private Const TARGET_SHEET_NAME As String = "分担予定表(案)"
Private Const START_ANCHOR As String = "C5"
' =========================================================================

' ---- 簡易キャッシュ ------------------------------------------------------
Private mCachePath As String
Private mCacheSet As Collection
' -------------------------------------------------------------------------


' ===================== 公開エントリポイント ===============================

Public Sub Get28DaysWithMonthHeaders()
    On Error GoTo ErrHandler

    Dim ws As Worksheet
    Dim userInput As String
    Dim startDate As Date, endDate As Date
    Dim baseCol As Long, i As Long
    Dim d As Date, tgtCol As Long
    Dim currentMonth As Integer, thisMonth As Integer
    Dim holSet As Collection
    Dim isWeekend As Boolean, isHoliday As Boolean

    ' 高速化
    Dim prevCalc As XlCalculation
    Dim prevScr As Boolean, prevEvt As Boolean
    prevScr = Application.ScreenUpdating
    prevEvt = Application.EnableEvents
    prevCalc = Application.Calculation
    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    Set ws = ThisWorkbook.Worksheets(TARGET_SHEET_NAME)
    baseCol = ws.Range(START_ANCHOR).Column

    ' 開始日入力（既定：直近日曜）
    Dim defaultStart As Date
    defaultStart = Date - (Weekday(Date, vbSunday) - 1)

    userInput = InputBox( _
        "開始日（必ず日曜日）を yyyy/mm/dd 形式で入力してください。", _
        "開始日入力", _
        Format(defaultStart, "yyyy/mm/dd"))

    If userInput = "" Then GoTo CleanExit
    If Not IsDate(userInput) Then
        MsgBox "有効な日付を入力してください。", vbExclamation
        GoTo CleanExit
    End If

    startDate = CDate(userInput)
    If Weekday(startDate, vbSunday) <> vbSunday Then
        MsgBox "開始日は日曜日である必要があります。", vbExclamation
        GoTo CleanExit
    End If
    endDate = startDate + 27

    ' ---- 祝日CSV読み込み ----
    Set holSet = Posms_FetchHolidaySetFromCSV()
    If holSet Is Nothing Then
        MsgBox "祝日CSVの読み込みに失敗しました。週末のみ色付けします。", vbExclamation
    End If

    ' 初期化（テンプレの仕様に合わせた固定範囲）
    ws.Range(ws.Cells(3, baseCol), ws.Cells(22, baseCol + 27)).Interior.Pattern = xlNone
    ws.Range(ws.Cells(3, baseCol), ws.Cells(3, baseCol + 27)).ClearContents
    ws.Range(ws.Cells(5, baseCol), ws.Cells(5, baseCol + 27)).ClearContents
    ws.Range(ws.Cells(22, baseCol), ws.Cells(22, baseCol + 27)).ClearContents

    currentMonth = Month(startDate)
    ws.Cells(3, baseCol).Value = currentMonth & "月"
    ws.Range(ws.Cells(5, baseCol), ws.Cells(5, baseCol + 27)).NumberFormat = "0"
    ws.Range(ws.Cells(22, baseCol), ws.Cells(22, baseCol + 27)).NumberFormat = "0"

    For i = 0 To 27
        d = startDate + i
        thisMonth = Month(d)
        tgtCol = baseCol + i

        ws.Cells(5, tgtCol).Value = Day(d)
        ws.Cells(22, tgtCol).Value = Day(d)

        If thisMonth <> currentMonth Then
            ws.Cells(3, tgtCol).Value = thisMonth & "月"
            currentMonth = thisMonth
        End If

        isWeekend = (Weekday(d, vbMonday) >= 6)
        isHoliday = Posms_IsHolidayBySet(holSet, d)

        If isHoliday Or isWeekend Then
            With ws.Range(ws.Cells(3, tgtCol), ws.Cells(22, tgtCol)).Interior
                .Pattern = xlSolid
                If isHoliday And isWeekend Then
                    .Color = RGB(255, 220, 230)
                ElseIf isHoliday Then
                    .Color = RGB(255, 235, 240)
                Else
                    .Color = RGB(230, 230, 230)
                End If
            End With
        End If
    Next i

    ws.Range("V1").NumberFormat = "yyyy年m月d日": ws.Range("V1").Value = startDate
    ws.Range("AA1").NumberFormat = "yyyy年m月d日": ws.Range("AA1").Value = endDate

    MsgBox "28日表を更新しました。", vbInformation

CleanExit:
    Application.ScreenUpdating = prevScr
    Application.EnableEvents = prevEvt
    Application.Calculation = prevCalc
    Exit Sub

ErrHandler:
    MsgBox "エラーが発生しました: " & Err.Description, vbExclamation
    Resume CleanExit
End Sub


' ===================== 祝日セット参照 ===============================

Private Function Posms_IsHolidayBySet(ByVal setCol As Collection, ByVal dt As Date) As Boolean
    If setCol Is Nothing Then Exit Function

    Dim k As String
    k = Posms_DateKey(dt)

    On Error Resume Next
    Dim tmp As Variant
    tmp = setCol(k)
    Posms_IsHolidayBySet = (Err.Number = 0)
    Err.Clear
End Function


' ===================== 祝日CSVローダ ===============================

Private Function Posms_FetchHolidaySetFromCSV() As Collection
    Dim path As String
    path = Posms_ResolveHolidayCsvPathInRepo()
    If Len(path) = 0 Then Exit Function

    If path = mCachePath And Not mCacheSet Is Nothing Then
        Set Posms_FetchHolidaySetFromCSV = mCacheSet
        Exit Function
    End If

    Dim col As New Collection
    Dim fh As Integer: fh = FreeFile
    Dim line As String, dt As Date
    Dim firstLine As Boolean: firstLine = True

    Open path For Input As #fh
    Do While Not EOF(fh)
        Line Input #fh, line
        If firstLine Then
            line = Posms_RemoveBOM(line)
            firstLine = False
        End If

        If Posms_TryParseHolidayCsvLine(line, dt) Then
            On Error Resume Next
            col.Add True, Posms_DateKey(dt)
            Err.Clear
        End If
    Loop
    Close #fh

    Set mCacheSet = col
    mCachePath = path
    Set Posms_FetchHolidaySetFromCSV = col
End Function


' ===================== パス解決（修正版） ===============================

Private Function Posms_ResolveHolidayCsvPathInRepo() As String
    Dim base As String: base = ThisWorkbook.path
    Dim candidate As String, p As String
    Dim i As Long

    If Len(base) = 0 Then
        ' 未保存ブックだと Path が空になる
        If PROMPT_IF_NOT_FOUND Then GoTo PickFile Else Exit Function
    End If

    candidate = Posms_JoinPath(Posms_JoinPath(base, POSMS_CSV_DIR), HOLIDAY_CSV_FILE)
    If Posms_FileExists(candidate) Then Posms_ResolveHolidayCsvPathInRepo = candidate: Exit Function

    p = base
    For i = 1 To MAX_PARENT_HOPS
        p = Posms_ParentDir(p)
        If Len(p) = 0 Then Exit For
        candidate = Posms_JoinPath(Posms_JoinPath(p, POSMS_CSV_DIR), HOLIDAY_CSV_FILE)
        If Posms_FileExists(candidate) Then Posms_ResolveHolidayCsvPathInRepo = candidate: Exit Function
    Next i

PickFile:
    If PROMPT_IF_NOT_FOUND Then
        With Application.FileDialog(msoFileDialogFilePicker)
            .title = "祝日CSVを選択してください"
            .AllowMultiSelect = False
            .Filters.Clear
            .Filters.Add "CSV", "*.csv"
            If .Show = -1 Then Posms_ResolveHolidayCsvPathInRepo = .SelectedItems(1)
        End With
    End If
End Function


' ===================== キー生成（統一） ===============================

Private Function Posms_DateKey(ByVal dt As Date) As String
    ' DateValueで時刻を落としてからCLngで日付シリアルにする（必ず同じ経路）
    Posms_DateKey = CStr(CLng(DateValue(dt)))
End Function


' ===================== CSV行のパース（堅牢版） ===============================

Private Function Posms_TryParseHolidayCsvLine(ByVal line As String, ByRef outDate As Date) As Boolean
    Dim t As String
    t = Trim$(line)
    If Len(t) = 0 Then Exit Function

    ' 先頭が日付で、以降に ",名称" 等が付いていてもOKにする
    Dim p As Long
    p = InStr(1, t, ",")
    If p > 0 Then t = Trim$(Left$(t, p - 1))

    On Error Resume Next
    outDate = DateValue(t)
    Posms_TryParseHolidayCsvLine = (Err.Number = 0)
    Err.Clear
End Function


' ===================== 補助 ===============================

Private Function Posms_JoinPath(ByVal a As String, ByVal b As String) As String
    Dim sep As String: sep = Application.PathSeparator
    If Len(a) = 0 Then
        Posms_JoinPath = b
    ElseIf Right$(a, 1) = sep Then
        Posms_JoinPath = a & b
    Else
        Posms_JoinPath = a & sep & b
    End If
End Function

Private Function Posms_ParentDir(ByVal p As String) As String
    Dim sep As String: sep = Application.PathSeparator
    Dim i As Long
    For i = Len(p) To 1 Step -1
        If Mid$(p, i, 1) = sep Then
            Posms_ParentDir = Left$(p, i - 1)
            Exit Function
        End If
    Next i
    Posms_ParentDir = ""
End Function

Private Function Posms_FileExists(ByVal path As String) As Boolean
    Posms_FileExists = (Len(Dir$(path, vbNormal)) > 0)
End Function

Private Function Posms_RemoveBOM(ByVal s As String) As String
    If Len(s) >= 3 Then
        If Left$(s, 3) = Chr$(239) & Chr$(187) & Chr$(191) Then s = Mid$(s, 4)
    End If
    Posms_RemoveBOM = s
End Function


