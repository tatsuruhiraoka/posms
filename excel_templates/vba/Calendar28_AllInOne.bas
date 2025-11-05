Attribute VB_Name = "Module4"
Option Explicit

' ====== İ’èi•K—v‚É‰‚¶‚Ä•ÏXj ==========================================
Private Const POSMS_CSV_DIR As String = "db/init/csv"
Private Const HOLIDAY_CSV_FILE As String = "holidays_jp_2020_2050.csv"
Private Const PROMPT_IF_NOT_FOUND As Boolean = True   ' Œ©‚Â‚©‚ç‚È‚¯‚ê‚Î‘I‘ğƒ_ƒCƒAƒƒO
Private Const MAX_PARENT_HOPS As Long = 8             ' ‰½ŠK‘w‚Ü‚Åe‚ğ’Tõ‚·‚é‚©
Private Const TARGET_SHEET_NAME As String = "•ª’S—\’è•\(ˆÄ)"  ' o—Í‘ÎÛƒV[ƒg–¼
Private Const START_ANCHOR As String = "C5"           ' 28“ú•\‚ÌŠJn—ñ‚ÌŠî€ƒZƒ‹
' =========================================================================

' ---- ŠÈˆÕƒLƒƒƒbƒVƒ…i“¯ˆêƒuƒbƒN“à‚Å‚ÌÄ“Ç‚ğÈ‚­j -----------------------
Private mCachePath As String
Private mCacheSet As Collection
' -------------------------------------------------------------------------

' ===================== ŒöŠJƒGƒ“ƒgƒŠƒ|ƒCƒ“ƒg ===============================

' 28“ú•\FŠJn“úi“ú—jj‚ğ“ü—Í‚µ‚ÄAj“ú{T––‚ÉF‚ğ•t‚¯‚é
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

    ' ‚‘¬‰»‚Ì‚½‚ß‚Ìˆêİ’è‘Ş”ğ
    Dim prevCalc As XlCalculation
    Dim prevScr As Boolean, prevEvt As Boolean
    prevScr = Application.ScreenUpdating
    prevEvt = Application.EnableEvents
    prevCalc = Application.Calculation
    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    ' ‘ÎÛƒV[ƒg‚ÆŠJn—ñiC5 ‚Ì—ñj
    Set ws = ThisWorkbook.Worksheets(TARGET_SHEET_NAME)
    baseCol = ws.Range(START_ANCHOR).Column

    ' “ü—ÍFŠJn“úi•K‚¸“ú—j“új\ Šù’è‚Í’¼‹ß‚Ì“ú—j“ú
    Dim defaultStart As Date
    defaultStart = Date - (Weekday(Date, vbSunday) - 1) ' ¡“ú‚ª“ú—j‚È‚ç“–“ú
    userInput = InputBox( _
        "ŠJn“úi•K‚¸“ú—j“új‚ğ yyyy/mm/dd Œ`®‚Å“ü—Í‚µ‚Ä‚­‚¾‚³‚¢B", _
        "ŠJn“ú“ü—Í", _
        Format(defaultStart, "yyyy/mm/dd") _
    )
    If userInput = "" Then GoTo CleanExit

    If Not IsDate(userInput) Then
        MsgBox "—LŒø‚È“ú•t‚ğ“ü—Í‚µ‚Ä‚­‚¾‚³‚¢B", vbExclamation
        GoTo CleanExit
    End If

    startDate = CDate(userInput)
    If Weekday(startDate, vbSunday) <> vbSunday Then
        MsgBox "ŠJn“ú‚Í“ú—j“ú‚Å‚ ‚é•K—v‚ª‚ ‚è‚Ü‚·B", vbExclamation
        GoTo CleanExit
    End If
    endDate = startDate + 27

    ' ¥¥ j“úƒZƒbƒgiCSV‚©‚çEƒIƒtƒ‰ƒCƒ“j ¥¥
    Set holSet = Posms_FetchHolidaySetFromCSV()
    If holSet Is Nothing Then
        MsgBox "j“úCSV‚Ì“Ç‚İ‚İ‚É¸”s‚µ‚Ü‚µ‚½BT––‚Ì‚İF•t‚¯‚µ‚Ü‚·B", vbExclamation
    ElseIf Year(startDate) < 2020 Or Year(endDate) > 2050 Then
        MsgBox "’ˆÓ: j“úCSV‚Í 2020`2050 ”N‚Å‚·B”ÍˆÍŠO‚Ìj“ú‚Í–¢”»’è‚É‚È‚è‚Ü‚·B", vbInformation
    End If
    ' ££ ‚±‚±‚Ü‚Å ££

    ' •\‰Šú‰»iF/ƒwƒbƒ_/“ú•tƒZƒ‹j
    ws.Range(ws.Cells(3, baseCol), ws.Cells(22, baseCol + 27)).Interior.Pattern = xlNone
    ws.Range(ws.Cells(3, baseCol), ws.Cells(3, baseCol + 27)).ClearContents
    ws.Range(ws.Cells(5, baseCol), ws.Cells(5, baseCol + 27)).ClearContents
    ws.Range(ws.Cells(22, baseCol), ws.Cells(22, baseCol + 27)).ClearContents

    ' ƒwƒbƒ_‰Šú•\¦‚Æ•\¦Œ`®
    currentMonth = Month(startDate)
    ws.Cells(3, baseCol).Value = currentMonth & "Œ"
    ws.Range(ws.Cells(5, baseCol), ws.Cells(5, baseCol + 27)).NumberFormat = "0"
    ws.Range(ws.Cells(22, baseCol), ws.Cells(22, baseCol + 27)).NumberFormat = "0"

    ' 28“ú•ª‚Ì•`‰æ
    For i = 0 To 27
        d = startDate + i
        thisMonth = Month(d)
        tgtCol = baseCol + i

        ' ã’i/‰º’i‚Ì“ú•t
        ws.Cells(5, tgtCol).Value = Day(d)
        ws.Cells(22, tgtCol).Value = Day(d)

        ' Œƒwƒbƒ_‚ÌØ‚è‘Ö‚¦
        If thisMonth <> currentMonth Then
            ws.Cells(3, tgtCol).Value = thisMonth & "Œ"
            currentMonth = thisMonth
        End If

        ' F•t‚¯ij“ú or T––j
        isWeekend = (Weekday(d, vbMonday) >= 6) ' “y“ú
        isHoliday = Posms_IsHolidayBySet(holSet, d)

        If isHoliday Or isWeekend Then
            With ws.Range(ws.Cells(3, tgtCol), ws.Cells(22, tgtCol)).Interior
                .Pattern = xlSolid
                If isHoliday And isWeekend Then
                    .Color = RGB(255, 220, 230)   ' j“ú‚©‚ÂT––
                ElseIf isHoliday Then
                    .Color = RGB(255, 235, 240)   ' j“ú
                Else
                    .Color = RGB(230, 230, 230)   ' T––
                End If
            End With
        End If
    Next i

    ' ŠúŠÔƒZƒ‹
    ws.Range("V1").NumberFormat = "yyyy”NmŒd“ú": ws.Range("V1").Value = startDate
    ws.Range("AA1").NumberFormat = "yyyy”NmŒd“ú": ws.Range("AA1").Value = endDate

    MsgBox "28“ú•\‚ğXV‚µ‚Ü‚µ‚½B", vbInformation

CleanExit:
    ' İ’è‚ğŒ³‚É–ß‚·
    Application.ScreenUpdating = prevScr
    Application.EnableEvents = prevEvt
    Application.Calculation = prevCalc
    Exit Sub

ErrHandler:
    MsgBox "ƒGƒ‰[‚ª”­¶‚µ‚Ü‚µ‚½: " & Err.Description, vbExclamation
    Resume CleanExit
End Sub

' ===================== j“úƒZƒbƒgQÆ—pi‚±‚Ìƒ‚ƒWƒ…[ƒ‹“àj ================

' j“úƒZƒbƒg“à”»’èi‚ª¬‚´‚Á‚Ä‚à“ú•t‚ÅƒL[‰»j
Private Function Posms_IsHolidayBySet(ByVal setCol As Collection, ByVal dt As Date) As Boolean
    Dim tmp As Variant
    If setCol Is Nothing Then
        Posms_IsHolidayBySet = False
        Exit Function
    End If
    On Error Resume Next
    tmp = setCol(CStr(CLng(DateValue(dt))))
    Posms_IsHolidayBySet = (Err.Number = 0)
    Err.Clear
    On Error GoTo 0
End Function

' ===================== j“úCSVƒ[ƒ_iposms ƒŠƒ|ƒWƒgƒŠj ===================

' CSV ¨ j“úƒZƒbƒg(Collection; key=CLng(Date), item=True)
Private Function Posms_FetchHolidaySetFromCSV() As Collection
    Dim path As String
    path = Posms_ResolveHolidayCsvPathInRepo()
    If Len(path) = 0 Then
        Set Posms_FetchHolidaySetFromCSV = Nothing
        Exit Function
    End If

    ' ƒLƒƒƒbƒVƒ…“K—p
    If StrComp(path, mCachePath, vbTextCompare) = 0 Then
        If Not mCacheSet Is Nothing Then
            Set Posms_FetchHolidaySetFromCSV = mCacheSet
            Exit Function
        End If
    End If

    Dim col As New Collection
    Dim fh As Integer: fh = FreeFile
    Dim line As String
    Dim firstLine As Boolean: firstLine = True
    Dim tokens As Variant
    Dim dt As Date

    On Error GoTo Fail
    Open path For Input As #fh
    Do While Not EOF(fh)
        Line Input #fh, line
        If firstLine Then
            line = Posms_RemoveBOM(line)
            firstLine = False
        End If
        line = Trim$(line)
        If Len(line) = 0 Then GoTo ContinueLoop

        tokens = Posms_SplitCsvLine(line)
        If IsArray(tokens) Then
            Dim j As Long
            For j = LBound(tokens) To UBound(tokens)
                If Posms_TryParseDateToken(CStr(tokens(j)), dt) Then
                    On Error Resume Next
                    col.Add True, CStr(CLng(dt)) ' d•¡‚Í–³‹
                    Err.Clear
                    On Error GoTo 0
                    Exit For
                End If
            Next j
        End If
ContinueLoop:
    Loop
    Close #fh

    Set mCacheSet = col
    mCachePath = path
    Set Posms_FetchHolidaySetFromCSV = col
    Exit Function

Fail:
    On Error Resume Next
    If fh <> 0 Then Close #fh
    Set Posms_FetchHolidaySetFromCSV = Nothing
End Function

' posms/db/init/csv/holidays_jp_2020_2050.csv ‚ğ©“®‰ğŒˆ
Private Function Posms_ResolveHolidayCsvPathInRepo() As String
    Dim base As String: base = ThisWorkbook.path
    Dim candidate As String, p As String
    Dim i As Long

    ' ƒuƒbƒN’¼‰º
    candidate = Posms_JoinPath(Posms_JoinPath(base, POSMS_CSV_DIR), HOLIDAY_CSV_FILE)
    If Posms_FileExists(candidate) Then Posms_ResolveHolidayCsvPathInRepo = candidate: Exit Function

    ' e‚Ö MAX_PARENT_HOPS ŠK‘w‚Ü‚Å
    p = base
    For i = 1 To MAX_PARENT_HOPS
        p = Posms_ParentDir(p)
        If Len(p) = 0 Then Exit For
        candidate = Posms_JoinPath(Posms_JoinPath(p, POSMS_CSV_DIR), HOLIDAY_CSV_FILE)
        If Posms_FileExists(candidate) Then Posms_ResolveHolidayCsvPathInRepo = candidate: Exit Function
    Next i

    ' Œ©‚Â‚©‚ç‚È‚¯‚ê‚Î‘I‘ğ
    If PROMPT_IF_NOT_FOUND Then
        Dim fd As FileDialog
        Set fd = Application.FileDialog(msoFileDialogFilePicker)
        With fd
            .Title = "j“úCSVi" & POSMS_CSV_DIR & "/" & HOLIDAY_CSV_FILE & "j‚ğ‘I‘ğ"
            .AllowMultiSelect = False
            .Filters.Clear
            .Filters.Add "CSV", "*.csv"
            .InitialFileName = Posms_JoinPath(base, HOLIDAY_CSV_FILE)
            If .Show = -1 Then
                Posms_ResolveHolidayCsvPathInRepo = .SelectedItems(1)
            End If
        End With
    End If
End Function

' ===================== •â•iƒpƒX/CSV/“ú•t‰ğÍj ===========================

Private Function Posms_FileExists(ByVal path As String) As Boolean
    On Error Resume Next
    Posms_FileExists = (Len(Dir$(path, vbNormal)) > 0)
    On Error GoTo 0
End Function

Private Function Posms_JoinPath(ByVal a As String, ByVal b As String) As String
    Dim sep As String
    If InStr(a, "/") > 0 Then
        sep = "/"
    Else
        sep = "€"
    End If
    If Len(a) = 0 Then
        Posms_JoinPath = b
    ElseIf Len(b) = 0 Then
        Posms_JoinPath = a
    ElseIf Right$(a, 1) = "€" Or Right$(a, 1) = "/" Then
        Posms_JoinPath = a & b
    Else
        Posms_JoinPath = a & sep & b
    End If
End Function

Private Function Posms_ParentDir(ByVal p As String) As String
    Dim i As Long
    For i = Len(p) To 1 Step -1
        If Mid$(p, i, 1) = "€" Or Mid$(p, i, 1) = "/" Then
            Posms_ParentDir = Left$(p, i - 1)
            Exit Function
        End If
    Next i
    Posms_ParentDir = ""
End Function

' UTF-8 BOM œ‹
Private Function Posms_RemoveBOM(ByVal s As String) As String
    If Len(s) >= 3 Then
        If Left$(s, 3) = Chr$(239) & Chr$(187) & Chr$(191) Then s = Mid$(s, 4)
    End If
    If Len(s) > 0 Then
        If AscW(Left$(s, 1)) = &HFEFF Then s = Mid$(s, 2)
    End If
    Posms_RemoveBOM = s
End Function

' CSV 1s‚ğ "..." •ÛŒì‚µ‚È‚ª‚ç•ªŠ„
Private Function Posms_SplitCsvLine(ByVal s As String) As Variant
    Dim res() As String
    Dim i As Long, ch As String, inQ As Boolean
    Dim cur As String, n As Long
    inQ = False: cur = "": n = 0
    For i = 1 To Len(s)
        ch = Mid$(s, i, 1)
        If ch = """" Then
            If inQ And i < Len(s) And Mid$(s, i + 1, 1) = """" Then
                cur = cur & """": i = i + 1
            Else
                inQ = Not inQ
            End If
        ElseIf ch = "," And Not inQ Then
            ReDim Preserve res(n): res(n) = cur: cur = "": n = n + 1
        Else
            cur = cur & ch
        End If
    Next i
    ReDim Preserve res(n): res(n) = cur
    Posms_SplitCsvLine = res
End Function

' "yyyy-mm-dd"/"yyyy/mm/dd"/"yyyymmdd" ‚ğ Date ‚É
Private Function Posms_TryParseDateToken(ByVal token As String, ByRef outDate As Date) As Boolean
    Dim t As String: t = Trim$(token)
    If Len(t) = 0 Then Exit Function

    If Left$(t, 1) = """" And Right$(t, 1) = """" Then t = Mid$(t, 2, Len(t) - 2)
    t = Trim$(t)

    Dim t2 As String
    t2 = Replace(Replace(t, ".", "/"), "-", "/")
    If InStr(t2, "/") > 0 Then
        On Error Resume Next
        outDate = DateValue(t2)
        If Err.Number = 0 Then Posms_TryParseDateToken = True: Exit Function
        Err.Clear: On Error GoTo 0
    End If

    Dim digits As String
    digits = Replace(Replace(t, "/", ""), "-", "")
    If Len(digits) = 8 Then
        Dim i As Long, ch As String
        For i = 1 To 8
            ch = Mid$(digits, i, 1)
            If ch < "0" Or ch > "9" Then Exit Function
        Next i
        On Error Resume Next
        outDate = DateSerial(CInt(Left$(digits, 4)), CInt(Mid$(digits, 5, 2)), CInt(Right$(digits, 2)))
        If Err.Number = 0 Then Posms_TryParseDateToken = True: Exit Function
        Err.Clear: On Error GoTo 0
    End If
End Function


