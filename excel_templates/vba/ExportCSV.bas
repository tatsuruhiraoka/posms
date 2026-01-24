Attribute VB_Name = "ExportCsv"
Option Explicit

' ==== ï™íSó\íËï\(àƒ)ÉåÉCÉAÉEÉgÅEÉVÅ[Égñºê›íË ====
Private Const SM_DST_SHEET  As String = "ï™íSó\íËï\(àƒ)"
Private Const SM_START_ROW  As Long = 23
Private Const SM_END_ROW    As Long = 122
Private Const SM_COL_NAME   As Long = 2     ' BóÒÅFéÅñºÅiï\é¶ópÅBCSVÇ…ÇÕégÇÌÇ»Ç¢Åj
Private Const SM_COL_FIRST  As Long = 3     ' CóÒÅFäJénì˙
Private Const SM_COL_LAST   As Long = 30    ' ADóÒÅFç≈èIì˙
Private Const SM_COL_EMPNO  As Long = 31    ' ÅöAEóÒÅFé–àıî‘çÜÅiè„íiçsÇÃÇ›Å^îÒï\é¶êÑèßÅj

Private Const SM_LABEL_HK   As String = "îpãx"
Private Const SM_LABEL_MC   As String = "É}Éãí¥"

Private Const SHEET_LEAVE   As String = "ãxâ…éÌóﬁ"
Private Const SHEET_SPECIAL As String = "ì¡éÍãÊï™"

' ==== èoóÕêÊÉLÉÉÉbÉVÉÖÅiìØàÍé¿çsíÜÇ…ï°êîâÒÉtÉHÉãÉ_ëIëÇ≥ÇπÇ»Ç¢Åj ====
Private mExportCsvDir As String

' =========================================================
'  ëççáÅFPOSMS óp CSV ÇëSïîèoóÕÅiÇ±ÇÃ1ñ{ÇÉ{É^ÉìÇ…äÑÇËìñÇƒÅj
' =========================================================
Public Sub ExportAllPosmsCsv()
    On Error GoTo ErrHandler

    Dim csvDir As String
    csvDir = EnsureExportCsvDir(False)
    If Len(csvDir) = 0 Then Exit Sub

    ExportPosmsCsvForShiftBuilder csvDir     ' É}ÉXÉ^ + shift_meta + leave/special
    ExportPreAssignmentsCsv csvDir           ' è„íi/â∫íi éñëOéwíËÅié–àıî‘çÜÅj
    ExportSpecialMarksFromColorsCsv csvDir   ' îpãx/É}Éãí¥ÅiêFÅjÅié–àıî‘çÜÅj

    MsgBox "Ç∑Ç◊ÇƒÇÃ POSMS óp CSV ÇèoóÕÇµÇ‹ÇµÇΩÅB" & vbCrLf & csvDir, vbInformation
    Exit Sub

ErrHandler:
    MsgBox "àÍäáCSVèoóÕíÜÇ…ÉGÉâÅ[Ç™î≠ê∂ÇµÇ‹ÇµÇΩÅF" & vbCrLf & Err.Description, vbCritical
End Sub

' =========================================================
'  ÉÅÉCÉìÅFÉ}ÉXÉ^ånÅ{shift_metaÅ{ãxâ…éÌóﬁÅ{ì¡éÍãÊï™
' =========================================================
Public Sub ExportPosmsCsvForShiftBuilder(Optional ByVal csvDir As String = "")
    Dim startVal As Variant

    ' ==== äJénì˙É`ÉFÉbÉNÅiï™íSó\íËï\(àƒ) V1Åj====
    startVal = ThisWorkbook.Worksheets(SM_DST_SHEET).Range("V1").Value

    If IsEmpty(startVal) Or IsNull(startVal) Or Trim$(CStr(startVal)) = "" Then
        MsgBox "äJénì˙Ç™ê›íËÇ≥ÇÍÇƒÇ¢Ç‹ÇπÇÒÅB" & vbCrLf & _
               SM_DST_SHEET & " ÇÃ V1 Ç…äJénì˙Çì¸óÕÇµÇƒÇ≠ÇæÇ≥Ç¢ÅB", vbExclamation
        Exit Sub
    End If

    If Not IsDate(startVal) Then
        MsgBox "äJénì˙Ç™ì˙ïtÇ∆ÇµÇƒîFéØÇ≈Ç´Ç‹ÇπÇÒÅB" & vbCrLf & _
               "V1 ÇÃílÇ yyyy/mm/dd Ç»Ç«ÇÃì˙ïtå`éÆÇ…ÇµÇƒÇ≠ÇæÇ≥Ç¢ÅB", vbExclamation
        Exit Sub
    End If

    If Len(csvDir) = 0 Then
        csvDir = EnsureExportCsvDir(False)
        If Len(csvDir) = 0 Then Exit Sub
    End If

    Application.ScreenUpdating = False
    On Error GoTo ErrHandler

    ' ==== äeÉ}ÉXÉ^ Å® CSVÅiUsedRangeÇÇªÇÃÇ‹Ç‹Åj ====
    ExportSheetRangeAsCsv ThisWorkbook.Worksheets("é–àı"), JoinPath(csvDir, "employees.csv")
    ExportSheetRangeAsCsv ThisWorkbook.Worksheets("ãÊèÓïÒ"), JoinPath(csvDir, "zones.csv")
    ExportSheetRangeAsCsv ThisWorkbook.Worksheets("é–àıï é˘óv"), JoinPath(csvDir, "employee_demand.csv")
    ExportSheetRangeAsCsv ThisWorkbook.Worksheets("ê≥é–àıïûñ±ï\"), JoinPath(csvDir, "jobtype_fulltime.csv")
    ExportSheetRangeAsCsv ThisWorkbook.Worksheets("ä˙ä‘åŸópé–àıïûñ±ï\"), JoinPath(csvDir, "jobtype_parttime.csv")

    ' ==== ãxâ…éÌóﬁÅEì¡éÍãÊï™ Å® êÍópÅi1óÒÅj ====
    ExportLeaveCsv ThisWorkbook, JoinPath(csvDir, "leave_types.csv")
    ExportSpecialCsv ThisWorkbook, JoinPath(csvDir, "special_attendance.csv")

    ' ==== äJénì˙ÉÅÉ^ ====
    ExportShiftMetaCsv JoinPath(csvDir, "shift_meta.csv"), startVal

    Application.ScreenUpdating = True
    Exit Sub

ErrHandler:
    Application.ScreenUpdating = True
    MsgBox "CSVèoóÕíÜÇ…ÉGÉâÅ[Ç™î≠ê∂ÇµÇ‹ÇµÇΩÅF" & vbCrLf & Err.Description, vbCritical
End Sub

' =========================================================
'  ÉVÅ[ÉgÇÃ UsedRange ÇÇªÇÃÇ‹Ç‹ CSV Ç…èoóÕ
' =========================================================
Private Sub ExportSheetRangeAsCsv(ByVal ws As Worksheet, ByVal csvPath As String)
    Dim rng As Range
    Dim r As Long, c As Long
    Dim rowCount As Long, colCount As Long
    Dim f As Integer
    Dim line As String
    Dim v As Variant

    Set rng = ws.UsedRange
    rowCount = rng.Rows.Count
    colCount = rng.Columns.Count

    f = FreeFile
    Open csvPath For Output As #f

    For r = 1 To rowCount
        line = ""
        For c = 1 To colCount
            v = rng.Cells(r, c).Value
            If c > 1 Then line = line & ","
            line = line & CsvEscape(v)
        Next c
        Print #f, line
    Next r

    Close #f
End Sub

' =========================================================
'  ãxâ…éÌóﬁÉVÅ[Ég Å® Åu1óÒÇæÇØÅvCSV èoóÕ
' =========================================================
Private Sub ExportLeaveCsv(ByVal wb As Workbook, ByVal csvPath As String)
    Dim ws As Worksheet
    Dim col As Long, lastR As Long, r As Long
    Dim f As Integer
    Dim v As Variant
    Dim added As Long
    Dim defaults As Variant

    defaults = Array("îÒî‘", "èTãx", "èjãx", "åvîN", "îNãx", "âƒä˙", "ì~ä˙", _
                     "ë„ãx", "è≥åá", "éYãx", "àÁãx", "âÓåÏ", "ïaãx", "ãxêE", "ÇªÇÃëº")

    Set ws = SheetByName(wb, SHEET_LEAVE)

    f = FreeFile
    Open csvPath For Output As #f
    Print #f, "leave_name"

    If Not ws Is Nothing Then
        col = FindHeaderColAny(ws, "ãxâ…éÌóﬁñº", "ãxâ…ñº", "leave_name")
        If col > 0 Then
            lastR = LastDataRow(ws, col)
            If lastR > 1 Then
                For r = 2 To lastR
                    v = ws.Cells(r, col).Value
                    If Trim$(CStr(v)) <> "" Then
                        Print #f, CsvEscape(v)
                        added = added + 1
                    End If
                Next r
            End If
        End If
    End If

    If added = 0 Then
        For r = LBound(defaults) To UBound(defaults)
            Print #f, CsvEscape(defaults(r))
        Next r
    End If

    Close #f
End Sub

' =========================================================
'  ì¡éÍãÊï™ÉVÅ[Ég Å® Åu1óÒÇæÇØÅvCSV èoóÕ
' =========================================================
Private Sub ExportSpecialCsv(ByVal wb As Workbook, ByVal csvPath As String)
    Dim ws As Worksheet
    Dim col As Long, lastR As Long, r As Long
    Dim f As Integer
    Dim v As Variant
    Dim added As Long
    Dim defaults As Variant

    defaults = Array("îpãx", "É}Éãí¥")

    Set ws = SheetByName(wb, SHEET_SPECIAL)

    f = FreeFile
    Open csvPath For Output As #f
    Print #f, "attendance_name"

    If Not ws Is Nothing Then
        col = FindHeaderColAny(ws, "ì¡ï ãÊï™ñº", "ãÊï™ñº", "attendance_name")
        If col > 0 Then
            lastR = LastDataRow(ws, col)
            If lastR > 1 Then
                For r = 2 To lastR
                    v = ws.Cells(r, col).Value
                    If Trim$(CStr(v)) <> "" Then
                        Print #f, CsvEscape(v)
                        added = added + 1
                    End If
                Next r
            End If
        End If
    End If

    If added = 0 Then
        For r = LBound(defaults) To UBound(defaults)
            Print #f, CsvEscape(defaults(r))
        Next r
    End If

    Close #f
End Sub

' =========================================================
'  äJénì˙ÇæÇØÇÃ shift_meta.csv ÇèoóÕ
' =========================================================
Private Sub ExportShiftMetaCsv(ByVal csvPath As String, ByVal startVal As Variant)
    Dim f As Integer

    f = FreeFile
    Open csvPath For Output As #f

    Print #f, "start_date"
    Print #f, Format$(CDate(startVal), "yyyy-MM-dd")

    Close #f
End Sub

'---------------------------------------------
' êFÇÉXÉLÉÉÉìÇµÇƒ CSV èoóÕÅiPuLPì¸óÕópÅj
' export_csv/special_marks.csv Ç…Åuemp_no,date,kindÅvÇèëÇ´èoÇ∑
'---------------------------------------------
Public Sub ExportSpecialMarksFromColorsCsv(Optional ByVal csvDir As String = "")
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets(SM_DST_SHEET)
    If ws Is Nothing Then Exit Sub

    If Len(csvDir) = 0 Then
        csvDir = EnsureExportCsvDir(False)
        If Len(csvDir) = 0 Then Exit Sub
    End If
    Dim csvPath As String: csvPath = JoinPath(csvDir, "special_marks.csv")

    Dim startVal As Variant: startVal = ws.Range("V1").Value
    If Not IsDate(startVal) Then
        MsgBox "äJénì˙(V1)Ç™ñ¢ê›íË/ïsê≥Ç≈Ç∑ÅB", vbExclamation
        Exit Sub
    End If
    Dim startDate As Date: startDate = CDate(startVal)

    Dim f As Integer: f = FreeFile
    Open csvPath For Output As #f
    Print #f, "emp_no,date,kind"

    Dim topRow As Long, empNo As String
    Dim c As Long, colorVal As Long, kind As String
    Dim d As Date

    For topRow = SM_START_ROW To SM_END_ROW Step 2
        empNo = Trim$(CStr(ws.Cells(topRow, SM_COL_EMPNO).Value)) ' AEóÒÅiè„íiÅj
        If empNo <> "" Then
            For c = SM_COL_FIRST To SM_COL_LAST
                d = DateAdd("d", c - SM_COL_FIRST, startDate)
                colorVal = ws.Cells(topRow + 1, c).Interior.Color   ' â∫íi

                If colorVal = RGB(255, 199, 206) Then
                    kind = SM_LABEL_HK
                ElseIf colorVal = RGB(255, 235, 156) Then
                    kind = SM_LABEL_MC
                Else
                    kind = ""
                End If

                If kind <> "" Then
                    Print #f, CsvLine_SM(empNo, Format$(d, "yyyy-MM-dd"), kind)
                End If
            Next c
        End If
    Next topRow

    Close #f
End Sub

'---------------------------------------------
' éñëOéwíËÅiè„íiÅ{â∫íiÇÃílÅjÇ CSV Ç…èoóÕ
' export_csv/pre_assignments.csv
'   óÒ: emp_no, date, row_kind, value
'---------------------------------------------
Public Sub ExportPreAssignmentsCsv(Optional ByVal csvDir As String = "")
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets(SM_DST_SHEET)
    If ws Is Nothing Then
        MsgBox "'" & SM_DST_SHEET & "' ÉVÅ[ÉgÇ™å©Ç¬Ç©ÇËÇ‹ÇπÇÒÅB", vbExclamation
        Exit Sub
    End If

    Dim startVal As Variant: startVal = ws.Range("V1").Value
    If Not IsDate(startVal) Then
        MsgBox "äJénì˙(V1)Ç™ñ¢ê›íË/ïsê≥Ç≈Ç∑ÅB", vbExclamation
        Exit Sub
    End If
    Dim startDate As Date: startDate = CDate(startVal)

    If Len(csvDir) = 0 Then
        csvDir = EnsureExportCsvDir(False)
        If Len(csvDir) = 0 Then Exit Sub
    End If
    Dim csvPath As String: csvPath = JoinPath(csvDir, "pre_assignments.csv")

    Dim f As Integer: f = FreeFile
    Open csvPath For Output As #f
    Print #f, "emp_no,date,row_kind,value"

    Dim topRow As Long, botRow As Long, c As Long
    Dim empNo As String, d As Date
    Dim vTop As Variant, vBot As Variant

    For topRow = SM_START_ROW To SM_END_ROW Step 2
        empNo = Trim$(CStr(ws.Cells(topRow, SM_COL_EMPNO).Value)) ' AEóÒÅiè„íiÅj
        If empNo <> "" Then
            botRow = topRow + 1
            For c = SM_COL_FIRST To SM_COL_LAST
                d = startDate + (c - SM_COL_FIRST)

                vTop = ws.Cells(topRow, c).Value
                If Not IsEmpty(vTop) And vTop <> "" Then
                    Print #f, CsvLine_SM(empNo, Format$(d, "yyyy-MM-dd"), "upper", CStr(vTop))
                End If

                vBot = ws.Cells(botRow, c).Value
                If Not IsEmpty(vBot) And vBot <> "" Then
                    Print #f, CsvLine_SM(empNo, Format$(d, "yyyy-MM-dd"), "lower", CStr(vBot))
                End If
            Next c
        End If
    Next topRow

    Close #f
End Sub

' =========================================================
'  èoóÕêÊÉtÉHÉãÉ_åàíËÅià¿íËî≈Åj
'  1) ï€ë∂çœÇ›ÉuÉbÉNÇÃó◊Ç… export_csv ÇçÏÇÈÅiç≈óDêÊÅj
'  2) ÇªÇÍÇ™ñ≥óùÇ»ÇÁÉtÉHÉãÉ_ëIëÅiå†å¿ïtó^ÅjÅ® ÇªÇÃíÜÇ… export_csv ÇçÏÇÈ
' =========================================================
Private Function EnsureExportCsvDir(Optional ByVal forcePick As Boolean = False) As String
    Dim baseDir As String, csvDir As String

    If Not forcePick Then
        If Len(mExportCsvDir) > 0 Then
            If Dir(mExportCsvDir, vbDirectory) <> "" Then
                EnsureExportCsvDir = mExportCsvDir
                Exit Function
            End If
        End If
    End If

    ' --- 1) ÉuÉbÉNó◊Åiï€ë∂çœÇ›Ç»ÇÁç≈ã≠Åj ---
    baseDir = ThisWorkbook.path
    If Len(baseDir) > 0 Then
        csvDir = JoinPath(baseDir, "export_csv")
        If EnsureFolder(csvDir) Then
            mExportCsvDir = csvDir
            EnsureExportCsvDir = csvDir
            Exit Function
        End If
    End If

    ' --- 2) ÉtÉHÉãÉ_ëIëÅiMacÇÃå†å¿ëŒçÙÅj ---
    Dim picked As String
    picked = PickFolder("CSV èoóÕêÊÉtÉHÉãÉ_ÇëIëÇµÇƒÇ≠ÇæÇ≥Ç¢ÅiÇªÇÃíÜÇ… export_csv ÇçÏê¨ÇµÇ‹Ç∑Åj")
    If Len(picked) = 0 Then
        EnsureExportCsvDir = ""
        Exit Function
    End If

    csvDir = JoinPath(picked, "export_csv")
    If Not EnsureFolder(csvDir) Then
        MsgBox "export_csv ÉtÉHÉãÉ_ÇçÏê¨Ç≈Ç´Ç‹ÇπÇÒÇ≈ÇµÇΩÅF" & vbCrLf & csvDir, vbCritical
        EnsureExportCsvDir = ""
        Exit Function
    End If

    mExportCsvDir = csvDir
    EnsureExportCsvDir = csvDir
End Function

Private Function EnsureFolder(ByVal folderPath As String) As Boolean
    On Error GoTo EH
    If Dir(folderPath, vbDirectory) = "" Then
        MkDir folderPath
    End If
    EnsureFolder = True
    Exit Function
EH:
    EnsureFolder = False
End Function

Private Function PickFolder(ByVal title As String) As String
    On Error GoTo EH
    With Application.FileDialog(msoFileDialogFolderPicker)
        .title = title
        .AllowMultiSelect = False
        If .Show = -1 Then
            PickFolder = .SelectedItems(1)
        Else
            PickFolder = ""
        End If
    End With
    Exit Function
EH:
    PickFolder = ""
End Function

Private Function JoinPath(ByVal a As String, ByVal b As String) As String
    a = TrimTrailingSeps(CStr(a))
    b = TrimLeadingSeps(CStr(b))

    Dim sep As String
    sep = DetectSep(a)

    If Len(a) = 0 Then
        JoinPath = b
    ElseIf Len(b) = 0 Then
        JoinPath = a
    Else
        JoinPath = a & sep & b
    End If
End Function

Private Function DetectSep(ByVal p As String) As String
    ' ï∂éöóÒÇÃåXå¸Ç©ÇÁÉZÉpÉåÅ[É^ÇåàÇﬂÇÈÅiMacÇÃ ":" / "/" ç¨ç›ÇîÇØÇÈÅj
    If InStr(1, p, "/", vbBinaryCompare) > 0 Then
        DetectSep = "/"
    ElseIf InStr(1, p, ":", vbBinaryCompare) > 0 Then
        DetectSep = ":"
    Else
        DetectSep = Application.PathSeparator   ' Windows ÇÕÇ±Ç±
    End If
End Function

Private Function TrimTrailingSeps(ByVal p As String) As String
    Do While Len(p) > 1
        Dim ch As String: ch = Right$(p, 1)
        If ch = "Ä" Or ch = "/" Or ch = ":" Then
            p = Left$(p, Len(p) - 1)
        Else
            Exit Do
        End If
    Loop
    TrimTrailingSeps = p
End Function

Private Function TrimLeadingSeps(ByVal p As String) As String
    Do While Len(p) > 0
        Dim ch As String: ch = Left$(p, 1)
        If ch = "Ä" Or ch = "/" Or ch = ":" Then
            p = Mid$(p, 2)
        Else
            Exit Do
        End If
    Loop
    TrimLeadingSeps = p
End Function

' =========================================================
'  CSV ÉGÉXÉPÅ[ÉvÅiÉ}ÉXÉ^ópÅj
' =========================================================
Private Function CsvEscape(ByVal v As Variant) As String
    Dim s As String
    s = CStr(v)
    If InStr(s, """") > 0 Then s = Replace(s, """", """""")
    If InStr(s, ",") > 0 Or InStr(s, vbLf) > 0 Or InStr(s, vbCr) > 0 Then s = """" & s & """"
    CsvEscape = s
End Function

' =========================================================
'  CSV ÉGÉXÉPÅ[ÉvÅispecial/preópÅj
' =========================================================
Private Function CsvLine_SM(ParamArray fields() As Variant) As String
    Dim i As Long, s As String
    For i = LBound(fields) To UBound(fields)
        If i > LBound(fields) Then s = s & ","
        s = s & CsvEscape_SM(fields(i))
    Next
    CsvLine_SM = s
End Function

Private Function CsvEscape_SM(ByVal v As Variant) As String
    Dim s As String: s = CStr(v)
    If InStr(s, """") > 0 Then s = Replace(s, """", """""")
    If InStr(s, ",") > 0 Or InStr(s, vbLf) > 0 Or InStr(s, vbCr) > 0 Then s = """" & s & """"
    CsvEscape_SM = s
End Function

' =========================================================
'  ÉVÅ[ÉgéÊìæ/ÉwÉbÉ_óÒåüèo/ç≈èIçs
' =========================================================
Private Function SheetByName(ByVal wb As Workbook, ByVal nm As String) As Worksheet
    On Error Resume Next
    Set SheetByName = wb.Worksheets(nm)
    On Error GoTo 0
End Function

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

' =========================================================
'  ÅiîCà”Åjé–àıî‘çÜóÒ AE ÇîÒï\é¶/ï\é¶
' =========================================================
Public Sub HideEmpNoColumn()
    On Error Resume Next
    ThisWorkbook.Worksheets(SM_DST_SHEET).Columns(SM_COL_EMPNO).Hidden = True
    On Error GoTo 0
End Sub

Public Sub ShowEmpNoColumn()
    On Error Resume Next
    ThisWorkbook.Worksheets(SM_DST_SHEET).Columns(SM_COL_EMPNO).Hidden = False
    On Error GoTo 0
End Sub




