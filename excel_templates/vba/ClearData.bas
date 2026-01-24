Attribute VB_Name = "ClearData"
Option Explicit

Public Sub ClearImportedAndInputData()

    ' ===== Ý’èiƒeƒ“ƒvƒŒ‚É‡‚í‚¹‚Ä’²®j =====
    Const DST_SHEET As String = "•ª’S—\’è•\(ˆÄ)"
    Const DEFAULT_CLEAR_ROW As Long = 122   ' ƒeƒ“ƒvƒŒ‚ÌŽÐˆõƒGƒŠƒA‰º’[iŒÅ’è‚È‚ç‚±‚ê‚ªˆê”ÔˆÀ‘Sj
    Const START_ROW As Long = 23            ' ŽÐˆõã’iŠJŽn
    Const COL_FIRST As String = "A"
    Const COL_LAST  As String = "AE"

    ' “ú•t—ñi‹Î–±“ü—Í—ñj
    Const COL_DATE_FIRST As Long = 3        ' C
    Const COL_DATE_LAST  As Long = 30       ' AD

    ' “ÁŽêƒ}[ƒNi”p‹x/ƒ}ƒ‹’´j‚ð“ü‚ê‚Ä‚¢‚ésF‰º’i
    Const LABEL_ROW_OFFSET As Long = 1      ' ã’i+1 = ‰º’i

    ' i”p‹x/ƒ}ƒ‹’´‚ÌFj¦“o˜^ƒ}ƒNƒ/CSVo—Í‚Æ‡‚í‚¹‚é
    Const COLOR_HK As Long = 13421823       ' RGB(255,199,206)
    Const COLOR_MC As Long = 10284031       ' RGB(255,235,156)

    ' j“ú/T––F•t‚¯‚ª“ü‚é”ÍˆÍiGet28DaysWithMonthHeaders ‚É‡‚í‚¹‚éj
    Const CAL_ROW_TOP As Long = 3
    Const CAL_ROW_BOTTOM As Long = 22
    ' =========================================

    Dim ws As Worksheet
    Dim listWs As Worksheet
    Dim lastRow As Long
    Dim clearTo As Long
    Dim resp As VbMsgBoxResult

    ' ‘Þ”ð
    Dim prevScr As Boolean, prevEvt As Boolean, prevCalc As XlCalculation
    prevScr = Application.ScreenUpdating
    prevEvt = Application.EnableEvents
    prevCalc = Application.Calculation

    resp = MsgBox( _
        "ƒCƒ“ƒ|[ƒgE“ü—Íƒf[ƒ^‚ð‚·‚×‚ÄƒNƒŠƒA‚µ‚Ü‚·B" & vbCrLf & _
        "i–¼•ëE‹Î–±E“ú•tEƒhƒƒbƒvƒ_ƒEƒ“E”p‹x/ƒ}ƒ‹’´Ej“úFj" & vbCrLf & _
        "‚æ‚ë‚µ‚¢‚Å‚·‚©H", _
        vbQuestion + vbYesNo, "ƒNƒŠƒA‚ÌŠm”F")
    If resp <> vbYes Then Exit Sub

    On Error GoTo FINALLY

    Set ws = ThisWorkbook.Worksheets(DST_SHEET)

    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    ' ---- ƒNƒŠƒAÅIs‚ÌŒˆ’èiˆÀ’è—ñ‚Å”»’èj ----
    Dim lastB As Long, lastAE As Long
    lastB = ws.Cells(ws.Rows.Count, "B").End(xlUp).row
    lastAE = ws.Cells(ws.Rows.Count, "AE").End(xlUp).row
    lastRow = Application.WorksheetFunction.Max(lastB, lastAE)

    ' ‰º’i‚Ü‚ÅŠÜ‚ß‚éiã’i‚ª“ü‚Á‚Ä‚¢‚ê‚Î +1j
    If lastRow >= START_ROW Then
        clearTo = lastRow + 1
    Else
        clearTo = START_ROW
    End If

    ' ƒeƒ“ƒvƒŒŒÅ’è‰º’[‚Æ”äŠr‚µ‚Ä‘å‚«‚¢•û‚ðÌ—piŽcƒSƒ~–hŽ~j
    If clearTo < DEFAULT_CLEAR_ROW Then clearTo = DEFAULT_CLEAR_ROW

    With ws
        ' -----------------------------
        ' …@ –¼•ëE“ü—Í—“iA?AEj’l‚¾‚¯ƒNƒŠƒA
        ' -----------------------------
        .Range(COL_FIRST & START_ROW & ":" & COL_LAST & clearTo).ClearContents

        ' -----------------------------
        ' …A ¬‡‹æ•\Ž¦
        ' -----------------------------
        .Range("B7:B14").ClearContents

        ' -----------------------------
        ' …B ƒhƒƒbƒvƒ_ƒEƒ“iC?ADj’lƒNƒŠƒA + ŒŸØíœ
        ' -----------------------------
        With .Range("C" & START_ROW & ":AD" & clearTo)
            .ClearContents
            On Error Resume Next
            .Validation.Delete
            On Error GoTo 0
        End With

        ' -----------------------------
        ' …C ƒwƒbƒ_[E“ú•tŒniƒeƒ“ƒvƒŒŽd—l‚É‡‚í‚¹‚Äj
        ' -----------------------------
        .Range("C5:AD20").ClearContents
        .Range("C22:AD22").ClearContents
        .Range("C3:AD3").ClearContents
        .Range("C6:AD6").ClearContents

        ' Œ‹‡ƒZƒ‹ˆÀ‘SƒNƒŠƒAFV1 / AA1
        ClearCellSafe ws, "V1"
        ClearCellSafe ws, "AA1"

        ' -----------------------------
        ' …D j“úŽæ“¾iGet28DaysWithMonthHeadersj‚ÌuFv‚àÁ‚·
        '   - ’l‚Í‚·‚Å‚É ClearContents Ï‚Ý‚È‚Ì‚ÅA”wŒiF‚¾‚¯ŠmŽÀ‚É—Ž‚Æ‚·
        ' -----------------------------
        .Range(.Cells(CAL_ROW_TOP, COL_DATE_FIRST), .Cells(CAL_ROW_BOTTOM, COL_DATE_LAST)).Interior.Pattern = xlPatternNone

        ' -----------------------------
        ' …E ”p‹x/ƒ}ƒ‹’´‚Ì“o˜^iFj‚à‰ðœi‰º’iƒZƒ‹j
        ' -----------------------------
        Dim r As Long, c As Long
        For r = START_ROW + LABEL_ROW_OFFSET To clearTo Step 2
            For c = COL_DATE_FIRST To COL_DATE_LAST
                Dim tgt As Range
                If .Cells(r, c).MergeCells Then
                    Set tgt = .Cells(r, c).MergeArea
                Else
                    Set tgt = .Cells(r, c)
                End If

                Dim colr As Long
                colr = tgt.Interior.Color

                If colr = COLOR_HK Or colr = COLOR_MC Then
                    tgt.Interior.Pattern = xlPatternNone
                    tgt.Font.ColorIndex = xlColorIndexAutomatic
                    ' ’l‚à“ü‚ê‚Ä‚¢‚é‰^—p‚È‚çŽŸ‚ð—LŒø‰»F
                    ' tgt.ClearContents
                End If
            Next c
        Next r
    End With

    ' -----------------------------
    ' …F Lists ƒV[ƒgi‚ ‚ê‚ÎjF‘Ž®‚ÍŽc‚µ‚Ä’†g‚¾‚¯Á‚·
    ' -----------------------------
    On Error Resume Next
    Set listWs = ThisWorkbook.Worksheets("Lists")
    On Error GoTo 0
    If Not listWs Is Nothing Then
        listWs.UsedRange.ClearContents
        listWs.Visible = xlSheetHidden
    End If

    ' -----------------------------
    ' …G –¼‘O•t‚«”ÍˆÍ‚ÌíœiV‹Œ—¼‘Î‰žj
    ' -----------------------------
    On Error Resume Next
        ThisWorkbook.Names("RegJobs").Delete
        ThisWorkbook.Names("TempJobs").Delete
        ThisWorkbook.Names("LowerChoices").Delete
        ThisWorkbook.Names("CombinedList").Delete
        ThisWorkbook.Names("WorkList").Delete
        ThisWorkbook.Names("LeaveList").Delete
    On Error GoTo 0

    MsgBox "“ü—Íƒf[ƒ^‚ðƒNƒŠƒA‚µ‚Ü‚µ‚½B", vbInformation

FINALLY:
    ' •œ‹ŒiƒGƒ‰[‚Å‚à•K‚¸–ß‚·j
    Application.Calculation = prevCalc
    Application.EnableEvents = prevEvt
    Application.ScreenUpdating = prevScr
End Sub

'==============================================================================
' Œ‹‡ƒZƒ‹‚Å‚àˆÀ‘S‚É ClearContents ‚·‚é
'==============================================================================
Private Sub ClearCellSafe(ByVal ws As Worksheet, ByVal addr As String)
    On Error GoTo EH
    Dim r As Range
    Set r = ws.Range(addr)

    If r.MergeCells Then
        r.MergeArea.ClearContents
    Else
        r.ClearContents
    End If
    Exit Sub
EH:
    ' ‰½‚à‚µ‚È‚¢iƒeƒ“ƒvƒŒ·ˆÙ‚ÅƒAƒhƒŒƒX‚ª–³‚¢“™‚Å‚à—Ž‚Æ‚³‚È‚¢j
End Sub


