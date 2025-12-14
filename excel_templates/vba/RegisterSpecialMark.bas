Attribute VB_Name = "RegisterSpecialMark"
Option Explicit

' ==== このモジュール専用の設定 ====
Private Const SM_DST_SHEET As String = "分担予定表(案)"
Private Const SM_START_ROW As Long = 23          ' 上段開始（2行で1名）
Private Const SM_END_ROW   As Long = 122
Private Const SM_COL_NAME  As Long = 2           ' B列：氏名（上段）
Private Const SM_COL_FIRST As Long = 3           ' C列：開始日
Private Const SM_COL_LAST  As Long = 30          ' AD列：最終日
Private Const SM_LABEL_HK  As String = "廃休"
Private Const SM_LABEL_MC  As String = "マル超"

'---------------------------------------------
' ボタン：社員セル → 日付列 → 区分(1=廃休/2=マル超)
' ※5行目が“日だけ”でも、V1(開始日)+列オフセットで実日付を復元
' ※セルの値は変えず、色のみで示す（必要なら値反映の行をコメント解除）
'---------------------------------------------
Public Sub RegisterSpecialMark()
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets(SM_DST_SHEET)
    If ws Is Nothing Then Exit Sub

    ' 開始日(V1)
    Dim startVal As Variant: startVal = ws.Range("V1").Value
    If Not IsDate(startVal) Then
        MsgBox "開始日(V1)を設定してください。", vbExclamation
        Exit Sub
    End If
    Dim startDate As Date: startDate = CDate(startVal)

    ' 1) 社員セル（上段/下段いずれでも可）
    Dim selEmp As Range
    On Error Resume Next
    Set selEmp = Application.InputBox( _
        prompt:="社員セルをクリック（" & SM_START_ROW & "～" & SM_END_ROW & " 行内）", Type:=8)
    On Error GoTo 0
    If selEmp Is Nothing Then Exit Sub
    If selEmp.Worksheet.Name <> SM_DST_SHEET Then Exit Sub
    If selEmp.row < SM_START_ROW Or selEmp.row > SM_END_ROW Then Exit Sub

    ' 2行1名 → 上段行／下段行
    Dim topRow As Long, botRow As Long
    topRow = SM_START_ROW + 2 * Int((selEmp.row - SM_START_ROW) / 2)
    botRow = topRow + 1

    Dim empName As String: empName = Trim$(CStr(ws.Cells(topRow, SM_COL_NAME).Value))
    If empName = "" Then Exit Sub

    ' 2) 対象日の列（C～AD の任意セル）
    Dim selCol As Range
    On Error Resume Next
    Set selCol = Application.InputBox(prompt:="対象日付の列（C～AD）で任意セルをクリック", Type:=8)
    On Error GoTo 0
    If selCol Is Nothing Then Exit Sub
    If selCol.Worksheet.Name <> SM_DST_SHEET Then Exit Sub

    Dim colSel As Long: colSel = selCol.Column
    If colSel < SM_COL_FIRST Or colSel > SM_COL_LAST Then Exit Sub

    ' 実日付（復元）：C列=開始日 → D=+1, E=+2, ...
    Dim dt As Date: dt = DateAdd("d", colSel - SM_COL_FIRST, startDate)

    ' 3) 区分選択（1=廃休 / 2=マル超）
    Dim choice As Variant, label As String
    choice = Application.InputBox(prompt:="区分: 1=廃休, 2=マル超", Type:=1)
    If VarType(choice) = vbBoolean Then Exit Sub
    Select Case CLng(choice)
        Case 1: label = SM_LABEL_HK
        Case 2: label = SM_LABEL_MC
        Case Else: Exit Sub
    End Select

    ' 4) 下段セルに色を付ける（値は変更しない。必要なら値を入れる行を有効化）
    Dim tgt As Range
    If ws.Cells(botRow, colSel).MergeCells Then
        Set tgt = ws.Cells(botRow, colSel).MergeArea
    Else
        Set tgt = ws.Cells(botRow, colSel)
    End If

    'tgt.Value = label  ' ← 値も入れたい場合はコメント解除
    ApplySpecialColor tgt, label

    MsgBox empName & " / " & Format$(dt, "yyyy-mm-dd") & " を「" & label & "」で 登録しました。", vbInformation
End Sub

'---------------------------------------------
' 色設定
'---------------------------------------------
Private Sub ApplySpecialColor(ByVal rng As Range, ByVal label As String)
    With rng
        .Interior.Pattern = xlSolid
        Select Case label
            Case SM_LABEL_HK
                .Interior.Color = RGB(255, 199, 206)   ' 廃休：薄ピンク
                .Font.Color = RGB(156, 0, 6)
            Case SM_LABEL_MC
                .Interior.Color = RGB(255, 235, 156)   ' マル超：薄黄色
                .Font.Color = RGB(0, 0, 0)
            Case Else
                .Interior.Pattern = xlPatternNone
                .Font.ColorIndex = xlColorIndexAutomatic
        End Select
    End With
End Sub

'---------------------------------------------
' 色をスキャンして CSV 出力（PuLP入力用）
' export_csv/special_marks.csv に「氏名,日付,区分」を書き出す
'---------------------------------------------
Public Sub ExportSpecialMarksFromColorsCsv()
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets(SM_DST_SHEET)
    If ws Is Nothing Then Exit Sub

    Dim baseDir As String: baseDir = ThisWorkbook.path
    If Len(baseDir) = 0 Then Exit Sub
    Dim csvPath As String: csvPath = baseDir & "/export_csv/special_marks.csv"

    Dim f As Integer: f = FreeFile
    Open csvPath For Output As #f
    Print #f, "氏名,日付,区分"

    Dim topRow As Long, nm As String, c As Long, colorVal As Long, kind As String
    Dim d As Date, startDate As Date

    Dim startVal As Variant: startVal = ws.Range("V1").Value
    If Not IsDate(startVal) Then Close #f: Exit Sub
    startDate = CDate(startVal)

    For topRow = SM_START_ROW To SM_END_ROW Step 2
        nm = Trim$(CStr(ws.Cells(topRow, SM_COL_NAME).Value))
        If nm <> "" Then
            For c = SM_COL_FIRST To SM_COL_LAST
                d = DateAdd("d", c - SM_COL_FIRST, startDate)
                colorVal = ws.Cells(topRow + 1, c).Interior.Color

                If colorVal = RGB(255, 199, 206) Then
                    kind = SM_LABEL_HK
                ElseIf colorVal = RGB(255, 235, 156) Then
                    kind = SM_LABEL_MC
                Else
                    kind = ""
                End If

                If kind <> "" Then
                    Print #f, CsvLine_SM(nm, Format$(d, "yyyy-mm-dd"), kind)
                End If
            Next c
        End If
    Next topRow

    Close #f
    MsgBox "特殊指定（色）→ CSV 出力完了：" & vbCrLf & csvPath, vbInformation
End Sub

' ==== CSVユーティリティ ====
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


