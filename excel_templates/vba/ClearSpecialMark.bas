Attribute VB_Name = "ClearSpecialMark"
Option Explicit

'==== 設定（シート名・レイアウト）====
Private Const SM_DST_SHEET As String = "分担予定表(案)" ' 宛先シート名（半角/全角カッコに対応）
Private Const SM_START_ROW As Long = 23                ' 上段開始（2行で1名）
Private Const SM_END_ROW   As Long = 122               ' 最終行（含む）
Private Const SM_COL_NAME  As Long = 2                 ' B列：氏名（上段）
Private Const SM_COL_FIRST As Long = 3                 ' C列：開始日
Private Const SM_COL_LAST  As Long = 30                ' AD列：最終日
Private Const SM_LABEL_HK  As String = "廃休"          ' 薄いピンク
Private Const SM_LABEL_MC  As String = "マル超"        ' 薄い黄色

'==== 参照ユーティリティ ====
' 宛先シートを堅牢に取得（半角/全角カッコ、案なし も試す）
Private Function ResolveDstSheet_SM() As Worksheet
    Dim wb As Workbook: Set wb = ThisWorkbook
    Dim nm1 As String, nm2 As String
    nm1 = SM_DST_SHEET
    nm2 = Replace(Replace(nm1, "(", "（"), ")", "）")
    On Error Resume Next
    Set ResolveDstSheet_SM = wb.Worksheets(nm1)
    If ResolveDstSheet_SM Is Nothing Then Set ResolveDstSheet_SM = wb.Worksheets(nm2)
    If ResolveDstSheet_SM Is Nothing Then Set ResolveDstSheet_SM = wb.Worksheets("分担予定表")
    On Error GoTo 0
End Function

' V1 = 開始日（必須）
Private Function GetStartDate_SM(ws As Worksheet) As Date
    Dim v As Variant: v = ws.Range("V1").Value
    If Not IsDate(v) Then Err.Raise vbObjectError + 5201, , "開始日(V1)が未設定/不正。"
    GetStartDate_SM = CDate(v)
End Function

' 列 → 実日付（C=開始日、D=+1, E=+2, …）
Private Function DateAtColumn_SM(ws As Worksheet, ByVal col As Long) As Date
    If col < SM_COL_FIRST Or col > SM_COL_LAST Then Err.Raise vbObjectError + 5202, , "日付列の範囲外。"
    DateAtColumn_SM = GetStartDate_SM(ws) + (col - SM_COL_FIRST)
End Function

'==== 登録：社員→列→区分（色でマーキング）====
Public Sub RegisterSpecialMark()
    Dim ws As Worksheet: Set ws = ResolveDstSheet_SM()
    If ws Is Nothing Then MsgBox "'" & SM_DST_SHEET & "' が見つかりません。", vbExclamation: Exit Sub

    ' 開始日確認
    On Error GoTo BADDATE
    Dim startDate As Date: startDate = GetStartDate_SM(ws)
    On Error GoTo 0

    ' 1) 社員セル
    Dim selEmp As Range
    On Error Resume Next
    Set selEmp = Application.InputBox( _
        prompt:="社員セルをクリック（" & SM_START_ROW & "～" & SM_END_ROW & " 行）", Type:=8)
    On Error GoTo 0
    If selEmp Is Nothing Then Exit Sub
    If selEmp.Worksheet.Name <> ws.Name Then Exit Sub
    If selEmp.row < SM_START_ROW Or selEmp.row > SM_END_ROW Then Exit Sub

    ' 2行1名 → 上段/下段
    Dim topRow As Long, botRow As Long
    topRow = SM_START_ROW + 2 * Int((selEmp.row - SM_START_ROW) / 2)
    botRow = topRow + 1

    Dim empName As String: empName = Trim$(CStr(ws.Cells(topRow, SM_COL_NAME).Value))
    If empName = "" Then Exit Sub

    ' 2) 対象日の列
    Dim selCol As Range, c As Long
    On Error Resume Next
    Set selCol = Application.InputBox(prompt:="対象日の列（C～AD）で任意セルをクリック", Type:=8)
    On Error GoTo 0
    If selCol Is Nothing Then Exit Sub
    If selCol.Worksheet.Name <> ws.Name Then Exit Sub
    c = selCol.Column
    If c < SM_COL_FIRST Or c > SM_COL_LAST Then Exit Sub

    ' 実日付
    Dim d As Date: d = DateAtColumn_SM(ws, c)

    ' 3) 区分選択（1=廃休 / 2=マル超）
    Dim choice As Variant, label As String
    choice = Application.InputBox(prompt:="区分: 1=廃休, 2=マル超", Type:=1)
    If VarType(choice) = vbBoolean Then Exit Sub
    Select Case CLng(choice)
        Case 1: label = SM_LABEL_HK
        Case 2: label = SM_LABEL_MC
        Case Else: Exit Sub
    End Select

    ' 4) 下段セルに色を適用（値は変更しない。必要なら下行のコメント解除）
    Dim tgt As Range
    If ws.Cells(botRow, c).MergeCells Then
        Set tgt = ws.Cells(botRow, c).MergeArea
    Else
        Set tgt = ws.Cells(botRow, c)
    End If
    'tgt.Value = label
    ApplySpecialColor tgt, label

    MsgBox empName & " / " & Format$(d, "yyyy-MM-dd") & " を「" & label & "」でマーキングしました。", vbInformation
    Exit Sub

BADDATE:
    MsgBox "開始日(V1)が未設定/不正です。", vbExclamation
End Sub

'==== 削除：社員→列（該当色ならクリア）====
Public Sub ClearSpecialMark()
    Dim ws As Worksheet: Set ws = ResolveDstSheet_SM()
    If ws Is Nothing Then MsgBox "'" & SM_DST_SHEET & "' が見つかりません。", vbExclamation: Exit Sub

    ' 1) 社員セル
    Dim selEmp As Range
    On Error Resume Next
    Set selEmp = Application.InputBox( _
        prompt:="社員セルをクリック（" & SM_START_ROW & "～" & SM_END_ROW & " 行）", Type:=8)
    On Error GoTo 0
    If selEmp Is Nothing Then Exit Sub
    If selEmp.Worksheet.Name <> ws.Name Then Exit Sub
    If selEmp.row < SM_START_ROW Or selEmp.row > SM_END_ROW Then Exit Sub

    ' 2行1名 → 上段/下段
    Dim topRow As Long, botRow As Long
    topRow = SM_START_ROW + 2 * Int((selEmp.row - SM_START_ROW) / 2)
    botRow = topRow + 1

    ' 2) 対象日の列
    Dim selCol As Range, c As Long
    On Error Resume Next
    Set selCol = Application.InputBox(prompt:="対象日付の列（C～AD）で任意セルをクリック", Type:=8)
    On Error GoTo 0
    If selCol Is Nothing Then Exit Sub
    If selCol.Worksheet.Name <> ws.Name Then Exit Sub
    c = selCol.Column
    If c < SM_COL_FIRST Or c > SM_COL_LAST Then Exit Sub

    ' 3) 下段セルの色が廃休(ピンク) or マル超(黄)ならクリア
    Dim tgt As Range, colVal As Long
    If ws.Cells(botRow, c).MergeCells Then
        Set tgt = ws.Cells(botRow, c).MergeArea
    Else
        Set tgt = ws.Cells(botRow, c)
    End If

    colVal = tgt.Interior.Color
    If colVal = RGB(255, 199, 206) Or colVal = RGB(255, 235, 156) Then
        tgt.Interior.Pattern = xlNone
        tgt.Font.ColorIndex = xlColorIndexAutomatic
        ' 値も消す運用なら次行を有効化
        'If tgt.Value = SM_LABEL_HK Or tgt.Value = SM_LABEL_MC Then tgt.ClearContents
        MsgBox " 登録を削除しました。", vbInformation
    Else
        MsgBox "そのセルは（廃休/マル超）ではありません。", vbInformation
    End If
End Sub

'==== 色→CSV 出力（export_csv/special_marks.csv）====
Public Sub ExportSpecialMarksFromColorsCsv()
    Dim ws As Worksheet: Set ws = ResolveDstSheet_SM()
    If ws Is Nothing Then MsgBox "'" & SM_DST_SHEET & "' が見つかりません。", vbExclamation: Exit Sub

    Dim baseDir As String: baseDir = ThisWorkbook.path
    If Len(baseDir) = 0 Then MsgBox "ブックを保存してください。", vbExclamation: Exit Sub
    Dim csvDir As String: csvDir = baseDir & "/export_csv"
    If Dir(csvDir, vbDirectory) = "" Then MsgBox "export_csv フォルダがありません。", vbExclamation: Exit Sub

    Dim f As Integer: f = FreeFile
    Dim csvPath As String: csvPath = csvDir & "/special_marks.csv"
    Open csvPath For Output As #f
    Print #f, "氏名,日付,区分"

    Dim startDate As Date
    On Error GoTo BADDATE
    startDate = GetStartDate_SM(ws)
    On Error GoTo 0

    Dim topRow As Long, nm As String, c As Long, colorVal As Long, kind As String, d As Date
    For topRow = SM_START_ROW To SM_END_ROW Step 2
        nm = Trim$(CStr(ws.Cells(topRow, SM_COL_NAME).Value))
        If nm <> "" Then
            For c = SM_COL_FIRST To SM_COL_LAST
                d = startDate + (c - SM_COL_FIRST)
                colorVal = ws.Cells(topRow + 1, c).Interior.Color
                If colorVal = RGB(255, 199, 206) Then
                    kind = SM_LABEL_HK
                ElseIf colorVal = RGB(255, 235, 156) Then
                    kind = SM_LABEL_MC
                Else
                    kind = ""
                End If
                If kind <> "" Then
                    Print #f, CsvLine_SM(nm, Format$(d, "yyyy-MM-dd"), kind)
                End If
            Next c
        End If
    Next topRow

    Close #f
    MsgBox "CSV 出力完了：" & vbCrLf & csvPath, vbInformation
    Exit Sub

BADDATE:
    On Error Resume Next: Close #f
    MsgBox "開始日(V1)が未設定/不正です。", vbExclamation
End Sub

'==== CSVユーティリティ ====
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


