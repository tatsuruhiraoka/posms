Attribute VB_Name = "SpecialMark"
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

' CSV出力側と合わせる色
Private Const COLOR_HK As Long = 13421823 ' RGB(255,199,206)
Private Const COLOR_MC As Long = 10284031 ' RGB(255,235,156)

'---------------------------------------------
' ボタン：選択セル1つ → 区分を選んで反映
'   1 = 廃休
'   2 = マル超
'   0 = 解除
'---------------------------------------------
Public Sub SetSpecialMark_Select()
    ' --- Application状態を退避 ---
    Dim prevScr As Boolean, prevEvt As Boolean, prevCalc As XlCalculation
    prevScr = Application.ScreenUpdating
    prevEvt = Application.EnableEvents
    prevCalc = Application.Calculation

    On Error GoTo ERR_HANDLER
    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    ' --- 対象シート ---
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = ThisWorkbook.Worksheets(SM_DST_SHEET)
    On Error GoTo ERR_HANDLER
    If ws Is Nothing Then GoTo FINALLY

    ' --- 開始日(V1) ---
    Dim startVal As Variant: startVal = ws.Range("V1").Value
    If Not IsDate(startVal) Then
        MsgBox "開始日(V1)を設定してください。", vbExclamation
        GoTo FINALLY
    End If
    Dim startDate As Date: startDate = CDate(startVal)

    ' --- 選択セル ---
    Dim sel As Range
    Set sel = ActiveCell
    If sel Is Nothing Then GoTo FINALLY
    If sel.Worksheet.Name <> SM_DST_SHEET Then GoTo FINALLY

    ' 行チェック
    If sel.row < SM_START_ROW Or sel.row > SM_END_ROW Then
        MsgBox "社員エリア内のセルを選んでください。", vbExclamation
        GoTo FINALLY
    End If

    ' 列チェック
    If sel.Column < SM_COL_FIRST Or sel.Column > SM_COL_LAST Then
        MsgBox "日付列（C～AD）内のセルを選んでください。", vbExclamation
        GoTo FINALLY
    End If

    ' 上段／下段行
    Dim topRow As Long, botRow As Long
    topRow = SM_START_ROW + 2 * Int((sel.row - SM_START_ROW) / 2)
    botRow = topRow + 1

    Dim empName As String
    empName = Trim$(CStr(ws.Cells(topRow, SM_COL_NAME).Value))
    If empName = "" Then GoTo FINALLY

    ' 実日付
    Dim colSel As Long: colSel = sel.Column
    Dim dt As Date: dt = DateAdd("d", colSel - SM_COL_FIRST, startDate)

    ' --- 区分選択 ---
    Dim choice As Variant
    choice = Application.InputBox( _
        prompt:="区分を選択してください：" & vbCrLf & _
                " 1 = 廃休" & vbCrLf & _
                " 2 = マル超" & vbCrLf & _
                " 0 = 解除", _
        Type:=1)
    If VarType(choice) = vbBoolean Then GoTo FINALLY
    If Not IsNumeric(choice) Then GoTo FINALLY

    Dim label As String
    Select Case CLng(choice)
        Case 1: label = SM_LABEL_HK
        Case 2: label = SM_LABEL_MC
        Case 0: label = ""
        Case Else: GoTo FINALLY
    End Select

    ' --- 下段セル ---
    Dim tgt As Range
    If ws.Cells(botRow, colSel).MergeCells Then
        Set tgt = ws.Cells(botRow, colSel).MergeArea
    Else
        Set tgt = ws.Cells(botRow, colSel)
    End If

    ' 値は変えず、色のみ
    ApplySpecialColor tgt, label

    ' --- 完了メッセージ ---
    If label = "" Then
        MsgBox empName & " / " & Format$(dt, "yyyy-mm-dd") & " を解除しました。", vbInformation
    Else
        MsgBox empName & " / " & Format$(dt, "yyyy-mm-dd") & " を「" & label & "」に設定しました。", vbInformation
    End If

    GoTo FINALLY

ERR_HANDLER:
    MsgBox "エラーが発生しました: " & Err.Description, vbExclamation

FINALLY:
    Application.Calculation = prevCalc
    Application.EnableEvents = prevEvt
    Application.ScreenUpdating = prevScr
End Sub

'---------------------------------------------
' 色設定（label="" のとき解除）
'---------------------------------------------
Private Sub ApplySpecialColor(ByVal rng As Range, ByVal label As String)
    With rng
        If label = "" Then
            .Interior.Pattern = xlPatternNone
            .Font.ColorIndex = xlColorIndexAutomatic
            Exit Sub
        End If

        .Interior.Pattern = xlSolid
        Select Case label
            Case SM_LABEL_HK
                .Interior.Color = COLOR_HK
                .Font.Color = RGB(156, 0, 6)
            Case SM_LABEL_MC
                .Interior.Color = COLOR_MC
                .Font.Color = RGB(0, 0, 0)
        End Select
    End With
End Sub


