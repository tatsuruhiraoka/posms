Attribute VB_Name = "EnsureButtons"
Option Explicit

'==============================================================================
' POSMS Buttons Self-Repair（完成版）
' - Workbook_Open から Posms_EnsureButtons を呼ぶ前提
' - ボタンの「表示文字（キャプション）」で探して OnAction を修復
' - OnAction は必ずラッパーを指す（ファイル名が変わっても壊れない）
'==============================================================================

Private Const UI_SHEET As String = "分担予定表(案)"

'-----------------------------
' ボタン表示文字（完全一致）
'-----------------------------
Private Const CAP_DATE  As String = "日付を取得"
Private Const CAP_TEAM  As String = "班データ取得"
Private Const CAP_CLEAR As String = "オールクリア"
Private Const CAP_SHIFT As String = "シフト作成"

' 廃休・マル超（改行あり）
Private Const CAP_SPECIAL_LF   As String = "廃休・マル超" & vbLf & "登録/解除"
Private Const CAP_SPECIAL_CRLF As String = "廃休・マル超" & vbCrLf & "登録/解除"

'==============================================================================
' 起動時に呼ぶ：すべてのボタン割当を修復
'==============================================================================
Public Sub Posms_EnsureButtons()
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = ThisWorkbook.Worksheets(UI_SHEET)
    On Error GoTo 0
    If ws Is Nothing Then Exit Sub

    ' 見つかったボタンはすべて修復
    FixButtonsOnActionByCaption ws, CAP_DATE, "Run_Get28Days_Button"
    FixButtonsOnActionByCaption ws, CAP_TEAM, "Run_FetchTeamData_Button"
    FixButtonsOnActionByCaption ws, CAP_CLEAR, "Run_ClearAll_Button"
    FixButtonsOnActionByCaption ws, CAP_SHIFT, "Run_BuildShift_Button"

    ' 廃休・マル超（改行の揺れを吸収）
    FixButtonsOnActionByCaption ws, CAP_SPECIAL_LF, "Run_Special_Button"
    FixButtonsOnActionByCaption ws, CAP_SPECIAL_CRLF, "Run_Special_Button"
End Sub

'==============================================================================
' キャプション一致のボタンをすべて修復
'==============================================================================
Private Sub FixButtonsOnActionByCaption(ByVal ws As Worksheet, ByVal caption As String, ByVal onActionMacro As String)
    Dim b As Button
    For Each b In ws.Buttons
        If Posms_ButtonCaption(b) = caption Then
            b.OnAction = onActionMacro
        End If
    Next b
End Sub

' ボタンの表示文字取得（Caption / Characters.Text 両対応）
Private Function Posms_ButtonCaption(ByVal b As Button) As String
    Dim s As String

    On Error Resume Next
    s = CStr(b.caption)
    On Error GoTo 0

    If Len(Trim$(s)) = 0 Then
        On Error Resume Next
        s = CStr(b.Characters.text)
        On Error GoTo 0
    End If

    Posms_ButtonCaption = Trim$(s)
End Function

'==============================================================================
' ラッパー（ボタンは必ずこれを呼ぶ）
'==============================================================================

Public Sub Run_Get28Days_Button()
    Posms_RunSafe "Get28DaysWithMonthHeaders"
End Sub

Public Sub Run_FetchTeamData_Button()
    Posms_RunSafe "ImportScheduleAndSetupLists"
End Sub

Public Sub Run_ClearAll_Button()
    Posms_RunSafe "ClearImportedAndInputData"
End Sub

Public Sub Run_Special_Button()
    ' 廃休・マル超（登録/解除）
    Posms_RunSafe "SetSpecialMark_Select"
End Sub

Public Sub Run_BuildShift_Button()
    ' シフト作成 → CSV出力
    Posms_RunSafe "ExportAllPosmsCsv"
End Sub

'==============================================================================
' 安全実行：Excel状態を必ず復旧し、エラーを見える化
'==============================================================================
Public Sub Posms_RunSafe(ByVal procName As String)
    Dim prevScr As Boolean, prevEvt As Boolean, prevCalc As XlCalculation
    prevScr = Application.ScreenUpdating
    prevEvt = Application.EnableEvents
    prevCalc = Application.Calculation

    On Error GoTo EH

    Application.ScreenUpdating = False
    Application.EnableEvents = False
    Application.Calculation = xlCalculationManual

    Application.Run procName

Clean:
    Application.Calculation = prevCalc
    Application.EnableEvents = prevEvt
    Application.ScreenUpdating = prevScr
    Exit Sub

EH:
    MsgBox "エラー: " & Err.Number & vbCrLf & _
           Err.Description & vbCrLf & _
           "場所: " & procName, vbExclamation
    Resume Clean
End Sub


