<#
    정비 작업 래퍼 — 로거를 멈추거나 재시작할 때 반드시 이것을 거친다.

    왜 필요한가:
      2026-10-20 에 라이브 DB 를 이전하려고 로거를 수동으로 멈췄는데, 그 사실이
      어디에도 기록되지 않았다. 나중에 데이터를 보니 6초 공백이 남아 있었고,
      스타링크 끊김인지 정비인지 가리는 데 계층별 대조까지 동원해야 했다.
      Windows 작업 스케줄러 Operational 로그와 프로세스 감사는 기본 비활성이라
      OS 로그로도 확인되지 않는다.

      그래서 중지·시작 시각을 직접 남긴다. 이 로그가 가용성 통계에서 제외할
      구간의 1차 근거가 된다.

    사용법:
      .\maintenance.ps1 stop    -Reason "라이브 DB 이전"
      .\maintenance.ps1 start   -Reason "라이브 DB 이전 완료"
      .\maintenance.ps1 restart -Reason "설정 변경 반영"
      .\maintenance.ps1 status

    기록 위치: probe\log\maintenance.log
    정비 이력 문서: docs\latency-probe\maintenance-log.md (여기에 사람이 정리)
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("stop", "start", "restart", "status")]
    [string]$Action,

    [string]$Reason = "",

    # 대상 작업. 기본은 둘 다 — 한쪽만 멈춰도 그쪽 기록에 공백이 생긴다.
    [string[]]$Tasks = @("LoRaProbeResponder", "LoRaE2ELogger")
)

$ErrorActionPreference = "Continue"
$base    = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir  = Join-Path $base "log"
$logFile = Join-Path $logDir "maintenance.log"
New-Item -ItemType Directory -Force $logDir | Out-Null

function Write-Log([string]$msg) {
    # UTC 도 같이 남긴다 — 측정 데이터의 타임스탬프 비교가 UTC 기준이다
    $now = Get-Date
    $line = "{0} KST / {1} UTC  {2}" -f `
        $now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
        $now.ToUniversalTime().ToString("HH:mm:ss.fff"),
        $msg
    Add-Content -Path $logFile -Value $line -Encoding utf8
    Write-Output "  $line"
}

function Get-LoggerPids {
    @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match 'probe_server|e2e_logger' } |
        ForEach-Object { $_.ProcessId })
}

function Show-Status {
    $pids = Get-LoggerPids
    Write-Output "  실행 중인 로거 PID: $(if ($pids) { $pids -join ', ' } else { '없음' })"
    foreach ($t in $Tasks) {
        $i = Get-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue
        if ($i) { Write-Output "  $t : $($i.State)" }
        else    { Write-Output "  $t : 등록 없음" }
    }
    Get-ChildItem (Join-Path $base "data") -Filter *.db -ErrorAction SilentlyContinue |
        ForEach-Object {
            $age = [int]((Get-Date) - $_.LastWriteTime).TotalSeconds
            $warn = if ($age -gt 60) { "  <-- 낡음! 스냅샷이 막혔을 수 있다" } else { "" }
            Write-Output ("  스냅샷 " + $_.Name + " : " + $age + "초 전$warn")
        }
}

function Stop-Loggers {
    Write-Log "STOP 시작 — $Reason"
    foreach ($t in $Tasks) { Stop-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 2
    $pids = Get-LoggerPids
    foreach ($p in $pids) { Stop-Process -Id $p -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 1
    $left = Get-LoggerPids
    if ($left) { Write-Log "STOP 경고 — 잔존 PID: $($left -join ', ')" }
    else       { Write-Log "STOP 완료 — 이 시각부터 기록 공백 시작" }
}

function Start-Loggers {
    foreach ($t in $Tasks) { Start-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue }
    Start-Sleep -Seconds 5
    $pids = Get-LoggerPids
    if ($pids) { Write-Log "START 완료 — PID $($pids -join ', ') / 기록 공백 종료 — $Reason" }
    else       { Write-Log "START 실패 — 프로세스가 올라오지 않았다! — $Reason" }
}

switch ($Action) {
    "status"  { Show-Status }
    "stop"    { Stop-Loggers;  Show-Status }
    "start"   { Start-Loggers; Show-Status }
    "restart" {
        Stop-Loggers
        Start-Loggers
        Show-Status
        Write-Output ""
        Write-Output "  정비 이력 문서에 공백 구간을 정리할 것:"
        Write-Output "    docs\latency-probe\maintenance-log.md"
    }
}
