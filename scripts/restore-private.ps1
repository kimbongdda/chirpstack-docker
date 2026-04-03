[CmdletBinding()]
param(
    [string]$BackupDir = "migration-data",
    [bool]$ResetVolumes = $true
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

function Wait-ForPostgres {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        Push-Location $RepoRoot
        try {
            & docker compose exec -T postgres pg_isready -U chirpstack -d chirpstack *> $null
            if ($LASTEXITCODE -eq 0) {
                return
            }
        }
        finally {
            Pop-Location
        }

        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for postgres to accept connections."
}

function Restore-VolumeFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$VolumeName,
        [Parameter(Mandatory = $true)]
        [string]$SourceFile
    )

    if (-not (Test-Path -LiteralPath $SourceFile)) {
        Write-Warning "Skipped missing backup file: $SourceFile"
        return
    }

    $sourceDir = Split-Path -Parent $SourceFile
    $fileName = Split-Path -Leaf $SourceFile

    & docker volume create $VolumeName *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create or access volume $VolumeName."
    }

    Push-Location $sourceDir
    try {
        & docker run --rm `
            -v "${VolumeName}:/to" `
            -v "${sourceDir}:/from" `
            alpine sh -c "cp /from/$fileName /to/$fileName"

        if ($LASTEXITCODE -ne 0) {
            throw "Failed to restore $fileName into volume $VolumeName."
        }
    }
    finally {
        Pop-Location
    }
}

$repoRoot = Get-RepoRoot -ScriptPath $PSCommandPath
$backupRoot = if ([System.IO.Path]::IsPathRooted($BackupDir)) {
    $BackupDir
}
else {
    Join-Path $repoRoot $BackupDir
}

$dashboardEnvSource = Join-Path $backupRoot "dashboard.env"
$dashboardEnvTarget = Join-Path $repoRoot "dashboard/.env"
$postgresDumpPath = Join-Path $backupRoot "chirpstack.sql"
$dashboardDbPath = Join-Path $backupRoot "lora_history.db"
$redisDumpPath = Join-Path $backupRoot "dump.rdb"

if (-not (Test-Path -LiteralPath $backupRoot)) {
    throw "Backup directory not found: $backupRoot"
}

if (-not (Test-Path -LiteralPath $postgresDumpPath)) {
    throw "Required file is missing: $postgresDumpPath"
}

Push-Location $repoRoot
try {
    if ($ResetVolumes) {
        & docker compose down -v --remove-orphans
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to stop and reset the existing stack."
        }
    }
    else {
        & docker compose down --remove-orphans
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to stop the existing stack."
        }
    }

    if (Test-Path -LiteralPath $dashboardEnvSource) {
        Copy-Item -LiteralPath $dashboardEnvSource -Destination $dashboardEnvTarget -Force
    }
    else {
        Write-Warning "dashboard.env is missing in the backup package. Existing dashboard/.env will be kept."
    }

    Restore-VolumeFile -VolumeName $script:DashboardVolumeName -SourceFile $dashboardDbPath
    Restore-VolumeFile -VolumeName $script:RedisVolumeName -SourceFile $redisDumpPath

    & docker compose up -d postgres
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start postgres."
    }

    Wait-ForPostgres -RepoRoot $repoRoot

    $postgresContainerId = (& docker compose ps -q postgres).Trim()
    if (-not $postgresContainerId) {
        throw "Could not determine postgres container id."
    }

    & docker cp $postgresDumpPath "${postgresContainerId}:/tmp/chirpstack-restore.sql"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy postgres dump into the container."
    }

    & docker compose exec -T postgres psql -U chirpstack -d chirpstack -f /tmp/chirpstack-restore.sql
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to restore postgres data."
    }

    & docker compose exec -T postgres rm -f /tmp/chirpstack-restore.sql *> $null
    & docker compose up --build -d
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start the full stack after restore."
    }

    Write-Host "Restore completed successfully."
}
finally {
    Pop-Location
}
