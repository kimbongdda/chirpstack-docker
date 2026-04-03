[CmdletBinding()]
param(
    [string]$OutputDir = "artifacts/private"
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

function Get-RunningComposeServices {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot
    )

    Push-Location $RepoRoot
    try {
        $services = @(& docker compose ps --status running --services 2>$null)
        return @($services | Where-Object { $_ })
    }
    finally {
        Pop-Location
    }
}

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

function Export-VolumeFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$VolumeName,
        [Parameter(Mandatory = $true)]
        [string]$FileName,
        [Parameter(Mandatory = $true)]
        [string]$DestinationDir
    )

    New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null

    Push-Location $DestinationDir
    try {
        & docker run --rm `
            -v "${VolumeName}:/from" `
            -v "${DestinationDir}:/to" `
            alpine sh -c "if [ -f /from/$FileName ]; then cp /from/$FileName /to/$FileName; else exit 3; fi"

        if ($LASTEXITCODE -eq 3) {
            Write-Warning "Skipped $FileName because it was not found in volume $VolumeName."
            return
        }

        if ($LASTEXITCODE -ne 0) {
            throw "Failed to export $FileName from volume $VolumeName."
        }
    }
    finally {
        Pop-Location
    }
}

$repoRoot = Get-RepoRoot -ScriptPath $PSCommandPath
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$packageName = "chirpstack-docker-private-$timestamp"
$stagingRoot = New-StagingDirectory -Prefix $packageName
$stagingPackageRoot = Join-Path $stagingRoot $packageName
$migrationDataRoot = Join-Path $stagingPackageRoot "migration-data"
$outputRoot = Join-Path $repoRoot $OutputDir
$zipPath = Join-Path $outputRoot ($packageName + ".zip")
$dashboardEnvPath = Join-Path $repoRoot "dashboard/.env"
$postgresWasRunning = $false

try {
    New-Item -ItemType Directory -Force -Path $outputRoot, $migrationDataRoot | Out-Null

    Copy-ProjectSnapshot -SourceRoot $repoRoot -DestinationRoot $stagingPackageRoot

    if (Test-Path -LiteralPath $dashboardEnvPath) {
        Copy-Item -LiteralPath $dashboardEnvPath -Destination (Join-Path $migrationDataRoot "dashboard.env") -Force
    }
    else {
        Write-Warning "dashboard/.env was not found and will not be included."
    }

    $postgresWasRunning = (Get-RunningComposeServices -RepoRoot $repoRoot) -contains "postgres"

    Push-Location $repoRoot
    try {
        if (-not $postgresWasRunning) {
            & docker compose up -d postgres
            if ($LASTEXITCODE -ne 0) {
                throw "Failed to start postgres for backup."
            }
        }

        Wait-ForPostgres -RepoRoot $repoRoot

        & docker compose exec -T postgres sh -lc "pg_dump -U chirpstack -d chirpstack -f /tmp/chirpstack-backup.sql"
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create postgres dump inside the container."
        }

        $postgresContainerId = (& docker compose ps -q postgres).Trim()
        if (-not $postgresContainerId) {
            throw "Could not determine postgres container id."
        }

        & docker cp "${postgresContainerId}:/tmp/chirpstack-backup.sql" (Join-Path $migrationDataRoot "chirpstack.sql")
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to copy postgres dump from the container."
        }

        & docker compose exec -T postgres rm -f /tmp/chirpstack-backup.sql *> $null
    }
    finally {
        if (-not $postgresWasRunning) {
            & docker compose stop postgres *> $null
        }

        Pop-Location
    }

    Export-VolumeFile -VolumeName $script:DashboardVolumeName -FileName "lora_history.db" -DestinationDir $migrationDataRoot
    Export-VolumeFile -VolumeName $script:RedisVolumeName -FileName "dump.rdb" -DestinationDir $migrationDataRoot

    $manifest = [ordered]@{
        package_type = "private-migration"
        created_at = (Get-Date).ToString("s")
        compose_project_name = $script:ComposeProjectName
        includes = @(
            "project snapshot",
            "migration-data/dashboard.env",
            "migration-data/chirpstack.sql",
            "migration-data/lora_history.db",
            "migration-data/dump.rdb"
        )
    }
    $manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $migrationDataRoot "manifest.json") -Encoding UTF8

    Push-Location $stagingRoot
    try {
        Compress-Archive -Path $packageName -DestinationPath $zipPath -Force
    }
    finally {
        Pop-Location
    }

    Write-Host "Created private migration package:"
    Write-Host $zipPath
}
finally {
    Remove-StagingDirectory -Path $stagingRoot
}
