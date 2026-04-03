Set-StrictMode -Version Latest

$script:ComposeProjectName = "chirpstack-docker"
$script:DashboardVolumeName = "$($script:ComposeProjectName)_dashboarddata"
$script:RedisVolumeName = "$($script:ComposeProjectName)_redisdata"

function Get-RepoRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScriptPath
    )

    return (Resolve-Path (Join-Path (Split-Path -Parent $ScriptPath) "..")).Path
}

function New-StagingDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Prefix
    )

    $path = Join-Path ([System.IO.Path]::GetTempPath()) ("codex-" + $Prefix + "-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force -Path $path | Out-Null
    return $path
}

function Remove-StagingDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
}

function Get-RelativeProjectPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string]$FullPath
    )

    $normalizedRoot = (Resolve-Path $Root).Path
    if (-not $normalizedRoot.EndsWith("\")) {
        $normalizedRoot += "\"
    }

    $rootUri = New-Object System.Uri($normalizedRoot)
    $fileUri = New-Object System.Uri((Resolve-Path $FullPath).Path)
    return [System.Uri]::UnescapeDataString($rootUri.MakeRelativeUri($fileUri).ToString()).Replace("\", "/")
}

function Test-SnapshotPathExcluded {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativePath
    )

    $normalized = $RelativePath.Replace("\", "/")
    $segments = $normalized -split "/"
    $excludedDirectoryNames = @(
        ".git",
        "artifacts",
        "backups",
        "__pycache__",
        ".venv",
        "node_modules"
    )

    foreach ($segment in $segments) {
        if ($excludedDirectoryNames -contains $segment) {
            return $true
        }
    }

    $excludedRelativePaths = @(
        "dashboard/.env",
        "chirpstack.sql",
        "lora_history.db",
        "dump.rdb"
    )

    foreach ($excludedPath in $excludedRelativePaths) {
        if ($normalized -ieq $excludedPath) {
            return $true
        }
    }

    $fileName = [System.IO.Path]::GetFileName($normalized)
    $excludedFilePatterns = @(
        "*.db",
        "*.sqlite",
        "*.sqlite3",
        "*.sql",
        "*.dump",
        "*.sql.gz",
        "*.rdb",
        "*.bak",
        "*.log",
        "*.zip"
    )

    foreach ($pattern in $excludedFilePatterns) {
        if ($fileName -like $pattern) {
            return $true
        }
    }

    return $false
}

function Copy-ProjectSnapshot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceRoot,
        [Parameter(Mandatory = $true)]
        [string]$DestinationRoot
    )

    New-Item -ItemType Directory -Force -Path $DestinationRoot | Out-Null

    $files = Get-ChildItem -LiteralPath $SourceRoot -Recurse -Force -File | Sort-Object FullName

    foreach ($file in $files) {
        $relativePath = Get-RelativeProjectPath -Root $SourceRoot -FullPath $file.FullName
        if (Test-SnapshotPathExcluded -RelativePath $relativePath) {
            continue
        }

        $destinationPath = Join-Path $DestinationRoot $relativePath
        $destinationDir = Split-Path -Parent $destinationPath

        if ($destinationDir) {
            New-Item -ItemType Directory -Force -Path $destinationDir | Out-Null
        }

        Copy-Item -LiteralPath $file.FullName -Destination $destinationPath -Force
    }
}
