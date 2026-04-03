[CmdletBinding()]
param(
    [string]$OutputDir = "artifacts/public"
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "common.ps1")

$repoRoot = Get-RepoRoot -ScriptPath $PSCommandPath
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$packageName = "chirpstack-docker-public-$timestamp"
$stagingRoot = New-StagingDirectory -Prefix $packageName
$stagingPackageRoot = Join-Path $stagingRoot $packageName
$outputRoot = Join-Path $repoRoot $OutputDir
$zipPath = Join-Path $outputRoot ($packageName + ".zip")

try {
    New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

    Copy-ProjectSnapshot -SourceRoot $repoRoot -DestinationRoot $stagingPackageRoot

    Push-Location $stagingRoot
    try {
        Compress-Archive -Path $packageName -DestinationPath $zipPath -Force
    }
    finally {
        Pop-Location
    }

    Write-Host "Created public package:"
    Write-Host $zipPath
}
finally {
    Remove-StagingDirectory -Path $stagingRoot
}
