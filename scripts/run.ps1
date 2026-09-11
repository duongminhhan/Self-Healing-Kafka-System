[CmdletBinding()]
param(
    [ValidateSet("dev", "uat", "prod")]
    [string]$Environment = "prod",
    [string]$PythonExecutable = $env:SELF_HEALTHY_KAFKA_PYTHON,
    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$envFile = if ($env:SELF_HEALTHY_KAFKA_ENV_FILE) {
    $env:SELF_HEALTHY_KAFKA_ENV_FILE
} else {
    Join-Path "env" "$Environment.env"
}
$absoluteEnvFile = if ([IO.Path]::IsPathRooted($envFile)) {
    $envFile
} else {
    Join-Path $projectRoot $envFile
}

if (-not (Test-Path -LiteralPath $absoluteEnvFile -PathType Leaf)) {
    throw "Missing environment file: $absoluteEnvFile"
}

if (-not $PythonExecutable) {
    $repoPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $repoPython -PathType Leaf) {
        $PythonExecutable = $repoPython
    } else {
        $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
        if (-not $pythonCommand) {
            throw "Python executable not found. Create .venv or pass -PythonExecutable."
        }
        $PythonExecutable = $pythonCommand.Source
    }
}
if (-not (Test-Path -LiteralPath $PythonExecutable -PathType Leaf)) {
    $pythonCommand = Get-Command $PythonExecutable -ErrorAction SilentlyContinue
    if (-not $pythonCommand) {
        throw "Python executable not found: $PythonExecutable"
    }
    $PythonExecutable = $pythonCommand.Source
}

$srcPath = Join-Path $projectRoot "src"
$vendoredPath = Join-Path $projectRoot "lib\python"
$pythonPathParts = @($srcPath, $vendoredPath)
if ($env:PYTHONPATH) {
    $pythonPathParts += $env:PYTHONPATH
}

$env:APP_ENV = $Environment
$env:SELF_HEALTHY_KAFKA_ENV_FILE = $envFile
$env:PYTHONPATH = [string]::Join([IO.Path]::PathSeparator, $pythonPathParts)

$bootstrapScript = Join-Path $projectRoot "scripts\_repo_bootstrap.py"
$resolvedPackage = & $PythonExecutable $bootstrapScript --check
if ($LASTEXITCODE -ne 0) {
    throw "Python package identity check failed for $PythonExecutable"
}
Write-Host "Using package: $resolvedPackage"
if ($CheckOnly) {
    Write-Host "Runtime identity check passed."
    return
}

Push-Location $projectRoot
try {
    & $PythonExecutable $bootstrapScript --run-module self_healthy_kafka.main
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
