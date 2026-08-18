param(
    [ValidateSet("dev", "uat", "prod")]
    [string]$Environment = "prod"
)

$envFile = if ($env:SELF_HEALTHY_KAFKA_ENV_FILE) {
    $env:SELF_HEALTHY_KAFKA_ENV_FILE
} else {
    "env/$Environment.env"
}

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Missing environment file: $envFile"
}

$env:APP_ENV = $Environment
$env:SELF_HEALTHY_KAFKA_ENV_FILE = $envFile
$projectRoot = (Resolve-Path "$PSScriptRoot/..").Path
$libPath = Join-Path $projectRoot "lib/python"
$srcPath = Join-Path $projectRoot "src"
$env:PYTHONPATH = "$libPath;$srcPath;$env:PYTHONPATH"
python -m self_healthy_kafka.main
exit $LASTEXITCODE
