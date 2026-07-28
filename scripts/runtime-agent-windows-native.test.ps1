$ErrorActionPreference = "Stop"

if (-not $IsWindows) {
    throw "runtime-agent Windows native acceptance must run on Windows"
}

$ServiceDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$WorkspaceDir = Split-Path $ServiceDir -Parent
$ArtifactDir = if ($env:RUNTIME_AGENT_WINDOWS_ARTIFACT_DIR) {
    $env:RUNTIME_AGENT_WINDOWS_ARTIFACT_DIR
} else {
    Join-Path $ServiceDir "artifacts\runtime-agent-windows"
}
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
$LogPath = Join-Path $ArtifactDir "acceptance.log"

Start-Transcript -Path $LogPath -Force | Out-Null
try {
    Set-Location $ServiceDir

    $RuntimeBinary = Join-Path $ArtifactDir "runtime-agent.exe"
    go build -o $RuntimeBinary ./cmd/runtime-agent
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $RuntimeBinary -PathType Leaf)) {
        throw "native runtime-agent build failed"
    }

    $env:RUNTIME_AGENT_BIN = $RuntimeBinary
    $PrintedCommand = (& (Join-Path $PSScriptRoot "start-runtime-agent.ps1") --print-command -- --help) -join "`n"
    if ($LASTEXITCODE -ne 0) {
        throw "Windows launcher command selection failed"
    }
    if (-not $PrintedCommand.Contains($RuntimeBinary) -or -not $PrintedCommand.Contains("--help")) {
        throw "Windows launcher did not select runtime-agent.exe and preserve --help: $PrintedCommand"
    }

    go test ./cmd/runtime-agent -run TestWorkerIPCListenerUsesLoopbackTCP -count=1 -v
    if ($LASTEXITCODE -ne 0) {
        throw "loopback worker IPC test failed"
    }
    go test ./internal/runtimeagent `
        -run "TestWorkerIPCServerPassesImmutableGenerationAndDisconnectIdentity|TestForgetManagedWorkerPreservesDifferentGenerationWithReusedPID|TestAgentRestartSession|TestTerminalRetryStore" `
        -count=1 -v
    if ($LASTEXITCODE -ne 0) {
        throw "Windows lifecycle tests failed"
    }

    uv sync --frozen --extra dev
    if ($LASTEXITCODE -ne 0) {
        throw "locked Python environment installation failed"
    }
    uv run --frozen --extra dev pytest tests/test_restart_bare_worker_session.py -q
    if ($LASTEXITCODE -ne 0) {
        throw "Windows bare restart helper tests failed"
    }
    $env:HUSHINE_BLOCKED_WORKER_SECONDS = "30"
    $env:HUSHINE_BLOCKED_WORKER_OBSERVE_SECONDS = "5"
    go test -tags=integration ./internal/runtimeagent `
        -run TestBlockedWorkerKeepsRuntimeHeartbeatAndCanBeReplaced `
        -count=1 -timeout 120s -v
    if ($LASTEXITCODE -ne 0) {
        throw "real blocked-worker integration failed on Windows"
    }

    $StrategyServiceSHA = (git -C $ServiceDir rev-parse HEAD).Trim()
    $GolangLibSHA = (git -C (Join-Path $WorkspaceDir "golang-lib") rev-parse HEAD).Trim()
    $StrategyLibrarySHA = (git -C (Join-Path $WorkspaceDir "strategy-library") rev-parse HEAD).Trim()
    @(
        "strategy-service=$StrategyServiceSHA"
        "golang-lib=$GolangLibSHA"
        "strategy-library=$StrategyLibrarySHA"
    ) | Set-Content -Path (Join-Path $ArtifactDir "SHAS.txt") -Encoding utf8
} finally {
    Stop-Transcript | Out-Null
}
