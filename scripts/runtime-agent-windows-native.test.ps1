$ErrorActionPreference = "Stop"

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw "runtime-agent Windows native acceptance must run on Windows"
}

function Resolve-UvExecutable {
    $Configured = if (-not [string]::IsNullOrWhiteSpace($env:UV_BIN)) {
        $env:UV_BIN.Trim()
    } elseif (-not [string]::IsNullOrWhiteSpace($env:UV)) {
        $env:UV.Trim()
    } else {
        $null
    }

    if ($null -ne $Configured) {
        $Command = Get-Command -Name $Configured -CommandType Application -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -ne $Command) {
            return $Command.Path
        }
        if (Test-Path -LiteralPath $Configured -PathType Leaf) {
            return (Resolve-Path -LiteralPath $Configured).Path
        }
        throw "configured uv executable was not found: $Configured"
    }

    $Command = Get-Command -Name "uv" -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -ne $Command) {
        return $Command.Path
    }
    $ProfileHome = if (-not [string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
        $env:USERPROFILE
    } else {
        $HOME
    }
    foreach ($Name in @("uv.exe", "uv")) {
        $Candidate = Join-Path $ProfileHome ".local\bin\$Name"
        if (Test-Path -LiteralPath $Candidate -PathType Leaf) {
            return (Resolve-Path -LiteralPath $Candidate).Path
        }
    }
    throw "uv is required (set UV_BIN/UV or install it in PATH or .local\bin)"
}

$ServiceDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$WorkspaceDir = Split-Path $ServiceDir -Parent
$UvExecutable = Resolve-UvExecutable
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

    & $UvExecutable sync --frozen --extra dev
    if ($LASTEXITCODE -ne 0) {
        throw "locked Python environment installation failed"
    }
    & $UvExecutable run --frozen --extra dev pytest tests/test_restart_bare_worker_session.py -q
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
