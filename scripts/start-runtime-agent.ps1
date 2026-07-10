$ErrorActionPreference = "Stop"

$ServiceDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PrintCommand = $false
$RemainingArgs = @()

foreach ($arg in $args) {
    if ($arg -eq "--print-command") {
        $PrintCommand = $true
        continue
    }
    if ($arg -eq "--") {
        continue
    }
    $RemainingArgs += $arg
}

$archName = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($archName) {
    "X64" { $RuntimeArch = "amd64" }
    "Arm64" { $RuntimeArch = "arm64" }
    default { throw "unsupported architecture: $archName" }
}

$DistDir = if ($env:RUNTIME_AGENT_DIST_DIR) { $env:RUNTIME_AGENT_DIST_DIR } else { Join-Path $ServiceDir "dist\runtime-agent" }
$BinDir = if ($env:RUNTIME_AGENT_BIN_DIR) { $env:RUNTIME_AGENT_BIN_DIR } else { Join-Path $ServiceDir "bin" }
$BinaryName = "runtime-agent.exe"
$PlatformName = "windows-$RuntimeArch"
$SupportedPlatforms = @("windows-amd64", "windows-arm64")
if ($PlatformName -notin $SupportedPlatforms) {
    throw "unsupported platform: $PlatformName"
}

$Candidates = @()
if ($env:RUNTIME_AGENT_BIN) {
    $Candidates += $env:RUNTIME_AGENT_BIN
}
$Candidates += Join-Path (Join-Path $DistDir $PlatformName) $BinaryName
$Candidates += Join-Path $BinDir $BinaryName

$Command = @()
foreach ($candidate in $Candidates) {
    if (Test-Path -Path $candidate -PathType Leaf) {
        $Command = @($candidate)
        break
    }
}

if ($Command.Count -eq 0) {
    $allowGoRun = $env:RUNTIME_AGENT_ALLOW_GO_RUN
    if ($allowGoRun -in @("1", "true", "yes")) {
        $Command = @("go", "run", "./cmd/runtime-agent")
    } else {
        Write-Error "runtime-agent binary not found for $PlatformName. Run 'make build' for local development, or package release binaries with scripts/build-runtime-agent-release.sh."
        exit 1
    }
}

Set-Location $ServiceDir
if ($PrintCommand) {
    Write-Output (($Command + $RemainingArgs) -join " ")
    exit 0
}

$Exe = $Command[0]
$CommandArgs = @()
if ($Command.Count -gt 1) {
    $CommandArgs += $Command[1..($Command.Count - 1)]
}
$CommandArgs += $RemainingArgs
& $Exe @CommandArgs
exit $LASTEXITCODE
