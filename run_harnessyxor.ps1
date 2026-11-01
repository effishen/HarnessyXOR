[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$AppArguments)
$ErrorActionPreference = 'Stop'
function Write-Action([string]$Message) {
    Write-Host ('[{0}] {1}' -f [DateTimeOffset]::UtcNow.ToUnixTimeSeconds(), $Message)
}
$ProjectRoot = $PSScriptRoot
$Manifest = Join-Path $ProjectRoot 'Cargo.toml'
$Executable = Join-Path $ProjectRoot 'target/release/harnessyxor.exe'
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Action 'ACTION REQUIRED install Rust from https://rustup.rs, then reopen this terminal.'
    exit 1
}
Write-Action "BUILD HarnessyXOR: $Manifest"
$env:CARGO_TERM_COLOR = 'never'
$env:CARGO_TERM_PROGRESS_WHEN = 'never'
& cargo '+1.99.1' build --release --locked --manifest-path $Manifest --target-dir (Join-Path $ProjectRoot 'target') 2>&1 | ForEach-Object { Write-Action "BUILD $_" }
if ($LASTEXITCODE -ne 0) {
    Write-Action "FAILED build: $Manifest; review the compiler error above."
    exit $LASTEXITCODE
}
Write-Action "VERIFY built executable: $Executable"
Write-Action "START HarnessyXOR: $Executable"
& $Executable @AppArguments
exit $LASTEXITCODE
