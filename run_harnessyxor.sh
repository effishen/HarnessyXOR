#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
log() { printf '[%s] %s\n' "$(date +%s)" "$*"; }
if ! command -v cargo >/dev/null 2>&1; then
    log "ACTION REQUIRED install Rust from https://rustup.rs, then reopen this terminal."
    exit 1
fi
log "BUILD HarnessyXOR: ${script_dir}/Cargo.toml"
if ! CARGO_TERM_COLOR=never CARGO_TERM_PROGRESS_WHEN=never cargo +1.99.1 build --release --locked --manifest-path "${script_dir}/Cargo.toml" --target-dir "${script_dir}/target" 2>&1 | while IFS= read -r line; do log "BUILD ${line}"; done; then
    log "FAILED build: ${script_dir}/Cargo.toml; review the compiler error above."
    exit 1
fi
log "VERIFY built executable: ${script_dir}/target/release/harnessyxor"
log "START HarnessyXOR: ${script_dir}/target/release/harnessyxor"
exec "${script_dir}/target/release/harnessyxor" "$@"
