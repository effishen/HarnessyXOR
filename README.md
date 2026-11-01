# HarnessyXOR

HarnessyXOR is a Rust workspace for long-running projects and their agent teams. Create a workspace, create a project, and talk to its principal architect. The project keeps its source, decisions and regression history while agents work in separate copies.

**0.2.0-alpha.1 is an early foundation**, with a fresh project model. Existing HarnessyXO project and database formats are not a compatibility target.

## Start

Install [Rust through rustup](https://rustup.rs/), a C compiler and Git. The checkout pins Rust 1.99.1 and builds SQLite from source. macOS needs the Xcode command line tools; Windows needs the MSVC C++ build tools and Windows SDK.

Open a terminal in this folder. Linux or macOS:

```sh
./run_harnessyxor.sh account create effi
./run_harnessyxor.sh
```

Windows PowerShell:

```powershell
.\run_harnessyxor.ps1 account create effi
.\run_harnessyxor.ps1
```

The account command uses a hidden password prompt. Open **http://127.0.0.1:6768** and sign in. The default listener is `0.0.0.0:6768`, so the same page is available through this host's LAN address. Accounts are trusted controllers. Sessions have no automatic expiry; sign-out and password reset revoke them.

To build directly:

```sh
cargo build --release --locked
./target/release/harnessyxor account create effi
./target/release/harnessyxor serve --host 127.0.0.1 --port 6768
```

## Work on a project

1. Configure your existing OpenAI-compatible or Ollama model endpoint in Settings. Global Agent Slots limits active agent execution across projects and agents.
2. Create a workspace in a new folder, then create a project. Its principal architect is created automatically. Workspace configuration stays with the workspace.
3. For existing source, upload, extract or edit it during the initial baseline phase, then finalize that baseline before sending the first Architect message. This records an **unverified starting point**, not a passing test result. Empty projects can start directly with the architect.
4. Describe the objective to the architect. It delegates tasks to teammates in the same project. Open Team to inspect their work and read-only conversations. Normal conversation stays with the architect.
5. Use Changes to inspect commits, branches, diffs, proposals, discussions and reviews. Run the active regression catalog on the proposal's integrated candidate. Acceptance stays blocked until the required reviews and evidence match that exact version.

Finalizing the baseline or sending the first Architect message closes ordinary edits to canonical source. Subsequent implementation belongs in private working copies and goes through proposals. Existing uncommitted owner edits are preserved; integration blocks instead of resetting them.

The architect yields its inference slot while waiting. A team can therefore proceed sequentially with one global slot, or overlap runnable work when more slots are available. Pause/resume and cancellation are explicit. A restart does not replay model commands. Use Changes → Download committed source ZIP to export the canonical commit without altering frozen source.

## Project knowledge and tests

Knowledge preserves requirements and accepted, proposed, rejected or superseded decisions. A proposed revision does not hide the accepted design. Retiring an agent preserves its identity and historical work.

Regression catalogs have immutable versions. Retiring an active check requires a reason and keeps its earlier definition and results. New user guidance, requirements, decisions, workspace policy, catalog revisions or canonical commits can stale pending evidence. Browsing status sends no new instruction.

The controller currently executes file existence, required-text, JSON and Git whitespace checks. **Arbitrary command checks and manual/physical checks remain pending without their isolated executor or operator-evidence support.** Model claims and posted JSON cannot establish a pass. There is no host-shell fallback.

## Files and state

Each project owns a dedicated source folder. Working copies, candidates and Git metadata live in the workspace's private controller area. See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for layout and acceptance rules.

| Host | Application data |
| --- | --- |
| Linux | `$XDG_DATA_HOME/harnessyxor` or `~/.local/share/harnessyxor` |
| macOS | `~/Library/Application Support/HarnessyXOR` |
| Windows | `%LOCALAPPDATA%\HarnessyXOR` |

A committed source ZIP contains source files, not the complete project history or account state. Back up the application data directory and the whole workspace, including its private controller area, together.

Use `--data-dir /absolute/path` or `HARNESSYXOR_DATA_DIR` for another application directory, separate from workspace source.

```sh
./run_harnessyxor.sh doctor
./run_harnessyxor.sh account reset-password effi
```

Diagnostics exclude project files, conversations, memory, credentials and raw model/tool payloads. Export reads reviewed counts and saved observations without contacting guests or external devices.

## Current limits

This foundation does not reproduce every HarnessyXO feature. Automatic VM provisioning, complete guest execution for private working copies, physical-test evidence capture, remote Git hosting and native platform validation need further work. A registered VM, model reply or successful compilation is not a passed hardware test.

Automated checks use local synthetic model services where needed. Native macOS/Windows, real model inference, actual guest/hardware success paths and browser rendering require their own verification. The GPLv3 license from the HarnessyXO reference is retained in [LICENSE](LICENSE).
