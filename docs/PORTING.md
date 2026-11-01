# Historical 0.1 port assessment

This describes the earlier 0.1 alpha. It was superseded by the fresh project-first architecture in 0.2. See [ARCHITECTURE.md](ARCHITECTURE.md) for the current design.

## Earlier baseline

The port starts from HarnessyXO **v0.7.36**, commit [`099d1062d80d5e92bb76196b44af58700d18a13e`](https://github.com/effishen/HarnessyXO/commit/099d1062d80d5e92bb76196b44af58700d18a13e). That baseline contains **179 Python files, 117,639 lines, 81 declared tools and 36 SQLite tables**. Those are source inventory counts, not a measure of how much of the Rust application is finished.

HarnessyXOR **0.1.0-alpha.1** has a native Rust backend and a new browser interface. It is a working subset of the application, with a separate database format and explicit unsupported features. It has no Python compatibility server running behind it. Matching a feature name or route does not establish parity with the original implementation.

## Available now

- Persistent workspaces, projects, parent relationships, chats, JSON memory, search/bookmark endpoints, JSON/Markdown export and archive/lock write controls. Markdown export omits disabled Thinking/tool/statistics sections from the file.
- OpenAI-compatible SSE and Ollama streaming, reasoning/tool events, usage measurements, conservative context trimming, retained saved history and bounded completion checks.
- A shared agent-slot limit, queued/running state, cancellation, tool approvals, interrupted-run recovery and `CONTINUE` retries. A restart does not replay model commands.
- Nine implemented model tools: `list_files`, `read_file`, `write_file`, `pack_zip`, `list_environments`, `run_command`, `read_memory`, `save_memory` and `chat_history`. Their schemas are HarnessyXOR's current API, not compatibility aliases for all 81 original tools.
- Project file browsing, UTF-8 editing, binary upload/download, folders, rename, ZIP packaging/extraction and recoverable trash/restore. Unix file operations hold directory handles and reject symbolic links, hardlinks and unsafe paths.
- Existing Lima VM registration and lifecycle controls. Model commands require an enabled Linux guest, explicit project grants, verified mounts and guest `bwrap` file isolation. No host-shell fallback is used.
- Separate Argon2id login accounts, revocable sessions, request-origin checks and content-free diagnostics assembled from reviewed counts and saved status enums.
- Copy-based legacy import that preserves the original and retains unsupported source state in an inactive snapshot.

## Coverage by subsystem

**Partial** means some useful behavior exists and more work remains. **Missing** means that subsystem has no equivalent product flow yet. No row represents full HarnessyXO parity.

| Source subsystem | Status | Remaining work |
| --- | --- | --- |
| Agent loop, completion and history | Partial | Full original orchestration, completion holds, delegation and history finalization |
| Accounts and credentials | Partial | Account-management UI and original credential-store migration |
| Context, memory and instructions | Partial | Backend token counting/calibration, structured instruction documents and full memory provenance |
| Host/platform support | Partial | Native host validation and provider-specific behavior |
| Database integrity and recovery | Partial | Original generation barriers, quarantine, recovery workers and full schema migration |
| Sanitized diagnostics | Partial | Detailed bounded VM, guest, SSH, storage and model observations |
| PDF and template exports | Missing | Printable PDFs, preview pages, content selection and templates |
| Files, archives and output ownership | Partial | References, broader file operations, original output ownership and history integration |
| Branch, rewind and prompt navigation | Partial | Branches, transactional rewinds, edit/replace and original navigator parity |
| Host runtime prerequisites | Partial | Automatic verified Lima/QEMU/runtime installation and repair |
| Launchers, updates and distribution | Partial | Automatic verified application updates and native release validation |
| Model backends, streaming and telemetry | Partial | Full provider-specific options, oMLX/llama.cpp progress probes and original performance accounting |
| Notifications and remote replies | Missing | Notification providers, remote reply sessions and command handling |
| Package structure | Partial | Complete native distribution and remaining source-resource migration |
| External-target permissions | Missing | External SSH targets and their action/credential policies |
| Project permissions and isolation | Partial | Read-only upstream/reference views, copied deployments and original access/projection controls |
| Scheduling and queue recovery | Partial | Original durable admission/completion holds, richer scheduling and delegated-task limits |
| Storage, queue and settings | Partial | All original workspace/machine records and lossless active migration |
| Browser, research, code and process tools | Missing | Browser automation, web research, code intelligence, Git/process tools and remaining tool families |
| VM desktops and VNC | Missing | Desktop readiness, browser desktops and proxy lifecycle |
| VM lifecycle and snapshots | Partial | Creation, provisioning, rebuild/reset, snapshots, resources and all original providers |
| VM mounts, files and SSH | Partial | Original SSHFS/SFTP file views, mount repair and broader transport recovery |
| OpenWrt and emulated wireless | Missing | MIPS providers, images/packages, Wi-Fi/Bluetooth setup and their diagnostics |
| Windows ISO and unattended setup | Missing | Architecture-specific ISO acquisition, checksums, unattended setup and guest readiness |
| Browser UI interactions | Partial | Full original controls, historical operations, exports and native/visual verification |

The same status map is available in [port-status.json](port-status.json).

## Data and execution boundaries

The Rust database is not a replacement file for `.harnessy/workspace.db`. Use a separate application directory and a new workspace. Legacy import copies supported project/conversation data into the new format and preserves unsupported tables, references, history, VM state and settings for later work. Preservation does not activate those features or establish a supported VM-disk migration. Original login accounts are not imported; create a separate Rust account from the host CLI.

Database snapshots include committed WAL data. Files copied while another application is changing them do not form one transaction with that database snapshot. Symbolic links and special files cause import refusal rather than being silently followed. Review the import result and keep the original until your required workflows have been verified.

Linux VM command isolation currently limits filesystem access to the granted project view and a small guest runtime. It does not reproduce the original network-policy implementation. Windows/OpenWrt commands remain unavailable, and a registered/stopped VM is never described as a verified Ready guest without actual checks.

Diagnostics exports contain no workspace or reference contents, transcript/memory bodies, raw tool arguments/results, credentials or host paths. Export uses saved observations and never starts a VM or contacts an external device.

## Validation and remaining gates

Current checks run on Linux and cover real API requests, persistence and legacy WAL import, file safety, archive/lock behavior, authentication, saved-key handling and synthetic streaming-model/tool/cancellation flows. Synthetic endpoints verify the harness protocol and control flow; they do not establish compatibility with every real model server. Browser visual checks, native macOS/Windows hosts, real Lima/KVM/VZ guests, Windows installation, OpenWrt radios and physical hardware remain unverified.

A replacement release needs complete source-derived workflow coverage, lossless migration/recovery checks and native testing for each advertised host/provider. The current alpha should stay labelled as a partial port until those gates are met.
