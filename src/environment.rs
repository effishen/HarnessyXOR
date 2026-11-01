//! Existing Lima VM registration and guest-only commands. This module does not
//! provision VMs or claim that a running controller proves guest readiness.
use crate::{files, store::Store, AppState};
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{Path as AxPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;

const STORE_KEY: &str = "environments";
const OUTPUT_LIMIT: usize = 2 * 1024 * 1024;
static CONFIG_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static RUNTIME_DIR: OnceLock<PathBuf> = OnceLock::new();
type SshConfig = (PathBuf, String, PathBuf);
type SshConfigCache = Mutex<HashMap<String, SshConfig>>;
type EnvironmentGate = Arc<tokio::sync::RwLock<()>>;
type EnvironmentGateCache = Mutex<HashMap<String, EnvironmentGate>>;
static SSH_CONFIGS: OnceLock<SshConfigCache> = OnceLock::new();
static ENV_GATES: OnceLock<EnvironmentGateCache> = OnceLock::new();

fn environment_gate(id: &str) -> Result<Arc<tokio::sync::RwLock<()>>> {
    let mut gates = ENV_GATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow!("Environment operation lock is unavailable"))?;
    Ok(gates
        .entry(id.into())
        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())))
        .clone())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Environment {
    pub id: String,
    pub name: String,
    pub workspace_id: String,
    pub provider: String,
    pub instance: String,
    pub lima_home: Option<String>,
    pub guest_family: String,
    pub enabled: bool,
    pub allowed_project_ids: Vec<String>,
    pub host_project_root: String,
    pub guest_project_root: String,
}
impl Default for Environment {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            workspace_id: String::new(),
            provider: "lima".into(),
            instance: String::new(),
            lima_home: None,
            guest_family: "linux".into(),
            enabled: false,
            allowed_project_ids: Vec::new(),
            host_project_root: String::new(),
            guest_project_root: String::new(),
        }
    }
}
fn log(action: &str, id: &str) {
    eprintln!(
        "[{}] {}: environment {}",
        chrono::Utc::now().timestamp(),
        action,
        id
    );
}
fn all_environments(store: &Store) -> Result<Vec<Environment>> {
    let value = store.kv_get(STORE_KEY)?.unwrap_or(json!([]));
    serde_json::from_value(value).context("Saved environment settings are invalid")
}
fn get_environment(store: &Store, id: &str) -> Result<Environment> {
    all_environments(store)?
        .into_iter()
        .find(|env| env.id == id)
        .ok_or_else(|| anyhow!("Environment was not found"))
}
pub fn list_environments(store: &Store, project_id: &str) -> Result<Value> {
    let project = store.project(project_id)?;
    let environments=all_environments(store)?.into_iter().filter(|env| env.workspace_id==project.workspace_id && env.enabled && env.allowed_project_ids.iter().any(|id|id==project_id)).map(|env|json!({"id":env.id,"name":env.name,"provider":env.provider,"instance":env.instance,"guest_family":env.guest_family,"guest_project_root":env.guest_project_root,"command_support":env.guest_family=="linux","requires_guest_bubblewrap":true})).collect::<Vec<_>>();
    Ok(json!(environments))
}
fn guest_path_valid(path: &str) -> bool {
    if !path.starts_with('/') || path.len() > 2048 || path.contains(['\0', '\n', '\r', '\\', ':']) {
        return false;
    }
    let parts = Path::new(path).components().collect::<Vec<_>>();
    parts.len() >= 3
        && parts
            .iter()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
}
fn validate_environment(store: &Store, mut env: Environment) -> Result<Environment> {
    if env.id.is_empty() {
        env.id = crate::types::id();
    }
    uuid::Uuid::parse_str(&env.id).context("Environment ID must be a UUID")?;
    if env.name.trim().is_empty() || env.name.len() > 160 {
        bail!("Use an environment name between 1 and 160 characters");
    }
    store.workspace(&env.workspace_id)?;
    if env.provider != "lima" {
        bail!("Only existing Lima VM registration is supported in this version");
    }
    if env.instance.is_empty()
        || env.instance.len() > 63
        || !env
            .instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        || env.instance.starts_with('-')
    {
        bail!("Use a Lima instance name containing letters, numbers, underscores or dashes");
    }
    if !matches!(env.guest_family.as_str(), "linux" | "openwrt" | "windows") {
        bail!("Choose linux, openwrt or windows as the guest family");
    }
    if let Some(home) = &env.lima_home {
        let path = Path::new(home);
        if !path.is_absolute() || !path.is_dir() {
            bail!("Lima home must be an existing absolute host folder");
        }
        env.lima_home = Some(path.canonicalize()?.to_string_lossy().to_string());
    }
    if env.allowed_project_ids.len() > 1 {
        bail!("This version registers one project per environment; separate VM views need separate environments");
    }
    if env.enabled && env.allowed_project_ids.len() != 1 {
        bail!("Select the exact project allowed to use this environment");
    }
    if env.enabled && env.guest_family != "linux" {
        bail!("Model commands currently support isolated Linux guests only; keep this guest disabled for model use");
    }
    if !guest_path_valid(&env.guest_project_root) {
        bail!("Use the absolute guest folder where the owned project is mounted, for example /workspace/Files/Project-ID");
    }
    let host = Path::new(&env.host_project_root);
    if !host.is_absolute() || !host.is_dir() {
        bail!("Host project root must be an existing absolute project folder");
    }
    let canonical_host = host.canonicalize()?;
    for id in &env.allowed_project_ids {
        let project = store.project(id)?;
        if project.workspace_id != env.workspace_id {
            bail!("The selected project belongs to another workspace");
        }
        let project_root = store.project_root(id)?.canonicalize()?;
        if project_root != canonical_host {
            bail!(
                "Host project root must be exactly the selected project's folder, not its parent"
            );
        }
    }
    env.host_project_root = canonical_host.to_string_lossy().to_string();
    Ok(env)
}

fn lima_command(env: &Environment) -> Command {
    let mut command = Command::new("limactl");
    if let Some(home) = &env.lima_home {
        command.env("LIMA_HOME", home);
    }
    command.env("NO_COLOR", "1");
    command.env("TERM", "dumb");
    command.arg("--tty=false");
    command
}

#[derive(Debug)]
struct ProcessOutput {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    truncated: bool,
    stdout_bytes: u64,
    stderr_bytes: u64,
}
async fn bounded_read<R: AsyncRead + Unpin>(
    mut reader: R,
    count: Arc<AtomicU64>,
) -> (String, bool) {
    let mut collected = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 16384];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                count.fetch_add(n as u64, Ordering::Relaxed);
                let remaining = OUTPUT_LIMIT.saturating_sub(collected.len());
                collected.extend_from_slice(&buffer[..n.min(remaining)]);
                if n > remaining {
                    truncated = true;
                }
            }
            Err(_) => break,
        }
    }
    (String::from_utf8_lossy(&collected).to_string(), truncated)
}
async fn run_bounded(
    mut command: Command,
    timeout: Duration,
    cancel: CancellationToken,
    scope: &str,
) -> Result<ProcessOutput> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .context("Required host executable is unavailable")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("Cannot capture process output"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("Cannot capture process errors"))?;
    let stdout_count = Arc::new(AtomicU64::new(0));
    let stderr_count = Arc::new(AtomicU64::new(0));
    let mut out_task = tokio::spawn(bounded_read(stdout, stdout_count.clone()));
    let mut err_task = tokio::spawn(bounded_read(stderr, stderr_count.clone()));
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );
    let status = loop {
        tokio::select! {
            status=child.wait()=>break status.context("Host controller stopped unexpectedly"),
            _=tokio::time::sleep_until(deadline)=>{
                let _=child.kill().await;let _=child.wait().await;
                break Err(anyhow!("Operation timed out"));
            },
            _=cancel.cancelled()=>{
                let _=child.kill().await;let _=child.wait().await;
                break Err(anyhow!("Operation cancelled"));
            },
            _=heartbeat.tick()=>{
                eprintln!("[{}] WAIT environment process: {} elapsed={}s stdout_bytes={} stderr_bytes={}",chrono::Utc::now().timestamp(),scope.escape_debug(),started.elapsed().as_secs(),stdout_count.load(Ordering::Relaxed),stderr_count.load(Ordering::Relaxed));
            }
        }
    };
    // A detached descendant must not keep an HTTP request waiting on a pipe.
    let output = tokio::time::timeout(Duration::from_secs(2), async {
        Ok::<_, anyhow::Error>(((&mut out_task).await?, (&mut err_task).await?))
    })
    .await;
    if output.is_err() {
        out_task.abort();
        err_task.abort();
    }
    let status = status?;
    let ((stdout, out_truncated), (stderr, err_truncated)) =
        output.map_err(|_| anyhow!("Controller output did not close"))??;
    Ok(ProcessOutput {
        code: status.code(),
        stdout,
        stderr,
        truncated: out_truncated || err_truncated,
        stdout_bytes: stdout_count.load(Ordering::Relaxed),
        stderr_bytes: stderr_count.load(Ordering::Relaxed),
    })
}

async fn status_of(env: &Environment) -> Value {
    let mut command = lima_command(env);
    command.args(["list", "--json", &env.instance]);
    let result = run_bounded(
        command,
        Duration::from_secs(15),
        CancellationToken::new(),
        &env.id,
    )
    .await;
    match result {
        Ok(output) if output.code == Some(0) => {
            let row = output
                .stdout
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .find(|row| row["name"].as_str() == Some(env.instance.as_str()));
            if let Some(row) = row {
                let status = match row["status"]
                    .as_str()
                    .unwrap_or("")
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "running" => "running",
                    "stopped" => "stopped",
                    _ => "unavailable",
                };
                json!({"environment_id":env.id,"provider":"lima","instance":env.instance,"status":status,"guest_ready":false,"mount_verified":false,"model_access":env.enabled,"note":"Controller state only. Guest mapping and isolation are verified before each model command."})
            } else {
                json!({"environment_id":env.id,"status":"unavailable","reason":"INSTANCE_NOT_FOUND"})
            }
        }
        Ok(output) => {
            json!({"environment_id":env.id,"status":"unavailable","reason":"CONTROLLER_FAILED","exit_code":output.code})
        }
        Err(_) => {
            json!({"environment_id":env.id,"status":"unavailable","reason":"CONTROLLER_UNAVAILABLE"})
        }
    }
}

fn runtime_dir() -> Result<&'static PathBuf> {
    if let Some(path) = RUNTIME_DIR.get() {
        return Ok(path);
    }
    let path = std::env::temp_dir().join(format!("hxor-ssh-{}", uuid::Uuid::new_v4().simple()));
    eprintln!(
        "[{}] CREATE private SSH runtime folder: {}",
        chrono::Utc::now().timestamp(),
        path.display()
    );
    fs::create_dir(&path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    }
    // A racing initializer owns its directory and can remove the empty loser.
    if RUNTIME_DIR.set(path.clone()).is_err() {
        let _ = fs::remove_dir(&path);
    }
    RUNTIME_DIR
        .get()
        .ok_or_else(|| anyhow!("SSH runtime folder is unavailable"))
}
fn ssh_fingerprint(env: &Environment) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(
        format!(
            "{}|{}|{}",
            env.id,
            env.instance,
            env.lima_home.as_deref().unwrap_or("")
        )
        .as_bytes(),
    );
    hex::encode(digest)[..24].to_string()
}
async fn ssh_config(env: &Environment) -> Result<(PathBuf, String, PathBuf)> {
    let fingerprint = ssh_fingerprint(env);
    if let Some(config) = SSH_CONFIGS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow!("SSH cache unavailable"))?
        .get(&fingerprint)
        .cloned()
    {
        return Ok(config);
    }
    log("READ Lima SSH configuration", &env.id);
    let mut command = lima_command(env);
    command.args(["show-ssh", "--format=config", &env.instance]);
    let output = run_bounded(
        command,
        Duration::from_secs(15),
        CancellationToken::new(),
        &env.id,
    )
    .await?;
    if output.code != Some(0) || output.truncated || output.stdout.len() > 65536 {
        bail!("Cannot read this instance's SSH configuration");
    }
    let alias = output
        .stdout
        .lines()
        .filter_map(|line| line.trim().strip_prefix("Host "))
        .map(str::trim)
        .find(|alias| {
            !alias.is_empty()
                && alias
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
        .ok_or_else(|| anyhow!("Lima returned an unsupported SSH configuration"))?
        .to_string();
    let path = runtime_dir()?.join(format!("{}.config", fingerprint));
    // Each writer stages its own file. The final cache lock decides which one
    // becomes authoritative without exposing half-written configuration.
    let staging = runtime_dir()?.join(format!("{}.tmp", crate::types::id()));
    eprintln!(
        "[{}] WRITE private SSH configuration: {}",
        chrono::Utc::now().timestamp(),
        path.display()
    );
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&staging)?;
    file.write_all(output.stdout.as_bytes())?;
    file.sync_all()?;
    drop(file);
    let mut configs = SSH_CONFIGS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow!("SSH cache unavailable"))?;
    if let Some(existing) = configs.get(&fingerprint) {
        let _ = fs::remove_file(&staging);
        return Ok(existing.clone());
    }
    fs::rename(&staging, &path)?;
    let socket = runtime_dir()?.join(format!("{}.sock", uuid::Uuid::new_v4().simple()));
    configs.insert(fingerprint, (path.clone(), alias.clone(), socket.clone()));
    Ok((path, alias, socket))
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
async fn ssh_command(env: &Environment, script: &str) -> Result<Command> {
    let (config, alias, socket) = ssh_config(env).await?;
    let mut command = Command::new("ssh");
    command
        .arg("-F")
        .arg(config)
        .args([
            "-T",
            "-oBatchMode=yes",
            "-oConnectTimeout=10",
            "-oConnectionAttempts=1",
            "-oServerAliveInterval=15",
            "-oServerAliveCountMax=3",
            "-oControlMaster=auto",
            "-oControlPersist=60",
        ])
        .arg(format!("-oControlPath={}", socket.display()))
        .arg(alias)
        .arg(format!("/bin/sh -c {}", quote(script)));
    Ok(command)
}
fn invalidate_ssh(env: &Environment) {
    if let Some(configs) = SSH_CONFIGS.get() {
        if let Ok(mut guard) = configs.lock() {
            if let Some((path, _, _socket)) = guard.remove(&ssh_fingerprint(env)) {
                let _ = fs::remove_file(path);
            }
        }
    }
}
fn validate_access(store: &Store, project_id: &str, env: &Environment) -> Result<PathBuf> {
    let saved = get_environment(store, &env.id)?;
    if !saved.enabled || !saved.allowed_project_ids.iter().any(|id| id == project_id) {
        bail!("This project is not allowed to use the selected environment");
    }
    if saved.instance != env.instance
        || saved.lima_home != env.lima_home
        || saved.host_project_root != env.host_project_root
        || saved.guest_project_root != env.guest_project_root
    {
        bail!("Environment settings changed; retry the command");
    }
    files::require_writable(store, project_id)?;
    let project = store.project(project_id)?;
    if project.workspace_id != saved.workspace_id {
        bail!("The project and environment belong to different workspaces");
    }
    let root = store.project_root(project_id)?.canonicalize()?;
    if root != Path::new(&saved.host_project_root).canonicalize()? {
        bail!("Project mount settings no longer match this project");
    }
    if saved.allowed_project_ids.len() != 1 {
        bail!("A scoped Linux environment must grant exactly one project");
    }
    Ok(root)
}

struct ProbeFile(PathBuf);
impl Drop for ProbeFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Execute only through the selected Linux VM. A fresh unpredictable nonce
/// verifies the mounted project; bubblewrap then exposes only that project as
/// /workspace, plus read-only guest system directories. No host-shell fallback.
pub async fn execute_command(
    store: &Store,
    project_id: &str,
    environment_id: &str,
    command: &str,
    timeout_secs: u64,
    cancel: CancellationToken,
) -> Result<Value> {
    if command.is_empty() || command.len() > 256 * 1024 || command.contains('\0') {
        bail!("Command is empty or exceeds the supported size");
    }
    let gate = environment_gate(environment_id)?;
    let _running = tokio::select! {guard=gate.read()=>guard,_=cancel.cancelled()=>return Err(anyhow!("Operation cancelled"))};
    let env = get_environment(store, environment_id)?;
    if env.provider != "lima" || env.guest_family != "linux" {
        bail!("Guest command isolation is currently implemented only for existing Linux Lima VMs");
    }
    let host_root = validate_access(store, project_id, &env)?;
    if cancel.is_cancelled() {
        bail!("Operation cancelled");
    }
    let nonce = crate::types::id();
    let probe_name = format!(".harnessyxor-probe-{}", crate::types::id());
    let probe_path = host_root.join(&probe_name);
    eprintln!(
        "[{}] CREATE project mount proof: {}",
        chrono::Utc::now().timestamp(),
        probe_path.display()
    );
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    // The VM's normal user must read this one random nonce through its existing
    // export. It contains no user data or reusable credential.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o644);
    }
    let mut probe = options.open(&probe_path)?;
    probe.write_all(nonce.as_bytes())?;
    probe.sync_all()?;
    drop(probe);
    let _probe_cleanup = ProbeFile(probe_path);
    let guest_probe = format!(
        "{}/{}",
        env.guest_project_root.trim_end_matches('/'),
        probe_name
    );
    let proof_script=format!("command -v bwrap >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1 && command -v setsid >/dev/null 2>&1 || exit 71; test -d {} || exit 72; cat -- {}",quote(&env.guest_project_root),quote(&guest_probe));
    log("VERIFY guest mount and isolation prerequisites", &env.id);
    let proof = run_bounded(
        ssh_command(&env, &proof_script).await?,
        Duration::from_secs(20),
        cancel.clone(),
        &env.id,
    )
    .await?;
    if proof.code == Some(71) {
        bail!("Install bubblewrap, timeout and setsid inside this Linux VM before running model commands");
    }
    if proof.code != Some(0) || proof.stdout != nonce {
        bail!("The selected guest folder does not show this project's current files; no model command was sent");
    }
    validate_access(store, project_id, &env)?;
    let timeout_secs = timeout_secs.clamp(1, 3600);
    let token = uuid::Uuid::new_v4().simple().to_string();
    let pidfile = format!("/tmp/hxor-job-{token}.pid");
    let project = quote(&env.guest_project_root);
    // Only guest binaries/configuration and this project's source enter the
    // namespace. Guest /home, other host exports and References are absent.
    let sandbox=format!("bwrap --unshare-all --share-net --die-with-parent --new-session --ro-bind /usr /usr --ro-bind-try /bin /bin --ro-bind-try /sbin /sbin --ro-bind-try /lib /lib --ro-bind-try /lib64 /lib64 --ro-bind /etc /etc --proc /proc --dev /dev --tmpfs /tmp --dir /workspace --bind {project} /workspace --setenv HOME /tmp --setenv PWD /workspace --chdir /workspace /bin/sh -c {}",quote(command));
    let script=format!("umask 077; job_file={}; trap 'rm -f -- \"$job_file\"' EXIT; setsid env HXOR_JOB_TOKEN={} timeout --signal=TERM --kill-after=5 {}s {} & job_pid=$!; printf '%s' \"$job_pid\" >\"$job_file\"; wait \"$job_pid\"",quote(&pidfile),quote(&token),timeout_secs,sandbox);
    log("CONNECT scoped Linux command", &env.id);
    record_observation(store, &env, "command_running", "NONE", None);
    let process = run_bounded(
        ssh_command(&env, &script).await?,
        Duration::from_secs(timeout_secs + 15),
        cancel.clone(),
        &env.id,
    )
    .await;
    if cancel.is_cancelled() || process.is_err() {
        // A second channel terminates only the process group carrying this
        // controller-generated token. Never replay the user's command.
        let cleanup=format!("f={}; if test -f \"$f\"; then p=$(cat -- \"$f\"); case \"$p\" in ''|*[!0-9]*) exit 1;; esac; if test -r \"/proc/$p/environ\" && tr '\\000' '\\n' <\"/proc/$p/environ\" | grep -Fqx {}; then kill -TERM -- -\"$p\" 2>/dev/null || true; fi; rm -f -- \"$f\"; fi",quote(&pidfile),quote(&format!("HXOR_JOB_TOKEN={token}")));
        if let Ok(cleanup_command) = ssh_command(&env, &cleanup).await {
            let _ = run_bounded(
                cleanup_command,
                Duration::from_secs(10),
                CancellationToken::new(),
                &env.id,
            )
            .await;
        }
    }
    if process.is_err() {
        record_observation(
            store,
            &env,
            "command_failed",
            if cancel.is_cancelled() {
                "CANCELLED"
            } else {
                "TRANSPORT_FAILED"
            },
            None,
        );
    }
    let output = process?;
    record_observation(
        store,
        &env,
        if output.code == Some(0) {
            "command_completed"
        } else {
            "command_failed"
        },
        if output.code == Some(124) {
            "TIMEOUT"
        } else {
            "NONE"
        },
        Some(&output),
    );
    log("VERIFY scoped Linux command exit", &env.id);
    Ok(
        json!({"environment_id":env.id,"exit_code":output.code,"stdout":output.stdout,"stderr":output.stderr,"stdout_bytes":output.stdout_bytes,"stderr_bytes":output.stderr_bytes,"truncated":output.truncated,"timed_out":output.code==Some(124),"working_directory":"/workspace","mount_verified":true,"isolation":"bubblewrap-owned-project"}),
    )
}

fn record_observation(
    store: &Store,
    env: &Environment,
    phase: &str,
    reason: &str,
    output: Option<&ProcessOutput>,
) {
    // Call sites supply only fixed enums and scalar process counters. Tool
    // output and command text are deliberately unavailable to this record.
    let receipt = json!({"phase":phase,"reason":reason,"observed_at":crate::types::now(),"exit_code":output.and_then(|o|o.code),"stdout_bytes":output.map(|o|o.stdout_bytes),"stderr_bytes":output.map(|o|o.stderr_bytes),"truncated":output.map(|o|o.truncated)});
    let _ = store.kv_set(&format!("environment-observation:{}", env.id), &receipt);
}

/// Export saved scalar observations only. This function never starts a
/// controller, contacts a guest, reads project files or exports arbitrary JSON.
pub fn diagnostic_summary(store: &Store) -> Value {
    let Ok(rows) = all_environments(store) else {
        return json!({"available":false});
    };
    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    let mut enabled = 0usize;
    let mut observed = 0usize;
    for env in &rows {
        if env.enabled {
            enabled += 1;
        }
        if let Ok(Some(receipt)) = store.kv_get(&format!("environment-observation:{}", env.id)) {
            let phase = receipt["phase"].as_str().unwrap_or("unknown");
            if matches!(
                phase,
                "running"
                    | "stopped"
                    | "unavailable"
                    | "starting"
                    | "stopping"
                    | "controller_failed"
                    | "command_running"
                    | "command_completed"
                    | "command_failed"
            ) {
                observed += 1;
                *counts.entry(phase.to_string()).or_default() += 1;
            }
        }
    }
    json!({"available":true,"configured_count":rows.len(),"model_enabled_count":enabled,"saved_observation_count":observed,"saved_phases":counts,"collection":"saved scalar counts only; no guest connections or file contents"})
}

#[derive(Default, Deserialize)]
struct ListQuery {
    workspace_id: Option<String>,
}
struct EnvError(anyhow::Error);
impl From<anyhow::Error> for EnvError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}
impl IntoResponse for EnvError {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":self.0.to_string()})),
        )
            .into_response()
    }
}
async fn list_handler(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, EnvError> {
    let rows = all_environments(&state.store)?
        .into_iter()
        .filter(|env| {
            query
                .workspace_id
                .as_deref()
                .map(|id| id == env.workspace_id)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    Ok(Json(json!(rows)))
}
fn save_environment(
    store: &Store,
    env: Environment,
    existing: Option<&str>,
) -> Result<Environment> {
    let _guard = CONFIG_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow!("Environment settings are busy"))?;
    let mut env = env;
    if let Some(id) = existing {
        env.id = id.to_string();
    }
    let env = validate_environment(store, env)?;
    let mut rows = all_environments(store)?;
    if existing.is_some() {
        let previous = rows
            .iter_mut()
            .find(|row| row.id == env.id)
            .ok_or_else(|| anyhow!("Environment was not found"))?;
        invalidate_ssh(previous);
        *previous = env.clone();
    } else {
        if rows.iter().any(|row| row.id == env.id) {
            bail!("An environment already uses this ID");
        }
        rows.push(env.clone());
    }
    log("WRITE environment registration", &env.id);
    store.kv_set(STORE_KEY, &serde_json::to_value(rows)?)?;
    Ok(env)
}
async fn create_handler(
    State(state): State<AppState>,
    Json(env): Json<Environment>,
) -> Result<Json<Environment>, EnvError> {
    Ok(Json(save_environment(&state.store, env, None)?))
}
async fn update_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(env): Json<Environment>,
) -> Result<Json<Environment>, EnvError> {
    Ok(Json(save_environment(&state.store, env, Some(&id))?))
}
async fn status_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Value>, EnvError> {
    let env = get_environment(&state.store, &id)?;
    let status = status_of(&env).await;
    let phase = match status["status"].as_str() {
        Some("running") => "running",
        Some("stopped") => "stopped",
        _ => "unavailable",
    };
    record_observation(&state.store, &env, phase, "NONE", None);
    Ok(Json(status))
}
async fn control(state: AppState, id: String, action: &str) -> Result<Json<Value>, EnvError> {
    let env = get_environment(&state.store, &id)?;
    let before = status_of(&env).await;
    if before["status"] == "unavailable" {
        return Err(anyhow!("The existing Lima instance is unavailable; registration does not create or provision VMs").into());
    }
    if action == "start" && before["status"] == "running" {
        return Ok(Json(before));
    }
    if action == "stop" && before["status"] == "stopped" {
        return Ok(Json(before));
    }
    // Keep a start/stop from racing a model command. Stop never silently
    // terminates work; cancel the run, then retry the explicit human action.
    let gate = environment_gate(&id)?;
    let _operation = gate.try_write().map_err(|_| {
        anyhow!("Cancel active work using this environment before changing its power state")
    })?;
    log(
        if action == "start" {
            "START existing Lima instance"
        } else {
            "STOP existing Lima instance"
        },
        &env.id,
    );
    record_observation(
        &state.store,
        &env,
        if action == "start" {
            "starting"
        } else {
            "stopping"
        },
        "NONE",
        None,
    );
    let mut command = lima_command(&env);
    command.args([action, &env.instance]);
    let result = run_bounded(
        command,
        Duration::from_secs(180),
        CancellationToken::new(),
        &env.id,
    )
    .await?;
    if result.code != Some(0) {
        record_observation(
            &state.store,
            &env,
            "controller_failed",
            "CONTROLLER_FAILED",
            Some(&result),
        );
        return Err(anyhow!(
            "Lima controller operation failed (exit code {:?}); VM disks were not replaced",
            result.code
        )
        .into());
    }
    invalidate_ssh(&env);
    let result = status_of(&env).await;
    let expected = if action == "start" {
        "running"
    } else {
        "stopped"
    };
    if result["status"] != expected {
        return Err(anyhow!(
            "Lima accepted the request, but the expected controller state was not verified"
        )
        .into());
    }
    log("VERIFY Lima controller state", &env.id);
    record_observation(&state.store, &env, expected, "NONE", None);
    Ok(Json(result))
}
async fn start_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Value>, EnvError> {
    control(state, id, "start").await
}
async fn stop_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Value>, EnvError> {
    control(state, id, "stop").await
}
async fn capabilities_handler() -> Json<Value> {
    Json(
        json!({"provider":"lima","existing_instance_registration":true,"controller_status":true,"start_stop":true,"linux_commands":"requires existing verified project mount, exact project grant and guest bubblewrap/timeout/setsid","host_shell_fallback":false,"guest_command_families":["linux"],"automatic_vm_creation":false,"windows_unattended_installation":false,"openwrt_radio_setup":false,"snapshots":false,"browser_desktop":false,"project_ancestors_and_references":false,"network_policy":"Guest networking is retained; configurable egress policy has not been ported.","note":"This Rust implementation does not yet replace HarnessyXO's VM provisioning, desktop, radio, snapshot or multi-project mount controllers."}),
    )
}
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/environments", get(list_handler).post(create_handler))
        .route("/environments/:id", axum::routing::put(update_handler))
        .route("/environments/:id/status", get(status_handler))
        .route("/environments/:id/start", post(start_handler))
        .route("/environments/:id/stop", post(stop_handler))
        .route("/environment-capabilities", get(capabilities_handler))
}
