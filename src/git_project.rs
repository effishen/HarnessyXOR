//! Controller-owned local Git history, private worker/candidate checkouts and
//! mechanically gated acceptance. No Git operation uses a host shell or network.
use crate::{auth::User, store::Store, AppState};
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{Path as AxPath, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use unicode_normalization::UnicodeNormalization;

const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const MAX_DIFF: usize = 2 * 1024 * 1024;
const MAX_EXPORT: usize = 64 * 1024 * 1024;
const EXPORT_ATTRIBUTES: &[u8] = b"* -export-ignore -export-subst\n";
type ProjectLock = Arc<Mutex<()>>;
static LOCKS: OnceLock<Mutex<HashMap<String, ProjectLock>>> = OnceLock::new();
fn project_lock(id: &str) -> Result<ProjectLock> {
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    Ok(locks
        .entry(id.into())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone())
}
fn fixed_log(action: &str, path: &Path) {
    crate::diagnostics::log(action, Some(path), None);
}
fn valid_sha(sha: &str) -> bool {
    matches!(sha.len(), 40 | 64) && sha.bytes().all(|b| b.is_ascii_hexdigit())
}
fn checked_sha(sha: &str) -> Result<&str> {
    if !valid_sha(sha) {
        bail!("Git returned an invalid object identity");
    }
    Ok(sha)
}
fn valid_id(id: &str) -> Result<()> {
    uuid::Uuid::parse_str(id).context("Git actor or proposal ID must be a UUID")?;
    Ok(())
}
fn private_dir(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if !meta.is_dir() || meta.file_type().is_symlink() {
                bail!("Git private paths cannot be links or special files");
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            fixed_log("CREATE private Git directory", path);
            fs::create_dir(path)?;
        }
        Err(e) => return Err(e.into()),
    };
    if path.canonicalize()? != path {
        bail!("Git private directory identity changed");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
fn read_small(path: &Path) -> Result<Vec<u8>> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        bail!("Git controller metadata cannot be a symbolic link");
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() || file.metadata()?.len() > 65536 {
        bail!("Git controller metadata is invalid");
    }
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        bail!("Git controller metadata is too large");
    }
    Ok(bytes)
}
fn durable_json(path: &Path, value: &Value) -> Result<()> {
    let temp = path.with_file_name(format!(".git-state-{}", crate::types::id()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        File::open(path.parent().unwrap())?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

#[derive(Clone)]
struct Repository {
    project_id: String,
    accepted: PathBuf,
    private: PathBuf,
    git_dir: PathBuf,
    hooks: PathBuf,
    empty_config: PathBuf,
}
struct GitOutput {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr_bytes: u64,
    truncated: bool,
}
impl GitOutput {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).to_string()
    }
    fn success(&self) -> bool {
        self.code == Some(0) && !self.truncated
    }
}
fn capture<R: Read>(
    mut reader: R,
    limit: usize,
    oversized: Arc<AtomicBool>,
) -> (Vec<u8>, u64, bool) {
    let mut data = Vec::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 16384];
    while let Ok(n) = reader.read(&mut buffer) {
        if n == 0 {
            break;
        }
        total = total.saturating_add(n as u64);
        let remain = limit.saturating_sub(data.len());
        data.extend_from_slice(&buffer[..n.min(remain)]);
        if total > limit as u64 {
            oversized.store(true, Ordering::Relaxed);
        }
    }
    let truncated = total > limit as u64;
    (data, total, truncated)
}
impl Repository {
    fn locations(store: &Store, project_id: &str) -> Result<Self> {
        valid_id(project_id)?;
        let project = store.project(project_id)?;
        let accepted = store.project_root(project_id)?;
        let workspace = store.workspace(&project.workspace_id)?;
        let root = PathBuf::from(workspace.path);
        for path in [
            root.join(".harnessyxor"),
            root.join(".harnessyxor/projects"),
            root.join(".harnessyxor/projects").join(project_id),
        ] {
            private_dir(&path)?;
        }
        let private = root.join(".harnessyxor/projects").join(project_id);
        Ok(Self {
            project_id: project_id.into(),
            accepted,
            git_dir: private.join("repository.git"),
            hooks: private.join("empty-hooks"),
            empty_config: private.join("empty.gitconfig"),
            private,
        })
    }
    fn from_worktree(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        let pointer = String::from_utf8(read_small(&root.join(".git"))?)
            .context("Managed worktree pointer is invalid")?;
        let raw = pointer
            .trim()
            .strip_prefix("gitdir: ")
            .ok_or_else(|| anyhow!("This is not a controller-owned Git worktree"))?;
        let git_dir = PathBuf::from(raw).canonicalize()?;
        let common = if git_dir.join("commondir").exists() {
            let rel = String::from_utf8(read_small(&git_dir.join("commondir"))?)?;
            git_dir.join(rel.trim()).canonicalize()?
        } else {
            git_dir.clone()
        };
        let private = common
            .parent()
            .ok_or_else(|| anyhow!("Git repository metadata is invalid"))?
            .to_path_buf();
        let marker: Value =
            serde_json::from_slice(&read_small(&private.join("git-controller.json"))?)?;
        let project_id = marker["project_id"]
            .as_str()
            .ok_or_else(|| anyhow!("Git controller identity is missing"))?
            .to_string();
        valid_id(&project_id)?;
        let accepted = PathBuf::from(
            marker["accepted_root"]
                .as_str()
                .ok_or_else(|| anyhow!("Git controller root is missing"))?,
        );
        if common != private.join("repository.git") || accepted.canonicalize()? != accepted {
            bail!("Git repository identity changed");
        }
        if root != accepted
            && !(root.parent() == Some(private.join("worktrees").as_path())
                || root.parent() == Some(private.join("candidates").as_path()))
        {
            bail!("Worktree is outside the controller-owned project areas");
        }
        let repo = Self {
            project_id,
            accepted,
            git_dir: common,
            hooks: private.join("empty-hooks"),
            empty_config: private.join("empty.gitconfig"),
            private,
        };
        repo.validate_pointer(&root)?;
        Ok(repo)
    }
    fn marker(&self) -> PathBuf {
        self.private.join("git-controller.json")
    }
    fn journal(&self) -> PathBuf {
        self.private.join("integration-journal.json")
    }
    fn git_for_root(&self, root: &Path) -> Result<PathBuf> {
        if root == self.accepted {
            return Ok(self.git_dir.clone());
        }
        let pointer = String::from_utf8(read_small(&root.join(".git"))?)?;
        let raw = pointer
            .trim()
            .strip_prefix("gitdir: ")
            .ok_or_else(|| anyhow!("Managed worktree pointer changed"))?;
        let actual = PathBuf::from(raw).canonicalize()?;
        if !actual.starts_with(self.git_dir.join("worktrees")) {
            bail!("Managed worktree points outside this project repository");
        }
        let common = String::from_utf8(read_small(&actual.join("commondir"))?)?;
        if actual.join(common.trim()).canonicalize()? != self.git_dir {
            bail!("Managed worktree belongs to another repository");
        }
        let back = String::from_utf8(read_small(&actual.join("gitdir"))?)?;
        if Path::new(back.trim()) != root.join(".git") {
            bail!("Managed worktree identity no longer matches its directory");
        }
        Ok(actual)
    }
    fn validate_pointer(&self, root: &Path) -> Result<()> {
        if root.canonicalize()? != root {
            bail!("Git worktree moved or became a symbolic link");
        }
        if root == self.accepted {
            let pointer = String::from_utf8(read_small(&root.join(".git"))?)?;
            if pointer.trim() != format!("gitdir: {}", self.git_dir.display()) {
                bail!("Accepted project Git pointer changed");
            }
        } else {
            self.git_for_root(root)?;
        }
        Ok(())
    }
    fn command(&self, root: Option<&Path>) -> Result<Command> {
        if !read_small(&self.empty_config)?.is_empty()
            || fs::read_dir(&self.hooks)?.next().is_some()
        {
            bail!("Controlled Git configuration or disabled-hook directory was modified");
        }
        let mut command = Command::new("git");
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("GIT_") {
                command.env_remove(key);
            }
        }
        command
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", &self.empty_config)
            .env("GIT_CONFIG_SYSTEM", &self.empty_config)
            .env("GIT_ATTR_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_NO_REPLACE_OBJECTS", "1")
            .env("GIT_PAGER", "cat")
            .env("GIT_AUTHOR_NAME", "HarnessyXOR")
            .env("GIT_AUTHOR_EMAIL", "controller@harnessyxor.invalid")
            .env("GIT_COMMITTER_NAME", "HarnessyXOR")
            .env("GIT_COMMITTER_EMAIL", "controller@harnessyxor.invalid")
            .env("LC_ALL", "C");
        command.arg("--no-pager");
        for setting in [
            format!("core.hooksPath={}", self.hooks.display()),
            format!("core.attributesFile={}", self.empty_config.display()),
            format!("core.excludesFile={}", self.empty_config.display()),
            "core.fsmonitor=false".into(),
            "core.untrackedCache=false".into(),
            "core.autocrlf=false".into(),
            "core.quotePath=true".into(),
            "color.ui=false".into(),
            "commit.gpgsign=false".into(),
            "tag.gpgsign=false".into(),
            "gc.auto=0".into(),
            "maintenance.auto=false".into(),
            "protocol.allow=never".into(),
            "protocol.file.allow=never".into(),
        ] {
            command.arg("-c").arg(setting);
        }
        if let Some(root) = root {
            command
                .arg(format!("--git-dir={}", self.git_for_root(root)?.display()))
                .arg(format!("--work-tree={}", root.display()))
                .current_dir(root);
        } else {
            command.current_dir(&self.private);
        }
        Ok(command)
    }
    fn raw(
        &self,
        root: Option<&Path>,
        args: &[OsString],
        input: Option<&[u8]>,
    ) -> Result<GitOutput> {
        self.raw_limit(root, args, input, MAX_OUTPUT)
    }
    fn raw_limit(
        &self,
        root: Option<&Path>,
        args: &[OsString],
        input: Option<&[u8]>,
        limit: usize,
    ) -> Result<GitOutput> {
        let operation = match args.first().and_then(|arg| arg.to_str()).unwrap_or("") {
            name @ ("init" | "config" | "rev-parse" | "symbolic-ref" | "status" | "add"
            | "write-tree" | "commit-tree" | "update-ref" | "worktree" | "show-ref"
            | "merge-tree" | "merge-base" | "merge" | "diff" | "diff-tree" | "ls-files"
            | "diff-files" | "log" | "for-each-ref" | "ls-tree" | "archive") => name,
            _ => "operation",
        };
        crate::diagnostics::log(
            &format!("START local Git {operation}"),
            Some(root.unwrap_or(&self.private)),
            None,
        );
        let mut command = self.command(root)?;
        command
            .args(args)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .context("Git is not installed or cannot start")?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Cannot capture Git output"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("Cannot capture Git error output"))?;
        let oversized = Arc::new(AtomicBool::new(false));
        let out_flag = oversized.clone();
        let err_flag = oversized.clone();
        let out = std::thread::spawn(move || capture(stdout, limit, out_flag));
        let err = std::thread::spawn(move || capture(stderr, MAX_OUTPUT, err_flag));
        if let Some(bytes) = input {
            if let Some(mut stdin) = child.stdin.take() {
                stdin.write_all(bytes)?;
            }
        }
        let started = Instant::now();
        let mut last_log = 0u64;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if oversized.load(Ordering::Relaxed) {
                let _ = child.kill();
                break child.wait()?;
            }
            let elapsed = started.elapsed().as_secs();
            if elapsed >= 120 {
                let _ = child.kill();
                let _ = child.wait();
                bail!("Git operation timed out; repository and source files were retained");
            }
            if elapsed >= last_log + 15 {
                last_log = elapsed;
                fixed_log("WAIT local Git operation", root.unwrap_or(&self.private));
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let (stdout, _, truncated) = out
            .join()
            .map_err(|_| anyhow!("Git output worker stopped"))?;
        let (_, stderr_bytes, stderr_truncated) = err
            .join()
            .map_err(|_| anyhow!("Git error worker stopped"))?;
        eprintln!("[{}] FINISH local Git {}: exit={:?} elapsed_ms={} stdout_bytes={} stderr_bytes={} truncated={}",chrono::Utc::now().timestamp(),operation,status.code(),started.elapsed().as_millis(),stdout.len(),stderr_bytes,truncated||stderr_truncated);
        Ok(GitOutput {
            code: status.code(),
            stdout,
            stderr_bytes,
            truncated: truncated || stderr_truncated,
        })
    }
    fn check_config(&self) -> Result<()> {
        if !self.git_dir.exists() {
            return Ok(());
        }
        if read_small(&self.git_dir.join("info/attributes"))? != EXPORT_ATTRIBUTES {
            bail!("Controlled Git export attributes changed");
        }
        let args = os_args(["config", "--no-includes", "--file"]);
        let mut args = args;
        args.push(self.git_dir.join("config").into_os_string());
        args.extend(os_args(["--null", "--list"]));
        let output = self.raw(None, &args, None)?;
        if !output.success() {
            bail!("Cannot verify the private Git configuration");
        }
        for row in output
            .stdout
            .split(|b| *b == 0)
            .filter(|row| !row.is_empty())
        {
            let text = std::str::from_utf8(row)?;
            let (key, value) = text.split_once('\n').unwrap_or((text, ""));
            let allowed = matches!(
                key,
                "core.repositoryformatversion"
                    | "core.filemode"
                    | "core.bare"
                    | "core.logallrefupdates"
                    | "core.ignorecase"
                    | "core.precomposeunicode"
                    | "extensions.objectformat"
            ) || (key == "extensions.worktreeconfig" && value == "false")
                || (key == "core.worktree" && Path::new(value) == self.accepted);
            if !allowed {
                bail!("Private Git configuration contains an unsupported setting; hooks, filters, includes and external commands are not allowed");
            }
        }
        Ok(())
    }
    fn run(&self, root: &Path, args: &[&str]) -> Result<GitOutput> {
        self.validate_pointer(root)?;
        self.check_config()?;
        self.raw(Some(root), &os_args(args.iter().copied()), None)
    }
    fn checked(&self, root: &Path, args: &[&str]) -> Result<String> {
        let output = self.run(root, args)?;
        if !output.success() {
            let _ = output.stderr_bytes;
            bail!("Local Git operation failed; source files were retained");
        }
        Ok(output.text())
    }
    fn head(&self, root: &Path) -> Result<String> {
        let head = self.checked(root, &["rev-parse", "--verify", "HEAD"])?;
        checked_sha(head.trim())?;
        Ok(head.trim().into())
    }
    fn tree(&self, root: &Path, commit: &str) -> Result<String> {
        checked_sha(commit)?;
        let tree = self.checked(
            root,
            &["rev-parse", "--verify", &format!("{commit}^{{tree}}")],
        )?;
        checked_sha(tree.trim())?;
        Ok(tree.trim().into())
    }
    fn dirty(&self, root: &Path) -> Result<bool> {
        Ok(!self
            .checked(
                root,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            )?
            .is_empty())
    }
    fn strict_dirty(&self, root: &Path) -> Result<bool> {
        Ok(!self
            .checked(
                root,
                &[
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=all",
                    "--ignored=matching",
                ],
            )?
            .is_empty())
    }
    fn commit_object(
        &self,
        root: &Path,
        tree: &str,
        parents: &[&str],
        message: &str,
    ) -> Result<String> {
        checked_sha(tree)?;
        if message.trim().is_empty() || message.len() > 16384 || message.contains('\0') {
            bail!("Use a commit message between 1 and 16,384 bytes");
        }
        let mut args = os_args(["commit-tree", tree]);
        for parent in parents {
            checked_sha(parent)?;
            args.extend(os_args(["-p", *parent]));
        }
        args.extend(os_args(["-F", "-"]));
        self.check_config()?;
        let output = self.raw(Some(root), &args, Some(message.as_bytes()))?;
        if !output.success() {
            bail!("Cannot create a local Git commit");
        }
        let sha = output.text().trim().to_string();
        checked_sha(&sha)?;
        Ok(sha)
    }
    fn update_ref(&self, reference: &str, new: &str, old: &str) -> Result<()> {
        checked_sha(new)?;
        checked_sha(old)?;
        if !self
            .run(&self.accepted, &["update-ref", reference, new, old])?
            .success()
        {
            bail!("Git head changed; the update was not applied");
        }
        Ok(())
    }
}
fn os_args<I, S>(values: I) -> Vec<OsString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    values
        .into_iter()
        .map(|value| value.as_ref().to_os_string())
        .collect()
}

fn initialize_locked(store: &Store, repo: &Repository) -> Result<Value> {
    let saved_head = store.project_binding(&repo.project_id)?["canonical_head"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if !saved_head.is_empty() && !repo.git_dir.is_dir() {
        bail!("The controlled Git repository is missing; restore its private history instead of replacing accepted source history");
    }
    private_dir(&repo.hooks)?;
    if !repo.empty_config.exists() {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(&repo.empty_config)?.sync_all()?;
    }
    if repo.marker().exists() {
        let marker: Value = serde_json::from_slice(&read_small(&repo.marker())?)?;
        if marker["project_id"] != repo.project_id
            || marker["accepted_root"].as_str() != Some(repo.accepted.to_string_lossy().as_ref())
        {
            bail!("Git controller identity changed");
        }
    } else {
        if repo.git_dir.exists() || repo.accepted.join(".git").exists() {
            bail!("Existing Git metadata has not been adopted; HarnessyXOR will not replace it");
        }
        durable_json(
            &repo.marker(),
            &json!({"project_id":repo.project_id,"accepted_root":repo.accepted,"schema":1}),
        )?;
    }
    if !repo.git_dir.exists() {
        if repo.accepted.join(".git").exists() {
            bail!("Accepted source already contains unrelated Git metadata");
        }
        fixed_log("INITIALIZE controlled local Git repository", &repo.accepted);
        let mut args = os_args(["init", "--initial-branch=main", "--separate-git-dir"]);
        args.push(repo.git_dir.clone().into_os_string());
        args.push(repo.accepted.clone().into_os_string());
        if !repo.raw(None, &args, None)?.success() {
            bail!("Cannot initialize the project's local Git repository");
        }
    }
    let info = repo.git_dir.join("info");
    private_dir(&info)?;
    let attributes = info.join("attributes");
    if !attributes.exists() {
        fixed_log("WRITE controlled Git export attributes", &attributes);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&attributes)?;
        file.write_all(EXPORT_ATTRIBUTES)?;
        file.sync_all()?;
    }
    repo.validate_pointer(&repo.accepted)?;
    repo.check_config()?;
    let head = repo.run(&repo.accepted, &["rev-parse", "--verify", "HEAD"])?;
    if !head.success() {
        if !saved_head.is_empty() {
            bail!("Accepted Git history is unavailable; existing source and saved head were retained for recovery");
        }
        fixed_log("SNAPSHOT initial accepted project source", &repo.accepted);
        repo.checked(&repo.accepted, &["add", "-A", "--", "."])?;
        let tree = repo.checked(&repo.accepted, &["write-tree"])?;
        let commit =
            repo.commit_object(&repo.accepted, tree.trim(), &[], "Initial project source")?;
        let zeros = "0".repeat(commit.len());
        repo.update_ref("refs/heads/main", &commit, &zeros)?;
    }
    let branch = repo.checked(&repo.accepted, &["symbolic-ref", "--quiet", "HEAD"])?;
    if branch.trim() != "refs/heads/main" {
        bail!("Accepted source must remain on its controlled main branch");
    }
    let head = repo.head(&repo.accepted)?;
    let binding = store.project_binding(&repo.project_id)?;
    if binding["canonical_head"].as_str().unwrap_or("").is_empty() {
        store.initialize_project_head(&repo.project_id, &head)?;
    }
    recover_journal(store, repo)?;
    let binding = store.project_binding(&repo.project_id)?;
    if binding["canonical_head"].as_str() != Some(head.as_str()) {
        let current = repo.head(&repo.accepted)?;
        if binding["canonical_head"].as_str() != Some(current.as_str()) {
            bail!("Accepted Git head and saved project head disagree; source files were retained for recovery");
        }
    }
    Ok(
        json!({"initialized":true,"head":repo.head(&repo.accepted)?,"dirty":repo.strict_dirty(&repo.accepted)?,"branch":"main","bootstrap_open":store.project_bootstrap_open(&repo.project_id)?,"baseline_verification":"unverified"}),
    )
}

pub fn initialize_project(store: &Store, project_id: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)
}
pub fn bootstrap_open(store: &Store, project_id: &str) -> Result<bool> {
    store.project_bootstrap_open(project_id)
}
pub fn freeze_project_baseline(store: &Store, project_id: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    close_bootstrap_locked(
        store,
        &repo,
        "First team generation started; initial source baseline remains UNVERIFIED",
    )?;
    Ok(
        json!({"bootstrap_open":false,"head":repo.head(&repo.accepted)?,"verification":"unverified","binding":store.project_binding(project_id)?}),
    )
}
fn close_bootstrap_locked(store: &Store, repo: &Repository, reason: &str) -> Result<()> {
    let _files = crate::files::mutation_lock()?;
    if store.project_bootstrap_open(&repo.project_id)? {
        if repo.strict_dirty(&repo.accepted)? {
            bail!("Finalize the initial unverified source baseline before starting team work; current source edits were retained");
        }
        store.close_project_bootstrap(&repo.project_id, reason)?;
    }
    Ok(())
}

fn recover_journal(store: &Store, repo: &Repository) -> Result<()> {
    let path = repo.journal();
    if !path.exists() {
        return Ok(());
    }
    let journal: Value = serde_json::from_slice(&read_small(&path)?)?;
    if journal["project_id"] != repo.project_id {
        bail!("Integration recovery journal belongs to another project");
    }
    let old = journal["base_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Integration recovery journal is invalid"))?;
    let new = journal["candidate_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Integration recovery journal is invalid"))?;
    checked_sha(old)?;
    checked_sha(new)?;
    let head = repo.head(&repo.accepted)?;
    let binding = store.project_binding(&repo.project_id)?;
    let saved = binding["canonical_head"].as_str().unwrap_or("");
    if head == new && saved == new {
        fs::remove_file(path)?;
        return Ok(());
    }
    if head == old && saved == old {
        if journal["kind"] == "owner_commit" {
            // A one-time baseline action can stop after its permanent freeze
            // but before ref CAS. Finish that already prepared object, without
            // rewriting any working files or reopening the bootstrap.
            let _files = crate::files::mutation_lock()?;
            if repo.tree(&repo.accepted, new)? != journal["candidate_tree"].as_str().unwrap_or("") {
                bail!("Prepared baseline recovery object changed");
            }
            store.close_project_bootstrap(
                &repo.project_id,
                "Recovered the prepared initial UNVERIFIED baseline finalization",
            )?;
            repo.update_ref("refs/heads/main", new, old)?;
            store.set_project_head(&repo.project_id, old, new)?;
            fs::remove_file(path)?;
            return Ok(());
        }
        if !repo.strict_dirty(&repo.accepted)? {
            fs::remove_file(path)?;
            return Ok(());
        }
        bail!(
            "Integration recovery needs owner attention; uncommitted source edits were preserved"
        );
    }
    if head == new && saved == old {
        if journal["kind"] == "owner_commit" {
            store.set_project_head(&repo.project_id, old, new)?;
        } else {
            let proposal = journal["proposal_id"]
                .as_str()
                .ok_or_else(|| anyhow!("Integration journal has no proposal"))?;
            let expected = &journal["binding"];
            let mut complete = || -> Result<()> {
                if repo.head(&repo.accepted)? != new
                    || repo.tree(&repo.accepted, new)?
                        != journal["candidate_tree"].as_str().unwrap_or("")
                {
                    bail!("Integration recovery candidate changed");
                }
                if repo.strict_dirty(&repo.accepted)? {
                    // Ref CAS may have reached disk before checkout. Only roll
                    // forward an unchanged old index/worktree; never reset an
                    // edited or partially updated source tree.
                    let old_tree = repo.tree(&repo.accepted, old)?;
                    let index = repo.checked(&repo.accepted, &["write-tree"])?;
                    let files_clean = repo
                        .run(&repo.accepted, &["diff-files", "--quiet", "--"])?
                        .code
                        == Some(0);
                    let untracked = repo.checked(
                        &repo.accepted,
                        &["ls-files", "--others", "--exclude-standard", "-z"],
                    )?;
                    let ignored = repo.checked(
                        &repo.accepted,
                        &[
                            "ls-files",
                            "--others",
                            "--ignored",
                            "--exclude-standard",
                            "-z",
                        ],
                    )?;
                    if index.trim() != old_tree
                        || !files_clean
                        || !untracked.is_empty()
                        || !ignored.is_empty()
                    {
                        bail!("Integration recovery needs owner attention; current source edits were preserved");
                    }
                    if !repo
                        .run(&repo.accepted, &["read-tree", "-u", "-m", old, new])?
                        .success()
                        || repo.strict_dirty(&repo.accepted)?
                    {
                        bail!("Integration checkout recovery did not verify; source files were retained");
                    }
                }
                Ok(())
            };
            store.with_project_acceptance(&repo.project_id, proposal, expected, &mut complete)?;
        }
        fs::remove_file(path)?;
        return Ok(());
    }
    bail!("Integration recovery found conflicting heads; automatic acceptance is blocked and source files were retained")
}

fn author_role(role: &str) -> bool {
    matches!(role, "developer" | "tester")
}
pub fn ensure_agent_worktree(store: &Store, project_id: &str, agent_id: &str) -> Result<PathBuf> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    ensure_agent_locked(store, &repo, agent_id)
}
pub fn ensure_worktree(store: &Store, project_id: &str, agent_id: &str) -> Result<PathBuf> {
    ensure_agent_worktree(store, project_id, agent_id)
}
fn ensure_agent_locked(store: &Store, repo: &Repository, agent_id: &str) -> Result<PathBuf> {
    valid_id(agent_id)?;
    let actor = store.team_agent(&repo.project_id, agent_id)?;
    if actor.retired || !author_role(&actor.role) {
        bail!("This team role does not have a writable source worktree");
    }
    close_bootstrap_locked(
        store,
        repo,
        "First isolated worker source checkout; initial baseline remains unverified",
    )?;
    let parent = repo.private.join("worktrees");
    private_dir(&parent)?;
    let root = parent.join(agent_id);
    let reference = format!("refs/heads/agents/{agent_id}");
    if root.exists() {
        repo.validate_pointer(&root)?;
        let actual = repo.checked(&root, &["symbolic-ref", "--quiet", "HEAD"])?;
        if actual.trim() != reference {
            bail!("Agent worktree branch identity changed");
        }
        let worker_head = repo.head(&root)?;
        let accepted = repo.head(&repo.accepted)?;
        if worker_head != accepted
            && !repo.strict_dirty(&root)?
            && repo
                .run(
                    &root,
                    &["merge-base", "--is-ancestor", &worker_head, &accepted],
                )?
                .code
                == Some(0)
        {
            let _files = crate::files::mutation_lock()?;
            if repo.strict_dirty(&root)? {
                return Ok(root);
            }
            fixed_log(
                "FAST FORWARD clean agent worktree to accepted source",
                &root,
            );
            if !repo
                .run(&root, &["read-tree", "-u", "-m", &worker_head, &accepted])?
                .success()
            {
                bail!("Agent worktree changed while synchronizing; its files were retained");
            }
            repo.update_ref(&reference, &accepted, &worker_head)?;
        }
        return Ok(root);
    }
    fixed_log("CREATE isolated agent Git worktree", &root);
    let head = repo.head(&repo.accepted)?;
    let exists = repo
        .run(
            &repo.accepted,
            &["show-ref", "--verify", "--quiet", &reference],
        )?
        .code
        == Some(0);
    let mut args = os_args(["worktree", "add"]);
    if !exists {
        args.extend(os_args(["-b", &format!("agents/{agent_id}")]));
    }
    args.push(root.clone().into_os_string());
    args.push(if exists {
        OsString::from(format!("agents/{agent_id}"))
    } else {
        OsString::from(&head)
    });
    if !repo.raw(Some(&repo.accepted), &args, None)?.success() {
        bail!("Cannot create this agent's isolated Git worktree");
    }
    repo.validate_pointer(&root)?;
    Ok(root)
}

pub fn commit_agent(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    message: &str,
) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    store.ensure_writable(project_id)?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let root = ensure_agent_locked(store, &repo, agent_id)?;
    let old = repo.head(&root)?;
    let merge_path = repo.git_for_root(&root)?.join("MERGE_HEAD");
    let merge_head = if merge_path.exists() {
        let text = String::from_utf8(read_small(&merge_path)?)?;
        let head = text.trim().to_string();
        checked_sha(&head)?;
        Some(head)
    } else {
        None
    };
    fixed_log("COMMIT isolated agent source", &root);
    repo.checked(&root, &["add", "-A", "--", "."])?;
    let tree = repo.checked(&root, &["write-tree"])?;
    if merge_head.is_none() && tree.trim() == repo.tree(&root, &old)? {
        return Ok(json!({"sha":old,"tree":tree.trim(),"changed":false,"agent_id":agent_id}));
    }
    let mut parents = vec![old.as_str()];
    if let Some(head) = &merge_head {
        parents.push(head);
    }
    let new = repo.commit_object(&root, tree.trim(), &parents, message)?;
    repo.update_ref(&format!("refs/heads/agents/{agent_id}"), &new, &old)?;
    if merge_head.is_some() {
        repo.checked(&root, &["merge", "--quit"])?;
    }
    Ok(json!({"sha":new,"tree":tree.trim(),"changed":true,"agent_id":agent_id}))
}

fn owner_commit(store: &Store, project_id: &str, message: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    store.ensure_writable(project_id)?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let _files = crate::files::mutation_lock()?;
    if !store.project_bootstrap_open(project_id)? {
        bail!("The initial source baseline is finalized. Accepted main changes now require an independently reviewed and verified proposal");
    }
    let binding = store.project_binding(project_id)?;
    let old = repo.head(&repo.accepted)?;
    if binding["canonical_head"].as_str() != Some(old.as_str()) {
        bail!("Accepted source head changed");
    }
    repo.checked(&repo.accepted, &["add", "-A", "--", "."])?;
    let untracked = repo.checked(
        &repo.accepted,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?;
    let ignored = repo.checked(
        &repo.accepted,
        &[
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        ],
    )?;
    let unstaged = repo.run(&repo.accepted, &["diff-files", "--quiet", "--"])?;
    if !untracked.is_empty() || !ignored.is_empty() || unstaged.code != Some(0) {
        bail!("Initial source still contains ignored, untracked or unstaged files. Bootstrap remains open; export or move those files, or explicitly adjust .gitignore before finalizing. No source files were removed");
    }
    let tree = repo.checked(&repo.accepted, &["write-tree"])?;
    if tree.trim() == repo.tree(&repo.accepted, &old)? {
        store.close_project_bootstrap(
            project_id,
            "Owner finalized the initial UNVERIFIED source baseline",
        )?;
        return Ok(
            json!({"sha":old,"changed":false,"bootstrap_closed":true,"verification":"unverified"}),
        );
    }
    let new = repo.commit_object(&repo.accepted, tree.trim(), &[&old], message)?;
    let journal = json!({"kind":"owner_commit","project_id":project_id,"base_sha":old,"candidate_sha":new,"candidate_tree":tree.trim(),"binding":binding,"created_at":crate::types::now()});
    durable_json(&repo.journal(), &journal)?;
    store.close_project_bootstrap(
        project_id,
        "Owner finalized the initial UNVERIFIED source baseline",
    )?;
    repo.update_ref("refs/heads/main", &new, &old)?;
    store.set_project_head(project_id, &old, &new)?;
    fs::remove_file(repo.journal())?;
    Ok(
        json!({"sha":new,"tree":tree.trim(),"changed":true,"origin":"human","bootstrap_closed":true,"verification":"unverified"}),
    )
}

pub fn merge_current_main(store: &Store, project_id: &str, agent_id: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    store.ensure_writable(project_id)?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let root = ensure_agent_locked(store, &repo, agent_id)?;
    let _files = crate::files::mutation_lock()?;
    if repo.dirty(&root)? {
        bail!("Commit or preserve the worker's current edits before merging accepted main");
    }
    let main = repo.head(&repo.accepted)?;
    let old = repo.head(&root)?;
    if old == main {
        return Ok(
            json!({"status":"up_to_date","sha":old,"main_sha":main,"conflicts":[],"requires_commit":false}),
        );
    }
    let local = repo.checked(&root, &["ls-files", "--others", "-z"])?;
    let incoming = repo.checked(
        &root,
        &[
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--name-only",
            "--no-renames",
            "-z",
            &old,
            &main,
            "--",
        ],
    )?;
    let normalized = |path: &str| -> Result<String> {
        if path.contains('\\') || path.chars().any(char::is_control) {
            bail!("The worker contains unsupported local path names; its files were retained");
        }
        Ok(path
            .nfc()
            .flat_map(char::to_lowercase)
            .collect::<String>()
            .nfc()
            .collect())
    };
    let incoming = incoming
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(normalized)
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    for path in local.split('\0').filter(|path| !path.is_empty()) {
        let path = normalized(path)?;
        let prefix = format!("{path}/");
        let descendant = incoming
            .range(prefix.clone()..)
            .next()
            .is_some_and(|entry| entry.starts_with(&prefix));
        let mut ancestor = path.as_str();
        let mut ancestor_collision = false;
        while let Some((parent, _)) = ancestor.rsplit_once('/') {
            if incoming.contains(parent) {
                ancestor_collision = true;
                break;
            }
            ancestor = parent;
        }
        if incoming.contains(&path) || descendant || ancestor_collision {
            bail!("Ignored or untracked worker files overlap incoming main changes. Move or export those files before merging; no worker files were overwritten");
        }
    }
    fixed_log("MERGE accepted main into isolated worker branch", &root);
    let result = repo.run(
        &root,
        &[
            "merge",
            "--no-ff",
            "--no-commit",
            "--no-edit",
            "--no-overwrite-ignore",
            &main,
        ],
    )?;
    let conflicts = repo.run(&root, &["diff", "--name-only", "--diff-filter=U", "-z"])?;
    if !conflicts.success() {
        bail!("Cannot inspect the worker merge state; its files were retained");
    }
    let paths = conflicts
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).to_string())
        .collect::<Vec<_>>();
    let merge_pending = repo.git_for_root(&root)?.join("MERGE_HEAD").exists();
    if !result.success() && paths.is_empty() {
        bail!("The worker merge could not finish safely; its files and ignored build outputs were retained");
    }
    Ok(
        json!({"status":if paths.is_empty(){"merge_ready"}else{"conflicts"},"sha":old,"main_sha":main,"conflicts":paths,"requires_commit":merge_pending,"next":"Resolve the listed conflict markers using worker file tools, then commit_agent preserves both merge parents. Accepted main has not changed."}),
    )
}

pub fn propose_change(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    title: &str,
) -> Result<Value> {
    propose_change_bound(store, project_id, agent_id, None, title)
}
pub fn propose_change_for_run(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    run_id: &str,
    title: &str,
) -> Result<Value> {
    propose_change_bound(store, project_id, agent_id, Some(run_id), title)
}
fn propose_change_bound(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    run_id: Option<&str>,
    title: &str,
) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    store.ensure_writable(project_id)?;
    if title.trim().is_empty() || title.len() > 512 || title.chars().any(char::is_control) {
        bail!("Use a proposal title between 1 and 512 characters");
    }
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let root = ensure_agent_locked(store, &repo, agent_id)?;
    if repo.dirty(&root)? {
        bail!("Commit the agent worktree before proposing its changes");
    }
    let agent_sha = repo.head(&root)?;
    let base = repo.head(&repo.accepted)?;
    let binding = store.project_binding(project_id)?;
    let expected_context = if let Some(run_id) = run_id {
        let run = store
            .kv_get(&format!("run:{run_id}"))?
            .ok_or_else(|| anyhow!("Proposal run identity is missing"))?;
        if run["project_id"] != project_id || run["agent_id"] != agent_id {
            bail!("Proposal run does not belong to the current team actor");
        }
        let context = run["context_binding"].clone();
        for key in [
            "canonical_head",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            if context.get(key) != binding.get(key) {
                bail!("Project intent or source changed while this model response was being generated; refresh before proposing");
            }
        }
        Some(context)
    } else {
        None
    };
    if binding["canonical_head"].as_str() != Some(base.as_str()) {
        bail!("Accepted source head changed");
    }
    fixed_log(
        "INTEGRATE proposed agent source in private candidate",
        &root,
    );
    let merged = repo.run(
        &repo.accepted,
        &[
            "merge-tree",
            "--write-tree",
            "--no-messages",
            &base,
            &agent_sha,
        ],
    )?;
    if !merged.success() {
        if merged.code == Some(1) {
            bail!("The agent branch conflicts with accepted main; use merge_current_main, resolve its conflict markers and commit before proposing again");
        }
        bail!("Native Git could not build an integrated candidate; check the Git installation and version. Source files were retained");
    }
    let tree = merged
        .text()
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    checked_sha(&tree)?;
    if tree == repo.tree(&repo.accepted, &base)? {
        bail!("The agent branch has no new source changes to propose");
    }
    let candidate = repo.commit_object(&repo.accepted, &tree, &[&base, &agent_sha], title)?;
    let id = crate::types::id();
    let reference = format!("refs/harnessyxor/proposals/{id}");
    repo.update_ref(&reference, &candidate, &"0".repeat(candidate.len()))?;
    let mut input = json!({"id":id,"author_agent_id":agent_id,"title":title,"base_sha":base,"agent_sha":agent_sha,"agent_branch":format!("agents/{agent_id}"),"candidate_sha":candidate,"candidate_tree":tree,"catalog_revision":binding["catalog_revision"],"catalog_hash":binding["catalog_hash"],"requirements_revision":binding["requirements_revision"],"decision_revision":binding["decision_revision"],"policy_revision":binding["policy_revision"]});
    if let (Some(run_id), Some(context)) = (run_id, expected_context) {
        input["expected_run_id"] = json!(run_id);
        input["expected_context"] = context;
    }
    let proposal = store.record_proposal(project_id, &input)?;
    let proposal_id = proposal["id"]
        .as_str()
        .ok_or_else(|| anyhow!("Saved proposal has no identity"))?;
    ensure_candidate_locked(store, &repo, proposal_id)?;
    Ok(proposal)
}

fn ensure_candidate_locked(store: &Store, repo: &Repository, proposal_id: &str) -> Result<PathBuf> {
    valid_id(proposal_id)?;
    let proposal = store.get_proposal(&repo.project_id, proposal_id)?;
    let sha = proposal["candidate_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal candidate is missing"))?;
    let tree = proposal["candidate_tree"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal tree is missing"))?;
    checked_sha(sha)?;
    checked_sha(tree)?;
    if repo.tree(&repo.accepted, sha)? != tree {
        bail!("Proposal candidate tree does not match its immutable commit");
    }
    let parent = repo.private.join("candidates");
    private_dir(&parent)?;
    let root = parent.join(proposal_id);
    if !root.exists() {
        fixed_log("CREATE private integrated candidate checkout", &root);
        let mut args = os_args(["worktree", "add", "--detach"]);
        args.push(root.clone().into_os_string());
        args.push(OsString::from(sha));
        if !repo.raw(Some(&repo.accepted), &args, None)?.success() {
            bail!("Cannot create the private candidate checkout");
        }
    }
    repo.validate_pointer(&root)?;
    if repo.head(&root)? != sha || repo.strict_dirty(&root)? {
        bail!("Candidate checkout changed; its reviews and verification cannot be used");
    }
    Ok(root)
}
pub fn candidate_snapshot(store: &Store, project_id: &str, proposal_id: &str) -> Result<PathBuf> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    ensure_candidate_locked(store, &repo, proposal_id)
}
pub fn proposal_details(store: &Store, project_id: &str, proposal_id: &str) -> Result<Value> {
    let mut proposal = store.get_proposal(project_id, proposal_id)?;
    proposal["reviews"] = json!(store.reviews(project_id, proposal_id)?);
    proposal["receipts"] = json!(store.receipts(project_id, proposal_id)?);
    proposal["discussion"] = json!(store.proposal_discussion(project_id, proposal_id)?);
    proposal["gate"] = store.proposal_gate(project_id, proposal_id)?;
    Ok(proposal)
}
pub fn inspect_worktree(root: &Path) -> Result<Value> {
    let repo = Repository::from_worktree(root)?;
    let root = root.canonicalize()?;
    let sha = repo.head(&root)?;
    Ok(json!({"sha":sha,"tree":repo.tree(&root,&sha)?,"dirty":repo.strict_dirty(&root)?}))
}
pub fn diff_check(root: &Path) -> Result<bool> {
    let repo = Repository::from_worktree(root)?;
    let root = root.canonicalize()?;
    let output = repo.run(&root, &["diff-tree", "--check", "HEAD^", "HEAD"])?;
    Ok(output.code == Some(0) && !output.truncated)
}
pub fn export_committed_source(store: &Store, project_id: &str) -> Result<(String, Vec<u8>)> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let sha = repo.head(&repo.accepted)?;
    fixed_log("EXPORT exact committed project source ZIP", &repo.accepted);
    let manifest = repo.run(
        &repo.accepted,
        &["ls-tree", "-r", "-z", "--full-tree", &sha],
    )?;
    if !manifest.success() {
        bail!("Committed source inventory exceeds the export limit or cannot be read");
    }
    let mut expected = std::collections::BTreeSet::<Vec<u8>>::new();
    for entry in manifest
        .stdout
        .split(|b| *b == 0)
        .filter(|entry| !entry.is_empty())
    {
        let tab = entry
            .iter()
            .position(|b| *b == b'\t')
            .ok_or_else(|| anyhow!("Committed Git inventory is invalid"))?;
        let fields = std::str::from_utf8(&entry[..tab])?
            .split_whitespace()
            .collect::<Vec<_>>();
        if fields.len() != 3 || fields[1] != "blob" {
            bail!("Committed source contains submodules or unsupported tree entries; a complete ZIP cannot be exported by this version");
        }
        expected.insert(entry[tab + 1..].to_vec());
    }
    repo.check_config()?;
    let output = repo.raw_limit(
        Some(&repo.accepted),
        &os_args(["archive", "--format=zip", &sha]),
        None,
        MAX_EXPORT,
    )?;
    if output.truncated {
        bail!("Committed source ZIP exceeds the 64 MiB download limit; no partial archive was returned");
    }
    if output.code != Some(0) {
        bail!("Cannot export the committed source ZIP");
    }
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(output.stdout.as_slice()))
        .context("Generated source ZIP did not validate")?;
    let mut actual = std::collections::BTreeSet::new();
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        if !actual.insert(entry.name_raw().to_vec()) {
            bail!("Generated source ZIP contains duplicate file names");
        }
    }
    if actual != expected {
        bail!("Generated source ZIP does not contain the complete committed inventory; no partial archive was returned");
    }
    let filename = format!("HarnessyXOR-{project_id}-{}.zip", &sha[..12]);
    eprintln!(
        "[{}] VERIFY committed source ZIP: project={} commit={} files={} bytes={}",
        chrono::Utc::now().timestamp(),
        project_id,
        sha,
        expected.len(),
        output.stdout.len()
    );
    Ok((filename, output.stdout))
}
pub fn candidate_diff(store: &Store, project_id: &str, proposal_id: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let proposal = store.get_proposal(project_id, proposal_id)?;
    let base = proposal["base_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal base is missing"))?;
    let candidate = proposal["candidate_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal candidate is missing"))?;
    checked_sha(base)?;
    checked_sha(candidate)?;
    let output = repo.run(
        &repo.accepted,
        &[
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--no-color",
            base,
            candidate,
            "--",
        ],
    )?;
    if output.code != Some(0) {
        bail!("Cannot read the proposal diff");
    }
    let truncated = output.truncated || output.stdout.len() > MAX_DIFF;
    let diff =
        String::from_utf8_lossy(&output.stdout[..output.stdout.len().min(MAX_DIFF)]).to_string();
    Ok(
        json!({"diff":diff,"truncated":truncated,"base_sha":base,"candidate_sha":candidate,"candidate_tree":proposal["candidate_tree"]}),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn record_agent_review(
    store: &Store,
    project_id: &str,
    proposal_id: &str,
    agent_id: &str,
    run_id: &str,
    kind: &str,
    verdict: &str,
    notes: &str,
) -> Result<Value> {
    let actor = store.team_agent(project_id, agent_id)?;
    let expected = match kind {
        "code" => "code_reviewer",
        "architecture" => "architecture_reviewer",
        _ => bail!("Choose code or architecture review"),
    };
    if actor.retired || actor.role != expected {
        bail!("This agent does not hold the required independent review role");
    }
    let run = store
        .kv_get(&format!("run:{run_id}"))?
        .ok_or_else(|| anyhow!("Review run identity was not found"))?;
    if run["project_id"] != project_id || run["agent_id"] != agent_id {
        bail!("Review run does not belong to this immutable team actor");
    }
    let task_id = run["task_id"]
        .as_str()
        .ok_or_else(|| anyhow!("Review run is not bound to a proposal task"))?;
    let task = store.team_task(project_id, task_id)?;
    if task.proposal_id.as_deref() != Some(proposal_id) {
        bail!("Review task does not target this proposal");
    }
    let proposal = store.get_proposal(project_id, proposal_id)?;
    candidate_snapshot(store, project_id, proposal_id)?;
    let input = json!({"proposal_id":proposal_id,"reviewer_agent_id":agent_id,"run_id":run_id,"kind":kind,"verdict":verdict,"notes":notes,"origin":"agent","candidate_sha":proposal["candidate_sha"],"candidate_tree":proposal["candidate_tree"],"catalog_revision":proposal["catalog_revision"],"catalog_hash":proposal["catalog_hash"],"requirements_revision":proposal["requirements_revision"],"decision_revision":proposal["decision_revision"],"policy_revision":proposal["policy_revision"]});
    store.record_agent_review(project_id, agent_id, task_id, run_id, &input)
}

fn accept_proposal(store: &Store, project_id: &str, proposal_id: &str) -> Result<Value> {
    let gate = project_lock(project_id)?;
    let _lock = gate
        .lock()
        .map_err(|_| anyhow!("Git project lock is unavailable"))?;
    store.ensure_writable(project_id)?;
    let repo = Repository::locations(store, project_id)?;
    initialize_locked(store, &repo)?;
    let proposal = store.get_proposal(project_id, proposal_id)?;
    let root = ensure_candidate_locked(store, &repo, proposal_id)?;
    let expected = store.project_binding(project_id)?;
    let base = proposal["base_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal base is missing"))?;
    let candidate = proposal["candidate_sha"]
        .as_str()
        .ok_or_else(|| anyhow!("Proposal candidate is missing"))?;
    if expected["canonical_head"].as_str() != Some(base) || repo.head(&repo.accepted)? != base {
        bail!("Accepted main changed; create and review a new integrated candidate");
    }
    if repo.strict_dirty(&repo.accepted)? {
        bail!("Accepted main has uncommitted edits; commit or preserve them before accepting a candidate");
    }
    let observed = inspect_worktree(&root)?;
    if observed["sha"] != candidate
        || observed["tree"] != proposal["candidate_tree"]
        || observed["dirty"] != false
    {
        bail!("Candidate changed after review or verification");
    }
    let journal = json!({"kind":"proposal_acceptance","project_id":project_id,"proposal_id":proposal_id,"base_sha":base,"candidate_sha":candidate,"candidate_tree":proposal["candidate_tree"],"binding":expected,"created_at":crate::types::now()});
    durable_json(&repo.journal(), &journal)?;
    let mut apply = || -> Result<()> {
        if repo.strict_dirty(&repo.accepted)? || repo.head(&repo.accepted)? != base {
            bail!("Accepted source changed before integration; user edits were retained");
        }
        let check = inspect_worktree(&root)?;
        if check["sha"] != candidate
            || check["tree"] != proposal["candidate_tree"]
            || check["dirty"] != false
        {
            bail!("Candidate changed during acceptance");
        }
        repo.update_ref("refs/heads/main", candidate, base)?;
        if !repo
            .run(&repo.accepted, &["read-tree", "-u", "-m", base, candidate])?
            .success()
        {
            bail!("Git could not update accepted source safely; integration recovery is required and user edits were retained");
        }
        if repo.head(&repo.accepted)? != candidate || repo.strict_dirty(&repo.accepted)? {
            bail!("Accepted checkout did not verify; integration recovery is required");
        }
        Ok(())
    };
    let result = store.with_project_acceptance(project_id, proposal_id, &expected, &mut apply);
    if result.is_ok() {
        fs::remove_file(repo.journal())?;
    }
    result
}

struct GitError(anyhow::Error);
impl From<anyhow::Error> for GitError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for GitError {
    fn into_response(self) -> Response {
        crate::diagnostics::log("Git request blocked or failed", None, None);
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":self.0.to_string()})),
        )
            .into_response()
    }
}
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T, GitError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| GitError(anyhow!("Git operation was interrupted")))?
        .map_err(GitError)
}
#[derive(Default, Deserialize)]
struct DiffQuery {
    proposal_id: Option<String>,
}
#[derive(Deserialize)]
struct CommitBody {
    message: String,
}
#[derive(Deserialize)]
struct ProposalBody {
    agent_id: String,
    title: String,
}
#[derive(Deserialize)]
struct ReviewBody {
    kind: String,
    verdict: String,
    #[serde(default)]
    notes: String,
}
#[derive(Deserialize)]
struct MergeBody {
    agent_id: String,
}
#[derive(Deserialize)]
struct CommentBody {
    body: String,
    reply_to: Option<String>,
}
#[derive(Deserialize)]
struct CloseBody {
    status: String,
    reason: String,
    replacement_id: Option<String>,
}

async fn status_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(blocking(move||{let gate=project_lock(&id)?;let _lock=gate.lock().map_err(|_|anyhow!("Git project lock is unavailable"))?;let repo=Repository::locations(&state.store,&id)?;let mut status=initialize_locked(&state.store,&repo)?;let branches=repo.checked(&repo.accepted,&["for-each-ref","--format=%(refname:short)%00%(objectname)","refs/heads"])?;status["branches"]=json!(branches.lines().filter_map(|line|line.split_once('\0').map(|(name,sha)|json!({"name":name,"sha":sha}))).collect::<Vec<_>>());let log=repo.checked(&repo.accepted,&["log","-30","--format=%H%x00%s%x00%an%x00%aI"])?;status["log"]=json!(log.lines().filter_map(|line|{let fields=line.split('\0').collect::<Vec<_>>();if fields.len()==4{Some(json!({"sha":fields[0],"subject":fields[1],"author":fields[2],"created_at":fields[3]}))}else{None}}).collect::<Vec<_>>());status["proposals"]=json!(state.store.proposals(&id)?);Ok(status)}).await?))
}
async fn diff_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(query): Query<DiffQuery>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(blocking(move||{if let Some(proposal)=query.proposal_id{return candidate_diff(&state.store,&id,&proposal);}let gate=project_lock(&id)?;let _lock=gate.lock().map_err(|_|anyhow!("Git project lock is unavailable"))?;let repo=Repository::locations(&state.store,&id)?;initialize_locked(&state.store,&repo)?;let output=repo.run(&repo.accepted,&["diff","--no-ext-diff","--no-textconv","--no-color","HEAD","--"])?;if output.code!=Some(0){bail!("Cannot read accepted source changes");}let diff=String::from_utf8_lossy(&output.stdout[..output.stdout.len().min(MAX_DIFF)]).to_string();Ok(json!({"diff":diff,"truncated":output.truncated||output.stdout.len()>MAX_DIFF,"base_sha":repo.head(&repo.accepted)?,"dirty":repo.dirty(&repo.accepted)?}))}).await?))
}
async fn commit_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<CommitBody>,
) -> Result<Json<Value>, GitError> {
    let lease = state.agents.acquire_project_mutation(&id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            owner_commit(&state.store, &id, &body.message)
        })
        .await?,
    ))
}
async fn proposal_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<ProposalBody>,
) -> Result<Json<Value>, GitError> {
    let lease = state.agents.acquire_project_mutation(&id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            propose_change(&state.store, &id, &body.agent_id, &body.title)
        })
        .await?,
    ))
}
async fn details_handler(
    State(state): State<AppState>,
    AxPath((id, proposal)): AxPath<(String, String)>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(
        blocking(move || proposal_details(&state.store, &id, &proposal)).await?,
    ))
}
async fn candidate_handler(
    State(state): State<AppState>,
    AxPath((id, proposal)): AxPath<(String, String)>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(
        blocking(move || {
            candidate_snapshot(&state.store, &id, &proposal)?;
            proposal_details(&state.store, &id, &proposal)
        })
        .await?,
    ))
}
async fn review_handler(
    State(state): State<AppState>,
    Extension(user): Extension<User>,
    AxPath((id, proposal_id)): AxPath<(String, String)>,
    Json(body): Json<ReviewBody>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(blocking(move||{candidate_snapshot(&state.store,&id,&proposal_id)?;let proposal=state.store.get_proposal(&id,&proposal_id)?;state.store.record_review(&id,&json!({"proposal_id":proposal_id,"reviewer_user_id":user.id,"origin":"human","kind":body.kind,"verdict":body.verdict,"notes":body.notes,"candidate_sha":proposal["candidate_sha"],"candidate_tree":proposal["candidate_tree"],"catalog_revision":proposal["catalog_revision"],"catalog_hash":proposal["catalog_hash"],"requirements_revision":proposal["requirements_revision"],"decision_revision":proposal["decision_revision"],"policy_revision":proposal["policy_revision"]}))}).await?))
}
async fn verify_handler(
    State(state): State<AppState>,
    AxPath((id, proposal_id)): AxPath<(String, String)>,
) -> Result<Json<Value>, GitError> {
    let store = state.store.clone();
    let project = id.clone();
    let proposal = proposal_id.clone();
    let (root, details, catalog) = blocking(move || {
        Ok((
            candidate_snapshot(&store, &project, &proposal)?,
            store.get_proposal(&project, &proposal)?,
            store.catalog_snapshot(&project)?,
        ))
    })
    .await?;
    let sha = details["candidate_sha"]
        .as_str()
        .ok_or_else(|| GitError(anyhow!("Proposal candidate is missing")))?;
    let tree = details["candidate_tree"]
        .as_str()
        .ok_or_else(|| GitError(anyhow!("Proposal tree is missing")))?;
    let receipt = crate::verification::run_suite(
        state.store.clone(),
        &id,
        &proposal_id,
        &root,
        sha,
        tree,
        &catalog,
        CancellationToken::new(),
    )
    .await?;
    let stored = blocking(move || state.store.record_controller_receipt(&id, &receipt)).await?;
    Ok(Json(stored))
}
async fn accept_handler(
    State(state): State<AppState>,
    AxPath((id, proposal)): AxPath<(String, String)>,
) -> Result<Json<Value>, GitError> {
    let lease = state.agents.acquire_project_mutation(&id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            accept_proposal(&state.store, &id, &proposal)
        })
        .await?,
    ))
}
async fn merge_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<MergeBody>,
) -> Result<Json<Value>, GitError> {
    let lease = state.agents.acquire_project_mutation(&id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            merge_current_main(&state.store, &id, &body.agent_id)
        })
        .await?,
    ))
}
async fn export_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Response, GitError> {
    let (filename, bytes) = blocking(move || export_committed_source(&state.store, &id)).await?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/zip".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
        bytes,
    )
        .into_response())
}
async fn comments_handler(
    State(state): State<AppState>,
    AxPath((id, proposal)): AxPath<(String, String)>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(
        blocking(move || Ok(json!({"comments":state.store.proposal_discussion(&id,&proposal)?})))
            .await?,
    ))
}
async fn comment_handler(
    State(state): State<AppState>,
    Extension(user): Extension<User>,
    AxPath((id, proposal)): AxPath<(String, String)>,
    Json(body): Json<CommentBody>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(
        blocking(move || {
            state.store.team_comment_proposal(
                &id,
                &proposal,
                "user",
                &user.id,
                &json!({"body":body.body,"reply_to":body.reply_to}),
            )
        })
        .await?,
    ))
}
async fn close_handler(
    State(state): State<AppState>,
    Extension(user): Extension<User>,
    AxPath((id, proposal)): AxPath<(String, String)>,
    Json(body): Json<CloseBody>,
) -> Result<Json<Value>, GitError> {
    Ok(Json(
        blocking(move || {
            state.store.close_proposal_as_user(
                &id,
                &user.id,
                &proposal,
                &body.status,
                &body.reason,
                body.replacement_id.as_deref(),
            )
        })
        .await?,
    ))
}
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/projects/:id/git", get(status_handler))
        .route("/projects/:id/git/diff", get(diff_handler))
        .route("/projects/:id/git/commit", post(commit_handler))
        .route("/projects/:id/git/merge-main", post(merge_handler))
        .route("/projects/:id/git/export.zip", get(export_handler))
        .route("/projects/:id/git/proposals", post(proposal_handler))
        .route(
            "/projects/:id/git/proposals/:proposal_id",
            get(details_handler),
        )
        .route(
            "/projects/:id/git/proposals/:proposal_id/candidate",
            post(candidate_handler),
        )
        .route(
            "/projects/:id/git/proposals/:proposal_id/reviews",
            post(review_handler),
        )
        .route(
            "/projects/:id/git/proposals/:proposal_id/verify",
            post(verify_handler),
        )
        .route(
            "/projects/:id/git/proposals/:proposal_id/accept",
            post(accept_handler),
        )
        .route(
            "/projects/:id/proposals/:proposal_id/comments",
            get(comments_handler).post(comment_handler),
        )
        .route(
            "/projects/:id/proposals/:proposal_id/close",
            post(close_handler),
        )
}
