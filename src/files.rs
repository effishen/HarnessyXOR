//! Project-owned file operations. Every path is resolved against the current
//! project root, and every mutation checks the current saved project policy.
use crate::{store::Store, AppState};
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    body::Bytes,
    extract::{Path as AxPath, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Mutex, OnceLock},
};
use unicode_normalization::UnicodeNormalization;
use walkdir::WalkDir;

const MAX_TEXT: u64 = 8 * 1024 * 1024;
const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_ARCHIVE: u64 = 512 * 1024 * 1024;
const MAX_ENTRIES: usize = 50_000;
static MUTATIONS: OnceLock<Mutex<()>> = OnceLock::new();

/// Keep directory handles across each operation. A guest can rename or replace
/// path components concurrently, so a canonical-path check is not the final
/// filesystem authorization on Unix hosts.
struct SafeParent {
    #[cfg(unix)]
    directory: File,
    #[cfg(not(unix))]
    directory: PathBuf,
}
impl SafeParent {
    fn open(root: &Path, target: &Path) -> Result<Self> {
        let parent = target
            .parent()
            .ok_or_else(|| anyhow!("Choose a project item"))?;
        let rel = parent
            .strip_prefix(root)
            .context("The destination leaves this project")?;
        #[cfg(unix)]
        {
            use std::os::unix::{
                ffi::OsStrExt,
                fs::OpenOptionsExt,
                io::{AsRawFd, FromRawFd},
            };
            let mut directory = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(root)?;
            for component in rel.components() {
                if !matches!(component, Component::Normal(_)) {
                    bail!("The destination leaves this project");
                }
                let name = std::ffi::CString::new(component.as_os_str().as_bytes())?;
                let fd = unsafe {
                    libc::openat(
                        directory.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                directory = unsafe { File::from_raw_fd(fd) };
            }
            Ok(Self { directory })
        }
        #[cfg(not(unix))]
        {
            let _ = rel;
            Ok(Self {
                directory: parent.to_path_buf(),
            })
        }
    }
    fn open_read(&self, name: &std::ffi::OsStr) -> Result<File> {
        #[cfg(unix)]
        {
            use std::os::unix::{
                ffi::OsStrExt,
                fs::MetadataExt,
                io::{AsRawFd, FromRawFd},
            };
            let name = std::ffi::CString::new(name.as_bytes())?;
            let fd = unsafe {
                libc::openat(
                    self.directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let meta = file.metadata()?;
            if !meta.is_file() || meta.nlink() != 1 {
                bail!("Only regular files without hard links can be read");
            }
            Ok(file)
        }
        #[cfg(not(unix))]
        {
            let path = self.directory.join(name);
            let meta = fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() || !meta.is_file() {
                bail!("Choose a regular file without symbolic links");
            }
            Ok(File::open(path)?)
        }
    }
    fn open_directory(&self, name: &std::ffi::OsStr) -> Result<File> {
        #[cfg(unix)]
        {
            use std::os::unix::{
                ffi::OsStrExt,
                io::{AsRawFd, FromRawFd},
            };
            let name = std::ffi::CString::new(name.as_bytes())?;
            let fd = unsafe {
                libc::openat(
                    self.directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(unsafe { File::from_raw_fd(fd) })
        }
        #[cfg(not(unix))]
        {
            Ok(File::open(self.directory.join(name))?)
        }
    }
    fn create_file(&self, name: &std::ffi::OsStr) -> Result<File> {
        #[cfg(unix)]
        {
            use std::os::unix::{
                ffi::OsStrExt,
                io::{AsRawFd, FromRawFd},
            };
            let name = std::ffi::CString::new(name.as_bytes())?;
            let fd = unsafe {
                libc::openat(
                    self.directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_WRONLY
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_NOFOLLOW
                        | libc::O_CLOEXEC,
                    0o600,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(unsafe { File::from_raw_fd(fd) })
        }
        #[cfg(not(unix))]
        {
            Ok(OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.join(name))?)
        }
    }
    fn remove_file(&self, name: &std::ffi::OsStr) {
        #[cfg(unix)]
        {
            use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
            if let Ok(name) = std::ffi::CString::new(name.as_bytes()) {
                unsafe {
                    libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0);
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = fs::remove_file(self.directory.join(name));
        }
    }
    fn install(
        &self,
        staging: &std::ffi::OsStr,
        name: &std::ffi::OsStr,
        overwrite: bool,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
            let old = std::ffi::CString::new(staging.as_bytes())?;
            let new = std::ffi::CString::new(name.as_bytes())?;
            let fd = self.directory.as_raw_fd();
            let rc = if overwrite {
                unsafe { libc::renameat(fd, old.as_ptr(), fd, new.as_ptr()) }
            } else {
                unsafe { libc::linkat(fd, old.as_ptr(), fd, new.as_ptr(), 0) }
            };
            if rc < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if !overwrite {
                self.remove_file(staging);
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let old = self.directory.join(staging);
            let new = self.directory.join(name);
            if !overwrite && new.exists() {
                bail!("A file already uses this path");
            }
            fs::rename(old, new)?;
            Ok(())
        }
    }
    fn mkdir(&self, name: &std::ffi::OsStr) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
            let name = std::ffi::CString::new(name.as_bytes())?;
            let rc = unsafe { libc::mkdirat(self.directory.as_raw_fd(), name.as_ptr(), 0o755) };
            if rc < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            fs::create_dir(self.directory.join(name))?;
            Ok(())
        }
    }
    fn move_to(
        &self,
        name: &std::ffi::OsStr,
        destination: &SafeParent,
        new_name: &std::ffi::OsStr,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
            let old = std::ffi::CString::new(name.as_bytes())?;
            let new = std::ffi::CString::new(new_name.as_bytes())?;
            #[cfg(target_os = "linux")]
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    self.directory.as_raw_fd(),
                    old.as_ptr(),
                    destination.directory.as_raw_fd(),
                    new.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            #[cfg(not(target_os = "linux"))]
            let rc = unsafe {
                libc::renameat(
                    self.directory.as_raw_fd(),
                    old.as_ptr(),
                    destination.directory.as_raw_fd(),
                    new.as_ptr(),
                )
            } as libc::c_long;
            if rc < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            fs::rename(
                self.directory.join(name),
                destination.directory.join(new_name),
            )?;
            Ok(())
        }
    }
    fn entry_kind(&self, name: &std::ffi::OsStr) -> Result<Option<&'static str>> {
        #[cfg(unix)]
        {
            use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
            let name = std::ffi::CString::new(name.as_bytes())?;
            let mut info = std::mem::MaybeUninit::<libc::stat>::uninit();
            let rc = unsafe {
                libc::fstatat(
                    self.directory.as_raw_fd(),
                    name.as_ptr(),
                    info.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if rc < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(error.into());
            }
            let info = unsafe { info.assume_init() };
            Ok(Some(match info.st_mode & libc::S_IFMT {
                libc::S_IFDIR => "directory",
                libc::S_IFREG => "file",
                libc::S_IFLNK => "symlink",
                _ => "special",
            }))
        }
        #[cfg(not(unix))]
        {
            match fs::symlink_metadata(self.directory.join(name)) {
                Ok(info) => Ok(Some(if info.file_type().is_symlink() {
                    "symlink"
                } else if info.is_dir() {
                    "directory"
                } else if info.is_file() {
                    "file"
                } else {
                    "special"
                })),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
            }
        }
    }
}
fn safe_read(root: &Path, target: &Path) -> Result<File> {
    let parent = SafeParent::open(root, target)?;
    parent.open_read(
        target
            .file_name()
            .ok_or_else(|| anyhow!("Choose a project file"))?,
    )
}

type DirectoryEntry = (String, bool, u64, Option<String>);
fn directory_entries(root: &Path, target: &Path) -> Result<Vec<DirectoryEntry>> {
    let mut entries = Vec::new();
    #[cfg(unix)]
    {
        use std::os::unix::{
            fs::OpenOptionsExt,
            io::{AsRawFd, IntoRawFd},
        };
        let file = if target == root {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(root)?
        } else {
            SafeParent::open(root, target)?.open_directory(target.file_name().unwrap())?
        };
        struct Directory(*mut libc::DIR);
        impl Drop for Directory {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }
        let raw = file.try_clone()?.into_raw_fd();
        let pointer = unsafe { libc::fdopendir(raw) };
        if pointer.is_null() {
            unsafe {
                libc::close(raw);
            };
            return Err(std::io::Error::last_os_error().into());
        }
        let dir = Directory(pointer);
        loop {
            let entry = unsafe { libc::readdir(dir.0) };
            if entry.is_null() {
                break;
            }
            let raw_name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            let Ok(name) = raw_name.to_str() else {
                continue;
            };
            if name == "." || name == ".." || name.chars().any(char::is_control) {
                continue;
            }
            if name.eq_ignore_ascii_case(".git")
                || name == ".harnessy"
                || name == ".harnessyxor"
                || name.starts_with(".harnessyxor-")
            {
                continue;
            }
            let mut meta = std::mem::MaybeUninit::<libc::stat>::uninit();
            let rc = unsafe {
                libc::fstatat(
                    file.as_raw_fd(),
                    raw_name.as_ptr(),
                    meta.as_mut_ptr(),
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            };
            if rc != 0 {
                continue;
            }
            let meta = unsafe { meta.assume_init() };
            let kind = meta.st_mode & libc::S_IFMT;
            if kind != libc::S_IFDIR && (kind != libc::S_IFREG || meta.st_nlink != 1) {
                continue;
            }
            if entries.len() >= 5000 {
                bail!("Folder has more than 5,000 entries; use a smaller folder");
            }
            // time_t varies across Unix targets; retain the portable cast.
            #[allow(clippy::unnecessary_cast)]
            let seconds = meta.st_mtime as i64;
            let modified = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
                .map(|time| time.to_rfc3339());
            entries.push((
                name.to_string(),
                kind == libc::S_IFDIR,
                meta.st_size.max(0) as u64,
                modified,
            ));
        }
    }
    #[cfg(not(unix))]
    {
        for entry in fs::read_dir(target)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.chars().any(char::is_control)
                || name.eq_ignore_ascii_case(".git")
                || name == ".harnessy"
                || name == ".harnessyxor"
                || name.starts_with(".harnessyxor-")
            {
                continue;
            }
            let meta = fs::symlink_metadata(entry.path())?;
            if meta.file_type().is_symlink() || (!meta.is_file() && !meta.is_dir()) {
                continue;
            }
            if entries.len() >= 5000 {
                bail!("Folder has more than 5,000 entries; use a smaller folder");
            }
            let modified = meta
                .modified()
                .ok()
                .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339());
            entries.push((name, meta.is_dir(), meta.len(), modified));
        }
    }
    Ok(entries)
}

fn log(action: &str, path: &Path) {
    eprintln!(
        "[{}] {}: {}",
        chrono::Utc::now().timestamp(),
        action,
        path.to_string_lossy().escape_debug()
    );
}

pub(crate) fn mutation_lock() -> Result<std::sync::MutexGuard<'static, ()>> {
    MUTATIONS
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow!("File operation lock is unavailable"))
}

pub fn require_writable(store: &Store, project_id: &str) -> Result<()> {
    require_project_writable(store, project_id)?;
    if !crate::git_project::bootstrap_open(store, project_id)? {
        bail!("The initial baseline is closed. Source changes must use an agent working copy and the review gate.");
    }
    Ok(())
}

fn require_project_writable(store: &Store, project_id: &str) -> Result<()> {
    let project = store.project(project_id)?;
    if project.locked || project.archived {
        bail!("This project is read-only while locked or archived");
    }
    Ok(())
}

fn require_scope_writable(store: &Store, project_id: &str, root: &Path) -> Result<()> {
    require_project_writable(store, project_id)?;
    if root == store.project_root(project_id)? {
        require_writable(store, project_id)?;
    }
    Ok(())
}

fn clean_relative(raw: &str, allow_root: bool) -> Result<PathBuf> {
    if raw.len() > 4096
        || raw.chars().any(char::is_control)
        || raw.contains('\\')
        || raw.contains(':')
    {
        bail!("Use a relative project path with forward slashes");
    }
    let path = Path::new(raw);
    let mut cleaned = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(name) => {
                let name_text = name.to_string_lossy();
                if name_text.eq_ignore_ascii_case(".git")
                    || name_text.ends_with([' ', '.'])
                    || name_text == ".harnessy"
                    || name_text == ".harnessyxor"
                    || name_text.starts_with(".harnessyxor-")
                {
                    bail!("Harness metadata and temporary files are private");
                }
                cleaned.push(name);
            }
            Component::CurDir => (),
            _ => bail!("The path must stay inside this project"),
        }
    }
    if cleaned.as_os_str().is_empty() && !allow_root {
        bail!("Choose a file or folder inside this project");
    }
    Ok(cleaned)
}

/// Reject every symlink component. Canonicalizing only the final existing
/// ancestor is insufficient for links to absent paths and renamed parents.
pub fn resolve_project_path(
    store: &Store,
    project_id: &str,
    raw: &str,
    allow_root: bool,
) -> Result<PathBuf> {
    let root = store.project_root(project_id)?;
    resolve_in_root(&root, raw, allow_root)
}

fn resolve_in_root(root: &Path, raw: &str, allow_root: bool) -> Result<PathBuf> {
    let rel = clean_relative(raw, allow_root)?;
    let original = root.to_path_buf();
    let root = root
        .canonicalize()
        .context("Working folder is unavailable")?;
    if original != root {
        bail!("Working folder identity changed");
    }
    let mut target = root.clone();
    for part in rel.components() {
        target.push(part.as_os_str());
        match fs::symlink_metadata(&target) {
            Ok(meta) if meta.file_type().is_symlink() => {
                bail!("Symbolic links are not accepted in project file operations")
            }
            Ok(meta) if !meta.is_file() && !meta.is_dir() => {
                bail!("Special files are not accepted in project file operations")
            }
            Ok(_) => {
                let actual = target.canonicalize()?;
                if !actual.starts_with(&root) {
                    bail!("The path leaves this project");
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Err(err) => return Err(err.into()),
        }
    }
    if !target.starts_with(&root) {
        bail!("The path leaves this project");
    }
    Ok(target)
}

fn public_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// A controller-resolved immutable candidate or working copy. No HTTP endpoint
/// accepts this root from a model or request body.
pub fn read_scoped_file(root: &Path, path: &str) -> Result<String> {
    let target = resolve_in_root(root, path, false)?;
    log("READ scoped text file", &target);
    let file = safe_read(root, &target)?;
    if file.metadata()?.len() > MAX_TEXT {
        bail!("Text files are limited to 8 MiB");
    }
    let mut bytes = Vec::new();
    file.take(MAX_TEXT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_TEXT {
        bail!("Text file grew beyond the 8 MiB limit");
    }
    String::from_utf8(bytes).context("This file is not UTF-8 text")
}

pub fn scoped_file_exists(root: &Path, path: &str) -> Result<bool> {
    let target = resolve_in_root(root, path, false)?;
    match safe_read(root, &target) {
        Ok(_) => Ok(true),
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn agent_scope(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    role: &str,
    write: bool,
) -> Result<PathBuf> {
    let actor = serde_json::to_value(store.team_agent(project_id, agent_id)?)?;
    if actor["retired"].as_bool().unwrap_or(true) || actor["role"].as_str() != Some(role) {
        bail!("The agent identity is no longer active for this role");
    }
    if write {
        require_project_writable(store, project_id)?;
        if !matches!(role, "developer" | "tester") {
            bail!("This role has read-only source access; delegate implementation to a developer");
        }
    }
    let accepted = store.project_root(project_id)?;
    if role == "architect" && !write {
        return Ok(accepted);
    }
    if matches!(role, "qa" | "code_reviewer" | "architecture_reviewer") {
        bail!("Reviewers must select a proposal and read its exact candidate version");
    }
    crate::git_project::ensure_agent_worktree(store, project_id, agent_id)
}

pub fn read_agent_file(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    role: &str,
    path: &str,
) -> Result<String> {
    let root = agent_scope(store, project_id, agent_id, role, false)?;
    read_scoped_file(&root, path)
}

pub fn list_agent_files(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    role: &str,
    path: &str,
) -> Result<Value> {
    let root = agent_scope(store, project_id, agent_id, role, false)?;
    let target = resolve_in_root(&root, path, true)?;
    let mut entries = directory_entries(&root, &target)?.into_iter().map(|(name,is_dir,size,modified)|
        json!({"name":name,"path":public_path(&root,&target.join(&name)),"is_dir":is_dir,"size":size,"modified":modified})).collect::<Vec<_>>();
    entries.sort_by(|a, b| {
        b["is_dir"]
            .as_bool()
            .cmp(&a["is_dir"].as_bool())
            .then_with(|| a["name"].as_str().cmp(&b["name"].as_str()))
    });
    Ok(
        json!({"path":path,"entries":entries,"read_only":!matches!(role,"developer"|"tester"),"agent_id":agent_id}),
    )
}

pub fn write_agent_file(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    role: &str,
    path: &str,
    content: &str,
) -> Result<Value> {
    let root = agent_scope(store, project_id, agent_id, role, true)?;
    if content.len() as u64 > MAX_TEXT {
        bail!("Text files are limited to 8 MiB");
    }
    let _guard = mutation_lock()?;
    // Coding agents can scaffold a new project without a host shell. Every
    // directory is created relative to a held safe parent in their own copy.
    let relative = clean_relative(path, false)?;
    if let Some(parent) = relative.parent() {
        if parent.components().count() > 64 {
            bail!("Working-copy folders are limited to 64 levels");
        }
        let mut current = root.clone();
        for part in parent.components() {
            current.push(part.as_os_str());
            match fs::symlink_metadata(&current) {
                Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
                Ok(_) => bail!("A parent path is not a safe working-copy folder"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    require_project_writable(store, project_id)?;
                    let safe = SafeParent::open(&root, &current)?;
                    log("CREATE working-copy folder", &current);
                    safe.mkdir(current.file_name().unwrap())?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    write_bytes_in_root(store, project_id, &root, path, content.as_bytes(), true)
}

pub fn pack_agent_zip(
    store: &Store,
    project_id: &str,
    agent_id: &str,
    role: &str,
    paths: &[String],
    output: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Value> {
    let root = agent_scope(store, project_id, agent_id, role, true)?;
    pack_zip_in_root(store, project_id, &root, paths, output, cancel)
}

pub fn list_files(store: &Store, project_id: &str, path: &str) -> Result<Value> {
    let project = store.project(project_id)?;
    let root = store.project_root(project_id)?.canonicalize()?;
    let target = resolve_project_path(store, project_id, path, true)?;
    if !target.is_dir() {
        bail!("Choose a project folder");
    }
    log("SCAN project folder", &target);
    let mut entries = Vec::new();
    for (name, is_dir, size, modified) in directory_entries(&root, &target)? {
        entries.push(json!({"name":name,"path":public_path(&root,&target.join(&name)),"is_dir":is_dir,"size":size,"modified":modified}));
    }
    entries.sort_by(|a, b| {
        b["is_dir"]
            .as_bool()
            .cmp(&a["is_dir"].as_bool())
            .then_with(|| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .to_lowercase()
                    .cmp(&b["name"].as_str().unwrap_or("").to_lowercase())
            })
    });
    Ok(
        json!({"path":public_path(&root,&target),"entries":entries,"read_only":project.locked || project.archived || !crate::git_project::bootstrap_open(store, project_id)?}),
    )
}

pub fn read_file(store: &Store, project_id: &str, path: &str) -> Result<String> {
    let target = resolve_project_path(store, project_id, path, false)?;
    let root = store.project_root(project_id)?.canonicalize()?;
    log("READ project text file", &target);
    let file = safe_read(&root, &target)?;
    if !file.metadata()?.is_file() {
        bail!("Choose a regular file");
    }
    if file.metadata()?.len() > MAX_TEXT {
        bail!("Text files are limited to 8 MiB; use Download for larger files");
    }
    let mut bytes = Vec::new();
    file.take(MAX_TEXT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_TEXT {
        bail!("Text file grew beyond the 8 MiB limit");
    }
    String::from_utf8(bytes).context("This file is not UTF-8 text; use Download")
}

fn write_bytes_locked(
    store: &Store,
    project_id: &str,
    path: &str,
    bytes: &[u8],
    overwrite: bool,
) -> Result<Value> {
    let root = store.project_root(project_id)?;
    write_bytes_in_root(store, project_id, &root, path, bytes, overwrite)
}

fn write_bytes_in_root(
    store: &Store,
    project_id: &str,
    root: &Path,
    path: &str,
    bytes: &[u8],
    overwrite: bool,
) -> Result<Value> {
    require_scope_writable(store, project_id, root)?;
    if bytes.len() as u64 > MAX_FILE {
        bail!("File uploads are limited to 64 MiB");
    }
    let target = resolve_in_root(root, path, false)?;
    if target.is_dir() {
        bail!("A folder already uses this path");
    }
    if !overwrite && target.exists() {
        bail!("A file already uses this path");
    }
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("Choose a project file"))?;
    if !parent.is_dir() {
        bail!("Create the destination folder first");
    }
    let safe_parent = SafeParent::open(root, &target)?;
    let staging = parent.join(format!(".harnessyxor-write-{}", crate::types::id()));
    let staging_name = staging.file_name().unwrap();
    log("CREATE staged project file", &staging);
    let result = (|| -> Result<()> {
        let mut file = safe_parent.create_file(staging_name)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        // Retain an existing executable bit without modifying its inode or a
        // hardlink pointing somewhere outside the project.
        if let Ok(previous) = safe_parent.open_read(target.file_name().unwrap()) {
            file.set_permissions(previous.metadata()?.permissions())?;
        }
        require_scope_writable(store, project_id, root)?;
        let fresh = resolve_in_root(root, path, false)?;
        if fresh != target {
            bail!("Project path changed during save; retry the operation");
        }
        if !overwrite && target.exists() {
            bail!("A file already uses this path");
        }
        log("REPLACE project file", &target);
        safe_parent.install(staging_name, target.file_name().unwrap(), overwrite)?;
        Ok(())
    })();
    if result.is_err() {
        safe_parent.remove_file(staging_name);
    }
    result?;
    Ok(json!({"path":path,"bytes":bytes.len()}))
}

pub fn write_file(store: &Store, project_id: &str, path: &str, content: &str) -> Result<Value> {
    if content.len() as u64 > MAX_TEXT {
        bail!("Text files are limited to 8 MiB");
    }
    let _guard = mutation_lock()?;
    write_bytes_locked(store, project_id, path, content.as_bytes(), true)
}

pub fn make_directory(store: &Store, project_id: &str, path: &str) -> Result<Value> {
    let _guard = mutation_lock()?;
    require_writable(store, project_id)?;
    let target = resolve_project_path(store, project_id, path, false)?;
    if target.exists() {
        bail!("A file or folder already uses this path");
    }
    if !target.parent().map(Path::is_dir).unwrap_or(false) {
        bail!("Create the parent folder first");
    }
    let root = store.project_root(project_id)?.canonicalize()?;
    let safe_parent = SafeParent::open(&root, &target)?;
    require_writable(store, project_id)?;
    log("CREATE project folder", &target);
    safe_parent.mkdir(target.file_name().unwrap())?;
    Ok(json!({"path":path}))
}

pub fn rename_file(store: &Store, project_id: &str, path: &str, new_path: &str) -> Result<Value> {
    let _guard = mutation_lock()?;
    require_writable(store, project_id)?;
    let source = resolve_project_path(store, project_id, path, false)?;
    let destination = resolve_project_path(store, project_id, new_path, false)?;
    if destination.exists() {
        bail!("A file or folder already uses the destination path");
    }
    if !destination.parent().map(Path::is_dir).unwrap_or(false) {
        bail!("Create the destination folder first");
    }
    if destination.starts_with(&source) {
        bail!("A folder cannot move into itself");
    }
    let root = store.project_root(project_id)?.canonicalize()?;
    let source_parent = SafeParent::open(&root, &source)?;
    let destination_parent = SafeParent::open(&root, &destination)?;
    require_writable(store, project_id)?;
    eprintln!(
        "[{}] MOVE project item: {} -> {}",
        chrono::Utc::now().timestamp(),
        source.to_string_lossy().escape_debug(),
        destination.to_string_lossy().escape_debug()
    );
    source_parent.move_to(
        source.file_name().unwrap(),
        &destination_parent,
        destination.file_name().unwrap(),
    )?;
    Ok(json!({"path":new_path}))
}

fn excluded_archive_name(path: &Path) -> bool {
    path.components().any(|part| {
        let name = part.as_os_str().to_string_lossy().to_lowercase();
        matches!(
            name.as_str(),
            ".harnessy"
                | ".harnessyxor"
                | ".git"
                | ".ssh"
                | ".gnupg"
                | ".env"
                | "login_users.json"
                | "id_rsa"
                | "id_ed25519"
                | "credentials.json"
        ) || name.starts_with(".harnessyxor-")
            || name.ends_with(".pem")
            || name.ends_with(".key")
    })
}

pub fn pack_zip(store: &Store, project_id: &str, paths: &[String], output: &str) -> Result<Value> {
    pack_zip_cancellable(
        store,
        project_id,
        paths,
        output,
        &tokio_util::sync::CancellationToken::new(),
    )
}
pub fn pack_zip_cancellable(
    store: &Store,
    project_id: &str,
    paths: &[String],
    output: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Value> {
    let root = store.project_root(project_id)?;
    pack_zip_in_root(store, project_id, &root, paths, output, cancel)
}

fn pack_zip_in_root(
    store: &Store,
    project_id: &str,
    root: &Path,
    paths: &[String],
    output: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Value> {
    let root = root.canonicalize()?;
    let _guard = mutation_lock()?;
    require_scope_writable(store, project_id, &root)?;
    if paths.len() > 1000 {
        bail!("Select at most 1,000 paths for one archive");
    }
    if !output.to_lowercase().ends_with(".zip") {
        bail!("Use a .zip filename");
    }
    let destination = resolve_in_root(&root, output, false)?;
    if destination.exists() {
        bail!("The archive already exists; choose another name");
    }
    if !destination.parent().map(Path::is_dir).unwrap_or(false) {
        bail!("Create the archive destination folder first");
    }
    let selected = if paths.is_empty() {
        vec![String::new()]
    } else {
        paths.to_vec()
    };
    let mut entries = std::collections::BTreeSet::new();
    let excluded_count = std::cell::Cell::new(0usize);
    let mut total_bytes = 0u64;
    for raw in &selected {
        let selected_path = resolve_in_root(&root, raw, true)?;
        if !selected_path.exists() {
            bail!("A selected archive input is missing");
        }
        let walker = WalkDir::new(&selected_path)
            .follow_links(false)
            .into_iter()
            .filter_entry(|entry| {
                let relative = entry.path().strip_prefix(&root).unwrap_or(entry.path());
                let excluded = excluded_archive_name(relative);
                if excluded {
                    excluded_count.set(excluded_count.get() + 1);
                }
                !excluded
            });
        for item in walker {
            if cancel.is_cancelled() {
                bail!("ZIP creation cancelled");
            }
            let item = item?;
            let path = item.path();
            if path == root {
                continue;
            }
            let rel = path.strip_prefix(&root)?;
            if excluded_archive_name(rel) {
                excluded_count.set(excluded_count.get() + 1);
                continue;
            }
            if item.file_type().is_symlink() {
                excluded_count.set(excluded_count.get() + 1);
                continue;
            }
            if !item.file_type().is_dir() && !item.file_type().is_file() {
                excluded_count.set(excluded_count.get() + 1);
                continue;
            }
            if entries.insert(rel.to_path_buf()) && item.file_type().is_file() {
                total_bytes = total_bytes
                    .checked_add(item.metadata()?.len())
                    .ok_or_else(|| anyhow!("Archive input is too large"))?;
                if total_bytes > MAX_ARCHIVE {
                    bail!("Archive inputs are limited to 512 MiB");
                }
            }
            if entries.len() > MAX_ENTRIES {
                bail!("Archives are limited to 50,000 entries");
            }
        }
    }
    let staging = destination
        .parent()
        .unwrap()
        .join(format!(".harnessyxor-zip-{}", crate::types::id()));
    let safe_parent = SafeParent::open(&root, &destination)?;
    let staging_name = staging.file_name().unwrap();
    log("PACKAGE project archive", &destination);
    let result = (|| -> Result<()> {
        let file = safe_parent.create_file(staging_name)?;
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::FileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o644);
        let mut written = 0u64;
        for rel in entries {
            if cancel.is_cancelled() {
                bail!("ZIP creation cancelled");
            }
            require_scope_writable(store, project_id, &root)?;
            let raw = rel.to_string_lossy().replace('\\', "/");
            let path = resolve_in_root(&root, &raw, false)?;
            let meta = fs::symlink_metadata(&path)?;
            if meta.is_dir() {
                archive.add_directory(format!("{raw}/"), options)?;
                continue;
            }
            if !meta.is_file() || meta.file_type().is_symlink() {
                bail!("An archive input changed; retry packaging");
            }
            let mut input = safe_read(&root, &path)?;
            #[cfg(unix)]
            let file_options = {
                use std::os::unix::fs::PermissionsExt;
                options.unix_permissions(meta.permissions().mode() & 0o777)
            };
            #[cfg(not(unix))]
            let file_options = options;
            archive.start_file(&raw, file_options)?;
            let mut buffer = [0u8; 65536];
            loop {
                if cancel.is_cancelled() {
                    bail!("ZIP creation cancelled");
                }
                let n = input.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                written += n as u64;
                if written > MAX_ARCHIVE {
                    bail!("Archive inputs grew beyond 512 MiB");
                }
                archive.write_all(&buffer[..n])?;
            }
        }
        let output_file = archive.finish()?;
        output_file.sync_all()?;
        require_scope_writable(store, project_id, &root)?;
        resolve_in_root(&root, output, false)?;
        if destination.exists() {
            bail!("The archive destination changed; choose another name");
        }
        if cancel.is_cancelled() {
            bail!("ZIP creation cancelled");
        }
        safe_parent.install(staging_name, destination.file_name().unwrap(), false)?;
        Ok(())
    })();
    if result.is_err() {
        safe_parent.remove_file(staging_name);
    }
    result?;
    let bytes = safe_parent
        .open_read(destination.file_name().unwrap())?
        .metadata()?
        .len();
    log("VERIFY completed project archive", &destination);
    Ok(
        json!({"path":output,"bytes":bytes,"excluded_count":excluded_count.get(),"exclusions":"Harness metadata, .git, .ssh, .gnupg, .env, login/credential files, private keys and symbolic links are excluded."}),
    )
}

/// Private payloads live beside Files/, outside the project's VM bind. Nothing
/// in this area is returned by the file browser or included in project ZIPs.
fn private_area(
    store: &Store,
    project_id: &str,
    area: &str,
    create: bool,
) -> Result<(PathBuf, SafeParent)> {
    let project = store.project(project_id)?;
    store.project_root(project_id)?; // Verify the workspace marker and identity.
    let workspace = store.workspace(&project.workspace_id)?;
    let workspace_root = PathBuf::from(workspace.path);
    if workspace_root.canonicalize()? != workspace_root {
        bail!("Workspace location changed");
    }
    let mut current = workspace_root.clone();
    for name in [".harnessyxor", area, project_id] {
        let next = current.join(name);
        let parent = SafeParent::open(&workspace_root, &next)?;
        match parent.entry_kind(std::ffi::OsStr::new(name))? {
            Some("directory") => (),
            None if create => {
                log("CREATE private file-operation folder", &next);
                parent.mkdir(std::ffi::OsStr::new(name))?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    parent
                        .open_directory(std::ffi::OsStr::new(name))?
                        .set_permissions(fs::Permissions::from_mode(0o700))?;
                }
            }
            None => bail!("Private file-operation folder is not present"),
            _ => bail!("Private file-operation folders cannot be links or special files"),
        }
        current = next;
    }
    let handle = SafeParent::open(&workspace_root, &current.join("placeholder"))?;
    Ok((current, handle))
}

pub fn move_to_trash(store: &Store, project_id: &str, path: &str) -> Result<Value> {
    let _guard = mutation_lock()?;
    require_writable(store, project_id)?;
    let source = resolve_project_path(store, project_id, path, false)?;
    let root = store.project_root(project_id)?.canonicalize()?;
    let source_parent = SafeParent::open(&root, &source)?;
    let kind = source_parent
        .entry_kind(source.file_name().unwrap())?
        .ok_or_else(|| anyhow!("The selected item is missing"))?;
    if !matches!(kind, "directory" | "file") {
        bail!("Only regular files and folders can be moved to trash");
    }
    let (trash, trash_parent) = private_area(store, project_id, "trash", true)?;
    let trash_id = crate::types::id();
    let record = json!({"id":trash_id,"path":public_path(&root,&source),"deleted_at":crate::types::now(),"is_dir":kind=="directory"});
    // Write recoverable metadata first. If the process stops after the atomic
    // move, the next trash listing finds the payload without needing a receipt.
    let key = format!("trash:{project_id}:{trash_id}");
    store.kv_set(&key, &record)?;
    require_writable(store, project_id)?;
    eprintln!(
        "[{}] MOVE project item to private trash: {} -> {}",
        chrono::Utc::now().timestamp(),
        source.to_string_lossy().escape_debug(),
        trash.join(&trash_id).to_string_lossy().escape_debug()
    );
    if let Err(error) = source_parent.move_to(
        source.file_name().unwrap(),
        &trash_parent,
        std::ffi::OsStr::new(&trash_id),
    ) {
        let _ = store.kv_delete(&key);
        return Err(error)
            .context("Cannot move this item to trash; the original item was not deleted");
    }
    Ok(record)
}

pub fn list_trash(store: &Store, project_id: &str) -> Result<Value> {
    let project = store.project(project_id)?;
    let prefix = format!("trash:{project_id}:");
    let records = store.kv_prefix(&prefix)?;
    if records.is_empty() {
        return Ok(json!({"entries":[],"read_only":project.archived||project.locked}));
    }
    let (_path, parent) = private_area(store, project_id, "trash", false)?;
    let mut entries = Vec::new();
    for (key, value) in records {
        let Some(id) = key.strip_prefix(&prefix) else {
            continue;
        };
        if uuid::Uuid::parse_str(id).is_err() {
            continue;
        }
        let Some(path) = value["path"].as_str() else {
            continue;
        };
        if clean_relative(path, false).is_err() {
            continue;
        }
        let Some(kind) = parent.entry_kind(std::ffi::OsStr::new(id))? else {
            continue;
        };
        if entries.len() >= 5000 {
            bail!("Trash contains more than 5,000 entries; restore some before continuing");
        }
        entries.push(json!({"id":id,"path":path,"deleted_at":value["deleted_at"].as_str(),"is_dir":kind=="directory"}));
    }
    entries.sort_by(|a, b| b["deleted_at"].as_str().cmp(&a["deleted_at"].as_str()));
    Ok(json!({"entries":entries,"read_only":project.archived||project.locked}))
}

pub fn restore_trash(
    store: &Store,
    project_id: &str,
    trash_id: &str,
    new_path: Option<&str>,
) -> Result<Value> {
    let _guard = mutation_lock()?;
    require_writable(store, project_id)?;
    uuid::Uuid::parse_str(trash_id).context("Trash ID must be a UUID")?;
    let key = format!("trash:{project_id}:{trash_id}");
    let record = store
        .kv_get(&key)?
        .ok_or_else(|| anyhow!("Trash item was not found"))?;
    let original = record["path"]
        .as_str()
        .ok_or_else(|| anyhow!("Trash metadata is invalid"))?;
    let path = new_path.unwrap_or(original);
    let destination = resolve_project_path(store, project_id, path, false)?;
    let root = store.project_root(project_id)?.canonicalize()?;
    let destination_parent = SafeParent::open(&root, &destination)?;
    if destination_parent
        .entry_kind(destination.file_name().unwrap())?
        .is_some()
    {
        bail!("The restore destination already exists; choose another path");
    }
    let (trash, trash_parent) = private_area(store, project_id, "trash", false)?;
    if trash_parent
        .entry_kind(std::ffi::OsStr::new(trash_id))?
        .is_none()
    {
        bail!("Trash payload is missing; its metadata has been retained");
    }
    require_writable(store, project_id)?;
    eprintln!(
        "[{}] RESTORE project item from private trash: {} -> {}",
        chrono::Utc::now().timestamp(),
        trash.join(trash_id).to_string_lossy().escape_debug(),
        destination.to_string_lossy().escape_debug()
    );
    trash_parent.move_to(
        std::ffi::OsStr::new(trash_id),
        &destination_parent,
        destination.file_name().unwrap(),
    )?;
    // An old metadata row cannot cause another restore: listings and restores
    // always require the corresponding private payload to remain present.
    if store.kv_delete(&key).is_err() {
        eprintln!(
            "[{}] WARN restored trash metadata cleanup pending: project {}",
            chrono::Utc::now().timestamp(),
            project_id.escape_debug()
        );
    }
    Ok(json!({"path":path,"restored":true}))
}

fn archive_member_path(name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || name.len() > 4096
        || name.chars().any(char::is_control)
        || name.contains(['\\', ':'])
    {
        bail!("ZIP contains an unsupported or unsafe member path");
    }
    let mut output = PathBuf::new();
    for component in Path::new(name).components() {
        match component {
            Component::CurDir => (),
            Component::Normal(part) => {
                let text = part
                    .to_str()
                    .ok_or_else(|| anyhow!("ZIP member names must be UTF-8"))?;
                if text.len() > 255 {
                    bail!("ZIP member names exceed the supported filesystem component length");
                }
                let stem = text.split('.').next().unwrap_or("").to_ascii_uppercase();
                let windows_reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
                    || (stem.len() == 4
                        && (stem.starts_with("COM") || stem.starts_with("LPT"))
                        && matches!(stem.as_bytes()[3], b'1'..=b'9'));
                if text.ends_with(['.', ' ']) || windows_reserved {
                    bail!("ZIP contains a name that is unsafe on supported host filesystems");
                }
                output.push(part);
            }
            _ => bail!("ZIP member path leaves the extraction folder"),
        }
    }
    if output.as_os_str().is_empty() {
        bail!("ZIP contains an empty member path");
    }
    if output.components().count() > 64 {
        bail!("ZIP member paths are limited to 64 folder levels");
    }
    Ok(output)
}

#[derive(Clone)]
struct ArchiveMember {
    index: usize,
    path: PathBuf,
    is_dir: bool,
    mode: u32,
    size: u64,
}

/// Extract into a new private staging directory, then atomically install the
/// complete tree. Existing project files are never overwritten or merged.
pub fn extract_zip(
    store: &Store,
    project_id: &str,
    path: &str,
    destination: &str,
) -> Result<Value> {
    extract_zip_cancellable(
        store,
        project_id,
        path,
        destination,
        &tokio_util::sync::CancellationToken::new(),
    )
}
pub fn extract_zip_cancellable(
    store: &Store,
    project_id: &str,
    path: &str,
    destination: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Value> {
    let _guard = mutation_lock()?;
    require_writable(store, project_id)?;
    let source = resolve_project_path(store, project_id, path, false)?;
    if !path.to_ascii_lowercase().ends_with(".zip") {
        bail!("This version extracts ZIP archives only");
    }
    let root = store.project_root(project_id)?.canonicalize()?;
    let target = resolve_project_path(store, project_id, destination, false)?;
    let target_parent = SafeParent::open(&root, &target)?;
    if target_parent
        .entry_kind(target.file_name().unwrap())?
        .is_some()
    {
        bail!("Extract into a new folder; the destination already exists");
    }
    let input = safe_read(&root, &source)?;
    if input.metadata()?.len() > MAX_ARCHIVE {
        bail!("ZIP input is limited to 512 MiB");
    }
    log("READ project ZIP for extraction", &source);
    let mut archive = zip::ZipArchive::new(input).context("Cannot open this ZIP archive")?;
    if archive.len() > MAX_ENTRIES {
        bail!("ZIP archives are limited to 50,000 entries");
    }
    let mut members = Vec::new();
    let mut paths = std::collections::BTreeMap::<String, (String, bool, bool)>::new();
    let mut expected_bytes = 0u64;
    let mut indexed_path_bytes = 0usize;
    let mut excluded_count = 0usize;
    for index in 0..archive.len() {
        if cancel.is_cancelled() {
            bail!("ZIP extraction cancelled");
        }
        let member = archive
            .by_index(index)
            .context("ZIP member cannot be read")?;
        let member_path = archive_member_path(member.name())?;
        let mode = member.unix_mode().unwrap_or(0);
        let kind = mode & 0o170000;
        let is_dir = member.is_dir();
        if (kind != 0 && kind != 0o100000 && kind != 0o040000)
            || (kind == 0o040000 && !is_dir)
            || (kind == 0o100000 && is_dir)
        {
            bail!("ZIP links and special files are not accepted");
        }
        if excluded_archive_name(&member_path) {
            excluded_count += 1;
            continue;
        }
        if member.size() > MAX_FILE {
            bail!("Extracted files are limited to 64 MiB each");
        }
        expected_bytes = expected_bytes
            .checked_add(member.size())
            .ok_or_else(|| anyhow!("ZIP expansion is too large"))?;
        if expected_bytes > MAX_ARCHIVE {
            bail!("ZIP expansion is limited to 512 MiB");
        }
        let components = member_path
            .components()
            .map(|part| part.as_os_str().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        for i in 0..components.len() {
            let prefix = components[..=i].join("/");
            let normalized = prefix.nfc().collect::<String>().to_lowercase();
            let directory = i + 1 < components.len() || is_dir;
            let explicit = i + 1 == components.len();
            match paths.get_mut(&normalized) {
                Some((previous, previous_dir, was_explicit)) => {
                    if previous != &prefix {
                        bail!("ZIP contains colliding case or Unicode member names");
                    }
                    if *previous_dir != directory {
                        bail!("ZIP uses one path as both a file and folder");
                    }
                    if explicit && *was_explicit {
                        bail!("ZIP contains duplicate member names");
                    }
                    *was_explicit |= explicit;
                }
                None => {
                    if paths.len() >= MAX_ENTRIES {
                        bail!("ZIP expansion is limited to 50,000 files and folders");
                    }
                    indexed_path_bytes = indexed_path_bytes
                        .checked_add(normalized.len() + prefix.len())
                        .ok_or_else(|| anyhow!("ZIP path index is too large"))?;
                    if indexed_path_bytes > 16 * 1024 * 1024 {
                        bail!("ZIP member path index exceeds 16 MiB");
                    }
                    paths.insert(normalized, (prefix, directory, explicit));
                }
            }
        }
        members.push(ArchiveMember {
            index,
            path: member_path,
            is_dir,
            mode,
            size: member.size(),
        });
    }
    let (private_root, private_parent) = private_area(store, project_id, "extractions", true)?;
    let staging_id = crate::types::id();
    let staging = private_root.join(&staging_id);
    log("CREATE private ZIP extraction staging folder", &staging);
    private_parent.mkdir(std::ffi::OsStr::new(&staging_id))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        private_parent
            .open_directory(std::ffi::OsStr::new(&staging_id))?
            .set_permissions(fs::Permissions::from_mode(0o700))?;
    }
    let result = (|| -> Result<Value> {
        // Create every directory in depth order through held no-follow handles.
        let mut directories = paths
            .values()
            .filter(|(_, dir, _)| *dir)
            .map(|(path, _, _)| path.clone())
            .collect::<Vec<_>>();
        directories.sort_by(|a, b| {
            a.matches('/')
                .count()
                .cmp(&b.matches('/').count())
                .then_with(|| a.cmp(b))
        });
        for directory in directories {
            if cancel.is_cancelled() {
                bail!("ZIP extraction cancelled");
            }
            require_writable(store, project_id)?;
            let target = staging.join(&directory);
            let parent = SafeParent::open(&staging, &target)?;
            log("CREATE extracted ZIP folder", &target);
            parent.mkdir(target.file_name().unwrap())?;
        }
        let mut actual_bytes = 0u64;
        let mut file_count = 0usize;
        for info in members {
            if info.is_dir {
                continue;
            }
            if cancel.is_cancelled() {
                bail!("ZIP extraction cancelled");
            }
            require_writable(store, project_id)?;
            let target = staging.join(&info.path);
            let parent = SafeParent::open(&staging, &target)?;
            log("WRITE extracted ZIP file", &target);
            let mut output = parent.create_file(target.file_name().unwrap())?;
            let mut member = archive.by_index(info.index)?;
            let mut file_bytes = 0u64;
            let mut buffer = [0u8; 65536];
            loop {
                if cancel.is_cancelled() {
                    bail!("ZIP extraction cancelled");
                }
                let n = member.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                file_bytes += n as u64;
                actual_bytes += n as u64;
                if file_bytes > MAX_FILE || actual_bytes > MAX_ARCHIVE {
                    bail!("ZIP expansion exceeds the configured limits");
                }
                output.write_all(&buffer[..n])?;
            }
            if file_bytes != info.size {
                bail!("ZIP member size changed during extraction");
            }
            output.sync_all()?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                output.set_permissions(fs::Permissions::from_mode(0o600 | (info.mode & 0o100)))?;
            }
            file_count += 1;
        }
        require_writable(store, project_id)?;
        resolve_project_path(store, project_id, destination, false)?;
        if cancel.is_cancelled() {
            bail!("ZIP extraction cancelled");
        }
        log("INSTALL verified extracted ZIP folder", &target);
        private_parent.move_to(
            std::ffi::OsStr::new(&staging_id),
            &target_parent,
            target.file_name().unwrap(),
        )?;
        Ok(
            json!({"path":destination,"files":file_count,"bytes":actual_bytes,"excluded_count":excluded_count,"exclusions":"Harness metadata, credential files and private keys are excluded. ZIP links and special files are rejected."}),
        )
    })();
    if result.is_err() {
        log("REMOVE incomplete private ZIP extraction", &staging);
        let _ = fs::remove_dir_all(&staging);
    }
    result
}

#[derive(Default, Deserialize)]
struct FileQuery {
    #[serde(default)]
    path: String,
}
#[derive(Deserialize)]
struct TextBody {
    content: String,
}
#[derive(Deserialize)]
struct PathBody {
    path: String,
}
#[derive(Deserialize)]
struct RenameBody {
    path: String,
    new_path: String,
}
#[derive(Deserialize)]
struct ZipBody {
    #[serde(default)]
    paths: Vec<String>,
    output: String,
}
#[derive(Deserialize)]
struct ExtractBody {
    path: String,
    destination: String,
}
#[derive(Default, Deserialize)]
struct RestoreBody {
    path: Option<String>,
}

struct FileError(anyhow::Error);
impl From<anyhow::Error> for FileError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for FileError {
    fn into_response(self) -> Response {
        // Application errors are useful here, but arbitrary OS paths are not
        // echoed through public diagnostics or the request log.
        let message = self.0.to_string();
        (StatusCode::BAD_REQUEST, Json(json!({"error":message}))).into_response()
    }
}
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T, FileError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| FileError(anyhow!("File worker stopped")))?
        .map_err(FileError)
}
async fn list_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(q): Query<FileQuery>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || list_files(&state.store, &id, &q.path)).await?,
    ))
}
async fn read_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(q): Query<FileQuery>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || Ok(json!({"path":q.path,"content":read_file(&state.store,&id,&q.path)?})))
            .await?,
    ))
}
async fn write_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(q): Query<FileQuery>,
    Json(body): Json<TextBody>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || write_file(&state.store, &id, &q.path, &body.content)).await?,
    ))
}
async fn mkdir_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<PathBody>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || make_directory(&state.store, &id, &body.path)).await?,
    ))
}
async fn rename_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<RenameBody>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || rename_file(&state.store, &id, &body.path, &body.new_path)).await?,
    ))
}
async fn zip_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<ZipBody>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || pack_zip(&state.store, &id, &body.paths, &body.output)).await?,
    ))
}
fn require_idle(
    state: &AppState,
    project_id: &str,
) -> Result<crate::agent::ProjectMutationGuard, FileError> {
    Ok(state.agents.acquire_project_mutation(project_id)?)
}
async fn delete_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(query): Query<FileQuery>,
) -> Result<Json<Value>, FileError> {
    let lease = require_idle(&state, &id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            move_to_trash(&state.store, &id, &query.path)
        })
        .await?,
    ))
}
async fn trash_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Value>, FileError> {
    Ok(Json(blocking(move || list_trash(&state.store, &id)).await?))
}
async fn restore_handler(
    State(state): State<AppState>,
    AxPath((id, trash_id)): AxPath<(String, String)>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<Value>, FileError> {
    let lease = require_idle(&state, &id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            restore_trash(&state.store, &id, &trash_id, body.path.as_deref())
        })
        .await?,
    ))
}
async fn extract_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<ExtractBody>,
) -> Result<Json<Value>, FileError> {
    let lease = require_idle(&state, &id)?;
    Ok(Json(
        blocking(move || {
            let _lease = lease;
            extract_zip(&state.store, &id, &body.path, &body.destination)
        })
        .await?,
    ))
}
async fn upload_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(q): Query<FileQuery>,
    bytes: Bytes,
) -> Result<Json<Value>, FileError> {
    Ok(Json(
        blocking(move || {
            let _lock = mutation_lock()?;
            write_bytes_locked(&state.store, &id, &q.path, &bytes, false)
        })
        .await?,
    ))
}
async fn download_handler(
    State(state): State<AppState>,
    AxPath(id): AxPath<String>,
    Query(q): Query<FileQuery>,
) -> Result<Response, FileError> {
    let (name, bytes) = blocking(move || {
        let target = resolve_project_path(&state.store, &id, &q.path, false)?;
        let root = state.store.project_root(&id)?.canonicalize()?;
        log("READ project download", &target);
        let file = safe_read(&root, &target)?;
        if !file.metadata()?.is_file() {
            bail!("Choose a regular file");
        }
        if file.metadata()?.len() > MAX_ARCHIVE {
            bail!("Downloads are limited to 512 MiB");
        }
        let mut bytes = Vec::new();
        file.take(MAX_ARCHIVE + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_ARCHIVE {
            bail!("File grew beyond the download limit");
        }
        let name = target
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || "._-".contains(c) {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>();
        Ok((name, bytes))
    })
    .await?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{name}\""),
            ),
            (header::CACHE_CONTROL, "no-store".to_string()),
        ],
        bytes,
    )
        .into_response())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/projects/:id/files", get(list_handler))
        .route(
            "/projects/:id/file",
            get(read_handler).put(write_handler).delete(delete_handler),
        )
        .route("/projects/:id/directories", post(mkdir_handler))
        .route("/projects/:id/rename", post(rename_handler))
        .route("/projects/:id/zip", post(zip_handler))
        .route("/projects/:id/extract", post(extract_handler))
        .route("/projects/:id/trash", get(trash_handler))
        .route(
            "/projects/:id/trash/:trash_id/restore",
            post(restore_handler),
        )
        .route(
            "/projects/:id/upload",
            post(upload_handler).layer(axum::extract::DefaultBodyLimit::max(MAX_FILE as usize)),
        )
        .route("/projects/:id/download", get(download_handler))
}
