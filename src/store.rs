//! Durable HarnessyXOR state. Legacy workspaces are imported into a separate directory.
use crate::types::{id, now, Message, Project, Settings, Workspace};
use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Row};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
    sync::{Mutex, MutexGuard},
    time::Duration,
};

const MAX_TEXT: usize = 64 * 1024 * 1024;
const MAX_JSON: usize = 4 * 1024 * 1024;
const SCHEMA: i64 = 3;

pub struct Store {
    data_dir: PathBuf,
    db_path: PathBuf,
    connection: Mutex<Connection>,
}

fn log(action: &str, path: &Path, scope: &str) {
    let clean = |value: &str| {
        value
            .chars()
            .map(|c| {
                if c.is_control() {
                    c.escape_default().to_string()
                } else {
                    c.to_string()
                }
            })
            .collect::<String>()
    };
    eprintln!(
        "[{}] {}: {} {}",
        chrono::Utc::now().timestamp(),
        action,
        clean(&path.to_string_lossy()),
        clean(scope)
    );
}

fn private_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        log("CREATE directory", path, "");
        fs::create_dir_all(path)
            .with_context(|| format!("Cannot create directory: {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn create_directory(path: &Path) -> Result<()> {
    log("CREATE directory", path, "");
    fs::create_dir(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn valid_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("Invalid record ID");
    }
    Ok(())
}

fn label(value: &str, kind: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 512 || value.chars().any(|c| c.is_control()) {
        bail!("{kind} must contain 1 to 512 characters without control characters");
    }
    Ok(value.to_string())
}

fn encode(value: &Value) -> Result<String> {
    let value = serde_json::to_string(value)?;
    if value.len() > MAX_JSON {
        bail!("Saved JSON exceeds the 4 MiB limit");
    }
    Ok(value)
}

fn workspace_row(row: &Row<'_>) -> rusqlite::Result<Workspace> {
    Ok(Workspace {
        id: row.get(0)?,
        name: row.get(1)?,
        path: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn project_row(row: &Row<'_>) -> rusqlite::Result<Project> {
    Ok(Project {
        id: row.get(0)?,
        workspace_id: row.get(1)?,
        title: row.get(2)?,
        parent_id: row.get(3)?,
        archived: row.get(4)?,
        locked: row.get(5)?,
        pinned: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn message_row(row: &Row<'_>) -> rusqlite::Result<Message> {
    let metadata: String = row.get(6)?;
    let metadata = serde_json::from_str(&metadata).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(Message {
        id: row.get(0)?,
        project_id: row.get(1)?,
        role: row.get(2)?,
        content: row.get(3)?,
        created_at: row.get(4)?,
        metadata,
    })
}

impl Store {
    pub fn open(data_dir: &Path) -> Result<Self> {
        if data_dir.join(".harnessy").exists() {
            bail!("Choose a separate HarnessyXOR data directory; this is a HarnessyXO workspace");
        }
        if fs::symlink_metadata(data_dir)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            bail!("Application data directory cannot be a symbolic link");
        }
        private_directory(data_dir)?;
        let data_dir = fs::canonicalize(data_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if fs::metadata(&data_dir)?.uid() != unsafe { libc::geteuid() } {
                bail!("Application data directory must belong to the current account");
            }
            fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700))?;
        }
        let db_path = data_dir.join("harnessyxor.db");
        if fs::symlink_metadata(&db_path)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
        {
            bail!("The application database cannot be a symbolic link");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if db_path.exists() && fs::metadata(&db_path)?.uid() != unsafe { libc::geteuid() } {
                bail!("Application database must belong to the current account");
            }
        }
        log("OPEN database", &db_path, "");
        let connection = Connection::open(&db_path).context("Cannot open HarnessyXOR database")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&db_path, fs::Permissions::from_mode(0o600))?;
        }
        connection.busy_timeout(Duration::from_secs(10))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if version > SCHEMA {
            bail!("This database needs a newer HarnessyXOR version");
        }
        let application: i64 =
            connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let table_count: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if application != 0x48584f52 && (application != 0 || table_count != 0) {
            bail!("This file is not a HarnessyXOR database; it has not been modified");
        }
        let health: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if health != "ok" {
            bail!("The HarnessyXOR database failed its integrity check; keep it for recovery");
        }
        let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if mode.to_lowercase() != "wal" {
            bail!("This database needs local storage that supports SQLite WAL");
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        log("VERIFY database", &db_path, "integrity and WAL");
        log(
            "CREATE database tables",
            &db_path,
            "workspaces projects messages kv memory bookmarks",
        );
        connection.execute_batch("
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS workspaces (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, path TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
                hidden INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                title TEXT NOT NULL, parent_id TEXT REFERENCES projects(id) ON DELETE RESTRICT,
                archived INTEGER NOT NULL DEFAULT 0, locked INTEGER NOT NULL DEFAULT 0,
                pinned INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                CHECK(parent_id IS NULL OR parent_id <> id)
            );
            CREATE INDEX IF NOT EXISTS projects_workspace ON projects(workspace_id, archived, pinned, updated_at);
            CREATE INDEX IF NOT EXISTS projects_parent ON projects(parent_id);
            CREATE TABLE IF NOT EXISTS messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                role TEXT NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS messages_project ON messages(project_id, sequence);
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS memory (
                project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
                value_json TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bookmarks (
                message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL
            );
            PRAGMA application_id=1213747026;
            COMMIT;
        ")?;
        let has_hidden = {
            let mut statement = connection.prepare("PRAGMA table_info(workspaces)")?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            columns.iter().any(|column| column == "hidden")
        };
        if !has_hidden {
            log(
                "MIGRATE database",
                &db_path,
                "workspaces hidden import state; schema/2",
            );
            connection.execute_batch("BEGIN IMMEDIATE; ALTER TABLE workspaces ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0; PRAGMA user_version=2; COMMIT;")?;
        } else {
            connection.pragma_update(None, "user_version", SCHEMA)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&db_path, fs::Permissions::from_mode(0o600))?;
        }
        log(
            "CREATE project team schema and immutable histories",
            &db_path,
            "schema/3",
        );
        crate::team::init_schema(&connection)?;
        connection.pragma_update(None, "user_version", SCHEMA)?;
        Ok(Self {
            data_dir,
            db_path,
            connection: Mutex::new(connection),
        })
    }

    pub(crate) fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("Database access was interrupted; restart HarnessyXOR"))
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn settings(&self) -> Result<Settings> {
        log("READ settings", &self.db_path, "kv/settings");
        match self.kv_get("settings")? {
            Some(value) => serde_json::from_value(value)
                .context("Saved settings are invalid; they have not been replaced"),
            None => Ok(Settings::default()),
        }
    }

    pub fn save_settings(&self, settings: &Settings) -> Result<()> {
        if !(1..=64).contains(&settings.agent_slots) {
            bail!("Global Agent Slots must be between 1 and 64");
        }
        if settings.max_steps > 100_000 {
            bail!("Step limit cannot exceed 100000");
        }
        if settings.profiles.is_empty() || settings.profiles.len() > 256 {
            bail!("Save between 1 and 256 model profiles");
        }
        let mut ids = HashSet::new();
        for profile in &settings.profiles {
            valid_id(&profile.id)?;
            if !ids.insert(&profile.id) {
                bail!("Model profile IDs must be unique");
            }
            if profile.context_window < 512 || profile.context_window > 16_777_216 {
                bail!("Context window must be between 512 and 16777216 tokens");
            }
            if profile.max_output_tokens == 0 || profile.max_output_tokens >= profile.context_window
            {
                bail!("Output token limit must be positive and below the context window");
            }
            let url = url::Url::parse(&profile.base_url).context("Model base URL is invalid")?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                bail!(
                    "Model base URL must be an HTTP(S) URL without credentials, query or fragment"
                );
            }
            if let Some(temperature) = profile.temperature {
                if !temperature.is_finite() || !(0.0..=10.0).contains(&temperature) {
                    bail!("Temperature must be between 0 and 10");
                }
            }
            if !matches!(
                profile.api_mode.as_str(),
                "openai" | "ollama" | "auto" | "llamacpp" | "omlx"
            ) {
                bail!("Unsupported model API mode");
            }
            if profile.api_key.len() > 65536 || profile.api_key.contains(['\r', '\n']) {
                bail!("API key has an invalid length or line break");
            }
        }
        if !ids.contains(&settings.active_profile_id) {
            bail!("Choose a saved active model profile");
        }
        let permissions = settings
            .tool_permissions
            .as_object()
            .ok_or_else(|| anyhow!("Tool permissions must be an object"))?;
        if permissions
            .values()
            .any(|value| !matches!(value.as_str(), Some("on" | "off" | "ask")))
        {
            bail!("Tool permissions must be on, off or ask");
        }
        log(
            "WRITE settings",
            &self.db_path,
            "kv/settings; credentials omitted",
        );
        self.kv_set("settings", &serde_json::to_value(settings)?)
    }

    pub fn workspaces(&self) -> Result<Vec<Workspace>> {
        log("READ database", &self.db_path, "workspaces");
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id,name,path,created_at FROM workspaces WHERE hidden=0 ORDER BY created_at,id",
        )?;
        let result = statement
            .query_map([], workspace_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(result)
    }

    /// Retained import stages for explicit local recovery, never diagnostics.
    pub fn pending_imports(&self) -> Result<Vec<Value>> {
        log(
            "READ database",
            &self.db_path,
            "pending import records; paths for local recovery",
        );
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let workspaces = {
            let mut statement = transaction.prepare("SELECT id,name,path,created_at FROM workspaces WHERE hidden=1 ORDER BY created_at,id")?;
            let rows = statement
                .query_map([], workspace_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let mut result = Vec::new();
        for workspace in workspaces {
            let details: Option<String> = transaction
                .query_row(
                    "SELECT value_json FROM kv WHERE key=?1",
                    [format!("legacy_import_pending/{}", workspace.id)],
                    |row| row.get(0),
                )
                .optional()?;
            let details = details
                .map(|value| serde_json::from_str::<Value>(&value))
                .transpose()?
                .unwrap_or(Value::Null);
            let completion_recorded: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM kv WHERE key=?1)",
                [format!("legacy_import/{}", workspace.id)],
                |row| row.get(0),
            )?;
            result.push(json!({"workspace":workspace,"details":details,"completion_recorded":completion_recorded,"state":"pending"}));
        }
        transaction.commit()?;
        Ok(result)
    }

    pub fn workspace(&self, workspace_id: &str) -> Result<Workspace> {
        self.workspace_record(workspace_id, false)
    }

    fn workspace_record(&self, workspace_id: &str, include_hidden: bool) -> Result<Workspace> {
        valid_id(workspace_id)?;
        log(
            "READ database",
            &self.db_path,
            &format!("workspaces/{workspace_id}"),
        );
        self.connection()?
            .query_row(
                "SELECT id,name,path,created_at FROM workspaces WHERE id=?1 AND (?2 OR hidden=0)",
                params![workspace_id, include_hidden],
                workspace_row,
            )
            .optional()?
            .ok_or_else(|| anyhow!("Workspace not found"))
    }

    pub fn create_workspace(&self, name: &str, path: &Path) -> Result<Workspace> {
        self.create_workspace_impl(name, path, false)
    }

    fn create_workspace_impl(&self, name: &str, path: &Path, hidden: bool) -> Result<Workspace> {
        let name = label(name, "Workspace name")?;
        if path.as_os_str().is_empty() {
            bail!("Choose a workspace directory");
        }
        let expected = absolute_destination(path)?;
        if expected.starts_with(&self.data_dir) || self.data_dir.starts_with(&expected) {
            bail!("Keep workspace files separate from the application data directory");
        }
        let root_existed = path.exists();
        if path.exists() {
            if !path.is_dir() {
                bail!("Workspace path is not a directory");
            }
            if fs::symlink_metadata(path)?.file_type().is_symlink() {
                bail!("Workspace path cannot be a symbolic link");
            }
            if fs::read_dir(path)?.next().is_some() {
                bail!(
                    "Choose an empty directory for a new workspace; use Import for HarnessyXO data"
                );
            }
        }
        private_directory(path)?;
        let root = fs::canonicalize(path)?;
        if root.starts_with(&self.data_dir) || self.data_dir.starts_with(&root) {
            bail!("Keep workspace files separate from the application data directory");
        }
        let workspace = Workspace {
            id: id(),
            name,
            path: root.to_string_lossy().into_owned(),
            created_at: now(),
        };
        let meta = root.join(".harnessyxor");
        let marker = meta.join("workspace.json");
        let mut created = Vec::new();
        let result = (|| -> Result<()> {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let registered: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspaces WHERE path=?1)",
                [&workspace.path],
                |row| row.get(0),
            )?;
            if registered {
                bail!("This directory is already registered; its workspace identity has not been replaced");
            }
            let paths = {
                let mut statement = transaction.prepare("SELECT path FROM workspaces")?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows
            };
            if paths.iter().any(|path| {
                let path = Path::new(path);
                root.starts_with(path) || path.starts_with(&root)
            }) {
                bail!("Workspace directories cannot overlap; choose a separate directory");
            }
            // Claim each child exclusively. Never replace files from a concurrent creator.
            for directory in [&meta, &root.join("Files"), &root.join("References")] {
                create_directory(directory)?;
                created.push(directory.to_path_buf());
            }
            log("WRITE workspace marker", &marker, &workspace.id);
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&marker)?;
            file.write_all(&serde_json::to_vec_pretty(
                &json!({"application":"HarnessyXOR","schema":SCHEMA,"workspace_id":workspace.id}),
            )?)?;
            file.sync_all()?;
            sync_directory(&meta)?;
            sync_directory(&root)?;
            if let Some(parent) = root.parent() {
                sync_directory(parent)?;
            }
            log(
                "INSERT database",
                &self.db_path,
                &format!("workspaces/{}", workspace.id),
            );
            transaction.execute(
                "INSERT INTO workspaces(id,name,path,created_at,hidden) VALUES(?1,?2,?3,?4,?5)",
                params![
                    workspace.id,
                    workspace.name,
                    workspace.path,
                    workspace.created_at,
                    hidden
                ],
            )?;
            transaction.commit()?;
            Ok(())
        })();
        if let Err(error) = result {
            // Delete only children this attempt created, and only while empty.
            if created.iter().any(|directory| directory == &meta) {
                let _ = fs::remove_file(&marker);
            }
            for directory in created.into_iter().rev() {
                let _ = fs::remove_dir(directory);
            }
            if !root_existed {
                let _ = fs::remove_dir(&root);
            }
            return Err(error);
        }
        Ok(workspace)
    }

    fn workspace_root(&self, workspace_id: &str) -> Result<PathBuf> {
        self.workspace_root_impl(workspace_id, false)
    }

    fn workspace_root_impl(&self, workspace_id: &str, include_hidden: bool) -> Result<PathBuf> {
        let workspace = self.workspace_record(workspace_id, include_hidden)?;
        let root = PathBuf::from(&workspace.path);
        let actual = fs::canonicalize(&root)
            .context("Workspace directory is missing; it has not been recreated")?;
        if actual != root || !root.is_dir() {
            bail!("Workspace location changed; reopen it explicitly");
        }
        let marker_path = root.join(".harnessyxor/workspace.json");
        if fs::symlink_metadata(&marker_path)?.file_type().is_symlink() {
            bail!("Workspace marker cannot be a symbolic link");
        }
        let marker: Value = serde_json::from_slice(&fs::read(marker_path)?)?;
        if marker["application"] != "HarnessyXOR" || marker["workspace_id"] != workspace_id {
            bail!("Workspace identity changed; access is blocked");
        }
        Ok(root)
    }

    pub fn projects(&self, workspace_id: &str, include_archived: bool) -> Result<Vec<Project>> {
        self.workspace(workspace_id)?;
        log(
            "READ database",
            &self.db_path,
            &format!("projects workspace/{workspace_id}"),
        );
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT id,workspace_id,title,parent_id,archived,locked,pinned,created_at,updated_at FROM projects WHERE workspace_id=?1 AND (?2 OR archived=0) ORDER BY pinned DESC,updated_at DESC,id")?;
        let result = statement
            .query_map(params![workspace_id, include_archived], project_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(result)
    }

    pub fn project(&self, project_id: &str) -> Result<Project> {
        valid_id(project_id)?;
        log(
            "READ database",
            &self.db_path,
            &format!("projects/{project_id}"),
        );
        self.connection()?.query_row("SELECT p.id,p.workspace_id,p.title,p.parent_id,p.archived,p.locked,p.pinned,p.created_at,p.updated_at FROM projects p JOIN workspaces w ON w.id=p.workspace_id WHERE p.id=?1 AND w.hidden=0", [project_id], project_row)
            .optional()?.ok_or_else(|| anyhow!("Project not found"))
    }

    pub fn create_project(
        &self,
        workspace_id: &str,
        title: &str,
        parent_id: Option<&str>,
    ) -> Result<Project> {
        if parent_id.is_some() {
            bail!(
                "Team agents belong to one project; create a teammate instead of a child project"
            );
        }
        self.create_project_impl(workspace_id, title, None, false)
    }

    fn create_project_impl(
        &self,
        workspace_id: &str,
        title: &str,
        parent_id: Option<&str>,
        include_hidden: bool,
    ) -> Result<Project> {
        let title = label(title, "Project title")?;
        let root = self.workspace_root_impl(workspace_id, include_hidden)?;
        if let Some(parent_id) = parent_id {
            let parent = self.project(parent_id)?;
            if parent.workspace_id != workspace_id {
                bail!("Parent project must belong to this workspace");
            }
            if parent.archived {
                bail!("Restore the parent project before adding a child");
            }
        }
        let timestamp = now();
        let project = Project {
            id: id(),
            workspace_id: workspace_id.into(),
            title,
            parent_id: parent_id.map(str::to_string),
            archived: false,
            locked: false,
            pinned: false,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        let files = root.join("Files");
        if fs::canonicalize(&files)? != files {
            bail!("Workspace Files directory changed");
        }
        let project_root = files.join(format!("Project-{}_output", project.id));
        let mut created = false;
        let result = (|| -> Result<()> {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if let Some(parent_id) = parent_id {
                let allowed:bool=transaction.query_row("SELECT EXISTS(SELECT 1 FROM projects WHERE id=?1 AND workspace_id=?2 AND archived=0)",params![parent_id,workspace_id],|row|row.get(0))?;
                if !allowed {
                    bail!(
                        "Parent project changed or was archived; reopen it before adding a child"
                    );
                }
            }
            create_directory(&project_root)?;
            created = true;
            sync_directory(&files)?;
            log(
                "INSERT database",
                &self.db_path,
                &format!("projects/{}", project.id),
            );
            transaction.execute("INSERT INTO projects(id,workspace_id,title,parent_id,archived,locked,pinned,created_at,updated_at) VALUES(?1,?2,?3,?4,0,0,0,?5,?6)",params![project.id,project.workspace_id,project.title,project.parent_id,project.created_at,project.updated_at])?;
            crate::team::init_project(&transaction, &project.id)?;
            transaction.commit()?;
            Ok(())
        })();
        if let Err(error) = result {
            if created {
                let _ = fs::remove_dir(&project_root);
            }
            return Err(error);
        }
        Ok(project)
    }

    pub fn update_project(&self, project_id: &str, patch: &Value) -> Result<Project> {
        valid_id(project_id)?;
        let patch = patch
            .as_object()
            .ok_or_else(|| anyhow!("Project changes must be an object"))?;
        if patch
            .keys()
            .any(|key| !matches!(key.as_str(), "title" | "archived" | "locked" | "pinned"))
        {
            bail!("Unsupported project field");
        }
        log(
            "UPDATE database",
            &self.db_path,
            &format!("projects/{project_id}"),
        );
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old=transaction.query_row("SELECT p.id,p.workspace_id,p.title,p.parent_id,p.archived,p.locked,p.pinned,p.created_at,p.updated_at FROM projects p JOIN workspaces w ON w.id=p.workspace_id WHERE p.id=?1 AND w.hidden=0",[project_id],project_row).optional()?.ok_or_else(||anyhow!("Project not found"))?;
        let title = match patch.get("title") {
            Some(value) => label(
                value
                    .as_str()
                    .ok_or_else(|| anyhow!("Project title must be text"))?,
                "Project title",
            )?,
            None => old.title.clone(),
        };
        let boolean = |name: &str, default: bool| -> Result<bool> {
            match patch.get(name) {
                Some(value) => value
                    .as_bool()
                    .ok_or_else(|| anyhow!("{name} must be true or false")),
                None => Ok(default),
            }
        };
        let archived = boolean("archived", old.archived)?;
        let locked = boolean("locked", old.locked)?;
        let pinned = boolean("pinned", old.pinned)?;
        if !archived {
            if let Some(parent_id) = &old.parent_id {
                let parent_archived: bool = transaction.query_row(
                    "SELECT archived FROM projects WHERE id=?1",
                    [parent_id],
                    |row| row.get(0),
                )?;
                if parent_archived {
                    bail!("Restore the parent project first");
                }
            }
        }
        let timestamp = now();
        transaction.execute("UPDATE projects SET title=?1,archived=?2,locked=?3,pinned=?4,updated_at=?5 WHERE id=?6",params![title,archived,locked,pinned,timestamp,project_id])?;
        if old.archived != archived || old.locked != locked {
            transaction.execute("UPDATE team_project_state SET policy_revision=policy_revision+1,updated_at=?1 WHERE project_id=?2",params![timestamp,project_id])?;
        }
        if old.archived != archived {
            log(
                "UPDATE database",
                &self.db_path,
                &format!("projects/{project_id} descendants archive cascade"),
            );
            transaction.execute("WITH RECURSIVE descendants(id) AS (SELECT id FROM projects WHERE parent_id=?1 UNION ALL SELECT p.id FROM projects p JOIN descendants d ON p.parent_id=d.id) UPDATE projects SET archived=?2,updated_at=?3 WHERE id IN (SELECT id FROM descendants)",params![project_id,archived,now()])?;
        }
        transaction.commit()?;
        Ok(Project {
            title,
            archived,
            locked,
            pinned,
            updated_at: timestamp,
            ..old
        })
    }

    pub fn project_root(&self, project_id: &str) -> Result<PathBuf> {
        let project = self.project(project_id)?;
        let workspace = self.workspace_root(&project.workspace_id)?;
        let files = workspace.join("Files");
        let root = files.join(format!("Project-{}_output", project.id));
        let actual = fs::canonicalize(&root)
            .context("Project directory is missing; it has not been recreated")?;
        if fs::canonicalize(&files)? != files || actual != root || !root.is_dir() {
            bail!("Project directory moved or became a symbolic link");
        }
        Ok(root)
    }

    pub fn ensure_writable(&self, project_id: &str) -> Result<Project> {
        let project = self.project(project_id)?;
        if project.archived {
            bail!("Archived projects are read-only");
        }
        if project.locked {
            bail!("Locked projects are read-only");
        }
        self.project_root(project_id)?;
        Ok(project)
    }

    pub fn messages(&self, project_id: &str) -> Result<Vec<Message>> {
        let architect = self.architect(project_id)?;
        self.agent_messages(project_id, &architect.id)
    }

    pub fn agent_messages(&self, project_id: &str, agent_id: &str) -> Result<Vec<Message>> {
        self.project(project_id)?;
        let actor = self.team_agent(project_id, agent_id)?;
        let inherit_architect_lineage =
            actor.role == "architect" && self.architect(project_id)?.id == agent_id;
        log(
            "READ database",
            &self.db_path,
            &format!("messages project/{project_id}"),
        );
        let connection = self.connection()?;
        let mut statement=connection.prepare("WITH RECURSIVE lineage(id) AS (SELECT ?2 UNION ALL SELECT a.replaces_agent_id FROM team_agents a JOIN lineage l ON a.id=l.id WHERE ?3 AND a.role='architect' AND a.replaces_agent_id IS NOT NULL) SELECT id,project_id,role,content,created_at,sequence,metadata_json FROM messages WHERE project_id=?1 AND json_extract(metadata_json,'$.agent_id') IN (SELECT id FROM lineage) ORDER BY sequence")?;
        let result = statement
            .query_map(
                params![project_id, agent_id, inherit_architect_lineage],
                message_row,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(result)
    }

    pub fn add_message(
        &self,
        project_id: &str,
        role: &str,
        content: &str,
        metadata: Value,
    ) -> Result<Message> {
        let architect = self.architect(project_id)?;
        self.add_agent_message(project_id, &architect.id, role, content, metadata)
    }

    pub fn add_agent_message(
        &self,
        project_id: &str,
        agent_id: &str,
        role: &str,
        content: &str,
        mut metadata: Value,
    ) -> Result<Message> {
        let agent = self.team_agent(project_id, agent_id)?;
        if agent.retired {
            bail!("Team agent is retired; its transcript remains available");
        }
        if !metadata.is_object() {
            metadata = json!({});
        }
        metadata["agent_id"] = json!(agent_id);
        metadata["agent_role"] = json!(agent.role);
        self.ensure_writable(project_id)?;
        if !matches!(
            role,
            "system" | "developer" | "user" | "assistant" | "tool" | "event"
        ) {
            bail!("Unsupported message role");
        }
        if content.len() > MAX_TEXT {
            bail!("Message exceeds the 64 MiB limit");
        }
        let metadata_json = encode(&metadata)?;
        let message = Message {
            id: id(),
            project_id: project_id.into(),
            role: role.into(),
            content: content.into(),
            created_at: now(),
            metadata,
        };
        log(
            "INSERT database",
            &self.db_path,
            &format!("messages/{} project/{project_id}", message.id),
        );
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        // Recheck the write guard in the same transaction as the insert.
        let blocked: bool = transaction.query_row(
            "SELECT archived OR locked FROM projects WHERE id=?1",
            [project_id],
            |row| row.get(0),
        )?;
        if blocked {
            bail!("Project became read-only");
        }
        crate::team::assert_active_agent(&transaction, project_id, agent_id)?;
        if role == "user" && message.metadata["trusted_user_origin"] == true {
            crate::team::user_guidance_received(&transaction, project_id, agent_id, &message.id)?;
        }
        transaction.execute("INSERT INTO messages(id,project_id,role,content,created_at,metadata_json) VALUES(?1,?2,?3,?4,?5,?6)",params![message.id,project_id,role,content,message.created_at,metadata_json])?;
        transaction.execute(
            "UPDATE projects SET updated_at=?1 WHERE id=?2",
            params![message.created_at, project_id],
        )?;
        transaction.commit()?;
        Ok(message)
    }

    /// Accept one prompt only when its message and durable queue state all commit.
    pub fn enqueue_run(
        &self,
        project_id: &str,
        prompt: &str,
        run_snapshot: &Value,
        run_index: &Value,
    ) -> Result<Message> {
        let architect = self.architect(project_id)?;
        self.enqueue_user_run(project_id, &architect.id, prompt, run_snapshot, run_index)
    }

    pub fn enqueue_agent_run(
        &self,
        project_id: &str,
        agent_id: &str,
        prompt: &str,
        run_snapshot: &Value,
        run_index: &Value,
    ) -> Result<Message> {
        self.enqueue_scoped_run(project_id, agent_id, prompt, run_snapshot, run_index, false)
    }
    pub fn enqueue_user_run(
        &self,
        project_id: &str,
        agent_id: &str,
        prompt: &str,
        run_snapshot: &Value,
        run_index: &Value,
    ) -> Result<Message> {
        self.enqueue_scoped_run(project_id, agent_id, prompt, run_snapshot, run_index, true)
    }
    fn enqueue_scoped_run(
        &self,
        project_id: &str,
        agent_id: &str,
        prompt: &str,
        run_snapshot: &Value,
        run_index: &Value,
        trusted_user_origin: bool,
    ) -> Result<Message> {
        let agent = self.team_agent(project_id, agent_id)?;
        if agent.retired {
            bail!("Team agent is retired; it cannot accept work");
        }
        if let Some(snapshot_agent) = run_snapshot.get("agent_id").and_then(Value::as_str) {
            if snapshot_agent != agent_id {
                bail!("Team run identity does not match the assigned agent");
            }
        }
        self.ensure_writable(project_id)?;
        if prompt.trim().is_empty() || prompt.len() > 256 * 1024 {
            bail!("Prompt must contain 1 to 262144 bytes");
        }
        let run_id = run_snapshot["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Run snapshot needs an ID"))?;
        valid_id(run_id)?;
        uuid::Uuid::parse_str(run_id).context("Run ID must be a UUID")?;
        if run_snapshot["project_id"].as_str() != Some(project_id)
            || run_snapshot["status"] != "queued"
        {
            bail!("Run snapshot must be queued for this project");
        }
        let index = run_index
            .as_array()
            .ok_or_else(|| anyhow!("Run index must be an array"))?;
        if index.is_empty() || index.len() > 256 {
            bail!("Run index must contain 1 to 256 IDs");
        }
        let mut seen = HashSet::new();
        for value in index {
            let value = value
                .as_str()
                .ok_or_else(|| anyhow!("Run index must contain IDs"))?;
            valid_id(value)?;
            if !seen.insert(value) {
                bail!("Run index cannot contain duplicate IDs");
            }
        }
        if !seen.contains(run_id) {
            bail!("Run index must contain the accepted run");
        }
        let mut scoped_snapshot = run_snapshot.clone();
        scoped_snapshot["agent_id"] = json!(agent_id);
        scoped_snapshot["agent_role"] = json!(agent.role);
        let run_json = encode(&scoped_snapshot)?;
        let index_json = encode(run_index)?;
        let message = Message {
            id: id(),
            project_id: project_id.into(),
            role: "user".into(),
            content: prompt.into(),
            created_at: now(),
            metadata: json!({"run_id":run_id,"agent_id":agent_id,"agent_role":agent.role,"trusted_user_origin":trusted_user_origin,"origin":if trusted_user_origin{"user"}else{"controller"}}),
        };
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let blocked: bool = transaction.query_row(
            "SELECT archived OR locked FROM projects WHERE id=?1",
            [project_id],
            |row| row.get(0),
        )?;
        if blocked {
            bail!("Project became read-only");
        }
        crate::team::assert_active_agent(&transaction, project_id, agent_id)?;
        if trusted_user_origin {
            crate::team::user_guidance_received(&transaction, project_id, agent_id, &message.id)?;
        }
        let key = format!("run:{run_id}");
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM kv WHERE key=?1)",
            [&key],
            |row| row.get(0),
        )?;
        if exists {
            bail!("Run ID already exists; prompt was not accepted twice");
        }
        log(
            "INSERT database",
            &self.db_path,
            &format!("messages/{} project/{project_id}", message.id),
        );
        transaction.execute("INSERT INTO messages(id,project_id,role,content,created_at,metadata_json) VALUES(?1,?2,'user',?3,?4,?5)",params![message.id,project_id,prompt,message.created_at,serde_json::to_string(&message.metadata)?])?;
        log(
            "INSERT database",
            &self.db_path,
            &format!("kv run/{run_id}; values omitted"),
        );
        transaction.execute(
            "INSERT INTO kv(key,value_json,updated_at) VALUES(?1,?2,?3)",
            params![key, run_json, message.created_at],
        )?;
        log(
            "UPSERT database",
            &self.db_path,
            "kv runs:index; values omitted",
        );
        transaction.execute("INSERT INTO kv(key,value_json,updated_at) VALUES('runs:index',?1,?2) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",params![index_json,message.created_at])?;
        log(
            "UPDATE database",
            &self.db_path,
            &format!("projects/{project_id} timestamp"),
        );
        transaction.execute(
            "UPDATE projects SET updated_at=?1 WHERE id=?2",
            params![message.created_at, project_id],
        )?;
        transaction.commit()?;
        Ok(message)
    }

    pub fn kv_get(&self, key: &str) -> Result<Option<Value>> {
        if key.len() > 512 {
            bail!("Saved state key is too long");
        }
        log("READ database", &self.db_path, "kv");
        let value: Option<String> = self
            .connection()?
            .query_row("SELECT value_json FROM kv WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?;
        value
            .map(|value| serde_json::from_str(&value).context("Saved state JSON is invalid"))
            .transpose()
    }

    pub fn kv_set(&self, key: &str, value: &Value) -> Result<()> {
        if key.is_empty() || key.len() > 512 || key.chars().any(char::is_control) {
            bail!("Invalid saved state key");
        }
        let encoded = encode(value)?;
        log("UPSERT database", &self.db_path, "kv; values omitted");
        self.connection()?.execute("INSERT INTO kv(key,value_json,updated_at) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",params![key,encoded,now()])?;
        Ok(())
    }

    pub fn kv_delete(&self, key: &str) -> Result<()> {
        if key.len() > 512 {
            bail!("Saved state key is too long");
        }
        log("DELETE database", &self.db_path, "kv; values omitted");
        self.connection()?
            .execute("DELETE FROM kv WHERE key=?1", [key])?;
        Ok(())
    }

    pub fn kv_prefix(&self, prefix: &str) -> Result<Vec<(String, Value)>> {
        if prefix.len() > 512 {
            bail!("Saved state prefix is too long");
        }
        log("READ database", &self.db_path, "kv prefix; values omitted");
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT key,value_json FROM kv WHERE substr(key,1,length(?1))=?1 ORDER BY key",
        )?;
        let rows = statement
            .query_map([prefix], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(key, value)| Ok((key, serde_json::from_str(&value)?)))
            .collect()
    }

    pub fn memory(&self, project_id: &str) -> Result<Value> {
        self.project(project_id)?;
        log(
            "READ database",
            &self.db_path,
            &format!("memory project/{project_id}"),
        );
        let value: Option<String> = self
            .connection()?
            .query_row(
                "SELECT value_json FROM memory WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()?;
        match value {
            Some(value) => Ok(serde_json::from_str(&value)?),
            None => Ok(json!({})),
        }
    }

    pub fn save_memory(&self, project_id: &str, value: &Value) -> Result<()> {
        self.ensure_writable(project_id)?;
        let value = encode(value)?;
        log(
            "UPSERT database",
            &self.db_path,
            &format!("memory project/{project_id}"),
        );
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let blocked: bool = transaction.query_row(
            "SELECT archived OR locked FROM projects WHERE id=?1",
            [project_id],
            |row| row.get(0),
        )?;
        if blocked {
            bail!("Project became read-only");
        }
        transaction.execute("INSERT INTO memory(project_id,value_json,updated_at) VALUES(?1,?2,?3) ON CONFLICT(project_id) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",params![project_id,value,now()])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn search_messages(
        &self,
        project_id: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Message>> {
        self.project(project_id)?;
        let architect = self.architect(project_id)?;
        if query.is_empty() || query.len() > 4096 {
            bail!("Search text must contain 1 to 4096 bytes");
        }
        log(
            "READ database",
            &self.db_path,
            &format!("messages search project/{project_id}; query omitted"),
        );
        let connection = self.connection()?;
        let mut statement=connection.prepare("WITH RECURSIVE lineage(id) AS (SELECT ?4 UNION ALL SELECT a.replaces_agent_id FROM team_agents a JOIN lineage l ON a.id=l.id WHERE a.role='architect' AND a.replaces_agent_id IS NOT NULL) SELECT id,project_id,role,content,created_at,sequence,metadata_json FROM messages WHERE project_id=?1 AND json_extract(metadata_json,'$.agent_id') IN(SELECT id FROM lineage) AND instr(lower(content),lower(?2))>0 ORDER BY sequence DESC LIMIT ?3")?;
        let result = statement
            .query_map(
                params![project_id, query, limit.clamp(1, 200) as i64, architect.id],
                message_row,
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(result)
    }

    pub fn bookmark_message(
        &self,
        project_id: &str,
        message_id: &str,
        enabled: bool,
    ) -> Result<()> {
        self.ensure_writable(project_id)?;
        valid_id(message_id)?;
        let mut connection = self.connection()?;
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let blocked: bool = transaction.query_row(
            "SELECT archived OR locked FROM projects WHERE id=?1",
            [project_id],
            |row| row.get(0),
        )?;
        if blocked {
            bail!("Project became read-only");
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE id=?1 AND project_id=?2)",
            params![message_id, project_id],
            |row| row.get(0),
        )?;
        if !exists {
            bail!("Message not found in this project");
        }
        log(
            if enabled {
                "INSERT database"
            } else {
                "DELETE database"
            },
            &self.db_path,
            &format!("bookmarks/{message_id}"),
        );
        if enabled {
            transaction.execute("INSERT OR IGNORE INTO bookmarks(message_id,project_id,created_at) VALUES(?1,?2,?3)",params![message_id,project_id,now()])?;
        } else {
            transaction.execute(
                "DELETE FROM bookmarks WHERE message_id=?1 AND project_id=?2",
                params![message_id, project_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn bookmarks(&self, project_id: &str) -> Result<Vec<String>> {
        self.project(project_id)?;
        log(
            "READ database",
            &self.db_path,
            &format!("bookmarks project/{project_id}"),
        );
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT message_id FROM bookmarks WHERE project_id=?1 ORDER BY created_at,message_id",
        )?;
        let result = statement
            .query_map([project_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(result)
    }

    pub fn export_project(&self, project_id: &str) -> Result<Value> {
        valid_id(project_id)?;
        log(
            "READ database snapshot",
            &self.db_path,
            &format!("project/{project_id} messages memory bookmarks"),
        );
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let project=transaction.query_row("SELECT p.id,p.workspace_id,p.title,p.parent_id,p.archived,p.locked,p.pinned,p.created_at,p.updated_at FROM projects p JOIN workspaces w ON w.id=p.workspace_id WHERE p.id=?1 AND w.hidden=0",[project_id],project_row).optional()?.ok_or_else(||anyhow!("Project not found"))?;
        let architect: String = transaction.query_row(
            "SELECT architect_agent_id FROM team_project_state WHERE project_id=?1",
            [project_id],
            |r| r.get(0),
        )?;
        let messages = {
            let mut statement=transaction.prepare("WITH RECURSIVE lineage(id) AS (SELECT ?2 UNION ALL SELECT a.replaces_agent_id FROM team_agents a JOIN lineage l ON a.id=l.id WHERE a.role='architect' AND a.replaces_agent_id IS NOT NULL) SELECT id,project_id,role,content,created_at,sequence,metadata_json FROM messages WHERE project_id=?1 AND json_extract(metadata_json,'$.agent_id') IN(SELECT id FROM lineage) ORDER BY sequence")?;
            let rows = statement
                .query_map(params![project_id, architect], message_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let memory: Option<String> = transaction
            .query_row(
                "SELECT value_json FROM memory WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()?;
        let memory = match memory {
            Some(value) => serde_json::from_str(&value)?,
            None => json!({}),
        };
        let bookmarks = {
            let mut statement=transaction.prepare("SELECT message_id FROM bookmarks WHERE project_id=?1 ORDER BY created_at,message_id")?;
            let rows = statement
                .query_map([project_id], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        transaction.commit()?;
        Ok(
            json!({"format":"harnessyxor-project","version":1,"exported_at":now(),"project":project,"messages":messages,"memory":memory,"bookmarks":bookmarks}),
        )
    }

    /// Export a coherent saved conversation; omitted sections leave no hidden payload.
    pub fn export_markdown(
        &self,
        project_id: &str,
        include_reasoning: bool,
        include_tools: bool,
        include_stats: bool,
    ) -> Result<String> {
        let snapshot = self.export_project(project_id)?;
        let project: Project = serde_json::from_value(snapshot["project"].clone())?;
        let messages: Vec<Message> = serde_json::from_value(snapshot["messages"].clone())?;
        let heading = project.title.replace(['\n', '\r'], " ");
        let mut output = format!(
            "# {heading}\n\nExported: {}\n\n",
            snapshot["exported_at"].as_str().unwrap_or_default()
        );
        for message in messages {
            if message.role == "tool" && !include_tools {
                continue;
            }
            let role = match message.role.as_str() {
                "user" => "You",
                "assistant" => "Assistant",
                "tool" => "Tool result",
                "event" => "Event",
                "system" => "System",
                "developer" => "Instructions",
                _ => "Message",
            };
            output.push_str(&format!(
                "## {role} · {}\n\n",
                message.created_at.replace(['\n', '\r'], " ")
            ));
            if !message.content.is_empty() {
                output.push_str(&message.content);
                output.push_str("\n\n");
            }
            if include_reasoning {
                if let Some(reasoning) = message
                    .metadata
                    .get("reasoning")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    output.push_str("### Thinking\n\n");
                    output.push_str(reasoning);
                    output.push_str("\n\n");
                }
            }
            if include_tools {
                if let Some(calls) = message
                    .metadata
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .filter(|calls| !calls.is_empty())
                {
                    output.push_str("### Tool calls\n\n");
                    let text = serde_json::to_string_pretty(calls)?;
                    let fence = markdown_fence(&text);
                    output.push_str(&format!("{fence}json\n{text}\n{fence}\n\n"));
                }
            }
            if include_stats {
                if let Some(metrics) = message.metadata.get("metrics").and_then(Value::as_object) {
                    let mut parts = Vec::new();
                    for (key, title) in [
                        ("prompt_tokens", "Prompt tokens"),
                        ("completion_tokens", "Output tokens"),
                        ("cached_tokens", "Cached tokens"),
                        ("total_ms", "Elapsed ms"),
                        ("first_token_ms", "First token ms"),
                        ("tokens_per_second", "Tokens/s"),
                        ("prompt_tokens_per_second", "Prompt tokens/s"),
                        ("cache_hit_percent", "Cache hit %"),
                    ] {
                        if let Some(value) = metrics.get(key).filter(|value| value.is_number()) {
                            parts.push(format!("{title}: {value}"));
                        }
                    }
                    if !parts.is_empty() {
                        output.push_str(&parts.join(" · "));
                        output.push_str("\n\n");
                    }
                }
            }
            if output.len() > MAX_TEXT {
                bail!("Markdown export exceeds the 64 MiB limit; export fewer sections");
            }
        }
        Ok(output)
    }

    /// Import a coherent legacy snapshot and copies of files into a new workspace.
    /// The original database, VMs and files are never opened for writing or started.
    pub fn import_legacy(
        &self,
        source: &Path,
        destination: &Path,
        name: Option<&str>,
    ) -> Result<Value> {
        let source =
            fs::canonicalize(source).context("HarnessyXO source directory does not exist")?;
        let legacy_db = source.join(".harnessy/workspace.db");
        if !legacy_db.is_file() {
            bail!("This directory does not contain a HarnessyXO workspace database");
        }
        let metadata_path = source.join(".harnessy/workspace.json");
        log("READ legacy workspace metadata", &metadata_path, "");
        let metadata: Value = serde_json::from_slice(
            &fs::read(&metadata_path).context("Legacy workspace metadata is missing")?,
        )
        .context("Legacy workspace metadata is invalid")?;
        if !matches!(metadata["schema_version"].as_u64(), Some(14 | 15)) {
            bail!("Only HarnessyXO workspace schemas 14 and 15 can be imported by this version");
        }
        if destination.exists() {
            bail!("Import destination must not exist");
        }
        let destination = absolute_destination(destination)?;
        if destination.starts_with(&source)
            || source.starts_with(&destination)
            || destination.starts_with(&self.data_dir)
            || self.data_dir.starts_with(&destination)
        {
            bail!("Import destination must be separate from the source and application data");
        }
        let parent = destination
            .parent()
            .ok_or_else(|| anyhow!("Choose a destination with a parent directory"))?;
        private_directory(parent)?;
        let stage = parent.join(format!(".harnessyxor-import-{}", id()));
        let workspace =
            self.create_workspace_impl(name.unwrap_or("Imported HarnessyXO"), &stage, true)?;
        let result = (|| -> Result<Value> {
            self.kv_set(&format!("legacy_import_pending/{}",workspace.id), &json!({"source":source,"destination":destination,"stage":stage,"started_at":now()}))?;
            let mut report = self.import_legacy_into(&source, &stage, &workspace)?;
            let mut connection = self.connection()?;
            let transaction = connection.transaction()?;
            log(
                "UPDATE database",
                &self.db_path,
                &format!("workspaces/{} destination", workspace.id),
            );
            transaction.execute(
                "UPDATE workspaces SET path=?1,hidden=0 WHERE id=?2",
                params![destination.to_string_lossy(), workspace.id],
            )?;
            transaction.execute(
                "DELETE FROM kv WHERE key=?1",
                [format!("legacy_import_pending/{}", workspace.id)],
            )?;
            log(
                "MOVE completed import",
                &stage,
                &format!("to {}", destination.display()),
            );
            rename_noreplace(&stage, &destination)
                .context("Cannot install completed import; destination may already exist")?;
            transaction.commit()?;
            drop(connection);
            log("VERIFY imported workspace", &destination, &workspace.id);
            report["workspace"] = serde_json::to_value(self.workspace(&workspace.id)?)?;
            Ok(report)
        })();
        match result {
            Ok(report) => Ok(report),
            Err(error) => {
                if let Err(cleanup) = self.rollback_import(&workspace, &stage, &destination) {
                    return Err(error.context(format!("Import failed; original HarnessyXO files were not changed. Incomplete import needs cleanup: {cleanup}")));
                }
                Err(error.context("Import failed; original HarnessyXO files were not changed"))
            }
        }
    }

    fn rollback_import(
        &self,
        workspace: &Workspace,
        stage: &Path,
        destination: &Path,
    ) -> Result<()> {
        log("ROLLBACK imported workspace", &self.db_path, &workspace.id);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM kv WHERE key IN (SELECT 'legacy_project/' || id FROM projects WHERE workspace_id=?1)",[&workspace.id])?;
        transaction.execute(
            "DELETE FROM kv WHERE key=?1 OR key=?2",
            params![
                format!("legacy_import/{}", workspace.id),
                format!("workspace_memory/{}", workspace.id)
            ],
        )?;
        transaction.execute(
            "DELETE FROM kv WHERE key=?1",
            [format!("legacy_import_pending/{}", workspace.id)],
        )?;
        transaction.execute(
            "UPDATE projects SET parent_id=NULL WHERE workspace_id=?1",
            [&workspace.id],
        )?;
        transaction.execute(
            "DELETE FROM projects WHERE workspace_id=?1",
            [&workspace.id],
        )?;
        transaction.execute("DELETE FROM workspaces WHERE id=?1", [&workspace.id])?;
        transaction.commit()?;
        drop(connection);
        for path in [stage, destination] {
            if !path.exists() {
                continue;
            }
            let marker = path.join(".harnessyxor/workspace.json");
            let owned = fs::read(&marker)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .map(|value| value["workspace_id"] == workspace.id)
                .unwrap_or(false);
            if !owned {
                continue;
            }
            if fs::symlink_metadata(path)?.file_type().is_symlink() {
                bail!("Incomplete import path became a symbolic link; it has been retained");
            }
            log("DELETE incomplete import", path, &workspace.id);
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    fn import_legacy_into(
        &self,
        source: &Path,
        stage: &Path,
        workspace: &Workspace,
    ) -> Result<Value> {
        let preserved = stage.join(".legacy-harnessyxo");
        private_directory(&preserved)?;
        copy_tree(
            source,
            &preserved,
            Some(&source.join(".harnessy/workspace.db")),
        )?;
        let snapshot = preserved.join(".harnessy/workspace.db");
        if let Some(parent) = snapshot.parent() {
            private_directory(parent)?;
        }
        log(
            "READ legacy database",
            &source.join(".harnessy/workspace.db"),
            "read-only snapshot",
        );
        let legacy = Connection::open_with_flags(
            source.join(".harnessy/workspace.db"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        legacy.busy_timeout(Duration::from_secs(5))?;
        let mut destination = Connection::open(&snapshot)?;
        log(
            "BACKUP database",
            &snapshot,
            "coherent legacy snapshot including committed WAL rows",
        );
        {
            let backup = rusqlite::backup::Backup::new(&legacy, &mut destination)?;
            let deadline = std::time::Instant::now() + Duration::from_secs(300);
            let mut last_report = std::time::Instant::now();
            loop {
                if std::time::Instant::now() >= deadline {
                    bail!("Legacy database snapshot did not finish within five minutes; close HarnessyXO and retry");
                }
                match backup.step(256)? {
                    rusqlite::backup::StepResult::Done => break,
                    rusqlite::backup::StepResult::More => {}
                    rusqlite::backup::StepResult::Busy | rusqlite::backup::StepResult::Locked => {
                        std::thread::sleep(Duration::from_millis(20))
                    }
                    _ => bail!("Legacy snapshot returned an unsupported backup status"),
                }
                if last_report.elapsed() >= Duration::from_secs(5) {
                    let progress = backup.progress();
                    log(
                        "BACKUP database progress",
                        &snapshot,
                        &format!(
                            "pages {}/{}",
                            progress.pagecount - progress.remaining,
                            progress.pagecount
                        ),
                    );
                    last_report = std::time::Instant::now();
                }
            }
        }
        drop(destination);
        drop(legacy);
        let legacy = Connection::open_with_flags(
            &snapshot,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let healthy: String = legacy.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if healthy != "ok" {
            bail!("Legacy database snapshot failed its integrity check");
        }
        let mut statement=legacy.prepare("SELECT id,title,created_at,updated_at,active_root,archived_at,pinned,locked FROM chats ORDER BY rowid")?;
        let rows = statement
            .query_map([], |row| {
                Ok(LegacyProject {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                    active_root: row.get(4)?,
                    archived: row.get::<_, Option<String>>(5)?.is_some(),
                    pinned: row.get(6)?,
                    locked: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut mapping = HashMap::new();
        let mut total_messages = 0usize;
        let mut total_files = 0usize;
        let mut warnings = Vec::<String>::new();
        for old in &rows {
            valid_id(&old.id)?;
            let project = self.create_project_impl(&workspace.id, &old.title, None, true)?;
            mapping.insert(old.id.clone(), project.id.clone());
            let mut messages=legacy.prepare("SELECT id,role,content,created_at,name,tool_call_id,tool_calls_json,metadata_json FROM messages WHERE chat_id=?1 ORDER BY rowid")?;
            let old_messages = messages
                .query_map([&old.id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut connection = self.connection()?;
            let transaction = connection.transaction()?;
            log(
                "INSERT database",
                &self.db_path,
                &format!(
                    "messages legacy project/{} rows/{}",
                    project.id,
                    old_messages.len()
                ),
            );
            for (legacy_id, role, content, created_at, name, tool_call_id, tool_calls, metadata) in
                old_messages
            {
                let mut metadata: Value = metadata
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_else(|| json!({}));
                if !metadata.is_object() {
                    metadata = json!({"legacy_metadata":metadata});
                }
                metadata["legacy_message_id"] = json!(legacy_id);
                if let Some(name) = name {
                    metadata["name"] = json!(name);
                }
                if let Some(tool_call_id) = tool_call_id {
                    metadata["tool_call_id"] = json!(tool_call_id);
                }
                if let Some(tool_calls) = tool_calls {
                    metadata["tool_calls"] = serde_json::from_str(&tool_calls)?;
                }
                transaction.execute("INSERT INTO messages(id,project_id,role,content,created_at,metadata_json) VALUES(?1,?2,?3,?4,?5,?6)",params![id(),project.id,role,content.unwrap_or_default(),created_at,serde_json::to_string(&metadata)?])?;
                total_messages += 1;
            }
            log(
                "UPDATE database",
                &self.db_path,
                &format!("projects/{} legacy timestamps", project.id),
            );
            transaction.execute(
                "UPDATE projects SET created_at=?1,updated_at=?2 WHERE id=?3",
                params![old.created_at, old.updated_at, project.id],
            )?;
            transaction.commit()?;
            drop(connection);
            if let Some(root) = legacy_project_root(&preserved, &old.active_root)? {
                let target = stage
                    .join("Files")
                    .join(format!("Project-{}_output", project.id));
                total_files += copy_tree(&root, &target, None)?;
            } else {
                warnings.push(format!("Project {} has no available working directory; its full legacy snapshot is retained.",project.id));
            }
            self.kv_set(&format!("legacy_project/{}",project.id),&json!({"legacy_chat_id":old.id,"legacy_active_root":old.active_root,"preserved_snapshot":".legacy-harnessyxo"}))?;
        }
        if table_exists(&legacy, "project_dependencies")? {
            let mut statement =
                legacy.prepare("SELECT child_chat_id,parent_chat_id FROM project_dependencies")?;
            let parents = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut ancestors = HashMap::<String, String>::new();
            for (child, parent) in &parents {
                ancestors.insert(child.clone(), parent.clone());
            }
            for (child, parent) in parents {
                let mut seen = HashSet::new();
                let mut cursor = child.as_str();
                while let Some(next) = ancestors.get(cursor) {
                    if !seen.insert(cursor.to_string()) {
                        bail!("Legacy project dependencies contain a cycle");
                    }
                    cursor = next;
                }
                if let (Some(child), Some(parent)) = (mapping.get(&child), mapping.get(&parent)) {
                    log(
                        "UPDATE database",
                        &self.db_path,
                        &format!("projects/{child} parent"),
                    );
                    self.connection()?.execute(
                        "UPDATE projects SET parent_id=?1 WHERE id=?2",
                        params![parent, child],
                    )?;
                } else {
                    bail!("Legacy project dependency references a missing project");
                }
            }
        }
        // Keep original archive/lock states only after importing the immutable history.
        for old in &rows {
            let project = &mapping[&old.id];
            log(
                "UPDATE database",
                &self.db_path,
                &format!("projects/{project} legacy flags"),
            );
            self.connection()?.execute(
                "UPDATE projects SET archived=?1,locked=?2,pinned=?3 WHERE id=?4",
                params![old.archived, old.locked, old.pinned, project],
            )?;
        }
        // Workspace memory and all non-ported tables remain recoverable in the snapshot.
        let mut memory_rows = Vec::new();
        if table_exists(&legacy, "memory")? {
            let mut statement = legacy.prepare(
                "SELECT id,kind,content,created_at,updated_at FROM memory ORDER BY rowid",
            )?;
            for row in statement.query_map([],|row|Ok(json!({"id":row.get::<_,String>(0)?,"kind":row.get::<_,String>(1)?,"content":row.get::<_,String>(2)?,"created_at":row.get::<_,String>(3)?,"updated_at":row.get::<_,String>(4)?})))? {memory_rows.push(row?);}
        }
        self.kv_set(
            &format!("workspace_memory/{}", workspace.id),
            &json!(memory_rows),
        )?;
        self.kv_set(&format!("legacy_import/{}",workspace.id),&json!({"imported_at":now(),"project_id_map":mapping,"legacy_snapshot":".legacy-harnessyxo"}))?;
        warnings.push("VM disks, settings, references, history checkpoints and unsupported tables are preserved in .legacy-harnessyxo; they are not activated or automatically migrated.".into());
        Ok(
            json!({"projects":rows.len(),"messages":total_messages,"copied_project_files":total_files,"warnings":warnings,"legacy_snapshot":".legacy-harnessyxo","source_unchanged":true}),
        )
    }
}

struct LegacyProject {
    id: String,
    title: String,
    created_at: String,
    updated_at: String,
    active_root: String,
    archived: bool,
    pinned: bool,
    locked: bool,
}

fn table_exists(connection: &Connection, name: &str) -> Result<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [name],
        |row| row.get(0),
    )?)
}

fn markdown_fence(text: &str) -> String {
    let mut longest = 0usize;
    let mut current = 0usize;
    for character in text.chars() {
        if character == '`' {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    "`".repeat((longest + 1).max(3))
}

fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::unix::ffi::OsStrExt;
        let source = std::ffi::CString::new(source.as_os_str().as_bytes())?;
        let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        let result =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        fs::rename(source, destination)?;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (source, destination);
        bail!("Atomic import installation is not supported on this host OS")
    }
}

fn absolute_destination(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if absolute
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        bail!("Choose an import destination without .. components");
    }
    let mut suffix = Vec::new();
    let mut ancestor = absolute.as_path();
    while !ancestor.exists() {
        suffix.push(
            ancestor
                .file_name()
                .ok_or_else(|| anyhow!("Invalid import destination"))?
                .to_os_string(),
        );
        ancestor = ancestor
            .parent()
            .ok_or_else(|| anyhow!("Invalid import destination"))?;
    }
    let mut result = fs::canonicalize(ancestor)?;
    for component in suffix.into_iter().rev() {
        result.push(component);
    }
    Ok(result)
}

fn legacy_project_root(preserved: &Path, reference: &str) -> Result<Option<PathBuf>> {
    let (base, relative) = if let Some(relative) = reference.strip_prefix("harnessy-private://") {
        (preserved.join(".harnessy/worktrees"), relative)
    } else {
        (preserved.join("Files"), reference)
    };
    let normalized = relative.replace('\\', "/");
    let relative = Path::new(&normalized);
    if relative.is_absolute()
        || relative.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::Prefix(_) | Component::RootDir
            )
        })
    {
        bail!("Legacy project root escapes its workspace");
    }
    let candidate = base.join(relative);
    if !candidate.exists() {
        return Ok(None);
    }
    let actual = fs::canonicalize(&candidate)?;
    let base = fs::canonicalize(base)?;
    if !actual.starts_with(&base) || !actual.is_dir() {
        bail!("Legacy project root is outside its workspace");
    }
    Ok(Some(actual))
}

fn copy_tree(source: &Path, destination: &Path, skip_database: Option<&Path>) -> Result<usize> {
    log(
        "SCAN import files",
        source,
        &format!("to {}", destination.display()),
    );
    let mut count = 0usize;
    for entry in walkdir::WalkDir::new(source).follow_links(false) {
        let entry = entry.context("Cannot scan an import file")?;
        let relative = entry.path().strip_prefix(source)?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        if let Some(db) = skip_database {
            if entry.path() == db
                || entry.path() == PathBuf::from(format!("{}-wal", db.display()))
                || entry.path() == PathBuf::from(format!("{}-shm", db.display()))
                || entry.path() == PathBuf::from(format!("{}-journal", db.display()))
            {
                continue;
            }
        }
        if entry.file_type().is_symlink() {
            bail!("Import found a symbolic link; remove or replace it explicitly before importing: {}",entry.path().display());
        }
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            private_directory(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                private_directory(parent)?;
            }
            log(
                "COPY import file",
                entry.path(),
                &format!("to {}", target.display()),
            );
            fs::copy(entry.path(), &target)
                .with_context(|| format!("Cannot copy import file: {}", entry.path().display()))?;
            count += 1;
        } else {
            bail!(
                "Import found an unsupported special file: {}",
                entry.path().display()
            );
        }
    }
    Ok(count)
}
