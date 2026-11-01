//! Machine-local trusted-controller accounts. Credentials and session tokens never
//! enter a workspace, a model request, or the diagnostics export.
use std::{
    collections::HashMap,
    fs,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{diagnostics::log, AppState};

const COOKIE_NAME: &str = "harnessyxor_session";
const MAX_ATTEMPTS: u32 = 10;
const ATTEMPT_WINDOW: Duration = Duration::from_secs(60);
const MAX_RATE_ENTRIES: usize = 2048;

#[derive(Debug, Clone, Serialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub role: String,
}

struct Attempts {
    started: Instant,
    count: u32,
}

pub struct Auth {
    db: Mutex<Connection>,
    path: PathBuf,
    dummy_hash: String,
    attempts: Mutex<HashMap<IpAddr, Attempts>>,
    verifications: Arc<tokio::sync::Semaphore>,
}

impl Auth {
    pub fn open(data_dir: &Path) -> Result<Self> {
        if !data_dir.exists() {
            log("CREATE application data directory", Some(data_dir), None);
            fs::create_dir_all(data_dir).context("Cannot create the application data directory")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700))?;
            }
        }
        // The private child cannot protect against a different OS user who can
        // rename it through a writable parent directory.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let parent =
                fs::metadata(data_dir).context("Cannot inspect the application data directory")?;
            if !parent.is_dir()
                || parent.uid() != unsafe { libc::geteuid() }
                || parent.mode() & 0o022 != 0
            {
                bail!("The application data directory must be owned by you and not writable by other users.");
            }
        }
        let root = data_dir.join("auth");
        if !root.exists() {
            log("CREATE authentication directory", Some(&root), None);
            fs::create_dir_all(&root).context("Cannot create the authentication directory")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
            }
        }
        check_private(&root, true)?;
        let path = root.join("accounts.db");
        if !path.exists() {
            use std::fs::OpenOptions;
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            log("CREATE authentication database", Some(&path), None);
            match options.open(&path) {
                Ok(file) => file.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error).context("Cannot create the authentication database")
                }
            }
        }
        check_private(&path, false)?;
        log("READ authentication database", Some(&path), None);
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let db = Connection::open_with_flags(&path, flags)?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS accounts (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                revision TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                username TEXT NOT NULL REFERENCES accounts(username) ON DELETE CASCADE,
                account_revision TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS sessions_by_user ON sessions(username);",
        )?;
        log("VERIFY authentication schema", Some(&path), None);
        // Equal password-hash work for unknown accounts avoids a username oracle.
        let dummy_hash = hash_password(&hex::encode(random_bytes()), &path)?;
        Ok(Self {
            db: Mutex::new(db),
            path,
            dummy_hash,
            attempts: Mutex::new(HashMap::new()),
            verifications: Arc::new(tokio::sync::Semaphore::new(4)),
        })
    }

    pub fn has_users(&self) -> Result<bool> {
        log("READ accounts count", Some(&self.path), None);
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        Ok(
            db.query_row("SELECT EXISTS(SELECT 1 FROM accounts)", [], |row| {
                row.get(0)
            })?,
        )
    }

    /// Create the first administrator only. Initial account creation deliberately
    /// stays in the host CLI: an unauthenticated LAN visitor cannot claim it.
    pub fn bootstrap(&self, username: &str, password: &str) -> Result<()> {
        let username = canonical_username(username)?;
        validate_password(password)?;
        log("DERIVE account password hash", Some(&self.path), None);
        let hash = hash_password(password, &self.path)?;
        log("GENERATE account revision", Some(&self.path), None);
        let revision = hex::encode(random_bytes());
        let mut db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM accounts)", [], |row| {
            row.get(0)
        })?;
        if exists {
            bail!("An account already exists. Initial setup cannot replace it.");
        }
        log(
            "INSERT first administrator into accounts",
            Some(&self.path),
            None,
        );
        tx.execute(
            "INSERT INTO accounts(username,password_hash,revision,created_at) VALUES(?1,?2,?3,?4)",
            params![username, hash, revision, crate::types::now()],
        )?;
        tx.commit()?;
        log("VERIFY account creation", Some(&self.path), None);
        Ok(())
    }

    /// Host-only recovery operation. Revision changes revoke every old session.
    pub fn set_password(&self, username: &str, password: &str) -> Result<()> {
        let username = canonical_username(username)?;
        validate_password(password)?;
        log("DERIVE replacement password hash", Some(&self.path), None);
        let hash = hash_password(password, &self.path)?;
        let mut db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        log(
            "UPDATE accounts password and revision",
            Some(&self.path),
            None,
        );
        let updated = tx.execute(
            "UPDATE accounts SET password_hash=?1,revision=?2 WHERE username=?3",
            params![hash, hex::encode(random_bytes()), username],
        )?;
        if updated != 1 {
            bail!("That account does not exist.");
        }
        log("DELETE revoked account sessions", Some(&self.path), None);
        tx.execute("DELETE FROM sessions WHERE username=?1", [&username])?;
        tx.commit()?;
        Ok(())
    }

    fn allow_attempt(&self, ip: IpAddr) -> bool {
        let Ok(mut attempts) = self.attempts.lock() else {
            return false;
        };
        let now = Instant::now();
        attempts.retain(|_, item| now.duration_since(item.started) < ATTEMPT_WINDOW);
        if !attempts.contains_key(&ip) && attempts.len() >= MAX_RATE_ENTRIES {
            return false;
        }
        let item = attempts.entry(ip).or_insert(Attempts {
            started: now,
            count: 0,
        });
        item.count += 1;
        item.count <= MAX_ATTEMPTS
    }

    fn login(&self, username: &str, password: &str) -> Result<Option<(User, String)>> {
        // Validation failures use the same Argon2 verification as unknown users.
        let username = canonical_username(username).ok();
        let row: Option<(String, String)> = {
            log("READ accounts password hash", Some(&self.path), None);
            let db = self
                .db
                .lock()
                .map_err(|_| anyhow!("Authentication store is unavailable"))?;
            if let Some(name) = &username {
                db.query_row(
                    "SELECT password_hash,revision FROM accounts WHERE username=?1",
                    [name],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
            } else {
                None
            }
        };
        let hash = row
            .as_ref()
            .map(|row| row.0.as_str())
            .unwrap_or(self.dummy_hash.as_str());
        let parsed =
            PasswordHash::new(hash).map_err(|_| anyhow!("Authentication store is invalid"))?;
        let password_valid = password.len() <= 1024 && !password.is_empty();
        let checked = Argon2::default()
            .verify_password(
                if password_valid {
                    password.as_bytes()
                } else {
                    b"invalid-password"
                },
                &parsed,
            )
            .is_ok();
        let Some((_, revision)) = row else {
            return Ok(None);
        };
        if !checked || !password_valid {
            return Ok(None);
        }
        let username = username.expect("Validated account username");
        log("GENERATE session token", Some(&self.path), None);
        let token = hex::encode(random_bytes());
        let token_hash = token_hash(&token);
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        // Recheck the revision in the INSERT so a simultaneous password change
        // cannot install an authenticated session for the old password.
        log("INSERT verified session", Some(&self.path), None);
        let changed = db.execute(
            "INSERT INTO sessions(token_hash,username,account_revision,created_at)
            SELECT ?1,username,revision,?2 FROM accounts WHERE username=?3 AND revision=?4",
            params![token_hash, crate::types::now(), username, revision],
        )?;
        if changed != 1 {
            return Ok(None);
        }
        Ok(Some((
            User {
                id: username.clone(),
                username,
                role: "admin".into(),
            },
            token,
        )))
    }

    fn authenticate(&self, token: &str) -> Result<Option<User>> {
        if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(None);
        }
        log("READ session and account revision", Some(&self.path), None);
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        let name: Option<String> = db
            .query_row(
                "SELECT a.username FROM sessions s JOIN accounts a ON a.username=s.username
            WHERE s.token_hash=?1 AND s.account_revision=a.revision",
                [token_hash(token)],
                |row| row.get(0),
            )
            .optional()?;
        Ok(name.map(|username| User {
            id: username.clone(),
            username,
            role: "admin".into(),
        }))
    }

    /// Recheck a long-lived connection against current session and account
    /// revisions. Storage errors revoke access; callers never retain or log a
    /// raw token separately from the original request headers.
    pub(crate) fn valid_session(&self, headers: &HeaderMap) -> bool {
        session_token(headers)
            .and_then(|token| self.authenticate(token).ok().flatten())
            .is_some()
    }

    fn logout(&self, token: &str) -> Result<()> {
        let db = self
            .db
            .lock()
            .map_err(|_| anyhow!("Authentication store is unavailable"))?;
        log("DELETE signed-out session", Some(&self.path), None);
        db.execute(
            "DELETE FROM sessions WHERE token_hash=?1",
            [token_hash(token)],
        )?;
        Ok(())
    }
}

fn check_private(path: &Path, directory: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path).context("Cannot inspect authentication storage")?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        bail!("Authentication storage must be a real private directory and regular file.");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let expected_uid = unsafe { libc::geteuid() };
        if metadata.uid() != expected_uid
            || metadata.mode() & 0o077 != 0
            || (!directory && metadata.nlink() != 1)
        {
            bail!("Authentication storage must be owned by you and private (directory 700, file 600).");
        }
    }
    Ok(())
}

fn random_bytes() -> [u8; 32] {
    let mut bytes = [0; 32];
    OsRng.fill_bytes(&mut bytes);
    bytes
}
fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn hash_password(password: &str, path: &Path) -> Result<String> {
    log("GENERATE password salt", Some(path), None);
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|_| anyhow!("Cannot derive the password hash"))
}

fn validate_password(password: &str) -> Result<()> {
    if password.is_empty() || password.len() > 1024 {
        bail!("Password must contain 1 to 1024 UTF-8 bytes.");
    }
    Ok(())
}

fn canonical_username(username: &str) -> Result<String> {
    if username.is_empty()
        || username.len() > 64
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        bail!("Username must contain 1 to 64 letters, numbers, underscores, dots or hyphens.");
    }
    Ok(username.to_ascii_lowercase())
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    let mut token = None;
    for cookie in headers.get_all(header::COOKIE) {
        let cookie = cookie.to_str().ok()?;
        for part in cookie.split(';') {
            if let Some((name, value)) = part.trim().split_once('=') {
                if name == COOKIE_NAME {
                    // Ambiguous duplicate cookies must not select arbitrary auth.
                    if token.is_some() {
                        return None;
                    }
                    token = Some(value);
                }
            }
        }
    }
    token
}

/// No trust in forwarded headers. Origin and the actual Host must agree, and
/// browser cross-site fetches are rejected before authentication or mutation.
fn valid_origin(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Ok(origin) = url::Url::parse(origin) else {
        return false;
    };
    if !matches!(origin.scheme(), "http" | "https")
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return false;
    }
    let Ok(expected) = url::Url::parse(&format!("{}://{}", origin.scheme(), host)) else {
        return false;
    };
    if !expected.username().is_empty()
        || expected.password().is_some()
        || expected.path() != "/"
        || expected.query().is_some()
        || expected.fragment().is_some()
    {
        return false;
    }
    if let Some(site) = headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
    {
        if site != "same-origin" && site != "none" {
            return false;
        }
    }
    origin.origin() == expected.origin()
}

fn failure(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let method = request.method();
    if !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
        && !valid_origin(request.headers())
    {
        return failure(
            StatusCode::FORBIDDEN,
            "This request must come from this HarnessyXOR page.",
        );
    }
    // Match both positions because route-layer middleware may run before or
    // after the outer /api prefix is stripped by Axum's nested router.
    let path = request
        .uri()
        .path()
        .strip_prefix("/api")
        .unwrap_or(request.uri().path());
    let public = (path == "/auth/status" && method == Method::GET)
        || (path == "/auth/login" && method == Method::POST);
    if public {
        return next.run(request).await;
    }
    let user = match session_token(request.headers()).map(|token| state.auth.authenticate(token)) {
        Some(Ok(Some(user))) => user,
        Some(Err(_)) => {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "The account store is unavailable.",
            )
        }
        _ => return failure(StatusCode::UNAUTHORIZED, "Sign in to HarnessyXOR."),
    };
    request.extensions_mut().insert(user);
    next.run(request).await
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/auth/status", get(status))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .layer(DefaultBodyLimit::max(8 * 1024))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        ))
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let configured = match state.auth.has_users() {
        Ok(value) => value,
        Err(_) => {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "The account store is unavailable.",
            )
        }
    };
    let user =
        session_token(&headers).and_then(|token| state.auth.authenticate(token).ok().flatten());
    Json(json!({"configured": configured,"authenticated":user.is_some(),"user":user}))
        .into_response()
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login(
    State(state): State<AppState>,
    peer: Option<ConnectInfo<SocketAddr>>,
    headers: HeaderMap,
    Json(input): Json<LoginRequest>,
) -> Response {
    // Also check here so future router composition cannot accidentally expose
    // login to CSRF if the account middleware is moved.
    if !valid_origin(&headers) {
        return failure(
            StatusCode::FORBIDDEN,
            "This request must come from this HarnessyXOR page.",
        );
    }
    let Some(ConnectInfo(peer)) = peer else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "The server could not identify this connection.",
        );
    };
    if !state.auth.allow_attempt(peer.ip()) {
        let mut response = failure(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many sign-in attempts. Wait one minute and try again.",
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
        return response;
    }
    if input.username.len() > 256 || input.password.len() > 4096 {
        return failure(
            StatusCode::UNAUTHORIZED,
            "The username or password is incorrect.",
        );
    }
    let permit = match state.auth.verifications.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let mut response = failure(
                StatusCode::TOO_MANY_REQUESTS,
                "Sign-in is busy. Try again in a moment.",
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    };
    let auth = state.auth.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        auth.login(&input.username, &input.password)
    })
    .await;
    match result {
        Ok(Ok(Some((user, token)))) => {
            let secure = headers
                .get(header::ORIGIN)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.starts_with("https://"))
                .unwrap_or(false);
            let cookie = format!(
                "{COOKIE_NAME}={token}; Path=/api; HttpOnly; SameSite=Strict{}",
                if secure { "; Secure" } else { "" }
            );
            let mut response = Json(json!({"user":user})).into_response();
            if let Ok(value) = HeaderValue::from_str(&cookie) {
                response.headers_mut().insert(header::SET_COOKIE, value);
            }
            response
        }
        Ok(Ok(None)) => failure(
            StatusCode::UNAUTHORIZED,
            "The username or password is incorrect.",
        ),
        _ => failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "The account store is unavailable.",
        ),
    }
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Some(token) = session_token(&headers) {
        if state.auth.logout(token).is_err() {
            return failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "The account store is unavailable.",
            );
        }
    }
    let mut response = Json(json!({"signed_out":true})).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "harnessyxor_session=; Path=/api; HttpOnly; SameSite=Strict; Max-Age=0",
        ),
    );
    response
}

async fn me(axum::Extension(user): axum::Extension<User>) -> Json<serde_json::Value> {
    Json(json!({"user":user}))
}
