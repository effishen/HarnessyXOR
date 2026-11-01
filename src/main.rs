use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use harnessyxor::{
    agent::AgentManager, auth::Auth, diagnostics::log, server, store::Store, AppState,
};
use std::{
    io::{self, BufRead, IsTerminal, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Parser)]
#[command(
    name = "harnessyxor",
    version,
    about = "HarnessyXOR — a native Rust local agent workspace"
)]
struct Cli {
    #[arg(long, global = true, env = "HARNESSYXOR_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the browser interface. This is the default command.
    Serve {
        #[arg(long, default_value = "0.0.0.0", env = "HARNESSYXOR_HOST")]
        host: IpAddr,
        #[arg(long, default_value_t = 6768, env = "HARNESSYXOR_PORT")]
        port: u16,
    },
    /// Create an account or reset its password from this host.
    Account {
        #[command(subcommand)]
        command: AccountCommand,
    },
    /// Check this host without starting or changing a VM.
    Doctor,
}

#[derive(Subcommand)]
enum AccountCommand {
    Create {
        username: String,
        #[arg(long)]
        password_stdin: bool,
    },
    ResetPassword {
        username: String,
        #[arg(long)]
        password_stdin: bool,
    },
}

fn default_data_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    if let Some(root) = std::env::var_os("LOCALAPPDATA") {
        return Ok(PathBuf::from(root).join("HarnessyXOR"));
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join("Library/Application Support/HarnessyXOR"));
    }
    if let Some(root) = std::env::var_os("XDG_DATA_HOME") {
        return Ok(PathBuf::from(root).join("harnessyxor"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        return Ok(PathBuf::from(home).join(".local/share/harnessyxor"));
    }
    bail!("Set --data-dir to a separate HarnessyXOR application directory.")
}

fn absolute(path: &Path) -> Result<PathBuf> {
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    })
}

fn password(stdin: bool) -> Result<String> {
    let mut value = if stdin {
        log(
            "READ password from standard input; value omitted",
            None,
            None,
        );
        let mut value = String::new();
        io::stdin().lock().read_line(&mut value)?;
        while value.ends_with(['\n', '\r']) {
            value.pop();
        }
        value
    } else {
        if !io::stdin().is_terminal() {
            bail!(
                "Use an interactive terminal, or pass --password-stdin and pipe one password line."
            );
        }
        log(
            "ACTION REQUIRED enter an account password at this terminal",
            None,
            None,
        );
        let first = rpassword::prompt_password(format!(
            "[{}] ACTION REQUIRED Password: ",
            chrono::Utc::now().timestamp()
        ))?;
        let second = rpassword::prompt_password(format!(
            "[{}] ACTION REQUIRED Repeat password: ",
            chrono::Utc::now().timestamp()
        ))?;
        if first != second {
            bail!("The passwords do not match.");
        }
        first
    };
    if value.len() < 12 || value.len() > 1024 {
        value.clear();
        bail!("Use a password between 12 and 1024 bytes.");
    }
    Ok(value)
}

async fn doctor(store: &Store) -> Result<()> {
    log("VERIFY application database", Some(store.db_path()), None);
    for pending in store.pending_imports()? {
        log("ACTION REQUIRED review interrupted workspace import; retained stage has not been removed", Some(store.data_dir()), None);
        println!("{}", serde_json::to_string_pretty(&pending)?);
    }
    for (program, arg) in [
        ("limactl", "--version"),
        ("ssh", "-V"),
        ("qemu-system-x86_64", "--version"),
        ("qemu-system-aarch64", "--version"),
    ] {
        log("SCAN host prerequisite", None, Some(program));
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::process::Command::new(program)
                .arg(arg)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .status(),
        )
        .await;
        log(
            if matches!(result,Ok(Ok(status)) if status.success()) {
                "VERIFY host prerequisite available"
            } else {
                "VERIFY host prerequisite unavailable"
            },
            None,
            Some(program),
        );
    }
    log(
        "VERIFY complete; no VM started or changed",
        Some(store.data_dir()),
        None,
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // CLI configuration/native failures are shown locally. They are never
        // diagnostic-export payloads, and no request/model content is included.
        let clean = error
            .to_string()
            .chars()
            .flat_map(|c| c.escape_debug())
            .collect::<String>();
        eprintln!("[{}] FAILED: {clean}", chrono::Utc::now().timestamp());
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run() -> Result<()> {
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }
    let cli = Cli::parse();
    let data_dir = absolute(&match cli.data_dir {
        Some(path) => path,
        None => default_data_dir()?,
    })?;
    log("START HarnessyXOR", None, Some(env!("CARGO_PKG_VERSION")));
    let store = Arc::new(Store::open(&data_dir)?);
    let command = match cli.command {
        Some(command) => command,
        None => Command::Serve {
            host: std::env::var("HARNESSYXOR_HOST")
                .ok()
                .map(|value| value.parse::<IpAddr>())
                .transpose()
                .context("HARNESSYXOR_HOST must be an IP address")?
                .unwrap_or(IpAddr::from([0, 0, 0, 0])),
            port: std::env::var("HARNESSYXOR_PORT")
                .ok()
                .map(|value| value.parse::<u16>())
                .transpose()
                .context("HARNESSYXOR_PORT must be a port number")?
                .unwrap_or(6768),
        },
    };
    match command {
        Command::Account { command } => {
            let auth = Auth::open(store.data_dir())?;
            let (username, stdin, reset) = match command {
                AccountCommand::Create {
                    username,
                    password_stdin,
                } => (username, password_stdin, false),
                AccountCommand::ResetPassword {
                    username,
                    password_stdin,
                } => (username, password_stdin, true),
            };
            let mut secret = password(stdin)?;
            let result = if reset {
                auth.set_password(&username, &secret)
            } else {
                auth.bootstrap(&username, &secret)
            };
            // Remove the owned password before any success/error reporting.
            secret.clear();
            result?;
            log(
                if reset {
                    "VERIFY account password reset and sessions revoked"
                } else {
                    "VERIFY account created"
                },
                Some(store.data_dir()),
                None,
            );
        }
        Command::Doctor => doctor(&store).await?,
        Command::Serve { host, port } => {
            let lock_path = store.data_dir().join("server.lock");
            log("OPEN server ownership lock", Some(&lock_path), None);
            let mut options = std::fs::OpenOptions::new();
            options.create(true).read(true).write(true).truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            }
            let mut lock = options
                .open(&lock_path)
                .context("Cannot open the server ownership lock")?;
            lock.try_lock_exclusive()
                .map_err(|_| anyhow!("HarnessyXOR is already running for this data directory."))?;
            lock.set_len(0)?;
            writeln!(lock, "{}", std::process::id())?;
            lock.sync_all()?;
            let auth = Arc::new(Auth::open(store.data_dir())?);
            if !auth.has_users()? {
                log("ACTION REQUIRED create the first account with: harnessyxor account create <username>",Some(store.data_dir()),None);
            }
            let agents = Arc::new(AgentManager::new());
            let _ = agents.snapshots(&store)?; // Mark interrupted runs; never replay commands.
            let state = AppState {
                store: store.clone(),
                agents: agents.clone(),
                auth,
            };
            let addr = SocketAddr::new(host, port);
            log(
                "CONNECT browser server listener",
                None,
                Some(&addr.to_string()),
            );
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .context("Cannot bind the browser server port")?;
            let address = listener.local_addr()?;
            log(
                "VERIFY browser server listening",
                None,
                Some(&address.to_string()),
            );
            let browser_host = if host.is_unspecified() {
                IpAddr::from([127, 0, 0, 1])
            } else {
                host
            };
            println!(
                "[{}] Open http://{}",
                chrono::Utc::now().timestamp(),
                SocketAddr::new(browser_host, address.port())
            );
            let (stop_send, stop_receive) = tokio::sync::oneshot::channel::<()>();
            let service = axum::serve(
                listener,
                server::router(state).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = stop_receive.await;
            });
            let service = std::future::IntoFuture::into_future(service);
            tokio::pin!(service);
            tokio::select! {
                result=&mut service=>{result?;},
                _=shutdown_signal()=>{
                    log("STOP browser server and cancel active runs",Some(store.data_dir()),None);
                    agents.cancel_all(&store);
                    let _=stop_send.send(());
                    match tokio::time::timeout(Duration::from_secs(15),&mut service).await {
                        Ok(result)=>result?,
                        Err(_)=>log("STOP remaining client connections after shutdown deadline",Some(store.data_dir()),None),
                    }
                }
            }
            // Let cancelled streams persist their partial replies before runtime exit.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while agents.snapshots(&store)?.iter().any(|r| {
                matches!(
                    r.status.as_str(),
                    "running" | "checking" | "awaiting_approval" | "queued"
                )
            }) && tokio::time::Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            log("VERIFY server stopped", Some(store.data_dir()), None);
            drop(lock);
        }
    }
    Ok(())
}
