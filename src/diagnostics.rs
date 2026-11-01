//! Diagnostics are assembled from a small fixed allowlist. This module never
//! reads workspace files, transcripts, model bodies, tools, credentials, process
//! arguments, VM outputs, paths or raw exceptions for an export.
use crate::AppState;
use axum::{
    extract::State,
    http::{header, HeaderValue},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use std::path::Path;

pub const SOURCE_COMMIT: &str = "099d1062d80d5e92bb76196b44af58700d18a13e";

/// Human console output only. Callers must pass fixed action names and logical
/// identifiers, never passwords, request bodies, tool arguments or output.
pub fn log(action: &str, path: Option<&Path>, id: Option<&str>) {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let clean = |text: &str| {
        text.chars()
            .flat_map(|ch| ch.escape_debug())
            .collect::<String>()
    };
    let mut line = format!("[{timestamp}] {}", clean(action));
    if let Some(path) = path {
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(path)
        };
        line.push_str(": ");
        line.push_str(&clean(&resolved.to_string_lossy()));
    }
    if let Some(id) = id {
        line.push_str(" [");
        line.push_str(&clean(id));
        line.push(']');
    }
    eprintln!("{line}");
}

pub fn router() -> Router<AppState> {
    Router::new().route("/diagnostics", get(download))
}

async fn download(State(state): State<AppState>) -> Response {
    // Read only record counts. Errors are fixed flags, not their raw messages.
    let workspaces = state.store.workspaces();
    let workspace_count = workspaces.as_ref().map(|rows| rows.len()).unwrap_or(0);
    let mut project_count = 0usize;
    let mut project_counts_available = workspaces.is_ok();
    if let Ok(rows) = workspaces.as_ref() {
        for workspace in rows {
            match state.store.projects(&workspace.id, true) {
                Ok(projects) => project_count = project_count.saturating_add(projects.len()),
                Err(_) => project_counts_available = false,
            }
        }
    }
    let settings = state.store.settings();
    let report = json!({
        "schema_version":1,
        "application":"HarnessyXOR",
        "version":env!("CARGO_PKG_VERSION"),
        "generated_at":crate::types::now(),
        "port_source":{"application":"HarnessyXO","version":"0.7.36","commit":SOURCE_COMMIT},
        "host":{"os":std::env::consts::OS,"architecture":std::env::consts::ARCH},
        "storage":{"workspace_count_available":workspaces.is_ok(),"workspace_count":workspace_count,
            "project_count_available":project_counts_available,"project_count":project_count},
        "runtime":{"settings_available":settings.is_ok(),"agent_slots":settings.as_ref().ok().map(|s|s.agent_slots)},
        "authentication":{"configured":state.auth.has_users().ok()},
        "environments":crate::environment::diagnostic_summary(&state.store),
        "collection":{"workspace_files":false,"transcripts":false,"tool_payloads":false,
            "credentials":false,"host_paths":false,"guest_connections":false,"external_device_connections":false}
    });
    let mut response = Json(report).into_response();
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=\"HarnessyXOR-Diagnostics.json\""),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
