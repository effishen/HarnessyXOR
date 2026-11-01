//! HTTP application. Blocking database and import operations stay off the async executor.
use crate::{auth, diagnostics, types::Settings, AppState};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;

const SAVED_KEY: &str = "__HARNESSYXOR_SAVED_KEY__";

struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}

fn public_error(error: anyhow::Error) -> ApiError {
    // Database/native errors can contain paths or raw input. Only reviewed
    // application validation messages are returned. The console records a fixed
    // failure action and never dumps an exception chain or request body.
    let message = error.to_string();
    let known = [
        "Invalid record ID",
        "Workspace not found",
        "Project not found",
        "Message not found",
        "Choose ",
        "Keep ",
        "Restore ",
        "Parent project",
        "Workspace path",
        "Workspace location",
        "Workspace identity",
        "Workspace marker",
        "Workspace Files",
        "Project directory",
        "Project location",
        "Project is",
        "This project",
        "Archived ",
        "Locked ",
        "Project title",
        "Workspace name",
        "Unsupported project",
        "Project changes",
        "archived must",
        "locked must",
        "pinned must",
        "Global Agent",
        "Step limit",
        "Save between",
        "Model profile",
        "Context window",
        "Output token",
        "Model base URL",
        "Temperature must",
        "Unsupported model API",
        "API key has",
        "Choose a saved",
        "Tool permissions",
        "Saved JSON exceeds",
        "Message content exceeds",
        "Legacy ",
        "The legacy ",
        "HarnessyXO import",
        "Import destination",
        "Destination ",
        "Source ",
        "The source ",
        "Only HarnessyXO",
        "Cannot import a workspace",
        "Import source",
        "Do not import",
    ];
    diagnostics::log("REQUEST failed validation or storage operation", None, None);
    let status = if message.ends_with("not found") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_REQUEST
    };
    ApiError(
        status,
        if known.iter().any(|prefix| message.starts_with(prefix)) {
            message
        } else {
            "The operation could not finish. Check the directory, permissions and saved data, then retry.".into()
        },
    )
}

async fn blocking<T, F>(work: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "The operation was interrupted.".into(),
            )
        })?
        .map_err(public_error)
}

fn redacted(settings: Settings) -> Value {
    let mut result = json!(settings);
    for profile in result["profiles"].as_array_mut().into_iter().flatten() {
        let saved = !profile["api_key"].as_str().unwrap_or_default().is_empty();
        profile["api_key_set"] = json!(saved);
        profile["api_key"] = json!(if saved { SAVED_KEY } else { "" });
    }
    result
}

pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/settings", get(settings).put(save_settings))
        .route("/workspaces", get(workspaces).post(create_workspace))
        .route(
            "/workspaces/:id/projects",
            get(projects).post(create_project),
        )
        .route("/projects/:id", get(project).patch(update_project))
        .route("/projects/:id/messages", get(messages))
        .route("/projects/:id/memory", get(memory).put(save_memory))
        .route("/projects/:id/export", get(export_project))
        .route("/projects/:id/export.md", get(export_markdown))
        .route("/projects/:id/search", get(search_messages))
        .route("/projects/:id/bookmarks", get(bookmarks))
        .route("/projects/:id/bookmarks/:message_id", put(save_bookmark))
        .merge(crate::team::router())
        .merge(crate::git_project::router())
        .merge(crate::agent::router())
        .merge(crate::files::router())
        .merge(crate::environment::router())
        .merge(crate::diagnostics::router())
        .merge(crate::auth::router())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth,
        ));
    Router::new()
        .route("/", get(||async {Html(include_str!("../assets/index.html"))}))
        .route("/assets/app.js", get(||async {([(header::CONTENT_TYPE,"text/javascript; charset=utf-8")],include_str!("../assets/app.js"))}))
        .route("/assets/app.css", get(||async {([(header::CONTENT_TYPE,"text/css; charset=utf-8")],include_str!("../assets/app.css"))}))
        .route("/port-status", get(port_status))
        .nest("/api", api)
        .fallback(||async {(StatusCode::NOT_FOUND,Json(json!({"error":"Page not found"})))})
        .layer(DefaultBodyLimit::max(64*1024*1024))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(header::CACHE_CONTROL,HeaderValue::from_static("no-store")))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(header::X_CONTENT_TYPE_OPTIONS,HeaderValue::from_static("nosniff")))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(header::X_FRAME_OPTIONS,HeaderValue::from_static("DENY")))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(header::REFERRER_POLICY,HeaderValue::from_static("no-referrer")))
        .layer(tower_http::set_header::SetResponseHeaderLayer::overriding(header::CONTENT_SECURITY_POLICY,HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(
        json!({"application":"HarnessyXOR","version":env!("CARGO_PKG_VERSION"),"status":"running","architecture":"project-first","workflow":"architect-led-teams","verification":"exact-candidate-evidence"}),
    )
}

async fn settings(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(redacted(
        blocking(move || state.store.settings()).await?,
    )))
}

async fn save_settings(
    State(state): State<AppState>,
    Json(mut input): Json<Settings>,
) -> Result<Json<Value>, ApiError> {
    let manager = state.agents.clone();
    let store = state.store.clone();
    let value = blocking(move || {
        let previous = store.settings()?;
        for profile in &mut input.profiles {
            if profile.api_key == SAVED_KEY {
                let old = previous
                    .profiles
                    .iter()
                    .find(|p| p.id == profile.id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Model profile needs its API key entered again")
                    })?;
                anyhow::ensure!(
                    old.base_url == profile.base_url,
                    "Model profile URL changed; enter its API key again"
                );
                profile.api_key = old.api_key.clone();
            }
        }
        store.save_settings(&input)?;
        Ok(redacted(input))
    })
    .await?;
    manager.pump(state.store.clone());
    Ok(Json(value))
}

async fn workspaces(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.workspaces()).await?
    )))
}
#[derive(Deserialize)]
struct NewWorkspace {
    name: String,
    path: PathBuf,
}
async fn create_workspace(
    State(state): State<AppState>,
    Json(input): Json<NewWorkspace>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.create_workspace(&input.name, &input.path)).await?
    )))
}
#[derive(Deserialize)]
struct ProjectQuery {
    #[serde(default)]
    archived: bool,
}
async fn projects(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ProjectQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.projects(&id, query.archived)).await?
    )))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewProject {
    title: String,
}
async fn create_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<NewProject>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        blocking(move || {
            let project = state.store.create_project(&id, &input.title, None)?;
            let mut response = json!(project);
            let initialized = crate::git_project::initialize_project(&state.store, &project.id);
            response["git_ready"] = json!(initialized.is_ok());
            if initialized.is_err() {
                response["git_status"] = json!(
                    "Git setup needs attention; the project and architect have been created."
                );
            }
            Ok(response)
        })
        .await?,
    ))
}
async fn project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.project(&id)).await?
    )))
}
async fn update_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    // The scheduler serializes policy changes with run admission. Archiving
    // checks all descendants before taking away their write permission.
    let result = tokio::task::spawn_blocking(move || {
        state
            .agents
            .update_project_policy(&state.store, &id, &input)
    })
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "The project update was interrupted.".into(),
        )
    })?;
    match result {
        Ok(project) => Ok(Json(json!(project))),
        Err(error)
            if [
                "Stop ",
                "Cancel queued or running work in this project and its children",
                "Wait for file or timeline changes in this project and its children",
            ]
            .iter()
            .any(|prefix| error.to_string().starts_with(prefix)) =>
        {
            Err(ApiError(StatusCode::CONFLICT, error.to_string()))
        }
        Err(error) => Err(public_error(error)),
    }
}
async fn messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.messages(&id)).await?
    )))
}
async fn memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(blocking(move || state.store.memory(&id)).await?))
}
async fn save_memory(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(value): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    blocking(move || state.store.save_memory(&id, &value)).await?;
    Ok(Json(json!({"saved":true})))
}
async fn export_project(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        blocking(move || state.store.export_project(&id)).await?,
    ))
}

#[derive(Deserialize)]
struct MarkdownQuery {
    #[serde(default = "yes")]
    reasoning: bool,
    #[serde(default = "yes")]
    tools: bool,
    #[serde(default = "yes")]
    stats: bool,
}
fn yes() -> bool {
    true
}
async fn export_markdown(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(options): Query<MarkdownQuery>,
) -> Result<Response, ApiError> {
    let body = blocking(move || {
        state
            .store
            .export_markdown(&id, options.reasoning, options.tools, options.stats)
    })
    .await?;
    Ok((
        [
            (header::CONTENT_TYPE, "text/markdown; charset=utf-8"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"HarnessyXOR-conversation.md\"",
            ),
        ],
        body,
    )
        .into_response())
}
#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    limit: Option<usize>,
}
async fn search_messages(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.search_messages(
            &id,
            &q.q,
            q.limit.unwrap_or(100).clamp(1, 500)
        ))
        .await?
    )))
}
async fn bookmarks(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(
        blocking(move || state.store.bookmarks(&id)).await?
    )))
}
#[derive(Deserialize)]
struct Bookmark {
    enabled: bool,
}
async fn save_bookmark(
    State(state): State<AppState>,
    Path((id, message_id)): Path<(String, String)>,
    Json(input): Json<Bookmark>,
) -> Result<Json<Value>, ApiError> {
    blocking(move || {
        state
            .store
            .bookmark_message(&id, &message_id, input.enabled)
    })
    .await?;
    Ok(Json(json!({"saved":true})))
}

async fn port_status() -> Html<&'static str> {
    Html(include_str!("../assets/port-status.html"))
}
