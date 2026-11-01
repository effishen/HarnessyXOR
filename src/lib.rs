pub mod agent;
pub mod auth;
pub mod diagnostics;
pub mod environment;
pub mod files;
pub mod git_project;
pub mod server;
pub mod store;
pub mod team;
pub mod types;
pub mod verification;

#[derive(Clone)]
pub struct AppState {
    pub store: std::sync::Arc<store::Store>,
    pub agents: std::sync::Arc<agent::AgentManager>,
    pub auth: std::sync::Arc<auth::Auth>,
}
