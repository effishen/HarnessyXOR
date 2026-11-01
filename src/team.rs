//! A project owns identities and append-only team knowledge. Teammates never own projects.
use crate::{
    diagnostics,
    store::Store,
    types::{id, now, Message},
    AppState,
};
use anyhow::{anyhow, ensure, Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

const MAX_TEXT: usize = 128 * 1024;
const MAX_JSON: usize = 1024 * 1024;
const TASK_STATES: &[&str] = &[
    "queued",
    "blocked",
    "running",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamAgent {
    pub id: String,
    pub project_id: String,
    pub role: String,
    pub label: String,
    pub instructions: String,
    pub parent_agent_id: Option<String>,
    pub replaces_agent_id: Option<String>,
    pub created_at: String,
    pub retired: bool,
    pub replacement_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamTask {
    pub id: String,
    pub project_id: String,
    pub assigned_agent_id: String,
    pub title: String,
    pub instructions: String,
    pub status: String,
    pub dependencies: Vec<String>,
    pub proposal_id: Option<String>,
    pub summary: Option<String>,
    pub revision: i64,
    pub created_at: String,
    pub updated_at: String,
}

fn ident(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "Team record ID is invalid"
    );
    Ok(())
}
fn text(value: &str, limit: usize, required: bool) -> Result<String> {
    ensure!(
        value.len() <= limit && (!required || !value.trim().is_empty()),
        "Team text is empty or exceeds its size limit"
    );
    ensure!(
        !value.chars().any(|c| c == '\0'),
        "Team text contains a null character"
    );
    Ok(value.to_owned())
}
fn string<'a>(input: &'a Value, key: &str) -> Result<&'a str> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Team field {key} must be text"))
}
fn encoded(value: &Value) -> Result<String> {
    let value = serde_json::to_string(value)?;
    ensure!(
        value.len() <= MAX_JSON,
        "Team record exceeds the 1 MiB limit"
    );
    Ok(value)
}
fn hash(value: &Value) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}
fn valid_sha(value: &str) -> Result<()> {
    ensure!(
        matches!(value.len(), 40 | 64) && value.bytes().all(|c| c.is_ascii_hexdigit()),
        "Team Git revision must be a complete SHA"
    );
    Ok(())
}
fn active(project: &crate::types::Project) -> Result<()> {
    ensure!(
        !project.archived && !project.locked,
        "Team project is read-only"
    );
    Ok(())
}

pub(crate) fn init_schema(connection: &Connection) -> Result<()> {
    let tx = connection.unchecked_transaction()?;
    tx.execute_batch("
    CREATE TABLE IF NOT EXISTS team_agents(
      id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), role TEXT NOT NULL,
      label TEXT NOT NULL, instructions TEXT NOT NULL, parent_agent_id TEXT REFERENCES team_agents(id),
      replaces_agent_id TEXT REFERENCES team_agents(id), created_at TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS team_agents_project ON team_agents(project_id,created_at);
    CREATE TABLE IF NOT EXISTS team_agent_state(
      agent_id TEXT PRIMARY KEY REFERENCES team_agents(id), retired INTEGER NOT NULL DEFAULT 0,
      replacement_agent_id TEXT REFERENCES team_agents(id), updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_agent_retirements(
      id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), agent_id TEXT NOT NULL REFERENCES team_agents(id),
      replacement_agent_id TEXT REFERENCES team_agents(id), reason TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_project_state(
      project_id TEXT PRIMARY KEY REFERENCES projects(id), architect_agent_id TEXT NOT NULL REFERENCES team_agents(id),
      canonical_head TEXT NOT NULL DEFAULT '', catalog_revision INTEGER NOT NULL DEFAULT 0, catalog_hash TEXT NOT NULL,
      requirements_revision INTEGER NOT NULL DEFAULT 0, decision_revision INTEGER NOT NULL DEFAULT 0,
      policy_revision INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_events(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      actor_agent_id TEXT REFERENCES team_agents(id),kind TEXT NOT NULL,data_json TEXT NOT NULL,
      origin TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS team_events_project ON team_events(project_id,sequence);
    CREATE TABLE IF NOT EXISTS team_documents(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      document_kind TEXT NOT NULL,logical_id TEXT NOT NULL,version INTEGER NOT NULL,title TEXT NOT NULL,content TEXT NOT NULL,
      status TEXT NOT NULL,kind TEXT NOT NULL,actor_agent_id TEXT REFERENCES team_agents(id),origin TEXT NOT NULL,created_at TEXT NOT NULL,
      UNIQUE(project_id,document_kind,logical_id,version));
    CREATE TABLE IF NOT EXISTS team_catalog_versions(
      id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),revision INTEGER NOT NULL,
      hash TEXT NOT NULL,tests_json TEXT NOT NULL,origin TEXT NOT NULL,created_at TEXT NOT NULL,
      UNIQUE(project_id,revision));
    CREATE TABLE IF NOT EXISTS team_tasks(
      id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),assigned_agent_id TEXT NOT NULL REFERENCES team_agents(id),
      title TEXT NOT NULL,instructions TEXT NOT NULL,status TEXT NOT NULL,dependencies_json TEXT NOT NULL,
      proposal_id TEXT,summary TEXT,revision INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_task_history(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      task_id TEXT NOT NULL REFERENCES team_tasks(id),revision INTEGER NOT NULL,snapshot_json TEXT NOT NULL,created_at TEXT NOT NULL,
      UNIQUE(task_id,revision));
    CREATE TABLE IF NOT EXISTS workspace_settings(
      workspace_id TEXT PRIMARY KEY REFERENCES workspaces(id),settings_json TEXT NOT NULL,revision INTEGER NOT NULL,updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_proposals(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      author_agent_id TEXT NOT NULL REFERENCES team_agents(id),base_sha TEXT NOT NULL,candidate_sha TEXT NOT NULL,candidate_tree TEXT NOT NULL,
      catalog_revision INTEGER NOT NULL,catalog_hash TEXT NOT NULL,requirements_revision INTEGER NOT NULL,decision_revision INTEGER NOT NULL,
      policy_revision INTEGER NOT NULL,payload_json TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_proposal_events(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      proposal_id TEXT NOT NULL REFERENCES team_proposals(id),kind TEXT NOT NULL,payload_json TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_reviews(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      proposal_id TEXT NOT NULL REFERENCES team_proposals(id),reviewer_agent_id TEXT REFERENCES team_agents(id),reviewer_user_id TEXT,
      kind TEXT NOT NULL,candidate_sha TEXT NOT NULL,catalog_hash TEXT NOT NULL,verdict TEXT NOT NULL,
      payload_json TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS team_receipts(
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,project_id TEXT NOT NULL REFERENCES projects(id),
      proposal_id TEXT NOT NULL REFERENCES team_proposals(id),kind TEXT NOT NULL,candidate_sha TEXT NOT NULL,candidate_tree TEXT NOT NULL,
      catalog_revision INTEGER NOT NULL,catalog_hash TEXT NOT NULL,requirements_revision INTEGER NOT NULL,decision_revision INTEGER NOT NULL,
      policy_revision INTEGER NOT NULL,status TEXT NOT NULL,exit_code INTEGER,payload_json TEXT NOT NULL,origin TEXT NOT NULL CHECK(origin='controller'),created_at TEXT NOT NULL);
    ")?;
    for table in [
        "team_agents",
        "team_agent_retirements",
        "team_events",
        "team_documents",
        "team_catalog_versions",
        "team_task_history",
        "team_proposals",
        "team_proposal_events",
        "team_reviews",
        "team_receipts",
    ] {
        tx.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS {table}_immutable_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT,'Team history is append-only'); END; CREATE TRIGGER IF NOT EXISTS {table}_immutable_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'Team history is append-only'); END;"))?;
    }
    tx.execute_batch("CREATE TRIGGER IF NOT EXISTS bootstrap_never_reopen_delete BEFORE DELETE ON kv WHEN substr(OLD.key,1,17)='bootstrap_closed:' BEGIN SELECT RAISE(ABORT,'Team bootstrap closure is permanent'); END; CREATE TRIGGER IF NOT EXISTS bootstrap_never_reopen_update BEFORE UPDATE ON kv WHEN substr(OLD.key,1,17)='bootstrap_closed:' AND (NEW.key<>OLD.key OR NEW.value_json<>'true') BEGIN SELECT RAISE(ABORT,'Team bootstrap closure is permanent'); END; CREATE TRIGGER IF NOT EXISTS bootstrap_closed_value BEFORE INSERT ON kv WHEN substr(NEW.key,1,17)='bootstrap_closed:' AND NEW.value_json<>'true' BEGIN SELECT RAISE(ABORT,'Team bootstrap closure is permanent'); END;")?;
    let projects = {
        let mut q = tx.prepare(
            "SELECT id FROM projects WHERE id NOT IN(SELECT project_id FROM team_project_state)",
        )?;
        let rows = q
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    for project in projects {
        init_project(&tx, &project)?;
    }
    tx.commit()?;
    Ok(())
}

pub(crate) fn init_project(connection: &Connection, project_id: &str) -> Result<()> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM team_project_state WHERE project_id=?1)",
        [project_id],
        |r| r.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let agent_id = id();
    let at = now();
    let empty = hash(&json!([]))?;
    connection.execute("INSERT INTO team_agents(id,project_id,role,label,instructions,parent_agent_id,replaces_agent_id,created_at) VALUES(?1,?2,'architect','Principal Architect',?3,NULL,NULL,?4)",params![agent_id,project_id,"Own the project architecture and curate its durable knowledge. Delegate implementation and independent review to teammates. Worker statements are not verification receipts. Preserve the accepted requirements and current regression catalog.",at])?;
    connection.execute(
        "INSERT INTO team_agent_state(agent_id,retired,updated_at) VALUES(?1,0,?2)",
        params![agent_id, at],
    )?;
    connection.execute("INSERT INTO team_project_state(project_id,architect_agent_id,catalog_hash,updated_at) VALUES(?1,?2,?3,?4)",params![project_id,agent_id,empty,at])?;
    connection.execute("INSERT INTO team_catalog_versions(id,project_id,revision,hash,tests_json,origin,created_at) VALUES(?1,?2,0,?3,'[]','controller',?4)",params![id(),project_id,empty,at])?;
    connection.execute("UPDATE messages SET metadata_json=json_set(CASE WHEN json_valid(metadata_json) AND json_type(metadata_json)='object' THEN metadata_json ELSE '{}' END,'$.agent_id',?1,'$.agent_role','architect') WHERE project_id=?2 AND json_extract(metadata_json,'$.agent_id') IS NULL",params![agent_id,project_id])?;
    insert_event(
        connection,
        project_id,
        Some(&agent_id),
        "project_created",
        &json!({"architect_agent_id":agent_id}),
        "controller",
    )?;
    Ok(())
}

fn agent_row(row: &Row<'_>) -> rusqlite::Result<TeamAgent> {
    Ok(TeamAgent {
        id: row.get(0)?,
        project_id: row.get(1)?,
        role: row.get(2)?,
        label: row.get(3)?,
        instructions: row.get(4)?,
        parent_agent_id: row.get(5)?,
        replaces_agent_id: row.get(6)?,
        created_at: row.get(7)?,
        retired: row.get(8)?,
        replacement_agent_id: row.get(9)?,
    })
}
fn query_agent(connection: &Connection, pid: &str, aid: &str) -> Result<TeamAgent> {
    connection.query_row("SELECT a.id,a.project_id,a.role,a.label,a.instructions,a.parent_agent_id,a.replaces_agent_id,a.created_at,s.retired,s.replacement_agent_id FROM team_agents a JOIN team_agent_state s ON s.agent_id=a.id WHERE a.project_id=?1 AND a.id=?2",params![pid,aid],agent_row).optional()?.ok_or_else(||anyhow!("Team agent not found"))
}
fn assert_writable_project(connection: &Connection, pid: &str) -> Result<()> {
    let writable:bool=connection.query_row("SELECT EXISTS(SELECT 1 FROM projects p JOIN workspaces w ON w.id=p.workspace_id WHERE p.id=?1 AND p.archived=0 AND p.locked=0 AND w.hidden=0)",[pid],|r|r.get(0))?;
    ensure!(writable, "Team project is read-only or unavailable");
    Ok(())
}
pub(crate) fn assert_active_agent(connection: &Connection, pid: &str, aid: &str) -> Result<()> {
    let actor = query_agent(connection, pid, aid)?;
    ensure!(
        !actor.retired,
        "Team agent is retired; its history remains available"
    );
    Ok(())
}
pub(crate) fn user_guidance_received(
    connection: &Connection,
    pid: &str,
    aid: &str,
    message_id: &str,
) -> Result<()> {
    assert_active_agent(connection, pid, aid)?;
    let changed=connection.execute("UPDATE team_project_state SET requirements_revision=requirements_revision+1,updated_at=?1 WHERE project_id=?2 AND architect_agent_id=?3",params![now(),pid,aid])?;
    ensure!(
        changed == 1,
        "Team user guidance can only target the current principal architect"
    );
    insert_event(
        connection,
        pid,
        Some(aid),
        "user_guidance_received",
        &json!({"message_id":message_id,"reason":"User guidance changed the project intent; prior proposal evidence must be re-evaluated."}),
        "user",
    )?;
    Ok(())
}
fn task_row(row: &Row<'_>) -> rusqlite::Result<TeamTask> {
    let deps: String = row.get(6)?;
    let deps = serde_json::from_str(&deps).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(TeamTask {
        id: row.get(0)?,
        project_id: row.get(1)?,
        assigned_agent_id: row.get(2)?,
        title: row.get(3)?,
        instructions: row.get(4)?,
        status: row.get(5)?,
        dependencies: deps,
        proposal_id: row.get(7)?,
        summary: row.get(8)?,
        revision: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}
fn query_task(connection: &Connection, pid: &str, tid: &str) -> Result<TeamTask> {
    connection.query_row("SELECT id,project_id,assigned_agent_id,title,instructions,status,dependencies_json,proposal_id,summary,revision,created_at,updated_at FROM team_tasks WHERE project_id=?1 AND id=?2",params![pid,tid],task_row).optional()?.ok_or_else(||anyhow!("Team task not found"))
}
fn insert_event(
    connection: &Connection,
    pid: &str,
    aid: Option<&str>,
    kind: &str,
    data: &Value,
    origin: &str,
) -> Result<Value> {
    let event = id();
    let at = now();
    connection.execute("INSERT INTO team_events(id,project_id,actor_agent_id,kind,data_json,origin,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![event,pid,aid,kind,encoded(data)?,origin,at])?;
    Ok(
        json!({"sequence":connection.last_insert_rowid(),"id":event,"project_id":pid,"actor_agent_id":aid,"kind":kind,"data":data,"origin":origin,"created_at":at}),
    )
}
fn task_history(connection: &Connection, task: &TeamTask) -> Result<()> {
    connection.execute("INSERT INTO team_task_history(id,project_id,task_id,revision,snapshot_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![id(),task.project_id,task.id,task.revision,encoded(&serde_json::to_value(task)?)?,now()])?;
    Ok(())
}
fn query_binding(connection: &Connection, pid: &str) -> Result<Value> {
    connection.query_row("SELECT canonical_head,catalog_revision,catalog_hash,requirements_revision,decision_revision,policy_revision,architect_agent_id FROM team_project_state WHERE project_id=?1",[pid],|r|Ok(json!({"project_id":pid,"canonical_head":r.get::<_,String>(0)?,"catalog_revision":r.get::<_,i64>(1)?,"catalog_hash":r.get::<_,String>(2)?,"requirements_revision":r.get::<_,i64>(3)?,"decision_revision":r.get::<_,i64>(4)?,"decisions_revision":r.get::<_,i64>(4)?,"policy_revision":r.get::<_,i64>(5)?,"architect_agent_id":r.get::<_,String>(6)?}))).optional()?.ok_or_else(||anyhow!("Team project state not found"))
}

impl Store {
    pub fn architect(&self, pid: &str) -> Result<TeamAgent> {
        self.project(pid)?;
        let connection = self.connection()?;
        let aid: String = connection.query_row(
            "SELECT architect_agent_id FROM team_project_state WHERE project_id=?1",
            [pid],
            |r| r.get(0),
        )?;
        query_agent(&connection, pid, &aid)
    }
    pub fn team_agent(&self, pid: &str, aid: &str) -> Result<TeamAgent> {
        self.project(pid)?;
        ident(aid)?;
        query_agent(&*self.connection()?, pid, aid)
    }
    pub fn team_agents(&self, pid: &str) -> Result<Vec<TeamAgent>> {
        self.project(pid)?;
        let connection = self.connection()?;
        let mut q=connection.prepare("SELECT a.id,a.project_id,a.role,a.label,a.instructions,a.parent_agent_id,a.replaces_agent_id,a.created_at,s.retired,s.replacement_agent_id FROM team_agents a JOIN team_agent_state s ON s.agent_id=a.id WHERE a.project_id=?1 ORDER BY a.created_at,a.rowid")?;
        let rows = q
            .query_map([pid], agent_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn create_worker(
        &self,
        pid: &str,
        parent: &str,
        role: &str,
        label: &str,
        instructions: &str,
    ) -> Result<TeamAgent> {
        active(&self.ensure_writable(pid)?)?;
        ident(parent)?;
        ensure!(
            matches!(
                role,
                "developer" | "tester" | "qa" | "code_reviewer" | "architecture_reviewer"
            ) || (role.starts_with("custom_")
                && role.len() <= 48
                && role
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')),
            "Team worker role is invalid"
        );
        let label = text(label, 160, true)?;
        let instructions = text(instructions, MAX_TEXT, false)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let binding = query_binding(&tx, pid)?;
        ensure!(
            binding["architect_agent_id"] == parent,
            "Team workers must be created by this project's principal architect"
        );
        ensure!(
            !query_agent(&tx, pid, parent)?.retired,
            "Team architect is retired"
        );
        let count: i64 = tx.query_row(
            "SELECT count(*) FROM team_agents WHERE project_id=?1",
            [pid],
            |r| r.get(0),
        )?;
        ensure!(
            count < 256,
            "Team identity limit reached; existing history has been retained"
        );
        let agent = TeamAgent {
            id: id(),
            project_id: pid.into(),
            role: role.into(),
            label,
            instructions,
            parent_agent_id: Some(parent.into()),
            replaces_agent_id: None,
            created_at: now(),
            retired: false,
            replacement_agent_id: None,
        };
        diagnostics::log(
            "INSERT immutable team identity",
            Some(self.db_path()),
            Some(&agent.id),
        );
        tx.execute("INSERT INTO team_agents(id,project_id,role,label,instructions,parent_agent_id,replaces_agent_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,NULL,?7)",params![agent.id,pid,agent.role,agent.label,agent.instructions,parent,agent.created_at])?;
        tx.execute(
            "INSERT INTO team_agent_state(agent_id,retired,updated_at) VALUES(?1,0,?2)",
            params![agent.id, agent.created_at],
        )?;
        insert_event(
            &tx,
            pid,
            Some(parent),
            "agent_created",
            &json!({"agent_id":agent.id,"role":role}),
            "controller",
        )?;
        tx.commit()?;
        Ok(agent)
    }

    pub fn team_retire_agent(
        &self,
        pid: &str,
        aid: &str,
        replacement: Option<&Value>,
        reason: &str,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let reason = text(reason, 8192, true)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let old = query_agent(&tx, pid, aid)?;
        ensure!(!old.retired, "Team agent is already retired");
        let busy:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM team_tasks WHERE project_id=?1 AND assigned_agent_id=?2 AND status IN('queued','running','blocked'))",params![pid,aid],|r|r.get(0))?;
        ensure!(
            !busy,
            "Team agent has unfinished tasks; cancel or complete them before retirement"
        );
        ensure!(
            old.role != "architect" || replacement.is_some(),
            "Team principal architect needs a replacement identity"
        );
        let new_agent = if let Some(input) = replacement {
            let agent = TeamAgent {
                id: id(),
                project_id: pid.into(),
                role: old.role.clone(),
                label: text(
                    input
                        .get("label")
                        .and_then(Value::as_str)
                        .unwrap_or(&old.label),
                    160,
                    true,
                )?,
                instructions: text(
                    input
                        .get("instructions")
                        .and_then(Value::as_str)
                        .unwrap_or(&old.instructions),
                    MAX_TEXT,
                    false,
                )?,
                parent_agent_id: old.parent_agent_id.clone(),
                replaces_agent_id: Some(aid.into()),
                created_at: now(),
                retired: false,
                replacement_agent_id: None,
            };
            tx.execute("INSERT INTO team_agents(id,project_id,role,label,instructions,parent_agent_id,replaces_agent_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![agent.id,pid,agent.role,agent.label,agent.instructions,agent.parent_agent_id,aid,agent.created_at])?;
            tx.execute(
                "INSERT INTO team_agent_state(agent_id,retired,updated_at) VALUES(?1,0,?2)",
                params![agent.id, agent.created_at],
            )?;
            Some(agent)
        } else {
            None
        };
        let replacement_id = new_agent.as_ref().map(|agent| agent.id.as_str());
        tx.execute("INSERT INTO team_agent_retirements(id,project_id,agent_id,replacement_agent_id,reason,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![id(),pid,aid,replacement_id,reason,now()])?;
        tx.execute("UPDATE team_agent_state SET retired=1,replacement_agent_id=?1,updated_at=?2 WHERE agent_id=?3",params![replacement_id,now(),aid])?;
        if old.role == "architect" {
            tx.execute("UPDATE team_project_state SET architect_agent_id=?1,policy_revision=policy_revision+1,updated_at=?2 WHERE project_id=?3",params![replacement_id,now(),pid])?;
        }
        insert_event(
            &tx,
            pid,
            Some(aid),
            "agent_retired",
            &json!({"agent_id":aid,"replacement_agent_id":replacement_id,"reason":reason}),
            "user",
        )?;
        diagnostics::log(
            "APPEND team retirement and replacement",
            Some(self.db_path()),
            Some(aid),
        );
        tx.commit()?;
        Ok(json!({"retired_agent_id":aid,"replacement":new_agent}))
    }

    pub fn team_tasks(&self, pid: &str) -> Result<Vec<TeamTask>> {
        self.project(pid)?;
        let connection = self.connection()?;
        let mut q=connection.prepare("SELECT id,project_id,assigned_agent_id,title,instructions,status,dependencies_json,proposal_id,summary,revision,created_at,updated_at FROM team_tasks WHERE project_id=?1 ORDER BY created_at,rowid")?;
        let rows = q
            .query_map([pid], task_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
    pub fn team_task(&self, pid: &str, tid: &str) -> Result<TeamTask> {
        self.project(pid)?;
        ident(tid)?;
        query_task(&*self.connection()?, pid, tid)
    }
    pub fn create_team_task(
        &self,
        pid: &str,
        assigned: &str,
        title: &str,
        instructions: &str,
        deps: &[String],
    ) -> Result<TeamTask> {
        self.ensure_writable(pid)?;
        let title = text(title, 512, true)?;
        let instructions = text(instructions, MAX_TEXT, false)?;
        ensure!(deps.len() <= 128, "Team task has too many dependencies");
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let agent = query_agent(&tx, pid, assigned)?;
        ensure!(!agent.retired, "Team agent is retired");
        let mut seen = HashSet::new();
        for dependency in deps {
            ident(dependency)?;
            ensure!(
                seen.insert(dependency),
                "Team task dependencies must be unique"
            );
            query_task(&tx, pid, dependency)?;
        }
        let at = now();
        let task = TeamTask {
            id: id(),
            project_id: pid.into(),
            assigned_agent_id: assigned.into(),
            title,
            instructions,
            status: if deps.is_empty() { "queued" } else { "blocked" }.into(),
            dependencies: deps.to_vec(),
            proposal_id: None,
            summary: None,
            revision: 1,
            created_at: at.clone(),
            updated_at: at,
        };
        tx.execute("INSERT INTO team_tasks(id,project_id,assigned_agent_id,title,instructions,status,dependencies_json,proposal_id,summary,revision,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,NULL,1,?8,?8)",params![task.id,pid,assigned,task.title,task.instructions,task.status,serde_json::to_string(deps)?,task.created_at])?;
        task_history(&tx, &task)?;
        insert_event(
            &tx,
            pid,
            Some(assigned),
            "task_created",
            &json!({"task_id":task.id,"assigned_agent_id":assigned,"dependencies":deps}),
            "controller",
        )?;
        diagnostics::log(
            "INSERT team task and immutable history",
            Some(self.db_path()),
            Some(&task.id),
        );
        tx.commit()?;
        Ok(task)
    }
    pub fn update_team_task(&self, pid: &str, tid: &str, patch: &Value) -> Result<TeamTask> {
        self.ensure_writable(pid)?;
        let patch = patch
            .as_object()
            .ok_or_else(|| anyhow!("Team task changes must be an object"))?;
        ensure!(
            patch
                .keys()
                .all(|key| matches!(key.as_str(), "status" | "summary" | "expected_revision")),
            "Team task identity and assignment are immutable"
        );
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let mut task = query_task(&tx, pid, tid)?;
        if let Some(expected) = patch.get("expected_revision") {
            ensure!(
                expected.as_i64() == Some(task.revision),
                "Team task changed; reload it before updating"
            );
        }
        if let Some(status) = patch.get("status") {
            let status = status
                .as_str()
                .ok_or_else(|| anyhow!("Team task status must be text"))?;
            ensure!(TASK_STATES.contains(&status), "Team task status is invalid");
            if matches!(status, "running" | "queued") {
                ensure!(
                    !query_agent(&tx, pid, &task.assigned_agent_id)?.retired,
                    "Team agent is retired"
                );
            }
            if status == "running" {
                for dependency in &task.dependencies {
                    ensure!(
                        query_task(&tx, pid, dependency)?.status == "completed",
                        "Team task dependencies are not completed"
                    );
                }
            }
            if task.status == "completed" {
                ensure!(
                    status == "completed",
                    "Team completed task cannot be silently reopened; create a follow-up task"
                );
            }
            task.status = status.into();
        }
        if let Some(summary) = patch.get("summary") {
            task.summary = if summary.is_null() {
                None
            } else {
                Some(text(
                    summary
                        .as_str()
                        .ok_or_else(|| anyhow!("Team task summary must be text"))?,
                    MAX_TEXT,
                    false,
                )?)
            };
        }
        task.revision += 1;
        task.updated_at = now();
        tx.execute("UPDATE team_tasks SET status=?1,summary=?2,revision=?3,updated_at=?4 WHERE project_id=?5 AND id=?6",params![task.status,task.summary,task.revision,task.updated_at,pid,tid])?;
        task_history(&tx, &task)?;
        insert_event(
            &tx,
            pid,
            Some(&task.assigned_agent_id),
            "task_updated",
            &json!({"task_id":tid,"status":task.status,"summary":task.summary,"verification":"agent_statement_only"}),
            "controller",
        )?;
        diagnostics::log(
            "UPDATE team task and APPEND history",
            Some(self.db_path()),
            Some(tid),
        );
        tx.commit()?;
        Ok(task)
    }
    pub fn bind_task_proposal(&self, pid: &str, tid: &str, proposal_id: &str) -> Result<TeamTask> {
        self.ensure_writable(pid)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let mut task = query_task(&tx, pid, tid)?;
        ensure!(
            task.proposal_id
                .as_deref()
                .map(|id| id == proposal_id)
                .unwrap_or(true),
            "Team task proposal binding cannot be replaced"
        );
        get_record(&tx, "team_proposals", pid, proposal_id)?;
        ensure!(
            task.status != "running" && task.status != "completed",
            "Team task proposal must be bound before execution"
        );
        task.proposal_id = Some(proposal_id.into());
        task.revision += 1;
        task.updated_at = now();
        tx.execute(
            "UPDATE team_tasks SET proposal_id=?1,revision=?2,updated_at=?3 WHERE id=?4",
            params![proposal_id, task.revision, task.updated_at, tid],
        )?;
        task_history(&tx, &task)?;
        tx.commit()?;
        Ok(task)
    }

    pub fn append_team_event(
        &self,
        pid: &str,
        aid: &str,
        kind: &str,
        data: &Value,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let agent = self.team_agent(pid, aid)?;
        ensure!(!agent.retired, "Team agent is retired");
        ensure!(
            [
                "agent_run_started",
                "agent_run_finished",
                "delegation",
                "progress",
                "paused",
                "resumed",
                "controller_summary",
                "worker_summary",
                "question",
                "answer",
                "note",
                "discussion",
                "error"
            ]
            .contains(&kind),
            "Team event kind is reserved for its controller"
        );
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        assert_active_agent(&tx, pid, aid)?;
        diagnostics::log("APPEND team event", Some(self.db_path()), Some(pid));
        let result = insert_event(&tx, pid, Some(aid), kind, data, "agent")?;
        tx.commit()?;
        Ok(result)
    }
    pub fn team_events(&self, pid: &str, after: i64, limit: usize) -> Result<Vec<Value>> {
        self.project(pid)?;
        let connection = self.connection()?;
        let mut q=connection.prepare("SELECT sequence,id,actor_agent_id,kind,data_json,origin,created_at FROM team_events WHERE project_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3")?;
        let rows = q
            .query_map(
                params![pid, after.max(0), limit.clamp(1, 500) as i64],
                |r| {
                    let data: String = r.get(4)?;
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, String>(3)?,
                        data,
                        r.get::<_, String>(5)?,
                        r.get::<_, String>(6)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(|(sequence,id,actor,kind,data,origin,created_at)|Ok(json!({"sequence":sequence,"id":id,"project_id":pid,"actor_agent_id":actor,"kind":kind,"data":serde_json::from_str::<Value>(&data)?,"origin":origin,"created_at":created_at}))).collect()
    }

    pub(crate) fn append_document(
        &self,
        pid: &str,
        aid: Option<&str>,
        document_kind: &str,
        input: &Value,
        origin: &str,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let title = text(string(input, "title")?, 512, true)?;
        let content = text(string(input, "content")?, MAX_TEXT, true)?;
        let logical_key = if document_kind == "decision" {
            "decision_id"
        } else {
            "requirement_id"
        };
        let logical = input
            .get(logical_key)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(id);
        ident(&logical)?;
        let user = origin == "user";
        let status = if user {
            input
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or(if document_kind == "decision" {
                    "accepted"
                } else {
                    "active"
                })
        } else {
            "proposed"
        };
        ensure!(
            if document_kind == "decision" {
                ["proposed", "accepted", "rejected", "superseded"].contains(&status)
            } else {
                ["proposed", "active", "retired", "superseded"].contains(&status)
            },
            "Team knowledge status is invalid"
        );
        if !user {
            ensure!(
                input
                    .get("status")
                    .and_then(Value::as_str)
                    .map(|v| v == "proposed")
                    .unwrap_or(true),
                "Team agents can propose knowledge; user acceptance is a separate action"
            );
        }
        let kind =
            input
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or(if document_kind == "decision" {
                    "decision"
                } else {
                    "requirement"
                });
        ensure!(
            ["decision", "architecture", "objective", "requirement"].contains(&kind),
            "Team knowledge kind is invalid"
        );
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        if let Some(aid) = aid {
            ensure!(
                !query_agent(&tx, pid, aid)?.retired,
                "Team agent is retired"
            );
        }
        let old:i64=tx.query_row("SELECT coalesce(max(version),0) FROM team_documents WHERE project_id=?1 AND document_kind=?2 AND logical_id=?3",params![pid,document_kind,logical],|r|r.get(0))?;
        if old > 0 {
            ensure!(
                input.get("expected_version").and_then(Value::as_i64) == Some(old),
                "Team knowledge changed; supply its latest expected_version"
            );
        }
        let version = old + 1;
        let record = id();
        let at = now();
        tx.execute("INSERT INTO team_documents(id,project_id,document_kind,logical_id,version,title,content,status,kind,actor_agent_id,origin,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![record,pid,document_kind,logical,version,title,content,status,kind,aid,origin,at])?;
        let column = if document_kind == "decision" {
            "decision_revision"
        } else {
            "requirements_revision"
        };
        tx.execute(&format!("UPDATE team_project_state SET {column}={column}+1,updated_at=?1 WHERE project_id=?2"),params![at,pid])?;
        let mut result = json!({"id":record,"project_id":pid,"version":version,"title":title,"content":content,"status":status,"kind":kind,"actor_agent_id":aid,"origin":origin,"created_at":at});
        result[logical_key] = json!(logical);
        insert_event(
            &tx,
            pid,
            aid,
            &format!("{document_kind}_version"),
            &json!({"record_id":record,"logical_id":logical,"version":version,"status":status}),
            origin,
        )?;
        diagnostics::log(
            "APPEND immutable knowledge version",
            Some(self.db_path()),
            Some(&record),
        );
        tx.commit()?;
        Ok(result)
    }
    pub fn record_decision(&self, pid: &str, aid: &str, input: &Value) -> Result<Value> {
        self.append_document(pid, Some(aid), "decision", input, "agent")
    }
    pub fn propose_requirement(&self, pid: &str, aid: &str, input: &Value) -> Result<Value> {
        self.append_document(pid, Some(aid), "requirement", input, "agent")
    }
    pub fn team_append_decision(&self, pid: &str, aid: &str, input: &Value) -> Result<Value> {
        self.record_decision(pid, aid, input)
    }
    pub fn team_append_requirement(&self, pid: &str, aid: &str, input: &Value) -> Result<Value> {
        self.propose_requirement(pid, aid, input)
    }
    pub fn team_documents(&self, pid: &str, kind: &str) -> Result<Vec<Value>> {
        self.project(pid)?;
        ensure!(
            ["decision", "requirement"].contains(&kind),
            "Team knowledge type is invalid"
        );
        let connection = self.connection()?;
        documents(&connection, pid, kind)
    }

    pub fn workspace_settings(&self, wid: &str) -> Result<Value> {
        self.workspace(wid)?;
        let connection = self.connection()?;
        let row:Option<(String,i64,String)>=connection.query_row("SELECT settings_json,revision,updated_at FROM workspace_settings WHERE workspace_id=?1",[wid],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        match row {
            Some((value, revision, at)) => Ok(
                json!({"workspace_id":wid,"settings":serde_json::from_str::<Value>(&value)?,"revision":revision,"updated_at":at}),
            ),
            None => {
                Ok(json!({"workspace_id":wid,"settings":{},"revision":0,"updated_at":Value::Null}))
            }
        }
    }
    pub fn save_workspace_settings(&self, wid: &str, input: &Value) -> Result<Value> {
        self.workspace(wid)?;
        let settings = input.get("settings").unwrap_or(input);
        ensure!(
            settings.is_object(),
            "Team workspace settings must be an object"
        );
        let value = encoded(settings)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: i64 = tx.query_row(
            "SELECT coalesce((SELECT revision FROM workspace_settings WHERE workspace_id=?1),0)",
            [wid],
            |r| r.get(0),
        )?;
        if let Some(expected) = input.get("expected_revision") {
            ensure!(
                expected.as_i64() == Some(old),
                "Team workspace settings changed; reload before saving"
            );
        }
        let at = now();
        tx.execute("INSERT INTO workspace_settings(workspace_id,settings_json,revision,updated_at) VALUES(?1,?2,?3,?4) ON CONFLICT(workspace_id) DO UPDATE SET settings_json=excluded.settings_json,revision=excluded.revision,updated_at=excluded.updated_at",params![wid,value,old+1,at])?;
        tx.execute("UPDATE team_project_state SET policy_revision=policy_revision+1,updated_at=?1 WHERE project_id IN(SELECT id FROM projects WHERE workspace_id=?2)",params![at,wid])?;
        diagnostics::log(
            "WRITE workspace-scoped settings and invalidate policy leases",
            Some(self.db_path()),
            Some(wid),
        );
        tx.commit()?;
        Ok(json!({"workspace_id":wid,"settings":settings,"revision":old+1,"updated_at":at}))
    }

    pub fn catalog_snapshot(&self, pid: &str) -> Result<Value> {
        self.project(pid)?;
        catalog(&*self.connection()?, pid)
    }
    pub fn team_catalog(&self, pid: &str) -> Result<Value> {
        self.catalog_snapshot(pid)
    }
    pub fn team_activate_catalog(
        &self,
        pid: &str,
        input: &Value,
        expected: Option<i64>,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let mut tests = normalize_catalog(
            input
                .get("tests")
                .or_else(|| input.get("entries"))
                .ok_or_else(|| anyhow!("Team catalog needs a tests array"))?,
        )?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let current = query_binding(&tx, pid)?;
        let revision = current["catalog_revision"].as_i64().unwrap_or(0);
        ensure!(
            expected.or_else(|| input.get("expected_revision").and_then(Value::as_i64))
                == Some(revision),
            "Team catalog changed; supply its current expected_revision"
        );
        preserve_retired_tests(&catalog(&tx, pid)?["tests"], &mut tests, input)?;
        let new_hash = hash(&tests)?;
        let at = now();
        tx.execute("INSERT INTO team_catalog_versions(id,project_id,revision,hash,tests_json,origin,created_at) VALUES(?1,?2,?3,?4,?5,'user',?6)",params![id(),pid,revision+1,new_hash,encoded(&tests)?,at])?;
        tx.execute("UPDATE team_project_state SET catalog_revision=?1,catalog_hash=?2,updated_at=?3 WHERE project_id=?4",params![revision+1,new_hash,at,pid])?;
        insert_event(
            &tx,
            pid,
            None,
            "catalog_activated",
            &json!({"revision":revision+1,"hash":new_hash,"test_count":tests.as_array().map(Vec::len)}),
            "user",
        )?;
        diagnostics::log(
            "APPEND user-authorized regression catalog version",
            Some(self.db_path()),
            Some(pid),
        );
        let result = catalog(&tx, pid)?;
        tx.commit()?;
        Ok(result)
    }
    pub fn project_binding(&self, pid: &str) -> Result<Value> {
        self.project(pid)?;
        query_binding(&*self.connection()?, pid)
    }
    pub fn initialize_project_head(&self, pid: &str, head: &str) -> Result<Value> {
        self.set_project_head(pid, "", head)
    }
    pub fn set_project_head(&self, pid: &str, expected: &str, head: &str) -> Result<Value> {
        self.ensure_writable(pid)?;
        valid_sha(head)?;
        if !expected.is_empty() {
            valid_sha(expected)?;
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let current = query_binding(&tx, pid)?;
        ensure!(
            current["canonical_head"] == expected,
            "Team canonical Git head changed; reload before committing"
        );
        tx.execute(
            "UPDATE team_project_state SET canonical_head=?1,updated_at=?2 WHERE project_id=?3",
            params![head, now(), pid],
        )?;
        insert_event(
            &tx,
            pid,
            None,
            "owner_head_updated",
            &json!({"before":expected,"after":head}),
            "controller",
        )?;
        let result = query_binding(&tx, pid)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn team_context(&self, pid: &str, aid: &str) -> Result<Value> {
        let agent = self.team_agent(pid, aid)?;
        let decisions = self.team_documents(pid, "decision")?;
        let requirements = self.team_documents(pid, "requirement")?;
        let decisions = current_documents(decisions, "decision_id");
        let requirements = current_documents(requirements, "requirement_id");
        let tasks = self
            .team_tasks(pid)?
            .into_iter()
            .filter(|task| agent.role == "architect" || task.assigned_agent_id == aid)
            .collect::<Vec<_>>();
        let events = self.recent_team_events(pid, 40)?;
        let warnings = self
            .team_documents(pid, "decision")?
            .into_iter()
            .filter(|v| v["status"] == "rejected" || v["status"] == "superseded")
            .rev()
            .take(40)
            .collect::<Vec<_>>();
        Ok(
            json!({"agent":agent,"accepted_decisions":decisions.iter().filter(|v|v["status"]=="accepted").collect::<Vec<_>>(),"knowledge_warnings":warnings,"requirements":requirements.iter().filter(|v|v["status"]=="active").collect::<Vec<_>>(),"catalog":self.catalog_snapshot(pid)?,"tasks":tasks,"team_feed":events,"authority":"Worker summaries and model statements are not verification receipts."}),
        )
    }
    pub fn team(&self, pid: &str) -> Result<Value> {
        let catalog = self.catalog_snapshot(pid)?;
        let connection = self.connection()?;
        let mut q=connection.prepare("SELECT revision,hash,tests_json,origin,created_at FROM team_catalog_versions WHERE project_id=?1 ORDER BY revision")?;
        let history=q.query_map([pid],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?.into_iter().map(|(revision,hash,tests,origin,at)|Ok(json!({"revision":revision,"hash":hash,"tests":serde_json::from_str::<Value>(&tests)?,"origin":origin,"created_at":at}))).collect::<Result<Vec<_>>>()?;
        drop(q);
        drop(connection);
        Ok(
            json!({"architect":self.architect(pid)?,"agents":self.team_agents(pid)?,"tasks":self.team_tasks(pid)?,"events":self.recent_team_events(pid,500)?,"decisions":self.team_documents(pid,"decision")?,"requirements":self.team_documents(pid,"requirement")?,"catalog":catalog,"catalog_history":history,"proposals":self.proposals(pid)?,"binding":self.project_binding(pid)?,"paused":self.kv_get(&format!("team:paused:{pid}"))?.unwrap_or(json!(false))}),
        )
    }
    fn recent_team_events(&self, pid: &str, limit: usize) -> Result<Vec<Value>> {
        let after = {
            let connection = self.connection()?;
            connection.query_row("SELECT coalesce((SELECT sequence FROM team_events WHERE project_id=?1 ORDER BY sequence DESC LIMIT 1 OFFSET ?2),0)",params![pid,limit.clamp(1,500) as i64],|r|r.get::<_,i64>(0))?
        };
        self.team_events(pid, after, limit)
    }
}

fn documents(connection: &Connection, pid: &str, document_kind: &str) -> Result<Vec<Value>> {
    let mut q=connection.prepare("SELECT id,logical_id,version,title,content,status,kind,actor_agent_id,origin,created_at FROM team_documents WHERE project_id=?1 AND document_kind=?2 ORDER BY sequence")?;
    let rows=q.query_map(params![pid,document_kind],|r|Ok(json!({"id":r.get::<_,String>(0)?,"project_id":pid,"logical_id":r.get::<_,String>(1)?,"version":r.get::<_,i64>(2)?,"title":r.get::<_,String>(3)?,"content":r.get::<_,String>(4)?,"status":r.get::<_,String>(5)?,"kind":r.get::<_,String>(6)?,"actor_agent_id":r.get::<_,Option<String>>(7)?,"origin":r.get::<_,String>(8)?,"created_at":r.get::<_,String>(9)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let key = if document_kind == "decision" {
        "decision_id"
    } else {
        "requirement_id"
    };
    Ok(rows
        .into_iter()
        .map(|mut value| {
            value[key] = value["logical_id"].clone();
            value
        })
        .collect())
}
fn current_documents(records: Vec<Value>, key: &str) -> Vec<Value> {
    let mut current = HashMap::<String, Value>::new();
    for record in records {
        if let Some(id) = record[key].as_str() {
            let id = id.to_owned();
            if (record["origin"] != "user" || record["status"] == "proposed")
                && current
                    .get(&id)
                    .is_some_and(|old| old["origin"] == "user" && old["status"] != "proposed")
            {
                continue;
            }
            current.insert(id, record);
        }
    }
    let mut rows = current.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| row[key].as_str().unwrap_or_default().to_owned());
    rows
}
fn normalize_catalog(value: &Value) -> Result<Value> {
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("Team catalog tests must be an array"))?;
    ensure!(rows.len() <= 512, "Team catalog exceeds 512 tests");
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for row in rows {
        ensure!(row.is_object(), "Team catalog test must be an object");
        let mut row = row.clone();
        let test_id = string(&row, "id")?.to_owned();
        ident(&test_id)?;
        ensure!(seen.insert(test_id), "Team catalog test IDs must be unique");
        text(string(&row, "name")?, 256, true)?;
        let kind = string(&row, "kind")?.to_owned();
        ensure!(
            [
                "file_exists",
                "file_contains",
                "json_valid",
                "git_diff_check",
                "manual",
                "command"
            ]
            .contains(&kind.as_str()),
            "Team catalog test kind is invalid"
        );
        for field in ["active", "required", "hardware"] {
            if row.get(field).is_none() {
                row[field] = json!(field != "hardware");
            }
            ensure!(
                row[field].is_boolean(),
                "Team catalog flags must be true or false"
            );
        }
        if matches!(
            kind.as_str(),
            "file_exists" | "file_contains" | "json_valid"
        ) {
            let path = string(&row, "path")?;
            ensure!(
                !path.is_empty()
                    && !std::path::Path::new(path).is_absolute()
                    && !path.contains(':')
                    && !path.replace('\\', "/").split('/').any(|part| [
                        "..",
                        ".git",
                        ".harnessyxor"
                    ]
                    .contains(&part)),
                "Team catalog file path must be relative to the candidate source"
            );
        }
        if kind == "file_contains" {
            text(string(&row, "expected")?, MAX_TEXT, true)?;
        }
        if kind == "command" {
            let command = row
                .get("command")
                .ok_or_else(|| anyhow!("Team command check needs a command"))?;
            ensure!(
                command
                    .as_str()
                    .map(|v| !v.is_empty() && v.len() <= 8192)
                    .unwrap_or_else(|| command
                        .as_array()
                        .map(|v| !v.is_empty()
                            && v.len() <= 128
                            && v.iter()
                                .all(|x| x.as_str().map(|s| s.len() <= 8192).unwrap_or(false)))
                        .unwrap_or(false)),
                "Team command must be text or an argument array"
            );
        }
        encoded(&row)?;
        result.push(row);
    }
    Ok(json!(result))
}
fn catalog(connection: &Connection, pid: &str) -> Result<Value> {
    let binding = query_binding(connection, pid)?;
    let (tests,origin,at):(String,String,String)=connection.query_row("SELECT tests_json,origin,created_at FROM team_catalog_versions WHERE project_id=?1 AND revision=?2",params![pid,binding["catalog_revision"].as_i64()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let tests: Value = serde_json::from_str(&tests)?;
    Ok(
        json!({"project_id":pid,"revision":binding["catalog_revision"],"hash":binding["catalog_hash"],"requirements_revision":binding["requirements_revision"],"decision_revision":binding["decision_revision"],"decisions_revision":binding["decision_revision"],"policy_revision":binding["policy_revision"],"tests":tests,"entries":tests,"origin":origin,"created_at":at}),
    )
}
fn preserve_retired_tests(previous: &Value, current: &mut Value, input: &Value) -> Result<()> {
    let old = previous
        .as_array()
        .ok_or_else(|| anyhow!("Team previous catalog is invalid"))?;
    let rows = current
        .as_array_mut()
        .ok_or_else(|| anyhow!("Team catalog tests must be an array"))?;
    for test in old {
        let test_id = test["id"]
            .as_str()
            .ok_or_else(|| anyhow!("Team previous test identity is invalid"))?;
        let position = rows.iter().position(|row| row["id"] == test_id);
        let retiring = test["active"] != false
            && position
                .map(|at| rows[at]["active"] == false)
                .unwrap_or(true);
        let reason = position
            .and_then(|at| rows[at].get("retirement_reason").and_then(Value::as_str))
            .or_else(|| {
                input
                    .get("retirements")
                    .and_then(Value::as_array)
                    .and_then(|items| items.iter().find(|item| item["id"] == test_id))
                    .and_then(|item| item["reason"].as_str())
            })
            .map(str::to_owned);
        if retiring {
            ensure!(
                reason
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty() && value.len() <= 8192),
                "Team retiring an active test requires an explicit user retirement_reason"
            );
        }
        if let Some(at) = position {
            if retiring {
                rows[at]["retirement_reason"] = json!(reason);
            }
        } else {
            let mut retained = test.clone();
            retained["active"] = json!(false);
            if retiring {
                retained["retirement_reason"] = json!(reason);
            }
            rows.push(retained);
        }
    }
    ensure!(
        rows.len() <= 512,
        "Team retained catalog history exceeds 512 test identities"
    );
    Ok(())
}

fn get_record(connection: &Connection, table: &str, pid: &str, record_id: &str) -> Result<Value> {
    ensure!(
        ["team_proposals", "team_reviews", "team_receipts"].contains(&table),
        "Team record table is invalid"
    );
    ident(record_id)?;
    let value: Option<String> = connection
        .query_row(
            &format!("SELECT payload_json FROM {table} WHERE project_id=?1 AND id=?2"),
            params![pid, record_id],
            |r| r.get(0),
        )
        .optional()?;
    value
        .map(|v| serde_json::from_str::<Value>(&v))
        .transpose()?
        .ok_or_else(|| anyhow!("Team record not found"))
}

impl Store {
    pub fn workspace_configuration(&self, wid: &str) -> Result<Value> {
        Ok(self.workspace_settings(wid)?["settings"].clone())
    }
    pub fn project_bootstrap_open(&self, pid: &str) -> Result<bool> {
        self.project(pid)?;
        Ok(self.kv_get(&format!("bootstrap_closed:{pid}"))?.is_none())
    }
    pub(crate) fn close_project_bootstrap(&self, pid: &str, reason: &str) -> Result<Value> {
        self.ensure_writable(pid)?;
        let reason = text(reason, 8192, true)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let key = format!("bootstrap_closed:{pid}");
        let changed = tx.execute(
            "INSERT OR IGNORE INTO kv(key,value_json,updated_at) VALUES(?1,'true',?2)",
            params![key, now()],
        )?;
        if changed > 0 {
            tx.execute("UPDATE team_project_state SET policy_revision=policy_revision+1,updated_at=?1 WHERE project_id=?2",params![now(),pid])?;
            insert_event(
                &tx,
                pid,
                None,
                "bootstrap_closed",
                &json!({"reason":reason,"reopening_allowed":false}),
                "controller",
            )?;
        }
        diagnostics::log(
            "CLOSE canonical source bootstrap; monotonic policy",
            Some(self.db_path()),
            Some(pid),
        );
        let mut binding = query_binding(&tx, pid)?;
        binding["bootstrap_open"] = json!(false);
        tx.commit()?;
        Ok(binding)
    }

    pub fn record_proposal(&self, pid: &str, input: &Value) -> Result<Value> {
        self.ensure_writable(pid)?;
        let author = string(input, "author_agent_id")?;
        let base = string(input, "base_sha")?;
        let candidate = string(input, "candidate_sha")?;
        let tree = string(input, "candidate_tree")?;
        valid_sha(base)?;
        valid_sha(candidate)?;
        valid_sha(tree)?;
        let record_id = input
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(id);
        ident(&record_id)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let agent = query_agent(&tx, pid, author)?;
        ensure!(
            !agent.retired && matches!(agent.role.as_str(), "developer" | "tester"),
            "Team proposal author must be an active developer or tester identity"
        );
        let binding = query_binding(&tx, pid)?;
        ensure!(
            binding["canonical_head"] == base,
            "Team proposal base no longer matches the canonical Git head"
        );
        if let Some(run_id) = input.get("expected_run_id").and_then(Value::as_str) {
            assert_run_context(
                &tx,
                pid,
                author,
                run_id,
                &binding,
                input.get("expected_context"),
            )?;
        }
        for key in [
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            if let Some(value) = input.get(key) {
                ensure!(
                    binding[key] == *value,
                    "Team proposal intent, catalog or policy binding changed"
                );
            }
        }
        let mut result = input.clone();
        ensure!(result.is_object(), "Team proposal must be an object");
        result["id"] = json!(record_id);
        result["project_id"] = json!(pid);
        result["status"] = json!("proposed");
        result["created_at"] = json!(now());
        for key in [
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            result[key] = binding[key].clone();
        }
        result["decisions_revision"] = binding["decision_revision"].clone();
        tx.execute("INSERT INTO team_proposals(id,project_id,author_agent_id,base_sha,candidate_sha,candidate_tree,catalog_revision,catalog_hash,requirements_revision,decision_revision,policy_revision,payload_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",params![record_id,pid,author,base,candidate,tree,binding["catalog_revision"].as_i64(),binding["catalog_hash"].as_str(),binding["requirements_revision"].as_i64(),binding["decision_revision"].as_i64(),binding["policy_revision"].as_i64(),encoded(&result)?,result["created_at"].as_str()])?;
        proposal_event(
            &tx,
            pid,
            &record_id,
            "proposed",
            &json!({"candidate_sha":candidate,"candidate_tree":tree}),
        )?;
        insert_event(
            &tx,
            pid,
            Some(author),
            "proposal_created",
            &json!({"proposal_id":record_id,"candidate_sha":candidate}),
            "controller",
        )?;
        diagnostics::log(
            "APPEND immutable integrated-candidate proposal",
            Some(self.db_path()),
            Some(&record_id),
        );
        tx.commit()?;
        Ok(result)
    }
    pub fn get_proposal(&self, pid: &str, proposal_id: &str) -> Result<Value> {
        self.project(pid)?;
        let connection = self.connection()?;
        proposal(&connection, pid, proposal_id)
    }
    pub fn proposals(&self, pid: &str) -> Result<Vec<Value>> {
        self.project(pid)?;
        let connection = self.connection()?;
        let mut q = connection.prepare(
            "SELECT id FROM team_proposals WHERE project_id=?1 ORDER BY sequence DESC LIMIT 512",
        )?;
        let ids = q
            .query_map([pid], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.into_iter()
            .map(|id| proposal(&connection, pid, &id))
            .collect()
    }
    pub fn reviews(&self, pid: &str, proposal_id: &str) -> Result<Vec<Value>> {
        self.project(pid)?;
        let connection = self.connection()?;
        get_record(&connection, "team_proposals", pid, proposal_id)?;
        records(&connection, "team_reviews", pid, proposal_id)
    }
    pub fn receipts(&self, pid: &str, proposal_id: &str) -> Result<Vec<Value>> {
        self.project(pid)?;
        let connection = self.connection()?;
        get_record(&connection, "team_proposals", pid, proposal_id)?;
        records(&connection, "team_receipts", pid, proposal_id)
    }

    pub fn team_comment_proposal(
        &self,
        pid: &str,
        proposal_id: &str,
        origin: &str,
        actor_id: &str,
        input: &Value,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let human = matches!(origin, "user" | "human");
        ensure!(
            human || origin == "agent",
            "Team proposal discussion origin is invalid"
        );
        let body = text(string(input, "body")?, MAX_TEXT, true)?;
        text(actor_id, 256, true)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        get_record(&tx, "team_proposals", pid, proposal_id)?;
        if !human {
            assert_active_agent(&tx, pid, actor_id)?;
        }
        let reply_to = input
            .get("reply_to")
            .filter(|v| !v.is_null())
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| anyhow!("Team discussion reply_to must be a comment ID"))
            })
            .transpose()?;
        if let Some(reply) = reply_to {
            let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM team_proposal_events WHERE project_id=?1 AND proposal_id=?2 AND id=?3 AND kind='comment')",params![pid,proposal_id,reply],|r|r.get(0))?;
            ensure!(
                exists,
                "Team discussion reply belongs to another proposal or does not exist"
            );
        }
        let resolves = input.get("resolves").cloned().unwrap_or(Value::Null);
        ensure!(
            resolves.is_null() || resolves.is_string() || resolves.is_boolean(),
            "Team discussion resolution must be a note or comment ID"
        );
        let comment = id();
        let result = json!({"id":comment,"project_id":pid,"proposal_id":proposal_id,"body":body,"reply_to":reply_to,"resolves":resolves,"actor_id":actor_id,"origin":if human{"user"}else{"agent"},"created_at":now(),"review_gate_effect":"none; a new independent review is required to change a review verdict"});
        tx.execute("INSERT INTO team_proposal_events(id,project_id,proposal_id,kind,payload_json,created_at) VALUES(?1,?2,?3,'comment',?4,?5)",params![comment,pid,proposal_id,encoded(&result)?,result["created_at"].as_str()])?;
        insert_event(
            &tx,
            pid,
            if human { None } else { Some(actor_id) },
            "proposal_comment",
            &json!({"proposal_id":proposal_id,"comment_id":comment,"reply_to":reply_to}),
            if human { "user" } else { "agent" },
        )?;
        diagnostics::log(
            "APPEND immutable proposal discussion",
            Some(self.db_path()),
            Some(&comment),
        );
        tx.commit()?;
        Ok(result)
    }
    pub fn proposal_discussion(&self, pid: &str, proposal_id: &str) -> Result<Vec<Value>> {
        self.project(pid)?;
        let connection = self.connection()?;
        get_record(&connection, "team_proposals", pid, proposal_id)?;
        let mut q=connection.prepare("SELECT payload_json FROM team_proposal_events WHERE project_id=?1 AND proposal_id=?2 AND kind='comment' ORDER BY sequence")?;
        let rows = q
            .query_map(params![pid, proposal_id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|raw| serde_json::from_str::<Value>(&raw).map_err(Into::into))
            .collect()
    }
    pub fn close_proposal(
        &self,
        pid: &str,
        proposal_id: &str,
        status: &str,
        reason: &str,
        replacement_id: Option<&str>,
    ) -> Result<Value> {
        self.close_proposal_scoped(pid, proposal_id, status, reason, replacement_id, None, None)
    }

    pub fn close_proposal_as_architect(
        &self,
        pid: &str,
        aid: &str,
        proposal_id: &str,
        status: &str,
        reason: &str,
        replacement_id: Option<&str>,
    ) -> Result<Value> {
        self.close_proposal_scoped(
            pid,
            proposal_id,
            status,
            reason,
            replacement_id,
            Some(aid),
            None,
        )
    }

    pub fn close_proposal_as_user(
        &self,
        pid: &str,
        user_id: &str,
        proposal_id: &str,
        status: &str,
        reason: &str,
        replacement_id: Option<&str>,
    ) -> Result<Value> {
        text(user_id, 256, true)?;
        self.close_proposal_scoped(
            pid,
            proposal_id,
            status,
            reason,
            replacement_id,
            None,
            Some(user_id),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn close_proposal_scoped(
        &self,
        pid: &str,
        proposal_id: &str,
        status: &str,
        reason: &str,
        replacement_id: Option<&str>,
        actor: Option<&str>,
        user: Option<&str>,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        ensure!(
            ["rejected", "superseded"].contains(&status),
            "Team proposal can only be rejected or superseded by this action"
        );
        let reason = text(reason, MAX_TEXT, true)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        if let Some(aid) = actor {
            assert_active_agent(&tx, pid, aid)?;
            ensure!(
                query_binding(&tx, pid)?["architect_agent_id"] == aid,
                "Team only the current principal architect can close a model proposal"
            );
        }
        ensure!(
            proposal(&tx, pid, proposal_id)?["status"] == "proposed",
            "Team accepted or closed proposal cannot be overwritten"
        );
        if let Some(replacement) = replacement_id {
            ensure!(
                replacement != proposal_id,
                "Team proposal cannot supersede itself"
            );
            get_record(&tx, "team_proposals", pid, replacement)?;
        }
        let origin = if actor.is_some() {
            "agent"
        } else if user.is_some() {
            "user"
        } else {
            "controller"
        };
        proposal_event(
            &tx,
            pid,
            proposal_id,
            status,
            &json!({"reason":reason,"replacement_id":replacement_id,"origin":origin,"actor_agent_id":actor,"actor_user_id":user}),
        )?;
        insert_event(
            &tx,
            pid,
            actor,
            "proposal_closed",
            &json!({"proposal_id":proposal_id,"status":status,"replacement_id":replacement_id,"actor_user_id":user}),
            origin,
        )?;
        let result = proposal(&tx, pid, proposal_id)?;
        diagnostics::log(
            "APPEND immutable proposal closure",
            Some(self.db_path()),
            Some(proposal_id),
        );
        tx.commit()?;
        Ok(result)
    }

    /// Called by authenticated owner routing or the actor-bound reviewer adapter.
    /// No model tool may forward caller-supplied origin or identity to this method.
    pub fn record_review(&self, pid: &str, input: &Value) -> Result<Value> {
        self.ensure_writable(pid)?;
        let proposal_id = string(input, "proposal_id")?;
        let kind = string(input, "kind")?;
        let verdict = string(input, "verdict")?;
        ensure!(
            ["code", "architecture"].contains(&kind),
            "Team review kind must be code or architecture"
        );
        ensure!(
            ["approved", "changes_requested"].contains(&verdict),
            "Team review verdict must be approved or changes_requested"
        );
        let human = input.get("origin").and_then(Value::as_str) == Some("human");
        let actor = if human {
            None
        } else {
            Some(string(input, "reviewer_agent_id")?)
        };
        let user = if human {
            let value = string(input, "reviewer_user_id")?;
            text(value, 256, true)?;
            Some(value)
        } else {
            None
        };
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let candidate = get_record(&tx, "team_proposals", pid, proposal_id)?;
        ensure!(
            proposal(&tx, pid, proposal_id)?["status"] == "proposed",
            "Team proposal is no longer open for review"
        );
        if let Some(actor) = actor {
            let agent = query_agent(&tx, pid, actor)?;
            ensure!(!agent.retired, "Team reviewer is retired");
            ensure!(
                candidate["author_agent_id"] != actor,
                "Team proposal authors cannot approve their own changes"
            );
            ensure!(
                agent.role
                    == if kind == "code" {
                        "code_reviewer"
                    } else {
                        "architecture_reviewer"
                    },
                "Team reviewer identity has the wrong immutable role"
            );
        }
        let binding = query_binding(&tx, pid)?;
        ensure!(
            proposal_binding_current(&candidate, &binding),
            "Team review candidate intent, catalog or canonical base is stale"
        );
        if let Some(actor) = actor {
            let run_id = string(input, "run_id")?;
            let run = assert_run_context(
                &tx,
                pid,
                actor,
                run_id,
                &binding,
                input.get("expected_context"),
            )?;
            let task_id = run["task_id"]
                .as_str()
                .ok_or_else(|| anyhow!("Team reviewer run is not assigned to a proposal task"))?;
            let task = query_task(&tx, pid, task_id)?;
            ensure!(
                task.assigned_agent_id == actor && task.proposal_id.as_deref() == Some(proposal_id),
                "Team reviewer task does not bind this proposal"
            );
        }
        for key in ["candidate_sha", "candidate_tree", "catalog_hash"] {
            if let Some(value) = input.get(key) {
                ensure!(
                    *value == candidate[key],
                    "Team review does not match the integrated candidate"
                );
            }
        }
        let record_id = id();
        let mut result = input.clone();
        result["id"] = json!(record_id);
        result["project_id"] = json!(pid);
        result["origin"] = json!(if human { "human" } else { "agent" });
        result["created_at"] = json!(now());
        if let Some(object) = result.as_object_mut() {
            if human {
                object.remove("reviewer_agent_id");
            } else {
                object.remove("reviewer_user_id");
            }
        }
        for key in [
            "candidate_sha",
            "candidate_tree",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            result[key] = candidate[key].clone();
        }
        tx.execute("INSERT INTO team_reviews(id,project_id,proposal_id,reviewer_agent_id,reviewer_user_id,kind,candidate_sha,catalog_hash,verdict,payload_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",params![record_id,pid,proposal_id,actor,user,kind,candidate["candidate_sha"].as_str(),candidate["catalog_hash"].as_str(),verdict,encoded(&result)?,result["created_at"].as_str()])?;
        insert_event(
            &tx,
            pid,
            actor,
            "proposal_reviewed",
            &json!({"proposal_id":proposal_id,"review_id":record_id,"kind":kind,"verdict":verdict,"origin":result["origin"]}),
            if human { "user" } else { "agent" },
        )?;
        diagnostics::log(
            "APPEND independent candidate review",
            Some(self.db_path()),
            Some(&record_id),
        );
        tx.commit()?;
        Ok(result)
    }

    pub fn record_agent_review(
        &self,
        pid: &str,
        aid: &str,
        task_id: &str,
        run_id: &str,
        input: &Value,
    ) -> Result<Value> {
        let task = self.team_task(pid, task_id)?;
        ensure!(
            task.assigned_agent_id == aid,
            "Team review task belongs to another agent"
        );
        let proposal_id = task
            .proposal_id
            .as_deref()
            .ok_or_else(|| anyhow!("Team review task has no bound proposal"))?;
        if let Some(requested) = input.get("proposal_id").and_then(Value::as_str) {
            ensure!(
                requested == proposal_id,
                "Team reviewer cannot switch its assigned proposal"
            );
        }
        let mut record = input.clone();
        ensure!(record.is_object(), "Team review must be an object");
        record["proposal_id"] = json!(proposal_id);
        record["reviewer_agent_id"] = json!(aid);
        record["run_id"] = json!(run_id);
        record["task_id"] = json!(task_id);
        record["origin"] = json!("agent");
        if let Some(object) = record.as_object_mut() {
            object.remove("reviewer_user_id");
        }
        self.record_review(pid, &record)
    }

    /// Executor-owned receipt ingestion. Intentionally absent from HTTP and model tools.
    pub(crate) fn record_controller_receipt(&self, pid: &str, input: &Value) -> Result<Value> {
        self.ensure_writable(pid)?;
        let proposal_id = string(input, "proposal_id")?;
        let status = string(input, "status")?;
        ensure!(
            ["pass", "fail", "error", "cancelled", "pending"].contains(&status),
            "Team receipt status is invalid"
        );
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let candidate = get_record(&tx, "team_proposals", pid, proposal_id)?;
        for key in [
            "candidate_sha",
            "candidate_tree",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            ensure!(
                input.get(key) == candidate.get(key),
                "Team receipt does not bind the exact integrated candidate and intent"
            );
        }
        ensure!(
            input["checkout_sha"] == candidate["candidate_sha"],
            "Team receipt checkout does not match the integrated candidate"
        );
        if status == "pass" {
            ensure!(input["exit_code"].as_i64()==Some(0)&&input["candidate_unchanged"].as_bool()==Some(true),"Team passing receipt needs a clean unchanged candidate and a successful executor exit");
        }
        let record_id = input
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(id);
        ident(&record_id)?;
        let at = now();
        let mut result = input.clone();
        result["id"] = json!(record_id);
        result["project_id"] = json!(pid);
        result["origin"] = json!("controller");
        result["kind"] = json!("full_suite");
        result["created_at"] = json!(at);
        if let Some(object) = result.as_object_mut() {
            object.remove("receipt_sha256");
        }
        result["receipt_sha256"] = json!(hash(&result)?);
        tx.execute("INSERT INTO team_receipts(id,project_id,proposal_id,kind,candidate_sha,candidate_tree,catalog_revision,catalog_hash,requirements_revision,decision_revision,policy_revision,status,exit_code,payload_json,origin,created_at) VALUES(?1,?2,?3,'full_suite',?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,'controller',?14)",params![record_id,pid,proposal_id,candidate["candidate_sha"].as_str(),candidate["candidate_tree"].as_str(),candidate["catalog_revision"].as_i64(),candidate["catalog_hash"].as_str(),candidate["requirements_revision"].as_i64(),candidate["decision_revision"].as_i64(),candidate["policy_revision"].as_i64(),status,input["exit_code"].as_i64(),encoded(&result)?,at])?;
        insert_event(
            &tx,
            pid,
            None,
            "controller_receipt",
            &json!({"proposal_id":proposal_id,"receipt_id":record_id,"status":status,"candidate_sha":candidate["candidate_sha"]}),
            "controller",
        )?;
        diagnostics::log(
            "APPEND executor-owned full-suite receipt",
            Some(self.db_path()),
            Some(&record_id),
        );
        tx.commit()?;
        Ok(result)
    }

    pub fn proposal_gate(&self, pid: &str, proposal_id: &str) -> Result<Value> {
        self.project(pid)?;
        let connection = self.connection()?;
        gate(&connection, pid, proposal_id)
    }
    pub fn with_project_acceptance(
        &self,
        pid: &str,
        proposal_id: &str,
        expected_binding: &Value,
        apply: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Value> {
        self.ensure_writable(pid)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_writable_project(&tx, pid)?;
        let binding = query_binding(&tx, pid)?;
        for key in [
            "canonical_head",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ] {
            ensure!(
                expected_binding.get(key) == binding.get(key),
                "Team acceptance lease changed; rerun review and the active suite"
            );
        }
        let result = gate(&tx, pid, proposal_id)?;
        ensure!(
            result["ready"] == true,
            "Team acceptance gate is blocked; inspect its reasons"
        );
        let candidate = get_record(&tx, "team_proposals", pid, proposal_id)?;
        // The Git owner fsyncs its prepared journal before this callback. A CAS
        // failure rolls SQLite back; a crash is reconciled against that journal.
        apply()?;
        tx.execute(
            "UPDATE team_project_state SET canonical_head=?1,updated_at=?2 WHERE project_id=?3",
            params![candidate["candidate_sha"].as_str(), now(), pid],
        )?;
        proposal_event(
            &tx,
            pid,
            proposal_id,
            "accepted",
            &json!({"candidate_sha":candidate["candidate_sha"],"binding":binding,"gate":result}),
        )?;
        insert_event(
            &tx,
            pid,
            None,
            "proposal_accepted",
            &json!({"proposal_id":proposal_id,"candidate_sha":candidate["candidate_sha"],"receipt_id":result["receipt_id"]}),
            "controller",
        )?;
        diagnostics::log(
            "ACCEPT gated integrated Git candidate",
            Some(self.db_path()),
            Some(proposal_id),
        );
        let output = proposal(&tx, pid, proposal_id)?;
        tx.commit()?;
        Ok(output)
    }
}

fn proposal_event(
    connection: &Connection,
    pid: &str,
    proposal_id: &str,
    kind: &str,
    payload: &Value,
) -> Result<()> {
    connection.execute("INSERT INTO team_proposal_events(id,project_id,proposal_id,kind,payload_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![id(),pid,proposal_id,kind,encoded(payload)?,now()])?;
    Ok(())
}
fn proposal(connection: &Connection, pid: &str, proposal_id: &str) -> Result<Value> {
    let mut result = get_record(connection, "team_proposals", pid, proposal_id)?;
    let status:Option<(String,String)>=connection.query_row("SELECT kind,payload_json FROM team_proposal_events WHERE project_id=?1 AND proposal_id=?2 AND kind IN('proposed','accepted','rejected','superseded') ORDER BY sequence DESC LIMIT 1",params![pid,proposal_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((status, payload)) = status {
        result["status"] = json!(status);
        result["status_details"] = serde_json::from_str(&payload)?;
    } else {
        result["status"] = json!("proposed");
    }
    Ok(result)
}
fn records(
    connection: &Connection,
    table: &str,
    pid: &str,
    proposal_id: &str,
) -> Result<Vec<Value>> {
    ensure!(
        ["team_reviews", "team_receipts"].contains(&table),
        "Team history type is invalid"
    );
    let mut q = connection.prepare(&format!(
        "SELECT payload_json FROM {table} WHERE project_id=?1 AND proposal_id=?2 ORDER BY sequence"
    ))?;
    let rows = q
        .query_map(params![pid, proposal_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .collect()
}
fn proposal_binding_current(proposal: &Value, binding: &Value) -> bool {
    proposal["base_sha"] == binding["canonical_head"]
        && [
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ]
        .iter()
        .all(|key| proposal[*key] == binding[*key])
}
fn assert_run_context(
    connection: &Connection,
    pid: &str,
    aid: &str,
    run_id: &str,
    binding: &Value,
    expected: Option<&Value>,
) -> Result<Value> {
    ident(run_id)?;
    let raw: Option<String> = connection
        .query_row(
            "SELECT value_json FROM kv WHERE key=?1",
            [format!("run:{run_id}")],
            |r| r.get(0),
        )
        .optional()?;
    let run: Value =
        serde_json::from_str(&raw.ok_or_else(|| anyhow!("Team model run context was not found"))?)?;
    ensure!(
        run["project_id"] == pid && run["agent_id"] == aid,
        "Team model run belongs to another immutable identity"
    );
    ensure!(
        [
            "canonical_head",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision"
        ]
        .iter()
        .all(
            |key| run["context_binding"].get(key) == binding.get(key) && binding.get(key).is_some()
        ),
        "Team model generation used stale project intent, catalog or Git context"
    );
    if let Some(expected) = expected {
        ensure!(
            *expected == run["context_binding"],
            "Team proposal generation context was replaced before recording"
        );
    }
    Ok(run)
}
fn gate(connection: &Connection, pid: &str, proposal_id: &str) -> Result<Value> {
    let candidate = proposal(connection, pid, proposal_id)?;
    let binding = query_binding(connection, pid)?;
    let catalog = catalog(connection, pid)?;
    let mut reasons = Vec::<String>::new();
    if candidate["status"] != "proposed" {
        reasons.push("Proposal is not open".into());
    }
    if !proposal_binding_current(&candidate, &binding) {
        reasons.push("Canonical base, intent, policy or active catalog changed".into());
    }
    let tests = catalog["tests"].as_array().cloned().unwrap_or_default();
    let active_tests = tests
        .iter()
        .filter(|test| test["active"] != false)
        .collect::<Vec<_>>();
    if active_tests.is_empty() {
        reasons.push("Regression catalog has no active checks".into());
    }
    let review_records = records(connection, "team_reviews", pid, proposal_id)?;
    let mut latest = HashMap::<(String, String), Value>::new();
    for review in review_records {
        let identity = if review["origin"] == "human" {
            format!(
                "human:{}",
                review["reviewer_user_id"].as_str().unwrap_or_default()
            )
        } else {
            format!(
                "agent:{}",
                review["reviewer_agent_id"].as_str().unwrap_or_default()
            )
        };
        latest.insert(
            (identity, review["kind"].as_str().unwrap_or_default().into()),
            review,
        );
    }
    let mut code = HashSet::new();
    let mut architecture = HashSet::new();
    for ((identity, kind), review) in latest {
        if review["candidate_sha"] != candidate["candidate_sha"]
            || review["candidate_tree"] != candidate["candidate_tree"]
            || review["catalog_hash"] != candidate["catalog_hash"]
        {
            continue;
        }
        if review["verdict"] == "changes_requested" {
            reasons.push("An independent review still requests changes".into());
        }
        if review["verdict"] == "approved" {
            if kind == "code" {
                code.insert(identity);
            } else if kind == "architecture" {
                architecture.insert(identity);
            }
        }
    }
    if !code
        .iter()
        .any(|code| architecture.iter().any(|architecture| architecture != code))
    {
        reasons.push("Distinct independent code and architecture approvals are required".into());
    }
    let receipt_records = records(connection, "team_receipts", pid, proposal_id)?;
    let mut receipt_id = Value::Null;
    if let Some(receipt) = receipt_records.last() {
        let same = [
            "candidate_sha",
            "candidate_tree",
            "catalog_revision",
            "catalog_hash",
            "requirements_revision",
            "decision_revision",
            "policy_revision",
        ]
        .iter()
        .all(|key| receipt[*key] == candidate[*key]);
        let cases = receipt["tests"].as_array().cloned().unwrap_or_default();
        let mut seen = HashSet::new();
        let unique = cases.iter().all(|case| {
            case["id"]
                .as_str()
                .map(|id| seen.insert(id.to_owned()))
                .unwrap_or(false)
        });
        let all_pass = unique
            && active_tests.iter().all(|test| {
                cases
                    .iter()
                    .any(|case| case["id"] == test["id"] && case["status"] == "pass")
            });
        if same
            && receipt["status"] == "pass"
            && receipt["origin"] == "controller"
            && receipt["exit_code"] == 0
            && receipt["candidate_unchanged"] == true
            && all_pass
        {
            receipt_id = receipt["id"].clone();
        } else {
            reasons.push("Latest controller receipt does not pass the complete active suite on this exact candidate".into());
        }
    } else {
        reasons.push("A controller-owned full-suite execution receipt is required".into());
    }
    reasons.sort();
    reasons.dedup();
    Ok(
        json!({"project_id":pid,"proposal_id":proposal_id,"ready":reasons.is_empty(),"reasons":reasons,"candidate_sha":candidate["candidate_sha"],"candidate_tree":candidate["candidate_tree"],"binding":binding,"receipt_id":receipt_id,"active_test_count":active_tests.len()}),
    )
}

type ApiResult<T> = std::result::Result<Json<T>, (StatusCode, Json<Value>)>;
fn api_error(error: anyhow::Error) -> (StatusCode, Json<Value>) {
    diagnostics::log("REQUEST team operation failed", None, None);
    let message = error.to_string();
    let safe = if message.starts_with("Team ")
        || message == "Project not found"
        || message == "Workspace not found"
        || message.starts_with("Archived ")
        || message.starts_with("Locked ")
    {
        message
    } else {
        "Team operation could not finish; reload the project and review its current state".into()
    };
    (
        if safe.ends_with("not found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::BAD_REQUEST
        },
        Json(json!({"error":safe})),
    )
}
async fn blocking<T, F>(work: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| api_error(anyhow!("Team operation was interrupted")))?
        .map(Json)
        .map_err(api_error)
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/projects/:pid/team", get(overview))
        .route("/projects/:pid/agents", post(create_agent))
        .route("/projects/:pid/agents/:aid/retire", post(retire_agent))
        .route("/projects/:pid/agents/:aid/messages", get(agent_messages))
        .route("/projects/:pid/decisions", post(user_decision))
        .route("/projects/:pid/requirements", post(user_requirement))
        .route("/projects/:pid/catalog", get(get_catalog).put(put_catalog))
        .route("/projects/:pid/tasks", post(create_task))
        .route(
            "/projects/:pid/tasks/:tid",
            axum::routing::patch(update_task),
        )
        .route("/projects/:pid/events", get(events))
        .route(
            "/workspaces/:wid/settings",
            get(get_workspace_settings).put(put_workspace_settings),
        )
}
async fn overview(State(state): State<AppState>, Path(pid): Path<String>) -> ApiResult<Value> {
    blocking(move || state.store.team(&pid)).await
}
async fn create_agent(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<TeamAgent> {
    blocking(move || {
        let architect = state.store.architect(&pid)?;
        state.store.create_worker(
            &pid,
            &architect.id,
            string(&input, "role")?,
            string(&input, "label")?,
            input
                .get("instructions")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )
    })
    .await
}
async fn retire_agent(
    State(state): State<AppState>,
    Path((pid, aid)): Path<(String, String)>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    blocking(move || {
        let retired=state.store.team_retire_agent(
            &pid,
            &aid,
            input.get("replacement").filter(|v| !v.is_null()),
            input
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("Retired by project owner"),
        )?;
        // Commit identity retirement first. Failed validation must not stop a
        // live agent, and a replacement stays idle until explicitly assigned.
        if let Err(error)=state.agents.stop_agent(state.store.clone(),&pid,&aid) {
            if error.to_string()!="Run not found" {
                diagnostics::log("STOP retired agent runtime could not finish",Some(state.store.db_path()),Some(&aid));
                return Err(anyhow!("Team agent is retired; runtime cancellation could not finish. Stop the project before resuming work."));
            }
        }
        Ok(retired)
    })
    .await
}
async fn agent_messages(
    State(state): State<AppState>,
    Path((pid, aid)): Path<(String, String)>,
) -> ApiResult<Vec<Message>> {
    blocking(move || state.store.agent_messages(&pid, &aid)).await
}
async fn user_decision(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    blocking(move || {
        state
            .store
            .append_document(&pid, None, "decision", &input, "user")
    })
    .await
}
async fn user_requirement(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    blocking(move || {
        state
            .store
            .append_document(&pid, None, "requirement", &input, "user")
    })
    .await
}
async fn get_catalog(State(state): State<AppState>, Path(pid): Path<String>) -> ApiResult<Value> {
    blocking(move || state.store.catalog_snapshot(&pid)).await
}
async fn put_catalog(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    blocking(move || state.store.team_activate_catalog(&pid, &input, None)).await
}
async fn create_task(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    let store = state.store.clone();
    let task_pid = pid.clone();
    let task = tokio::task::spawn_blocking(move || {
        let dependencies = input.get("dependencies").cloned().unwrap_or(json!([]));
        let dependencies: Vec<String> = serde_json::from_value(dependencies)
            .context("Team task dependencies must be an ID array")?;
        let task = store.create_team_task(
            &task_pid,
            string(&input, "assigned_agent_id")?,
            string(&input, "title")?,
            input
                .get("instructions")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            &dependencies,
        )?;
        if let Some(proposal) = input.get("proposal_id").and_then(Value::as_str) {
            store.bind_task_proposal(&task_pid, &task.id, proposal)
        } else {
            Ok(task)
        }
    })
    .await
    .map_err(|_| api_error(anyhow!("Team task operation was interrupted")))?
    .map_err(api_error)?;
    match state
        .agents
        .schedule_task(state.store.clone(), &pid, &task.id)
    {
        Ok(run) => Ok(Json(json!({"task":task,"run":run,"scheduled":true}))),
        Err(_) => Ok(Json(
            json!({"task":task,"run":Value::Null,"scheduled":false,"message":"Task is saved; dependencies or project state are waiting. Resume it when ready."}),
        )),
    }
}
async fn update_task(
    State(state): State<AppState>,
    Path((pid, tid)): Path<(String, String)>,
    Json(input): Json<Value>,
) -> ApiResult<TeamTask> {
    blocking(move || state.store.update_team_task(&pid, &tid, &input)).await
}
#[derive(Deserialize)]
struct EventQuery {
    #[serde(default)]
    after: i64,
    #[serde(default = "event_limit")]
    limit: usize,
}
fn event_limit() -> usize {
    200
}
async fn events(
    State(state): State<AppState>,
    Path(pid): Path<String>,
    Query(query): Query<EventQuery>,
) -> ApiResult<Vec<Value>> {
    blocking(move || state.store.team_events(&pid, query.after, query.limit)).await
}
async fn get_workspace_settings(
    State(state): State<AppState>,
    Path(wid): Path<String>,
) -> ApiResult<Value> {
    blocking(move || state.store.workspace_settings(&wid)).await
}
async fn put_workspace_settings(
    State(state): State<AppState>,
    Path(wid): Path<String>,
    Json(input): Json<Value>,
) -> ApiResult<Value> {
    blocking(move || state.store.save_workspace_settings(&wid, &input)).await
}
