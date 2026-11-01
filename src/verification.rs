//! Controller-owned regression execution. Model statements are never test receipts.
use crate::{diagnostics, files, git_project, store::Store};
use anyhow::{bail, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

fn identity(root: &Path) -> Result<Value> {
    git_project::inspect_worktree(root)
}
fn exact(snapshot: &Value, sha: &str, tree: &str) -> bool {
    snapshot["sha"].as_str() == Some(sha)
        && snapshot["tree"].as_str() == Some(tree)
        && snapshot["dirty"].as_bool() == Some(false)
}

/// Tests run outside the scheduler's inference slots. Only fixed native checks
/// execute here; command and physical tests stay pending until a suitable
/// isolated executor/operator-evidence adapter is configured.
#[allow(clippy::too_many_arguments)]
pub async fn run_suite(
    store: Arc<Store>,
    project_id: &str,
    proposal_id: &str,
    candidate_root: &Path,
    candidate_sha: &str,
    candidate_tree: &str,
    catalog: &Value,
    cancel: CancellationToken,
) -> Result<Value> {
    store.project(project_id)?;
    let root: PathBuf = candidate_root.to_path_buf();
    let sha = candidate_sha.to_owned();
    let tree = candidate_tree.to_owned();
    let proposal_id = proposal_id.to_owned();
    let project_id = project_id.to_owned();
    let catalog = catalog.clone();
    tokio::task::spawn_blocking(move || {
        let started_at=crate::types::now();
        diagnostics::log("VERIFY full active regression suite",Some(&root),Some(&proposal_id));
        let before=identity(&root)?;
        if !exact(&before,&sha,&tree) { bail!("Candidate checkout is dirty or no longer matches the proposed version"); }
        let tests=catalog["tests"].as_array().or_else(||catalog["entries"].as_array()).cloned().unwrap_or_default();
        let mut results=Vec::new();
        for test in tests.iter().filter(|t| t["active"].as_bool().unwrap_or(true)) {
            let id=test["id"].as_str().unwrap_or_default();
            let kind=test["kind"].as_str().unwrap_or("manual");
            let path=test["path"].as_str().unwrap_or_default();
            let (status,reason)=if cancel.is_cancelled() {
                ("cancelled","SUITE_CANCELLED")
            } else if test["hardware"].as_bool().unwrap_or(false) || matches!(kind,"manual"|"hardware") {
                ("pending","PHYSICAL_OR_MANUAL_EVIDENCE_REQUIRED")
            } else if kind=="command" {
                ("pending","ISOLATED_CANDIDATE_EXECUTOR_REQUIRED")
            } else {
                let checked: Result<bool> = match kind {
                    "file_exists"=>files::scoped_file_exists(&root,path),
                    "file_contains"=>files::read_scoped_file(&root,path).map(|s| test["expected"].as_str().is_some_and(|expected|s.contains(expected))),
                    "json_valid"=>files::read_scoped_file(&root,path).map(|s|serde_json::from_str::<Value>(&s).is_ok()),
                    "git_diff_check"=>git_project::diff_check(&root),
                    _=>Ok(false),
                };
                match checked {Ok(true)=>("pass","OBSERVED_EXPECTATION_MET"),Ok(false)=>("fail","OBSERVED_EXPECTATION_NOT_MET"),Err(_)=>("error","CHECK_COULD_NOT_COMPLETE")}
            };
            results.push(json!({"id":id,"kind":kind,"status":status,"reason":reason,"required":test["required"].as_bool().unwrap_or(true),"observed_at":crate::types::now(),"origin":"controller_executor"}));
        }
        let after=identity(&root)?;
        let unchanged=exact(&after,&sha,&tree);
        let status=if !unchanged {"error"} else if cancel.is_cancelled(){"cancelled"} else if results.is_empty(){"pending"} else if results.iter().any(|r|matches!(r["status"].as_str(),Some("fail"|"error"))){"fail"} else if results.iter().any(|r|r["status"]!="pass"){"pending"} else {"pass"};
        let mut receipt=json!({"id":crate::types::id(),"project_id":project_id,"proposal_id":proposal_id,"candidate_sha":sha,"checkout_sha":sha,"candidate_tree":tree,"catalog_revision":catalog["revision"],"catalog_hash":catalog["hash"],"requirements_revision":catalog["requirements_revision"],"decision_revision":catalog["decision_revision"],"policy_revision":catalog["policy_revision"],"status":status,"exit_code":if status=="pass"{Some(0)}else{None},"tests":results,"cases":results,"artifacts":[],"started_at":started_at,"finished_at":crate::types::now(),"runner":"controller-native-checks-v1","origin":"controller_executor","candidate_unchanged":unchanged,"limitation":"Command tests require an isolated candidate executor. Manual and hardware tests require independent operator evidence. Neither is inferred from compilation or agent claims."});
        receipt["receipt_sha256"]=json!(hex::encode(Sha256::digest(serde_json::to_vec(&receipt)?)));
        diagnostics::log("VERIFY regression suite recorded",Some(&root),Some(status));
        Ok(receipt)
    }).await.map_err(|_|anyhow::anyhow!("Regression execution was interrupted"))?
}
