//! Freeze command input once and obtain an acceptance receipt from the existing owner.
use super::{
    context::Context,
    resident::{Operation, Reply},
};
use crate::{
    coordinator::TaskSpec,
    error::{ApiError, ErrorCode},
    ids::*,
    wire::CommandSpec,
};
use serde::Serialize;
#[derive(Debug, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub source_id: SourceId,
    pub state: &'static str,
    #[serde(skip)]
    pub warnings: Vec<String>,
}
pub async fn command(
    context: Context,
    command: Vec<String>,
    revision: Option<String>,
) -> Result<Receipt, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    if command.is_empty() {
        return Err(invalid("command cannot be empty".into()));
    }
    let owner = context
        .owner()
        .await
        .map_err(|e| ApiError::new(ErrorCode::CommunicationUnknown, e.to_string()))?;
    let run_id = RunId::generate();
    let task_id = TaskId::generate();
    let (context, captured, run_id) = tokio::task::spawn_blocking(move || {
        let captured = super::input::capture(&context, &run_id, revision.as_deref());
        (context, captured, run_id)
    })
    .await
    .map_err(|e| invalid(e.to_string()))?;
    let captured = captured?;
    let source_id = captured.manifest.source_id.clone();
    let digest = Digest::parse(&captured.manifest.manifest_digest)
        .or_else(|_| Digest::parse(&format!("sha256:{}", captured.manifest.manifest_digest)))
        .map_err(|e| invalid(e.to_string()))?;
    let cwd = String::from_utf8(captured.manifest.cwd_relative.0.clone())
        .map_err(|e| invalid(e.to_string()))?;
    let env = context
        .config
        .pass_env
        .value
        .iter()
        .filter_map(|key| {
            context
                .env
                .get(key)
                .map(|value| (key.clone(), value.clone()))
        })
        .collect();
    let timeout = context
        .config
        .timeout_seconds
        .value
        .map(|s| {
            s.checked_mul(1000)
                .ok_or_else(|| invalid("command timeout overflow".into()))
        })
        .transpose()?;
    if !context.config.requires.value.is_empty() || context.config.profile.value.is_some() {
        return Err(ApiError::new(
            ErrorCode::NotImplemented,
            "command capability requirements and profiles need a frozen execution planner",
        ));
    }
    let has_fast_worktree = captured
        .manifest
        .object_manifest
        .entries
        .iter()
        .any(|e| e.path.0 == b".fast-worktree.toml");
    let setup = crate::setup::policy::prepare_spec(
        &context.config.prepare_commands,
        has_fast_worktree,
        false,
    );
    let mut artifacts = Vec::new();
    for output in &context.config.outputs {
        if output.paths.len() != 1
            || output.when != crate::config::model::OutputWhen::Always
            || output.paths[0].contains(['*', '?', '[', '{'])
        {
            return Err(ApiError::new(
                ErrorCode::NotImplemented,
                "conditional, multi-path and glob outputs need an output expansion planner",
            ));
        }
        artifacts.push(crate::wire::ArtifactDecl {
            name: output.name.clone(),
            path: output.paths[0].clone(),
            if_missing: if output.required {
                crate::wire::MissingPolicy::Required
            } else {
                crate::wire::MissingPolicy::Optional
            },
        });
    }
    let spec = TaskSpec {
        setup,
        plan_digest: Digest::of_bytes(
            &serde_json::to_vec(&command).map_err(|e| invalid(e.to_string()))?,
        ),
        manifest_digest: digest,
        commands: vec![CommandSpec {
            node_id: NodeId::parse("command").map_err(|e| invalid(e.to_string()))?,
            argv: command,
            cwd,
            env,
            depends_on: vec![],
            timeout_ms: timeout,
            setup_step: None,
        }],
        command_timeout_ms: timeout,
        deadline_ms: None,
        resource_wait_timeout_ms: None,
        has_secret_bundle: !captured.manifest.secret_files.is_empty(),
        artifacts,
    };
    match owner
        .request(Operation::SubmitCaptured {
            run_id: run_id.clone(),
            source_id: source_id.clone(),
            tasks: vec![(task_id.clone(), spec)],
        })
        .await
        .map_err(|e| {
            ApiError::new(
                ErrorCode::CommunicationUnknown,
                format!("submit unconfirmed for run {run_id}, task {task_id}: {e}"),
            )
        })? {
        Reply::Accepted { run_id: accepted } if accepted == run_id => Ok(Receipt {
            schema_version: 1,
            run_id,
            task_id,
            source_id,
            state: "coordinator_accepted",
            warnings: captured.warnings,
        }),
        Reply::Error { message } => Err(invalid(message)),
        _ => Err(ApiError::new(
            ErrorCode::CommunicationUnknown,
            "unexpected submit receipt",
        )),
    }
}
