//! One immutable, private input capture shared by command submission and CI planning.
use super::context::Context;
use crate::error::{ApiError, ErrorCode};
use crate::ids::RunId;
use crate::secrets::{
    capture::{capture_run, SecretInputs},
    vault::Vault,
};
use crate::source::{CaptureParams, Captured, SourceRequest, SourceStore};

/// The revision is explicit when configured to capture a commit; it never silently becomes HEAD.
pub fn capture(
    context: &Context,
    run: &RunId,
    revision: Option<&str>,
) -> Result<Captured, ApiError> {
    let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
    let request = match (context.repo_root.as_ref(), context.config.source_mode.value) {
        (None, crate::config::model::SourceMode::Commit) => {
            return Err(invalid(
                "commit snapshot requires a repository and an explicit revision".into(),
            ))
        }
        (None, _) if revision.is_some() => {
            return Err(invalid("a revision requires a repository".into()))
        }
        (None, _) => SourceRequest::Directory {
            dir: context.cwd.clone(),
        },
        (Some(_), crate::config::model::SourceMode::Worktree) if revision.is_none() => {
            SourceRequest::Worktree {
                dir: context.cwd.clone(),
            }
        }
        (Some(_), _) => SourceRequest::Commit {
            dir: context.cwd.clone(),
            rev: revision
                .ok_or_else(|| invalid("commit snapshot requires an explicit revision".into()))?
                .to_string(),
        },
    };
    let store = SourceStore::open(context.state_dir()).map_err(|e| invalid(e.to_string()))?;
    let vault = Vault::open(context.state_dir()).map_err(|e| e.to_api())?;
    let key =
        crate::secrets::key::load(&context.bootstrap.cluster_key_path).map_err(|e| e.to_api())?;
    let declared: Vec<_> = context
        .config
        .secret_files
        .value
        .iter()
        .map(|s| local_ci::source::SecretSpec {
            glob: s.glob.clone(),
            required: s.required,
        })
        .collect();
    capture_run(
        &context.env,
        &store,
        &vault,
        &SecretInputs {
            key: &key,
            declared: &declared,
        },
        run,
        &request,
        &CaptureParams {
            include: context.config.include.value.clone(),
            exclude: context.config.exclude.value.clone(),
            ..Default::default()
        },
    )
}

/// Materialize a previously bound input for read-only planning; never recapture.
/// The caller owns the destination and must not execute project hooks in this view.
pub fn source_view(
    context: &Context,
    run: &RunId,
    destination: &std::path::Path,
) -> Result<(), ApiError> {
    let internal = |e: anyhow::Error| ApiError::new(ErrorCode::Internal, e.to_string());
    let store = SourceStore::open(context.state_dir()).map_err(internal)?;
    let source = store
        .source_of_run(run)
        .map_err(internal)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "run input is unavailable"))?;
    let manifest = store
        .manifest(&source)
        .map_err(internal)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "source manifest is unavailable"))?;
    store.materialize(&manifest, destination).map_err(internal)
}
