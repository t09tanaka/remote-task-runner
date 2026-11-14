//! Explicit command context shared by submission and read-only commands.
use super::bootstrap::Spec;
use crate::config::{Overlay, ResolvedConfig};
use crate::error::{ApiError, ErrorCode};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub struct Context {
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub config: ResolvedConfig,
    pub repo_root: Option<PathBuf>,
    pub bootstrap: Spec,
}
impl Context {
    /// Loading never creates configuration, identities, keys or coordinator epochs.
    pub fn load(
        env: BTreeMap<String, String>,
        cwd: PathBuf,
        bootstrap: Spec,
    ) -> Result<Self, ApiError> {
        if !cwd.is_absolute() {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "command directory must be absolute",
            ));
        }
        let output = std::process::Command::new("git")
            .args(["rev-parse", "--show-toplevel"])
            .current_dir(&cwd)
            .env_clear()
            .envs(&env)
            .env("LC_ALL", "C")
            .output()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, format!("Git preflight: {e}")))?;
        let root = if output.status.success() {
            Some(PathBuf::from(
                String::from_utf8(output.stdout)
                    .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?
                    .strip_suffix('\n')
                    .ok_or_else(|| {
                        ApiError::new(ErrorCode::InvalidInput, "Git root response missing newline")
                    })?,
            ))
        } else if String::from_utf8_lossy(&output.stderr)
            .starts_with("fatal: not a git repository (or any of the parent directories)")
        {
            None
        } else {
            return Err(ApiError::new(
                ErrorCode::InvalidInput,
                "cannot determine the source repository; Git preflight failed",
            ));
        };
        let config = crate::config::load::load(&env, root.as_deref(), &Overlay::default())
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        Ok(Self {
            env,
            cwd,
            config,
            repo_root: root,
            bootstrap,
        })
    }
    pub fn discover(env: BTreeMap<String, String>, cwd: PathBuf) -> Result<Self, ApiError> {
        let file = crate::config::paths::user_config_path(&env)
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        let input = file
            .parent()
            .expect("configuration parent")
            .join("coordinator.json");
        if !input
            .try_exists()
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?
        {
            return Err(ApiError::new(
                ErrorCode::NotFound,
                "coordinator is not initialized; run remote-task-runner init",
            ));
        }
        let spec = super::bootstrap::read_input(&input)
            .map_err(|e| ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
        Self::load(env, cwd, spec)
    }
    pub fn state_dir(&self) -> &Path {
        &self.bootstrap.state_dir
    }
    pub async fn readonly(&self) -> Result<super::query::Reader, super::query::Error> {
        super::query::Reader::open(self.bootstrap.clone()).await
    }
    pub async fn owner(&self) -> anyhow::Result<super::resident::Client> {
        super::bootstrap::ensure(self.bootstrap.clone()).await
    }
}
