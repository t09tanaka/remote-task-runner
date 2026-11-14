//! Public metadata-only query protocol; pagination never embeds execution inputs.
use crate::ids::*;
use crate::state::{AttemptState, Observation, Receipts, Termination};
use crate::storage::query::{Page, Run, Task};
use crate::storage::{CoordinatorStore, Result};
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "query", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Runs {
        limit: u32,
        after: Option<RunId>,
    },
    Run {
        run_id: RunId,
    },
    Tasks {
        run_id: RunId,
        limit: u32,
        after: Option<TaskId>,
    },
    Task {
        task_id: TaskId,
    },
    Attempts {
        task_id: TaskId,
        limit: u32,
        after: Option<Generation>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub attempt_id: AttemptId,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub generation: Generation,
    pub state: AttemptState,
    pub observation: Observation,
    pub termination: Option<Termination>,
    pub receipts: Receipts,
    pub retired: bool,
    pub cancel_requested_at_ms: Option<u64>,
    pub collection_failed_at_ms: Option<u64>,
    pub created_at_ms: u64,
    pub terminal_at_ms: Option<u64>,
    /// None until the collection has closed; terminal execution alone is not complete.
    pub collection_verdict: Option<String>,
    pub collection_closed_at_ms: Option<u64>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    Runs { page: Page<Run> },
    Run { run: Option<Run> },
    Tasks { page: Page<Task> },
    Task { task: Option<Task> },
    Attempts { page: Page<Attempt> },
}
impl CoordinatorStore {
    pub fn query(&self, query: Query) -> Result<Response> {
        Ok(match query {
            Query::Runs { limit, after } => Response::Runs {
                page: self.query_runs(limit, after)?,
            },
            Query::Run { run_id } => Response::Run {
                run: self.query_run(run_id)?,
            },
            Query::Tasks {
                run_id,
                limit,
                after,
            } => Response::Tasks {
                page: self.query_tasks(run_id, limit, after)?,
            },
            Query::Task { task_id } => Response::Task {
                task: self.query_task(task_id)?,
            },
            Query::Attempts {
                task_id,
                limit,
                after,
            } => {
                let page = self.query_attempts(task_id, limit, after)?;
                let items = page
                    .items
                    .into_iter()
                    .map(|a| {
                        let (verdict, closed) = self.query_collection_metadata(&a.attempt_id)?;
                        Ok(Attempt {
                            attempt_id: a.attempt_id,
                            task_id: a.task_id,
                            worker_id: a.worker_id,
                            generation: a.generation,
                            state: a.state,
                            observation: a.observation,
                            termination: a.termination,
                            receipts: a.receipts,
                            retired: a.retired,
                            cancel_requested_at_ms: a.cancel_requested_at_ms,
                            collection_failed_at_ms: a.collection_failed_at_ms,
                            created_at_ms: a.created_at_ms,
                            terminal_at_ms: a.terminal_at_ms,
                            collection_verdict: verdict,
                            collection_closed_at_ms: closed,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Response::Attempts {
                    page: Page {
                        items,
                        next_cursor: page.next_cursor,
                    },
                }
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub code: crate::error::ErrorCode,
    pub message: String,
}
impl Error {
    pub(crate) fn new(code: crate::error::ErrorCode, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}
impl From<crate::ids::IdError> for Error {
    fn from(e: crate::ids::IdError) -> Self {
        Self::new(crate::error::ErrorCode::InvalidInput, e)
    }
}
impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Self::new(crate::error::ErrorCode::InvalidInput, e)
    }
}
/// A bounded show response. `next_cursor` applies to tasks of a run or the
/// attempt generations of a task; callers must follow it before assuming the
/// displayed history is complete.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Show {
    Run { run: Run, tasks: Page<Task> },
    Task { task: Task, attempts: Page<Attempt> },
}
/// Read-only durable access is available with the owner stopped. Opening this
/// handle never starts a daemon, takes ownership, or migrates the database.
pub enum Reader {
    Connected(super::resident::Client),
    Offline(std::sync::Arc<CoordinatorStore>),
}
impl Reader {
    pub async fn open(spec: super::bootstrap::Spec) -> std::result::Result<Self, Error> {
        let store = tokio::task::spawn_blocking(move || super::bootstrap::read_only_store(&spec))
            .await
            .map_err(|e| Error::new(crate::error::ErrorCode::Internal, e))?
            .map_err(|e| Error::new(crate::error::ErrorCode::InvalidInput, e))?;
        Ok(Self::Offline(std::sync::Arc::new(store)))
    }
    pub async fn query(&self, query: Query) -> std::result::Result<Response, Error> {
        match self {
            Self::Connected(client) => client.query(query).await,
            Self::Offline(store) => {
                let store = store.clone();
                tokio::task::spawn_blocking(move || store.query(query))
                    .await
                    .map_err(|e| Error::new(crate::error::ErrorCode::Internal, e))?
                    .map_err(|e| Error::new(e.code(), e))
            }
        }
    }
    pub async fn show(
        &self,
        id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> std::result::Result<Show, Error> {
        if id.starts_with("run_") {
            let run_id = RunId::parse(id)?;
            let after = cursor.map(TaskId::parse).transpose()?;
            let Response::Run { run: Some(run) } = self
                .query(Query::Run {
                    run_id: run_id.clone(),
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::NotFound,
                    format!("run {run_id} not found"),
                ));
            };
            let Response::Tasks { page: tasks } = self
                .query(Query::Tasks {
                    run_id,
                    limit,
                    after,
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::Internal,
                    "unexpected tasks response",
                ));
            };
            Ok(Show::Run { run, tasks })
        } else if id.starts_with("task_") {
            let task_id = TaskId::parse(id)?;
            let after = cursor.map(str::parse::<u64>).transpose()?.map(Generation);
            let Response::Task { task: Some(task) } = self
                .query(Query::Task {
                    task_id: task_id.clone(),
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::NotFound,
                    format!("task {task_id} not found"),
                ));
            };
            let Response::Attempts { page: attempts } = self
                .query(Query::Attempts {
                    task_id,
                    limit,
                    after,
                })
                .await?
            else {
                return Err(Error::new(
                    crate::error::ErrorCode::Internal,
                    "unexpected attempts response",
                ));
            };
            Ok(Show::Task { task, attempts })
        } else {
            Err(Error::new(
                crate::error::ErrorCode::InvalidInput,
                "expected run_ or task_ identifier",
            ))
        }
    }
}

impl super::resident::Client {
    pub async fn show(
        &self,
        id: &str,
        limit: u32,
        cursor: Option<&str>,
    ) -> std::result::Result<Show, Error> {
        Reader::Connected(self.clone())
            .show(id, limit, cursor)
            .await
    }
}
