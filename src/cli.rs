//! Command-line definition and dispatch.

use clap::{Args, Parser, Subcommand};
use serde::Serialize;

pub use crate::error::exit;
use crate::error::ErrorCode;

/// Version of the `--json` envelope.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Parser)]
#[command(name = "remote-task-runner", version, about)]
pub struct Cli {
    /// Print a structured result on stdout (progress and diagnostics go to stderr).
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the parent owner process (internal).
    #[command(hide = true)]
    Daemon(DaemonArgs),
    /// Configure the coordinator and the local worker.
    Init,
    /// Run CI for the project.
    Ci(CiArgs),
    /// Run one command and wait for its result.
    Exec(ExecArgs),
    /// Submit a command without waiting.
    Submit(SubmitArgs),
    /// List runs and tasks.
    Ls(ListArgs),
    /// Show one run or task.
    Show(ShowArgs),
    /// Show logs.
    Logs(LogsArgs),
    /// Wait for completion.
    Wait(IdArgs),
    /// Request cancellation.
    Cancel(IdArgs),
    /// Retry a terminal attempt.
    Retry(IdArgs),
    /// Fetch outputs.
    Outputs(OutputsArgs),
    /// Estimate a command.
    Estimate(SubmitArgs),
    /// Check the setup.
    Doctor,
    /// Manage workers.
    Workers(WorkersArgs),
    /// Run as a worker (internal).
    #[command(hide = true)]
    Worker(WorkerArgs),
    /// Supervise one attempt as its own process (internal).
    #[command(hide = true)]
    Supervise(SuperviseArgs),
}

#[derive(Debug, Args)]
pub struct DaemonArgs {
    /// Retained private service bootstrap input; absent means bounded stdin.
    #[arg(long)]
    pub input: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct SuperviseArgs {
    /// JSON spec file written by the agent.
    #[arg(long)]
    pub spec: std::path::PathBuf,
}

#[derive(Debug, Args)]
pub struct CiArgs {
    #[command(subcommand)]
    pub sub: Option<CiSub>,
    /// Snapshot kind (for example `commit`).
    #[arg(long)]
    pub snapshot: Option<String>,
    /// Git ref to run.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum CiSub {
    /// Print the plan without running.
    Plan,
}

#[derive(Debug, Args)]
pub struct ExecArgs {
    /// Run in the current directory instead of a snapshot.
    #[arg(long)]
    pub in_place: bool,
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

/// Metadata listing, ordered by canonical identifier. Cursors are exclusive.
#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(long, default_value_t = 32)]
    pub limit: u32,
    #[arg(long)]
    pub cursor: Option<String>,
}
#[derive(Debug, Args)]
pub struct ShowArgs {
    pub id: String,
    #[arg(long, default_value_t = 32)]
    pub limit: u32,
    #[arg(long)]
    pub cursor: Option<String>,
}

#[derive(Debug, Args)]
pub struct IdArgs {
    pub id: String,
}

#[derive(Debug, Args)]
pub struct LogsArgs {
    pub id: String,
    /// Keep streaming.
    #[arg(long)]
    pub follow: bool,
}

#[derive(Debug, Args)]
pub struct OutputsArgs {
    pub id: String,
    /// Destination directory.
    #[arg(long)]
    pub dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct WorkersArgs {
    #[command(subcommand)]
    pub sub: Option<WorkersSub>,
}

#[derive(Debug, Subcommand)]
pub enum WorkersSub {
    /// Register a worker.
    Add {
        name: String,
        /// SSH destination, for example `user@host`.
        #[arg(long)]
        ssh: Option<String>,
        /// Install the worker over SSH.
        #[arg(long)]
        install: bool,
    },
    /// Start a worker.
    Start { name: String },
    /// Stop new placement; finish accepted work.
    Drain { name: String },
    /// Resume placement.
    Resume { name: String },
    /// Stop a worker.
    Stop {
        name: String,
        /// Stop immediately and cancel running work.
        #[arg(long)]
        cancel_running: bool,
    },
    /// Upgrade a worker.
    Upgrade { name: String },
    /// Show differences from the project requirements.
    Diff { name: String },
    /// Sync a worker with the project requirements.
    Sync {
        name: String,
        /// Project to sync.
        #[arg(long)]
        project: Option<String>,
    },
    /// Forget a worker.
    Forget { name: String },
}

#[derive(Debug, Args)]
pub struct WorkerArgs {
    /// Speak the protocol over stdin/stdout.
    #[arg(long)]
    pub stdio: bool,
    /// Exit when the work is done.
    #[arg(long)]
    pub ephemeral: bool,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

/// The `--json` envelope written to stdout.
#[derive(Debug, Serialize)]
pub struct Envelope {
    pub schema_version: u32,
    pub ok: bool,
    pub error: ErrorBody,
}

impl Envelope {
    pub fn not_implemented() -> Self {
        Envelope {
            schema_version: SCHEMA_VERSION,
            ok: false,
            error: ErrorBody {
                code: ErrorCode::NotImplemented,
                message: "this command is not implemented yet".to_string(),
            },
        }
    }
}

/// Run a parsed command and return the process exit code.
pub fn run(cli: Cli) -> i32 {
    if matches!(cli.command, Command::Ls(_) | Command::Show(_)) {
        return run_query(&cli);
    }

    if let Command::Daemon(args) = &cli.command {
        return crate::service::bootstrap::daemon_main(args.input.as_deref());
    }
    if let Command::Worker(args) = &cli.command {
        if args.stdio && !args.ephemeral {
            return run_worker_stdio();
        }
    }
    if let Command::Supervise(args) = &cli.command {
        return crate::supervisor::process::main(&args.spec);
    }
    if let Command::Submit(args) = &cli.command {
        let result = (|| -> Result<crate::service::submit::Receipt, crate::error::ApiError> {
            let env = std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect();
            let cwd = std::env::current_dir()
                .map_err(|e| crate::error::ApiError::new(ErrorCode::InvalidInput, e.to_string()))?;
            let context = crate::service::context::Context::discover(env, cwd)?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| crate::error::ApiError::new(ErrorCode::Internal, e.to_string()))?;
            runtime.block_on(crate::service::submit::command(
                context,
                args.command.clone(),
                None,
            ))
        })();
        return match result {
            Ok(receipt) => {
                for warning in &receipt.warnings {
                    eprintln!("warning: {warning}");
                }
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":true,"result":receipt})
                    );
                } else {
                    println!("{} {}", receipt.run_id, receipt.task_id);
                }
                exit::SUCCESS
            }
            Err(error) => {
                if cli.json {
                    println!(
                        "{}",
                        serde_json::json!({"schema_version":1,"ok":false,"error":error})
                    );
                } else {
                    eprintln!("error: {}", error.message);
                }
                error.code.exit_code()
            }
        };
    }
    let envelope = Envelope::not_implemented();
    if cli.json {
        let line = serde_json::to_string(&envelope).expect("envelope serializes");
        println!("{line}");
    } else {
        eprintln!("error: {}", envelope.error.message);
    }
    ErrorCode::NotImplemented.exit_code()
}

/// `worker --stdio`: relay stdin and stdout to the worker agent's Unix socket.
/// stdout carries protocol bytes only, so every diagnostic goes to stderr and
/// `--json` is not honoured here. This is the one place the process environment
/// is read (to find the socket); the rest of the crate takes a map.
fn run_worker_stdio() -> i32 {
    use crate::protocol::transport::{bridge, default_agent_socket, BridgeError};
    // Only HOME is read, with `var_os`: `std::env::vars()` panics on any
    // non-UTF-8 variable, and a bridge started under an odd environment must
    // still work. A HOME that is not UTF-8 cannot be a worker socket path here.
    let mut env = std::collections::BTreeMap::new();
    if let Some(home) = std::env::var_os("HOME").and_then(|h| h.into_string().ok()) {
        env.insert("HOME".to_string(), home);
    }
    let Some(socket) = default_agent_socket(&env) else {
        eprintln!("error: cannot locate the worker agent socket: HOME is not set");
        return ErrorCode::InvalidInput.exit_code();
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: cannot start the runtime: {e}");
            return exit::FAILURE;
        }
    };
    let result = runtime.block_on(bridge(&socket, tokio::io::stdin(), tokio::io::stdout()));
    // `tokio::io::stdin` reads on a blocking thread. When the agent closes
    // first, that read may be parked on a stdin the peer keeps open, and a plain
    // drop of the runtime would wait for it forever.
    runtime.shutdown_background();
    match result {
        Ok(()) => exit::SUCCESS,
        Err(e @ BridgeError::AgentNotRunning { .. }) => {
            eprintln!("error: {e}");
            ErrorCode::NotFound.exit_code()
        }
        Err(e) => {
            eprintln!("error: {e}");
            exit::FAILURE
        }
    }
}

/// Entry point used by the binary: parse arguments, run, return the exit code.
pub fn main() -> i32 {
    match Cli::try_parse() {
        Ok(cli) => run(cli),
        Err(e) => {
            let code = if e.use_stderr() {
                exit::INPUT_ERROR
            } else {
                exit::SUCCESS
            };
            let _ = e.print();
            code
        }
    }
}

fn run_query(cli: &Cli) -> i32 {
    use crate::error::ApiError;
    use crate::service::query::Query;
    let result = (|| -> Result<serde_json::Value, ApiError> {
        // Validate limits and identifier syntax before touching runtime state.
        let invalid = |e: String| ApiError::new(ErrorCode::InvalidInput, e);
        let (limit, cursor) = match &cli.command {
            Command::Ls(a) => (a.limit, a.cursor.as_deref()),
            Command::Show(a) => (a.limit, a.cursor.as_deref()),
            _ => unreachable!(),
        };
        if limit == 0 || limit > 64 {
            return Err(invalid("limit must be 1..64".into()));
        }
        match &cli.command {
            Command::Ls(_) => {
                cursor
                    .map(crate::ids::RunId::parse)
                    .transpose()
                    .map_err(|e| invalid(e.to_string()))?;
            }
            Command::Show(a) => {
                if a.id.starts_with("run_") {
                    crate::ids::RunId::parse(&a.id).map_err(|e| invalid(e.to_string()))?;
                    cursor
                        .map(crate::ids::TaskId::parse)
                        .transpose()
                        .map_err(|e| invalid(e.to_string()))?;
                } else if a.id.starts_with("task_") {
                    crate::ids::TaskId::parse(&a.id).map_err(|e| invalid(e.to_string()))?;
                    cursor
                        .map(str::parse::<u64>)
                        .transpose()
                        .map_err(|e| invalid(e.to_string()))?;
                } else {
                    return Err(invalid("expected run_ or task_ identifier".into()));
                }
            }
            _ => unreachable!(),
        }
        let env = std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect();
        let cwd = std::env::current_dir().map_err(|e| invalid(e.to_string()))?;
        let context = crate::service::context::Context::discover(env, cwd)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
        runtime.block_on(async {
            let client = context
                .readonly()
                .await
                .map_err(|e| ApiError::new(e.code, e.to_string()))?;
            let value = match &cli.command {
                Command::Ls(_) => serde_json::to_value(
                    client
                        .query(Query::Runs {
                            limit,
                            after: cursor
                                .map(crate::ids::RunId::parse)
                                .transpose()
                                .map_err(|e| invalid(e.to_string()))?,
                        })
                        .await
                        .map_err(|e| ApiError::new(e.code, e.to_string()))?,
                ),
                Command::Show(a) => serde_json::to_value(
                    client
                        .show(&a.id, limit, cursor)
                        .await
                        .map_err(|e| ApiError::new(e.code, e.to_string()))?,
                ),
                _ => unreachable!(),
            }
            .map_err(|e| ApiError::new(ErrorCode::Internal, e.to_string()))?;
            Ok(value)
        })
    })();
    match result {
        Ok(value) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":true,"data":value})
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&value).expect("query serializes")
                );
            }
            0
        }
        Err(error) => {
            if cli.json {
                println!(
                    "{}",
                    serde_json::json!({"schema_version":1,"ok":false,"error":error})
                );
            } else {
                eprintln!("error: {}", error.message);
            }
            error.code.exit_code()
        }
    }
}
