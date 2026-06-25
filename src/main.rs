mod config;
mod platform;
mod security;

use axum::{extract::State, http::StatusCode, response::Html, routing::get, Json, Router};
use clap::{Parser, ValueEnum};
use platform::{build_shell_command, configure_process_stdio, operating_system, resolve_worker_id};
use security::{is_root, CommandPolicy};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    env,
    ffi::OsString,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use sysinfo::{Components, CpuRefreshKind, RefreshKind, System};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub mod aether_grpc {
    tonic::include_proto!("aether");
}

use aether_grpc::master_service_client::MasterServiceClient;
use aether_grpc::master_service_server::{MasterService, MasterServiceServer};
use aether_grpc::worker_service_client::WorkerServiceClient;
use aether_grpc::worker_service_server::{WorkerService, WorkerServiceServer};
use aether_grpc::{HeartbeatRequest, HeartbeatResponse, TaskRequest, TaskResponse};

const DEFAULT_TASK_TIMEOUT_SECONDS: u32 = 30;
const MAX_TASK_HISTORY: usize = 500;
const TASK_HISTORY_TRIM: usize = 100;
const TASK_TAIL_CHARS: usize = 4096;
const POWERSHELL_SHELL: &str = "powershell";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Master,
    Worker,
    Client,
}

#[derive(Parser, Debug)]
#[command(name = "aether", author = "CFSJCODE", version = "0.2.0")]
struct Args {
    #[arg(short, long, value_enum)]
    mode: Mode,

    #[arg(long, default_value = "0.0.0.0")]
    bind_addr: String,

    #[arg(short, long, default_value_t = 50051)]
    port: u16,

    #[arg(short = 'i', long, default_value = "127.0.0.1")]
    master_ip: String,

    #[arg(short = 'c', long, default_value = "")]
    command: String,

    #[arg(long)]
    worker_id: Option<String>,

    #[arg(long)]
    worker_port: Option<u16>,

    #[arg(long, default_value_t = 2)]
    max_concurrent_tasks: u32,

    #[arg(long, default_value_t = 8080)]
    web_port: u16,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_web: bool,

    #[arg(long, default_value_t = 250)]
    scheduler_interval_ms: u64,

    #[arg(long, default_value_t = 6)]
    heartbeat_timeout_seconds: u64,

    #[arg(long, default_value_t = false)]
    allow_unsafe_commands: bool,

    #[arg(long, default_value_t = false)]
    allow_root: bool,

    #[arg(long, value_delimiter = ',', default_value = "")]
    tags: Vec<String>,

    #[arg(long, value_delimiter = ',')]
    allowed_command: Vec<String>,

    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    enable_process_thief: bool,
}

#[derive(Default)]
struct CliOverrides {
    bind_addr: bool,
    port: bool,
    master_ip: bool,
    worker_id: bool,
    worker_port: bool,
    max_concurrent_tasks: bool,
    web_port: bool,
    enable_web: bool,
    scheduler_interval_ms: bool,
    heartbeat_timeout_seconds: bool,
    allow_unsafe_commands: bool,
    allow_root: bool,
    tags: bool,
    allowed_command: bool,
}

impl CliOverrides {
    fn from_current_process() -> Self {
        let args: Vec<OsString> = env::args_os().collect();

        Self {
            bind_addr: has_arg(&args, "--bind-addr", None),
            port: has_arg(&args, "--port", Some("-p")),
            master_ip: has_arg(&args, "--master-ip", Some("-i")),
            worker_id: has_arg(&args, "--worker-id", None),
            worker_port: has_arg(&args, "--worker-port", None),
            max_concurrent_tasks: has_arg(&args, "--max-concurrent-tasks", None),
            web_port: has_arg(&args, "--web-port", None),
            enable_web: has_arg(&args, "--enable-web", None),
            scheduler_interval_ms: has_arg(&args, "--scheduler-interval-ms", None),
            heartbeat_timeout_seconds: has_arg(&args, "--heartbeat-timeout-seconds", None),
            allow_unsafe_commands: has_arg(&args, "--allow-unsafe-commands", None),
            allow_root: has_arg(&args, "--allow-root", None),
            tags: has_arg(&args, "--tags", None),
            allowed_command: has_arg(&args, "--allowed-command", None),
        }
    }
}

fn has_arg(args: &[OsString], long: &str, short: Option<&str>) -> bool {
    args.iter().skip(1).any(|arg| {
        let value = arg.to_string_lossy();
        value == long
            || value.starts_with(&format!("{long}="))
            || short.is_some_and(|short| {
                value == short || (!value.starts_with("--") && value.starts_with(short))
            })
    })
}

#[derive(Clone)]
struct WorkerSnapshot {
    ip: String,
    port: u32,
    last_heartbeat: Instant,
    cpu_usage: f32,
    ram_usage: f32,
    temperature: f32,
    running_tasks: u32,
    max_concurrent_tasks: u32,
    operating_system: String,
    shell: String,
    tags: Vec<String>,
}

impl WorkerSnapshot {
    fn score(&self) -> f32 {
        let cpu_score = (100.0 - self.cpu_usage).clamp(0.0, 100.0);
        let ram_score = (100.0 - self.ram_usage).clamp(0.0, 100.0);
        let thermal_score = if self.temperature <= 0.0 {
            85.0
        } else if self.temperature > 82.0 {
            0.0
        } else if self.temperature > 70.0 {
            (100.0 - (self.temperature - 70.0) * 5.0).clamp(0.0, 100.0)
        } else {
            100.0
        };
        let slot_score = if self.max_concurrent_tasks == 0 {
            0.0
        } else {
            (self.max_concurrent_tasks.saturating_sub(self.running_tasks) as f32
                / self.max_concurrent_tasks as f32
                * 100.0)
                .clamp(0.0, 100.0)
        };

        cpu_score * 0.35 + ram_score * 0.20 + thermal_score * 0.25 + slot_score * 0.20
    }

    fn matches_tags(&self, required_tags: &[String]) -> bool {
        required_tags.iter().all(|required_tag| {
            self.tags
                .iter()
                .any(|worker_tag| worker_tag.eq_ignore_ascii_case(required_tag))
        })
    }
}

#[derive(Clone)]
struct Task {
    id: String,
    command: String,
    required_tags: Vec<String>,
    timeout_seconds: u32,
    priority: i32,
}

#[derive(Clone, Serialize)]
struct TaskView {
    id: String,
    command: String,
    status: String,
    worker: Option<String>,
    stdout_tail: String,
    stderr_tail: String,
}

#[derive(Clone)]
struct MasterState {
    workers: Arc<Mutex<HashMap<String, WorkerSnapshot>>>,
    queue: Arc<Mutex<VecDeque<Task>>>,
    history: Arc<Mutex<Vec<TaskView>>>,
    policy: CommandPolicy,
}

impl MasterState {
    fn new(policy: CommandPolicy) -> Self {
        Self {
            workers: Arc::default(),
            queue: Arc::default(),
            history: Arc::default(),
            policy,
        }
    }

    fn enqueue_new_task(&self, task: Task) {
        self.history.lock().unwrap().push(TaskView {
            id: task.id.clone(),
            command: task.command.clone(),
            status: "QUEUED".into(),
            worker: None,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
        });
        self.enqueue_by_priority(task);
        self.trim_history();
    }

    fn enqueue_by_priority(&self, task: Task) {
        let mut queue = self.queue.lock().unwrap();
        let index = queue
            .iter()
            .position(|queued| task.priority > queued.priority)
            .unwrap_or(queue.len());
        queue.insert(index, task);
    }

    fn defer_task(&self, task: Task) {
        self.queue.lock().unwrap().push_back(task);
    }

    fn next_task(&self) -> Option<Task> {
        self.queue.lock().unwrap().pop_front()
    }

    fn update_task(
        &self,
        id: &str,
        status: &str,
        worker: Option<String>,
        stdout: &str,
        stderr: &str,
    ) {
        let mut history = self.history.lock().unwrap();
        if let Some(task) = history.iter_mut().find(|task| task.id == id) {
            task.status = status.into();
            if worker.is_some() {
                task.worker = worker;
            }
            task.stdout_tail = tail(stdout);
            task.stderr_tail = tail(stderr);
        }
    }

    fn release_worker_slot(&self, worker_id: &str) {
        if let Some(worker) = self.workers.lock().unwrap().get_mut(worker_id) {
            worker.running_tasks = worker.running_tasks.saturating_sub(1);
        }
    }

    fn trim_history(&self) {
        let mut history = self.history.lock().unwrap();
        if history.len() > MAX_TASK_HISTORY {
            history.drain(0..TASK_HISTORY_TRIM);
        }
    }
}

#[tonic::async_trait]
impl MasterService for MasterState {
    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let worker_ip = request
            .remote_addr()
            .map(|address| normalize_remote_ip(address.ip().to_string()))
            .unwrap_or_else(|| "127.0.0.1".into());
        let heartbeat = request.into_inner();
        let worker_id = heartbeat.worker_id.clone();
        let worker_tag = worker_identity_tag(&worker_id);
        let worker = WorkerSnapshot {
            ip: worker_ip,
            port: heartbeat.worker_port.max(50052),
            last_heartbeat: Instant::now(),
            cpu_usage: heartbeat.cpu_usage,
            ram_usage: heartbeat.ram_usage,
            temperature: heartbeat.temperature,
            running_tasks: heartbeat.running_tasks,
            max_concurrent_tasks: heartbeat.max_concurrent_tasks.max(1),
            operating_system: heartbeat.operating_system,
            shell: heartbeat.shell,
            tags: heartbeat.tags,
        };

        let is_new_worker = self
            .workers
            .lock()
            .unwrap()
            .insert(worker_id.clone(), worker.clone())
            .is_none();

        info!(worker_id = %worker_id, "heartbeat recebido");

        if is_new_worker {
            let command = initial_diagnostic_command(&worker.shell);
            if let Err(error) = self.policy.validate(command) {
                warn!(%error, "tarefa de diagnóstico inicial bloqueada pela política");
            } else {
                self.enqueue_new_task(Task {
                    id: format!(
                        "sysinfo_{}_{}",
                        worker_id_for_task(&worker_id),
                        Uuid::new_v4()
                    ),
                    command: command.into(),
                    required_tags: vec![worker_tag],
                    timeout_seconds: DEFAULT_TASK_TIMEOUT_SECONDS,
                    priority: 5,
                });
            }
        }

        Ok(Response::new(HeartbeatResponse { acknowledged: true }))
    }

    async fn inject_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<TaskResponse>, Status> {
        let request = request.into_inner();
        self.policy
            .validate(&request.command)
            .map_err(|error| Status::permission_denied(error.to_string()))?;

        let task_id = if request.task_id.trim().is_empty() {
            format!("client_{}", Uuid::new_v4())
        } else {
            format!("client_{}", request.task_id.trim())
        };

        self.enqueue_new_task(Task {
            id: task_id.clone(),
            command: request.command,
            required_tags: normalize_tags(request.required_tags),
            timeout_seconds: normalize_timeout(request.timeout_seconds),
            priority: request.priority,
        });

        Ok(Response::new(TaskResponse {
            task_id,
            exit_code: 0,
            stdout: "Tarefa enfileirada no Master.\n".into(),
            stderr: String::new(),
        }))
    }
}

#[derive(Clone)]
struct WorkerRuntime {
    policy: CommandPolicy,
    running_tasks: Arc<AtomicU32>,
    max_concurrent_tasks: u32,
}

impl WorkerRuntime {
    fn new(policy: CommandPolicy, max_concurrent_tasks: u32) -> Self {
        Self {
            policy,
            running_tasks: Arc::new(AtomicU32::new(0)),
            max_concurrent_tasks: max_concurrent_tasks.max(1),
        }
    }

    fn reserve_slot(&self) -> bool {
        self.running_tasks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |running| {
                (running < self.max_concurrent_tasks).then_some(running + 1)
            })
            .is_ok()
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerRuntime {
    type ExecuteTaskStream = ReceiverStream<Result<TaskResponse, Status>>;

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let request = request.into_inner();
        self.policy
            .validate(&request.command)
            .map_err(|error| Status::permission_denied(error.to_string()))?;

        if !self.reserve_slot() {
            return Err(Status::resource_exhausted("Worker sem slots livres"));
        }

        let task_id = request.task_id.clone();
        let running_tasks = self.running_tasks.clone();
        let timeout_seconds = normalize_timeout(request.timeout_seconds);
        let (sender, receiver) = tokio::sync::mpsc::channel(64);

        let mut command = build_shell_command(&request.command);
        configure_process_stdio(&mut command);
        command.kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            self.running_tasks.fetch_sub(1, Ordering::SeqCst);
            Status::internal(error.to_string())
        })?;

        if let Some(stdout) = child.stdout.take() {
            pipe_output(stdout, sender.clone(), task_id.clone(), false);
        }
        if let Some(stderr) = child.stderr.take() {
            pipe_output(stderr, sender.clone(), task_id.clone(), true);
        }

        let _wait_task = tokio::spawn(async move {
            let result =
                tokio::time::timeout(Duration::from_secs(timeout_seconds as u64), child.wait())
                    .await;
            running_tasks.fetch_sub(1, Ordering::SeqCst);

            let (exit_code, stdout, stderr) = match result {
                Ok(Ok(status)) => (
                    status.code().unwrap_or(-1),
                    "[WORKER] --- Micro-lote finalizado ---\n".into(),
                    String::new(),
                ),
                Ok(Err(error)) => (-1, String::new(), format!("{error}\n")),
                Err(_) => {
                    let _ = child.kill().await;
                    (-1, String::new(), "timeout\n".into())
                }
            };

            let _ = sender
                .send(Ok(TaskResponse {
                    task_id,
                    exit_code,
                    stdout,
                    stderr,
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

fn pipe_output<R>(
    output: R,
    sender: tokio::sync::mpsc::Sender<Result<TaskResponse, Status>>,
    task_id: String,
    is_stderr: bool,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let _pipe_task = tokio::spawn(async move {
        let mut lines = BufReader::new(output).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let response = if is_stderr {
                TaskResponse {
                    task_id: task_id.clone(),
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: format!("{line}\n"),
                }
            } else {
                TaskResponse {
                    task_id: task_id.clone(),
                    exit_code: 0,
                    stdout: format!("{line}\n"),
                    stderr: String::new(),
                }
            };

            if sender.send(Ok(response)).await.is_err() {
                break;
            }
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("aether=info,tower_http=warn"));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let cli_overrides = CliOverrides::from_current_process();
    let mut args = Args::parse();
    let config = config::load_config(args.config.as_deref())?;
    apply_config(&mut args, &config, &cli_overrides);
    let policy = build_command_policy(&args, &config, &cli_overrides);

    if args.enable_process_thief {
        warn!("Process Thief desativado por segurança");
    }

    match args.mode {
        Mode::Master => run_master(args, policy).await,
        Mode::Worker => run_worker(args, policy).await,
        Mode::Client => run_client(args).await,
    }
}

async fn run_master(args: Args, policy: CommandPolicy) -> anyhow::Result<()> {
    let master = MasterState::new(policy);
    spawn_reaper(master.clone(), args.heartbeat_timeout_seconds);
    spawn_scheduler(master.clone(), args.scheduler_interval_ms);

    if args.enable_web {
        let web_state = master.clone();
        let web_addr = bind_socket_addr(&args.bind_addr, args.web_port)?;
        let _web_task = tokio::spawn(async move {
            if let Err(error) = run_web_console(web_state, web_addr).await {
                error!(%error, "Aether Console falhou");
            }
        });
    }

    let grpc_addr = bind_socket_addr(&args.bind_addr, args.port)?;
    info!(%grpc_addr, "Master gRPC iniciado");
    Server::builder()
        .add_service(MasterServiceServer::new(master))
        .serve(grpc_addr)
        .await?;
    Ok(())
}

async fn run_worker(args: Args, policy: CommandPolicy) -> anyhow::Result<()> {
    if is_root() && !args.allow_root {
        anyhow::bail!("recusando execução do Worker como root");
    }

    let worker_port = args.worker_port.unwrap_or(args.port + 1);
    let worker_id = resolve_worker_id(args.worker_id.clone());
    let worker_tags = build_worker_tags(args.tags, &worker_id);
    let runtime = WorkerRuntime::new(policy, args.max_concurrent_tasks);
    let heartbeat_runtime = runtime.clone();
    let worker_addr = bind_socket_addr("0.0.0.0", worker_port)?;

    let _worker_server = tokio::spawn(async move {
        if let Err(error) = Server::builder()
            .add_service(WorkerServiceServer::new(runtime))
            .serve(worker_addr)
            .await
        {
            error!(%error, "Worker falhou");
        }
    });

    let mut client =
        MasterServiceClient::connect(format!("http://{}:{}", args.master_ip, args.port)).await?;
    let mut system =
        System::new_with_specifics(RefreshKind::new().with_cpu(CpuRefreshKind::everything()));

    loop {
        system.refresh_cpu_usage();
        system.refresh_memory();
        let ram_usage = if system.total_memory() == 0 {
            0.0
        } else {
            system.used_memory() as f32 / system.total_memory() as f32 * 100.0
        };
        let cpu_usage = system.global_cpu_info().cpu_usage();

        let request = Request::new(HeartbeatRequest {
            worker_id: worker_id.clone(),
            cpu_usage,
            ram_usage,
            temperature: read_cpu_temperature(),
            worker_port: worker_port as u32,
            operating_system: operating_system().into(),
            running_tasks: heartbeat_runtime.running_tasks.load(Ordering::Relaxed),
            max_concurrent_tasks: args.max_concurrent_tasks.max(1),
            load_average: cpu_usage,
            tags: worker_tags.clone(),
            shell: platform::ShellKind::current().as_str().into(),
        });

        if let Err(error) = client.send_heartbeat(request).await {
            warn!(%error, "heartbeat falhou");
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_client(args: Args) -> anyhow::Result<()> {
    if args.command.trim().is_empty() {
        anyhow::bail!("forneça --command/-c");
    }

    let mut client =
        MasterServiceClient::connect(format!("http://{}:{}", args.master_ip, args.port)).await?;
    let response = client
        .inject_task(Request::new(TaskRequest {
            task_id: String::new(),
            command: args.command,
            payload: Vec::new(),
            timeout_seconds: DEFAULT_TASK_TIMEOUT_SECONDS,
            required_tags: normalize_tags(args.tags),
            priority: 5,
        }))
        .await?;

    println!("{}", response.into_inner().stdout.trim());
    Ok(())
}

fn apply_config(args: &mut Args, config: &config::FileConfig, cli: &CliOverrides) {
    match args.mode {
        Mode::Master => apply_master_config(args, config.master.as_ref(), cli),
        Mode::Worker => apply_worker_config(args, config.worker.as_ref(), cli),
        Mode::Client => apply_client_config(args, config, cli),
    }

    if let Some(security) = config.security.as_ref() {
        if !cli.allow_root {
            if let Some(allow_root) = security.allow_root {
                args.allow_root = allow_root;
            }
        }
        if !cli.allow_unsafe_commands {
            if let Some(allow_unsafe_commands) = security.allow_unsafe_commands {
                args.allow_unsafe_commands = allow_unsafe_commands;
            }
        }
    }
}

fn apply_master_config(args: &mut Args, config: Option<&config::MasterConfig>, cli: &CliOverrides) {
    let Some(config) = config else {
        return;
    };

    if !cli.bind_addr {
        if let Some(bind_addr) = config.bind_addr.as_ref() {
            args.bind_addr = bind_addr.clone();
        }
    }
    if !cli.port {
        if let Some(grpc_port) = config.grpc_port {
            args.port = grpc_port;
        }
    }
    if !cli.web_port {
        if let Some(web_port) = config.web_port {
            args.web_port = web_port;
        }
    }
    if !cli.enable_web {
        if let Some(web_enabled) = config.web_enabled {
            args.enable_web = web_enabled;
        }
    }
    if !cli.scheduler_interval_ms {
        if let Some(scheduler_interval_ms) = config.scheduler_interval_ms {
            args.scheduler_interval_ms = scheduler_interval_ms;
        }
    }
    if !cli.heartbeat_timeout_seconds {
        if let Some(heartbeat_timeout_seconds) = config.heartbeat_timeout_seconds {
            args.heartbeat_timeout_seconds = heartbeat_timeout_seconds;
        }
    }
}

fn apply_worker_config(args: &mut Args, config: Option<&config::WorkerConfig>, cli: &CliOverrides) {
    let Some(config) = config else {
        return;
    };

    if !cli.worker_id {
        args.worker_id = config.worker_id.clone();
    }
    if !cli.master_ip {
        if let Some(master_ip) = config.master_ip.as_ref() {
            args.master_ip = master_ip.clone();
        }
    }
    if !cli.port {
        if let Some(master_port) = config.master_port {
            args.port = master_port;
        }
    }
    if !cli.worker_port {
        args.worker_port = config.worker_port;
    }
    if !cli.max_concurrent_tasks {
        if let Some(max_concurrent_tasks) = config.max_concurrent_tasks {
            args.max_concurrent_tasks = max_concurrent_tasks;
        }
    }
    if !cli.tags {
        if let Some(tags) = config.tags.as_ref() {
            args.tags = tags.clone();
        }
    }
}

fn apply_client_config(args: &mut Args, config: &config::FileConfig, cli: &CliOverrides) {
    if !cli.master_ip {
        if let Some(master_ip) = config
            .worker
            .as_ref()
            .and_then(|worker| worker.master_ip.as_ref())
        {
            args.master_ip = master_ip.clone();
        }
    }

    if !cli.port {
        if let Some(master_port) = config
            .worker
            .as_ref()
            .and_then(|worker| worker.master_port)
            .or_else(|| config.master.as_ref().and_then(|master| master.grpc_port))
        {
            args.port = master_port;
        }
    }
}

fn build_command_policy(
    args: &Args,
    config: &config::FileConfig,
    cli: &CliOverrides,
) -> CommandPolicy {
    let mut policy = CommandPolicy::default();

    if let Some(security) = config.security.as_ref() {
        if let Some(allow_unsafe_commands) = security.allow_unsafe_commands {
            policy.allow_unsafe_commands = allow_unsafe_commands;
        }
        if !cli.allowed_command {
            if let Some(allowed_commands) = security.allowed_commands.as_ref() {
                policy.allowed_prefixes = normalize_tags(allowed_commands.clone());
            }
        }
    }

    if args.allow_unsafe_commands {
        policy.allow_unsafe_commands = true;
    }
    if !args.allowed_command.is_empty() {
        policy.allowed_prefixes = normalize_tags(args.allowed_command.clone());
    }

    policy
}

fn spawn_reaper(master: MasterState, heartbeat_timeout_seconds: u64) {
    let _reaper_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = Instant::now();
            master.workers.lock().unwrap().retain(|worker_id, worker| {
                let is_alive = now.duration_since(worker.last_heartbeat).as_secs()
                    <= heartbeat_timeout_seconds;
                if !is_alive {
                    warn!(worker_id = %worker_id, "Worker expirado");
                }
                is_alive
            });
        }
    });
}

fn spawn_scheduler(master: MasterState, scheduler_interval_ms: u64) {
    let _scheduler_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(scheduler_interval_ms.max(50))).await;

            let Some(task) = master.next_task() else {
                continue;
            };
            let Some(worker) = choose_worker(&master, &task.required_tags) else {
                master.defer_task(task);
                continue;
            };

            master.update_task(&task.id, "DISPATCHED", Some(worker.id.clone()), "", "");
            let dispatch_master = master.clone();
            let _dispatch_task = tokio::spawn(async move {
                dispatch_task(dispatch_master, task, worker).await;
            });
        }
    });
}

#[derive(Clone)]
struct WorkerLease {
    id: String,
    ip: String,
    port: u32,
}

fn choose_worker(master: &MasterState, required_tags: &[String]) -> Option<WorkerLease> {
    let mut best_worker = None;
    let mut workers = master.workers.lock().unwrap();

    for (worker_id, worker) in workers.iter_mut() {
        let score = worker.score();
        let has_capacity = worker.running_tasks < worker.max_concurrent_tasks;
        if score > 30.0
            && has_capacity
            && worker.matches_tags(required_tags)
            && best_worker
                .as_ref()
                .map(|(_, best_score)| score > *best_score)
                .unwrap_or(true)
        {
            best_worker = Some((
                WorkerLease {
                    id: worker_id.clone(),
                    ip: worker.ip.clone(),
                    port: worker.port,
                },
                score,
            ));
        }
    }

    if let Some((lease, _)) = best_worker.as_ref() {
        if let Some(worker) = workers.get_mut(&lease.id) {
            worker.running_tasks = worker.running_tasks.saturating_add(1);
        }
    }

    best_worker.map(|(lease, _)| lease)
}

async fn dispatch_task(master: MasterState, task: Task, worker: WorkerLease) {
    let result = execute_remote_task(&worker, &task).await;
    master.release_worker_slot(&worker.id);

    match result {
        DispatchResult::Completed {
            exit_code,
            stdout,
            stderr,
        } => {
            let status = if exit_code == 0 {
                "SUCCEEDED"
            } else {
                "FAILED"
            };
            master.update_task(&task.id, status, Some(worker.id), &stdout, &stderr);
        }
        DispatchResult::Retryable { stderr } => {
            master.update_task(&task.id, "RETRYING", Some(worker.id), "", &stderr);
            master.defer_task(task);
        }
    }
}

enum DispatchResult {
    Completed {
        exit_code: i32,
        stdout: String,
        stderr: String,
    },
    Retryable {
        stderr: String,
    },
}

async fn execute_remote_task(worker: &WorkerLease, task: &Task) -> DispatchResult {
    let endpoint = format!("http://{}:{}", worker.ip, worker.port);
    let Ok(mut client) = WorkerServiceClient::connect(endpoint).await else {
        warn!(worker_id = %worker.id, "conexão com Worker falhou");
        return DispatchResult::Retryable {
            stderr: "conexão com Worker falhou".into(),
        };
    };

    let response = client
        .execute_task(Request::new(TaskRequest {
            task_id: task.id.clone(),
            command: task.command.clone(),
            payload: Vec::new(),
            timeout_seconds: task.timeout_seconds,
            required_tags: task.required_tags.clone(),
            priority: task.priority,
        }))
        .await;

    let Ok(response) = response else {
        let error = response.unwrap_err().to_string();
        return DispatchResult::Retryable { stderr: error };
    };

    let mut stream = response.into_inner();
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut exit_code = -1;

    loop {
        match stream.message().await {
            Ok(Some(message)) => {
                stdout.push_str(&message.stdout);
                stderr.push_str(&message.stderr);
                exit_code = message.exit_code;
            }
            Ok(None) => break,
            Err(error) => {
                stderr.push_str(&format!("stream do Worker falhou: {error}\n"));
                exit_code = -1;
                break;
            }
        }
    }

    DispatchResult::Completed {
        exit_code,
        stdout,
        stderr,
    }
}

async fn run_web_console(master: MasterState, addr: SocketAddr) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(|| async { Html(INDEX) }))
        .route("/api/cluster/status", get(api_status))
        .route("/api/workers", get(api_workers))
        .route("/api/tasks", get(api_tasks).post(api_create_task))
        .layer(CorsLayer::permissive())
        .with_state(master);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "Aether Console iniciado");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn api_status(State(master): State<MasterState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "workers_online": master.workers.lock().unwrap().len(),
        "tasks_queued": master.queue.lock().unwrap().len(),
        "tasks_tracked": master.history.lock().unwrap().len()
    }))
}

#[derive(Serialize)]
struct WorkerView {
    id: String,
    ip: String,
    port: u32,
    cpu: f32,
    ram: f32,
    temp: f32,
    running: u32,
    max: u32,
    os: String,
    shell: String,
    tags: Vec<String>,
    score: f32,
}

async fn api_workers(State(master): State<MasterState>) -> Json<Vec<WorkerView>> {
    Json(
        master
            .workers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, worker)| WorkerView {
                id: id.clone(),
                ip: worker.ip.clone(),
                port: worker.port,
                cpu: worker.cpu_usage,
                ram: worker.ram_usage,
                temp: worker.temperature,
                running: worker.running_tasks,
                max: worker.max_concurrent_tasks,
                os: worker.operating_system.clone(),
                shell: worker.shell.clone(),
                tags: worker.tags.clone(),
                score: worker.score(),
            })
            .collect(),
    )
}

async fn api_tasks(State(master): State<MasterState>) -> Json<Vec<TaskView>> {
    Json(master.history.lock().unwrap().clone())
}

#[derive(Deserialize)]
struct WebTaskRequest {
    command: String,
    required_tags: Option<Vec<String>>,
    timeout_seconds: Option<u32>,
    priority: Option<i32>,
}

async fn api_create_task(
    State(master): State<MasterState>,
    Json(task): Json<WebTaskRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    master
        .policy
        .validate(&task.command)
        .map_err(|error| (StatusCode::FORBIDDEN, error.to_string()))?;

    let task_id = format!("web_{}", Uuid::new_v4());
    master.enqueue_new_task(Task {
        id: task_id.clone(),
        command: task.command,
        required_tags: normalize_tags(task.required_tags.unwrap_or_default()),
        timeout_seconds: normalize_timeout(
            task.timeout_seconds.unwrap_or(DEFAULT_TASK_TIMEOUT_SECONDS),
        ),
        priority: task.priority.unwrap_or(5),
    });

    Ok(Json(serde_json::json!({ "task_id": task_id })))
}

fn bind_socket_addr(bind_addr: &str, port: u16) -> anyhow::Result<SocketAddr> {
    Ok(format!("{bind_addr}:{port}").parse()?)
}

fn normalize_remote_ip(ip: String) -> String {
    if ip == "::1" {
        "127.0.0.1".into()
    } else {
        ip
    }
}

fn normalize_timeout(timeout_seconds: u32) -> u32 {
    timeout_seconds.max(DEFAULT_TASK_TIMEOUT_SECONDS)
}

fn normalize_tags(tags: Vec<String>) -> Vec<String> {
    let mut normalized = tags
        .into_iter()
        .map(|tag| tag.trim().to_lowercase())
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn build_worker_tags(tags: Vec<String>, worker_id: &str) -> Vec<String> {
    let mut tags = normalize_tags(tags);
    tags.push(operating_system().into());
    tags.push(platform::ShellKind::current().as_str().into());
    tags.push(worker_identity_tag(worker_id));
    if worker_id.to_lowercase().contains("local") {
        tags.push("local".into());
    }
    tags.sort();
    tags.dedup();
    tags
}

fn worker_identity_tag(worker_id: &str) -> String {
    format!("worker:{}", worker_id_for_task(worker_id))
}

fn worker_id_for_task(worker_id: &str) -> String {
    worker_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

fn initial_diagnostic_command(shell: &str) -> &'static str {
    if shell.eq_ignore_ascii_case(POWERSHELL_SHELL) {
        "hostname; whoami; Get-Date"
    } else {
        "hostname && uname -a && uptime"
    }
}

fn read_cpu_temperature() -> f32 {
    for component in &Components::new_with_refreshed_list() {
        let label = component.label().to_lowercase();
        if label.contains("cpu")
            || label.contains("core")
            || label.contains("tctl")
            || label.contains("tdie")
        {
            return component.temperature();
        }
    }
    0.0
}

fn tail(value: &str) -> String {
    let char_count = value.chars().count();
    if char_count <= TASK_TAIL_CHARS {
        value.into()
    } else {
        value.chars().skip(char_count - TASK_TAIL_CHARS).collect()
    }
}

const INDEX: &str = r#"<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Aether Console</title>
<style>
body{margin:0;font-family:Segoe UI,Arial,sans-serif;background:#0b1020;color:#e5e7eb}
main{max-width:1180px;margin:0 auto;padding:28px}
header{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:22px}
h1{font-size:28px;margin:0}
.grid{display:grid;grid-template-columns:360px 1fr;gap:18px}
section{background:#111827;border:1px solid #243044;border-radius:8px;padding:16px}
label{display:block;font-size:13px;color:#93a4bb;margin-bottom:8px}
textarea,input{box-sizing:border-box;width:100%;border:1px solid #344156;background:#070b16;color:#e5e7eb;border-radius:6px;padding:10px;font:14px Consolas,monospace}
textarea{min-height:120px;resize:vertical}
button{margin-top:12px;border:0;border-radius:6px;background:#38bdf8;color:#04111f;font-weight:700;padding:10px 14px;cursor:pointer}
pre{white-space:pre-wrap;word-break:break-word;background:#070b16;border:1px solid #243044;border-radius:6px;padding:12px;min-height:420px}
.row{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}
@media(max-width:860px){.grid{grid-template-columns:1fr}main{padding:18px}}
</style>
<main>
  <header>
    <h1>Aether Console</h1>
    <button onclick="refresh()">Atualizar</button>
  </header>
  <div class="grid">
    <section>
      <label for="cmd">Comando</label>
      <textarea id="cmd">hostname</textarea>
      <div class="row">
        <div>
          <label for="tags">Tags</label>
          <input id="tags" placeholder="local,linux">
        </div>
        <div>
          <label for="priority">Prioridade</label>
          <input id="priority" type="number" value="5">
        </div>
      </div>
      <button onclick="send()">Enfileirar</button>
    </section>
    <section>
      <label>Cluster</label>
      <pre id="output">Carregando...</pre>
    </section>
  </div>
</main>
<script>
async function json(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}
async function refresh() {
  const snapshot = {
    status: await json('/api/cluster/status'),
    workers: await json('/api/workers'),
    tasks: await json('/api/tasks')
  };
  output.textContent = JSON.stringify(snapshot, null, 2);
}
async function send() {
  const tags = tagsToArray(document.getElementById('tags').value);
  await json('/api/tasks', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      command: document.getElementById('cmd').value,
      required_tags: tags,
      priority: Number(document.getElementById('priority').value || 5)
    })
  });
  refresh();
}
function tagsToArray(value) {
  return value.split(',').map(tag => tag.trim()).filter(Boolean);
}
refresh();
setInterval(refresh, 2500);
</script>
"#;
