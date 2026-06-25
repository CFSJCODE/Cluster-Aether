use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileConfig {
    pub master: Option<MasterConfig>,
    pub worker: Option<WorkerConfig>,
    pub security: Option<SecurityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    pub bind_addr: Option<String>,
    pub grpc_port: Option<u16>,
    pub web_port: Option<u16>,
    pub web_enabled: Option<bool>,
    pub scheduler_interval_ms: Option<u64>,
    pub heartbeat_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub worker_id: Option<String>,
    pub master_ip: Option<String>,
    pub master_port: Option<u16>,
    pub worker_port: Option<u16>,
    pub max_concurrent_tasks: Option<u32>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub allow_unsafe_commands: Option<bool>,
    pub allowed_commands: Option<Vec<String>>,
    pub allow_root: Option<bool>,
}

/// Carrega configurações declarativas de um arquivo TOML.
///
/// A configuração é opcional porque o binário mantém compatibilidade com a CLI
/// original do projeto. Quando o arquivo é fornecido, seus valores funcionam
/// como defaults operacionais que podem ser refinados por argumentos explícitos.
pub fn load_config(path: Option<&Path>) -> anyhow::Result<FileConfig> {
    match path {
        Some(path) => {
            let raw = fs::read_to_string(path)?;
            Ok(toml::from_str(&raw)?)
        }
        None => Ok(FileConfig::default()),
    }
}
