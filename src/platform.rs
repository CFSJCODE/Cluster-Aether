use std::process::Stdio;
use tokio::process::Command;

#[derive(Debug, Clone, Copy)]
pub enum ShellKind {
    Sh,
    PowerShell,
}

impl ShellKind {
    pub fn current() -> Self {
        #[cfg(target_os = "windows")]
        {
            Self::PowerShell
        }

        #[cfg(not(target_os = "windows"))]
        {
            Self::Sh
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sh => "sh",
            Self::PowerShell => "powershell",
        }
    }
}

/// Cria o processo de shell adequado para o sistema operacional corrente.
///
/// O Aether usa execução por shell para manter compatibilidade com pipelines e
/// comandos compostos. Por segurança, a validação deve ocorrer antes desta função.
pub fn build_shell_command(command: &str) -> Command {
    match ShellKind::current() {
        ShellKind::Sh => {
            let mut cmd = Command::new("sh");
            cmd.arg("-c").arg(command);
            cmd
        }
        ShellKind::PowerShell => {
            let mut cmd = Command::new("powershell.exe");
            cmd.arg("-NoProfile")
                .arg("-ExecutionPolicy")
                .arg("Bypass")
                .arg("-Command")
                .arg(command);
            cmd
        }
    }
}

pub fn configure_process_stdio(command: &mut Command) -> &mut Command {
    command.stdout(Stdio::piped()).stderr(Stdio::piped())
}

pub fn resolve_worker_id(cli_worker_id: Option<String>) -> String {
    cli_worker_id
        .or_else(|| std::env::var("AETHER_WORKER_ID").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("worker-{}", std::process::id()))
}

pub fn operating_system() -> &'static str {
    std::env::consts::OS
}
