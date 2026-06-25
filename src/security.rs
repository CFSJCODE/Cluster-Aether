use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum CommandPolicyError {
    #[error("comando vazio")]
    Empty,
    #[error("comando bloqueado pela política de segurança: {0}")]
    Denied(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandPolicy {
    pub allow_unsafe_commands: bool,
    pub allowed_prefixes: Vec<String>,
    pub denied_fragments: Vec<String>,
}

impl Default for CommandPolicy {
    fn default() -> Self {
        Self {
            allow_unsafe_commands: false,
            allowed_prefixes: vec![
                "hostname".into(),
                "uptime".into(),
                "uname".into(),
                "free".into(),
                "df".into(),
                "nproc".into(),
                "lscpu".into(),
                "echo".into(),
                "seq".into(),
                "sleep".into(),
                "sensors".into(),
                "ip".into(),
                "ping".into(),
                "whoami".into(),
                "date".into(),
            ],
            denied_fragments: vec![
                "rm -rf".into(),
                "mkfs".into(),
                "dd if=".into(),
                "shutdown".into(),
                "reboot".into(),
                "curl".into(),
                "wget".into(),
                "chmod 777".into(),
                "chown".into(),
                "useradd".into(),
                "passwd".into(),
                ":(){".into(),
            ],
        }
    }
}

impl CommandPolicy {
    pub fn validate(&self, command: &str) -> Result<(), CommandPolicyError> {
        let command = command.trim();
        if command.is_empty() {
            return Err(CommandPolicyError::Empty);
        }

        let lower = command.to_lowercase();
        if self
            .denied_fragments
            .iter()
            .any(|fragment| lower.contains(&fragment.to_lowercase()))
        {
            return Err(CommandPolicyError::Denied(command.to_string()));
        }

        if self.allow_unsafe_commands {
            return Ok(());
        }

        if self
            .allowed_prefixes
            .iter()
            .any(|prefix| lower.starts_with(&prefix.to_lowercase()))
        {
            return Ok(());
        }

        Err(CommandPolicyError::Denied(command.to_string()))
    }
}

#[cfg(unix)]
pub fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
pub fn is_root() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::CommandPolicy;

    #[test]
    fn accepts_safe_prefixes() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("hostname && uptime").is_ok());
    }

    #[test]
    fn denies_unknown_commands_by_default() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("cat /etc/passwd").is_err());
    }

    #[test]
    fn denies_dangerous_fragments_even_when_unsafe_mode_is_enabled() {
        let mut policy = CommandPolicy::default();
        policy.allow_unsafe_commands = true;
        assert!(policy.validate("rm -rf /tmp/aether-test").is_err());
    }
}
