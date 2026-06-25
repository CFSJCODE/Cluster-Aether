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
                "get-date".into(),
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

        if command_segments(command).all(|segment| self.segment_is_allowed(segment)) {
            return Ok(());
        }

        Err(CommandPolicyError::Denied(command.to_string()))
    }

    fn segment_is_allowed(&self, segment: &str) -> bool {
        let segment = segment.trim().to_lowercase();
        if segment.is_empty() {
            return true;
        }

        self.allowed_prefixes.iter().any(|prefix| {
            let prefix = prefix.trim().to_lowercase();
            !prefix.is_empty()
                && (segment == prefix
                    || segment
                        .strip_prefix(&prefix)
                        .is_some_and(|rest| rest.starts_with(char::is_whitespace)))
        })
    }
}

fn command_segments(command: &str) -> impl Iterator<Item = &str> {
    command
        .split('\n')
        .flat_map(|line| line.split(';'))
        .flat_map(|segment| segment.split("&&"))
        .flat_map(|segment| segment.split("||"))
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
    fn accepts_safe_powershell_segments() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("hostname; whoami; Get-Date").is_ok());
    }

    #[test]
    fn denies_unknown_commands_by_default() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("cat /etc/passwd").is_err());
    }

    #[test]
    fn denies_dangerous_fragments_even_when_unsafe_mode_is_enabled() {
        let policy = CommandPolicy {
            allow_unsafe_commands: true,
            ..CommandPolicy::default()
        };
        assert!(policy.validate("rm -rf /tmp/aether-test").is_err());
    }

    #[test]
    fn denies_unknown_commands_after_safe_segments() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("hostname && cat /etc/passwd").is_err());
    }

    #[test]
    fn denies_prefix_smuggling() {
        let policy = CommandPolicy::default();
        assert!(policy.validate("hostnameevil").is_err());
    }

    #[test]
    fn empty_allowed_prefix_does_not_allow_everything() {
        let policy = CommandPolicy {
            allowed_prefixes: vec![String::new()],
            ..CommandPolicy::default()
        };
        assert!(policy.validate("hostname").is_err());
    }
}
