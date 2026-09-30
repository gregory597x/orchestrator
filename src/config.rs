//! Runtime configuration, loaded from a TOML file (see `config.example.toml`).
//! Every field has a default so an empty file is valid.

use std::path::Path;

use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    /// Address the HTTP API binds to.
    pub listen: String,
    /// SQLite database path. `:memory:` is allowed (tests).
    pub database: String,
    /// How long a lease lasts without a heartbeat before the job is re-queued.
    pub lease_ttl_secs: u64,
    /// Host status older than this is treated as "user active, unknown pressure".
    pub host_status_stale_secs: u64,
    /// How often the background loop reaps leases and re-evaluates model mode.
    pub tick_secs: u64,
    pub policy: Policy,
    pub limits: Limits,
    pub models: Models,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8780".into(),
            database: "orchestrator.db".into(),
            lease_ttl_secs: 120,
            host_status_stale_secs: 60,
            tick_secs: 5,
            policy: Policy::default(),
            limits: Limits::default(),
            models: Models::default(),
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Policy {
    /// Seconds of no keyboard/mouse input before the host counts as idle.
    pub background_idle_secs: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            background_idle_secs: 300,
        }
    }
}

/// Maximum concurrently leased jobs per resource. When a `[limits.*]` table
/// is given, all three fields are required, so a typo can't silently zero one.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    pub cpu: u32,
    pub neural_engine: u32,
    pub gpu: u32,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Limits {
    /// Applied while the user is active (or host status is stale).
    pub active: ResourceLimits,
    /// Applied while the host is idle.
    pub idle: ResourceLimits,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            active: ResourceLimits {
                cpu: 2,
                neural_engine: 1,
                gpu: 1,
            },
            idle: ResourceLimits {
                cpu: 6,
                neural_engine: 2,
                gpu: 1,
            },
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Models {
    /// Ollama base URL as seen from the orchestrator.
    pub ollama_url: String,
    /// Models that must run alone: entering solo mode unloads everything else.
    /// Any model not listed here is treated as resident (normal mode).
    pub solo: Vec<String>,
}

impl Default for Models {
    fn default() -> Self {
        Self {
            ollama_url: "http://host.docker.internal:11434".into(),
            solo: Vec::new(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("reading {}: {e}", p.display()))?;
                Ok(toml::from_str(&text)?)
            }
            None => Ok(Self::default()),
        }
    }

    pub fn is_solo_model(&self, model: &str) -> bool {
        self.models.solo.iter().any(|m| same_model(m, model))
    }
}

/// Ollama names models `name:tag`; a bare name means `name:latest`.
pub fn same_model(a: &str, b: &str) -> bool {
    let norm = |s: &str| {
        if s.contains(':') {
            s.to_string()
        } else {
            format!("{s}:latest")
        }
    };
    norm(a) == norm(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_file_gives_defaults() {
        let c: Config = toml::from_str("").unwrap();
        assert_eq!(c.lease_ttl_secs, 120);
        assert_eq!(c.limits.active.gpu, 1);
    }

    #[test]
    fn example_config_parses() {
        let text = include_str!("../config.example.toml");
        let c: Config = toml::from_str(text).unwrap();
        assert!(c.is_solo_model("qwen3-vl-32b-8k"));
        assert!(!c.is_solo_model("qwen3-vl:8b"));
        assert!(c.is_solo_model("qwen3-vl-32b-8k:latest"));
    }
}
