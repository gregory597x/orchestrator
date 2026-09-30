//! Keeps the local model server (Ollama) in the mode the scheduler chose.
//! Workers call Ollama themselves; the orchestrator only decides *when* a
//! model may run and unloads models on mode switches.

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::config::{Config, same_model};
use crate::scheduler::ModelMode;

pub struct Ollama {
    base: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct PsResponse {
    #[serde(default)]
    models: Vec<PsModel>,
}

#[derive(Deserialize)]
struct PsModel {
    name: String,
}

impl Ollama {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("static reqwest config"),
        }
    }

    /// Names of currently loaded models (`GET /api/ps`).
    pub async fn loaded(&self) -> anyhow::Result<Vec<String>> {
        let ps: PsResponse = self
            .http
            .get(format!("{}/api/ps", self.base))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(ps.models.into_iter().map(|m| m.name).collect())
    }

    /// Unload a model now (`keep_alive: 0`).
    pub async fn unload(&self, model: &str) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/api/generate", self.base))
            .json(&json!({ "model": model, "keep_alive": 0 }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// Models to unload when entering `mode`, given what is loaded now.
pub fn to_unload(mode: &ModelMode, loaded: &[String], cfg: &Config) -> Vec<String> {
    loaded
        .iter()
        .filter(|m| match mode {
            ModelMode::Solo { model } => !same_model(m, model),
            ModelMode::Normal => cfg.models.solo.iter().any(|s| same_model(m, s)),
        })
        .cloned()
        .collect()
}

/// Apply a mode switch. With no backend configured (tests), this is a no-op.
pub async fn enter(backend: Option<&Ollama>, mode: &ModelMode, cfg: &Config) -> anyhow::Result<()> {
    let Some(ollama) = backend else { return Ok(()) };
    for model in to_unload(mode, &ollama.loaded().await?, cfg) {
        tracing::info!(%model, "unloading for mode switch");
        ollama.unload(&model).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn solo_unloads_everything_else_normal_unloads_solo_models() {
        let mut cfg = Config::default();
        cfg.models.solo = vec!["qwen3-vl-32b-8k".into()];
        let loaded = vec![
            "deepseek-coder-v2-16k:latest".to_string(),
            "qwen3-vl-32b-8k:latest".to_string(),
        ];
        let solo = ModelMode::Solo {
            model: "qwen3-vl-32b-8k".into(),
        };
        assert_eq!(
            to_unload(&solo, &loaded, &cfg),
            vec!["deepseek-coder-v2-16k:latest"]
        );
        assert_eq!(
            to_unload(&ModelMode::Normal, &loaded, &cfg),
            vec!["qwen3-vl-32b-8k:latest"]
        );
    }
}
