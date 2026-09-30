//! Job model shared by the store, the scheduler and the HTTP API.
//!
//! The orchestrator is deliberately domain-agnostic: `job_type` is an opaque
//! string (e.g. `extract.pdf_text`) and `payload`/`result` are opaque JSON.
//! Workers that understand a job type live outside this repository and talk to
//! it only through the job API.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Scheduling class. Ordering matters: higher variants are leased first.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Runs only while the host is idle and memory pressure is normal.
    Background,
    /// Runs any time, within the "active" concurrency limits while the user is busy.
    Normal,
    /// Something the user is waiting on. Always first in line.
    Interactive,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Priority::Background => "background",
            Priority::Normal => "normal",
            Priority::Interactive => "interactive",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "background" => Some(Priority::Background),
            "normal" => Some(Priority::Normal),
            "interactive" => Some(Priority::Interactive),
            _ => None,
        }
    }
}

/// The hardware a job occupies while it runs.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Resource {
    /// Ordinary CPU work, usually inside a container.
    Cpu,
    /// Apple Neural Engine work (e.g. Vision OCR), run by a host-side worker.
    NeuralEngine,
    /// A call into the shared local model server (Ollama) for `model`.
    Gpu { model: String },
}

impl Resource {
    pub fn kind(&self) -> ResourceKind {
        match self {
            Resource::Cpu => ResourceKind::Cpu,
            Resource::NeuralEngine => ResourceKind::NeuralEngine,
            Resource::Gpu { .. } => ResourceKind::Gpu,
        }
    }

    pub fn model(&self) -> Option<&str> {
        match self {
            Resource::Gpu { model } => Some(model),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Cpu,
    NeuralEngine,
    Gpu,
}

impl ResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ResourceKind::Cpu => "cpu",
            ResourceKind::NeuralEngine => "neural_engine",
            ResourceKind::Gpu => "gpu",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Leased,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Leased => "leased",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(JobState::Queued),
            "leased" => Some(JobState::Leased),
            "succeeded" => Some(JobState::Succeeded),
            "failed" => Some(JobState::Failed),
            "cancelled" => Some(JobState::Cancelled),
            _ => None,
        }
    }
}

/// Body of `POST /v1/jobs`.
#[derive(Deserialize, Clone, Debug)]
pub struct NewJob {
    pub job_type: String,
    #[serde(default = "default_priority")]
    pub priority: Priority,
    pub resource: Resource,
    #[serde(default)]
    pub payload: Value,
    /// Submitting twice with the same key returns the existing job instead of
    /// queueing a duplicate (e.g. `extract:<sha256>`).
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

fn default_priority() -> Priority {
    Priority::Normal
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Job {
    pub id: Uuid,
    pub job_type: String,
    pub priority: Priority,
    pub resource: Resource,
    pub payload: Value,
    pub state: JobState,
    pub attempts: u32,
    pub max_attempts: u32,
    pub idempotency_key: Option<String>,
    pub worker_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub result: Option<Value>,
    /// True when the result is a draft that a human must review before anything
    /// acts on it. Always true for model (GPU) jobs; see `Store::complete`.
    pub review_required: bool,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
