//! Pure scheduling policy: which queued job may start now, and which model
//! mode the local model server should be in. No I/O here, so every rule is
//! unit-testable.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{Config, ResourceLimits, same_model};
use crate::job::{Job, Priority, Resource, ResourceKind};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPressure {
    Normal,
    Warn,
    Critical,
}

/// Reported by the host agent (`PUT /v1/host/status`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HostStatus {
    pub user_idle_secs: u64,
    pub memory_pressure: MemoryPressure,
    #[serde(default = "Utc::now")]
    pub reported_at: DateTime<Utc>,
}

/// What the scheduler believes about the host right now.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostView {
    pub idle: bool,
    pub pressure: MemoryPressure,
    pub stale: bool,
}

/// Missing or stale host status fails safe: the user counts as active and
/// memory pressure as elevated, so background work does not start.
pub fn host_view(status: Option<&HostStatus>, now: DateTime<Utc>, cfg: &Config) -> HostView {
    match status {
        Some(s) if (now - s.reported_at).num_seconds() <= cfg.host_status_stale_secs as i64 => {
            HostView {
                idle: s.user_idle_secs >= cfg.policy.background_idle_secs,
                pressure: s.memory_pressure,
                stale: false,
            }
        }
        _ => HostView {
            idle: false,
            pressure: MemoryPressure::Warn,
            stale: true,
        },
    }
}

/// Which models the local model server may have loaded.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ModelMode {
    /// Resident models (everything not listed as solo) share the GPU budget.
    Normal,
    /// One large model has the GPU to itself.
    Solo { model: String },
}

/// Currently leased jobs per resource.
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Running {
    pub cpu: u32,
    pub neural_engine: u32,
    pub gpu: u32,
}

impl Running {
    pub fn get(&self, kind: ResourceKind) -> u32 {
        match kind {
            ResourceKind::Cpu => self.cpu,
            ResourceKind::NeuralEngine => self.neural_engine,
            ResourceKind::Gpu => self.gpu,
        }
    }
}

fn limit(limits: &ResourceLimits, kind: ResourceKind) -> u32 {
    match kind {
        ResourceKind::Cpu => limits.cpu,
        ResourceKind::NeuralEngine => limits.neural_engine,
        ResourceKind::Gpu => limits.gpu,
    }
}

/// May `job` start right now?
pub fn admits(
    job: &Job,
    host: &HostView,
    mode: &ModelMode,
    running: &Running,
    cfg: &Config,
) -> bool {
    match job.priority {
        Priority::Background if !(host.idle && host.pressure == MemoryPressure::Normal) => {
            return false;
        }
        Priority::Normal if host.pressure == MemoryPressure::Critical => return false,
        _ => {}
    }

    // Interactive work gets the larger (idle) budget even while the user is active.
    let limits = if host.idle || job.priority == Priority::Interactive {
        &cfg.limits.idle
    } else {
        &cfg.limits.active
    };
    let kind = job.resource.kind();
    if running.get(kind) >= limit(limits, kind) {
        return false;
    }

    match (&job.resource, mode) {
        (Resource::Gpu { model }, ModelMode::Normal) => !cfg.is_solo_model(model),
        (Resource::Gpu { model }, ModelMode::Solo { model: solo }) => same_model(model, solo),
        _ => true,
    }
}

/// Pick the next job to lease from `candidates`: highest priority first; within
/// a priority, jobs for the model that is already warm first (fewer model
/// swaps), then oldest first.
pub fn pick<'a>(
    candidates: &'a [Job],
    host: &HostView,
    mode: &ModelMode,
    running: &Running,
    warm_model: Option<&str>,
    cfg: &Config,
) -> Option<&'a Job> {
    let mut ordered: Vec<&Job> = candidates.iter().collect();
    ordered.sort_by(|a, b| {
        let warm = |j: &Job| warm_model.is_some() && j.resource.model() == warm_model;
        b.priority
            .cmp(&a.priority)
            .then_with(|| warm(b).cmp(&warm(a)))
            .then_with(|| a.created_at.cmp(&b.created_at))
    });
    ordered
        .into_iter()
        .find(|j| admits(j, host, mode, running, cfg))
}

/// A queued GPU job, as seen by the mode planner.
#[derive(Clone, Debug)]
pub struct QueuedGpu {
    pub model: String,
    pub priority: Priority,
    pub created_at: DateTime<Utc>,
}

/// Decide whether to switch model mode. Switches only happen at a job
/// boundary (no GPU job running), so nothing is unloaded mid-generation.
///
/// * Normal -> Solo(m): the best queued job for a solo model outranks the best
///   resident job, and it is interactive or the host is idle with normal
///   memory pressure.
/// * Solo(m) -> Normal: nothing left for m, or the user needs resident models
///   back (an interactive resident job is waiting, or the host is no longer
///   idle and m's remaining work is not interactive).
pub fn next_mode(
    current: &ModelMode,
    queued: &[QueuedGpu],
    gpu_running: u32,
    host: &HostView,
    cfg: &Config,
) -> Option<ModelMode> {
    if gpu_running > 0 {
        return None;
    }
    let rank = |q: &&QueuedGpu| (q.priority, std::cmp::Reverse(q.created_at));
    let best_resident = queued
        .iter()
        .filter(|q| !cfg.is_solo_model(&q.model))
        .max_by_key(rank);
    let host_free = host.idle && host.pressure == MemoryPressure::Normal;

    match current {
        ModelMode::Normal => {
            let best_solo = queued
                .iter()
                .filter(|q| cfg.is_solo_model(&q.model))
                .max_by_key(rank)?;
            let allowed = best_solo.priority == Priority::Interactive || host_free;
            let outranks = best_resident.is_none_or(|r| rank(&best_solo) > rank(&r));
            (allowed && outranks).then(|| ModelMode::Solo {
                model: best_solo.model.clone(),
            })
        }
        ModelMode::Solo { model } => {
            let best_mine = queued
                .iter()
                .filter(|q| same_model(&q.model, model))
                .max_by_key(rank);
            let leave = match best_mine {
                None => true,
                Some(mine) => {
                    let mine_interactive = mine.priority == Priority::Interactive;
                    let resident_interactive =
                        best_resident.is_some_and(|r| r.priority == Priority::Interactive);
                    !mine_interactive && (resident_interactive || !host_free)
                }
            };
            leave.then_some(ModelMode::Normal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobState;
    use chrono::Duration;
    use serde_json::Value;
    use uuid::Uuid;

    fn cfg() -> Config {
        let mut c = Config::default();
        c.models.solo = vec!["big".into()];
        c
    }

    fn job(priority: Priority, resource: Resource, age_secs: i64) -> Job {
        let t = Utc::now() - Duration::seconds(age_secs);
        Job {
            id: Uuid::new_v4(),
            job_type: "t".into(),
            priority,
            resource,
            payload: Value::Null,
            state: JobState::Queued,
            attempts: 0,
            max_attempts: 3,
            idempotency_key: None,
            worker_id: None,
            lease_expires_at: None,
            result: None,
            review_required: false,
            error: None,
            created_at: t,
            updated_at: t,
        }
    }

    fn gpu(m: &str) -> Resource {
        Resource::Gpu { model: m.into() }
    }

    const ACTIVE: HostView = HostView {
        idle: false,
        pressure: MemoryPressure::Normal,
        stale: false,
    };
    const IDLE: HostView = HostView {
        idle: true,
        pressure: MemoryPressure::Normal,
        stale: false,
    };

    #[test]
    fn stale_host_status_fails_safe() {
        let c = cfg();
        let old = HostStatus {
            user_idle_secs: 10_000,
            memory_pressure: MemoryPressure::Normal,
            reported_at: Utc::now() - Duration::seconds(3600),
        };
        let v = host_view(Some(&old), Utc::now(), &c);
        assert!(!v.idle && v.stale);
        assert!(host_view(None, Utc::now(), &c).stale);
    }

    #[test]
    fn background_waits_for_idle_and_normal_pressure() {
        let c = cfg();
        let j = job(Priority::Background, Resource::Cpu, 0);
        let r = Running::default();
        assert!(!admits(&j, &ACTIVE, &ModelMode::Normal, &r, &c));
        assert!(admits(&j, &IDLE, &ModelMode::Normal, &r, &c));
        let warn = HostView {
            pressure: MemoryPressure::Warn,
            ..IDLE
        };
        assert!(!admits(&j, &warn, &ModelMode::Normal, &r, &c));
    }

    #[test]
    fn active_limits_throttle_but_interactive_gets_idle_budget() {
        let c = cfg();
        let r = Running {
            cpu: 2,
            ..Default::default()
        }; // active cpu limit is 2
        assert!(!admits(
            &job(Priority::Normal, Resource::Cpu, 0),
            &ACTIVE,
            &ModelMode::Normal,
            &r,
            &c
        ));
        assert!(admits(
            &job(Priority::Interactive, Resource::Cpu, 0),
            &ACTIVE,
            &ModelMode::Normal,
            &r,
            &c
        ));
    }

    #[test]
    fn gpu_jobs_respect_model_mode_and_single_slot() {
        let c = cfg();
        let r = Running::default();
        let small = job(Priority::Normal, gpu("small"), 0);
        let big = job(Priority::Normal, gpu("big"), 0);
        assert!(admits(&small, &IDLE, &ModelMode::Normal, &r, &c));
        assert!(!admits(&big, &IDLE, &ModelMode::Normal, &r, &c));
        let solo = ModelMode::Solo {
            model: "big".into(),
        };
        assert!(admits(&big, &IDLE, &solo, &r, &c));
        assert!(!admits(&small, &IDLE, &solo, &r, &c));
        let busy = Running {
            gpu: 1,
            ..Default::default()
        };
        assert!(!admits(&small, &IDLE, &ModelMode::Normal, &busy, &c));
    }

    #[test]
    fn pick_orders_by_priority_then_warm_model_then_age() {
        let c = cfg();
        let r = Running::default();
        let jobs = vec![
            job(Priority::Normal, gpu("a"), 100),
            job(Priority::Normal, gpu("b"), 10),
            job(Priority::Interactive, Resource::Cpu, 0),
        ];
        let first = pick(&jobs, &IDLE, &ModelMode::Normal, &r, Some("b"), &c).unwrap();
        assert_eq!(first.priority, Priority::Interactive);
        let rest = &jobs[..2];
        let next = pick(rest, &IDLE, &ModelMode::Normal, &r, Some("b"), &c).unwrap();
        assert_eq!(
            next.resource.model(),
            Some("b"),
            "warm model beats older job"
        );
        let cold = pick(rest, &IDLE, &ModelMode::Normal, &r, None, &c).unwrap();
        assert_eq!(cold.resource.model(), Some("a"), "otherwise oldest first");
    }

    fn q(model: &str, priority: Priority, age_secs: i64) -> QueuedGpu {
        QueuedGpu {
            model: model.into(),
            priority,
            created_at: Utc::now() - Duration::seconds(age_secs),
        }
    }

    #[test]
    fn enters_solo_only_when_idle_unless_interactive() {
        let c = cfg();
        let queued = vec![q("big", Priority::Background, 5)];
        assert_eq!(next_mode(&ModelMode::Normal, &queued, 0, &ACTIVE, &c), None);
        assert_eq!(
            next_mode(&ModelMode::Normal, &queued, 0, &IDLE, &c),
            Some(ModelMode::Solo {
                model: "big".into()
            })
        );
        let urgent = vec![q("big", Priority::Interactive, 5)];
        assert!(next_mode(&ModelMode::Normal, &urgent, 0, &ACTIVE, &c).is_some());
    }

    #[test]
    fn never_switches_mid_job_and_resident_work_first() {
        let c = cfg();
        let queued = vec![
            q("big", Priority::Background, 50),
            q("small", Priority::Normal, 1),
        ];
        assert_eq!(next_mode(&ModelMode::Normal, &queued, 1, &IDLE, &c), None);
        assert_eq!(next_mode(&ModelMode::Normal, &queued, 0, &IDLE, &c), None);
    }

    #[test]
    fn leaves_solo_when_drained_or_user_returns() {
        let c = cfg();
        let solo = ModelMode::Solo {
            model: "big".into(),
        };
        assert_eq!(next_mode(&solo, &[], 0, &IDLE, &c), Some(ModelMode::Normal));
        let mine = vec![q("big", Priority::Background, 5)];
        assert_eq!(next_mode(&solo, &mine, 0, &IDLE, &c), None);
        assert_eq!(
            next_mode(&solo, &mine, 0, &ACTIVE, &c),
            Some(ModelMode::Normal)
        );
        let contested = vec![
            q("big", Priority::Background, 5),
            q("small", Priority::Interactive, 1),
        ];
        assert_eq!(
            next_mode(&solo, &contested, 0, &IDLE, &c),
            Some(ModelMode::Normal)
        );
    }
}
