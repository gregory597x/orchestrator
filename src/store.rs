//! SQLite-backed job queue. Every state transition is a single guarded UPDATE
//! so a lost lease can never overwrite a newer owner's work.

use std::sync::Mutex;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::job::{Job, JobState, NewJob, Priority, Resource, ResourceKind};
use crate::scheduler::{QueuedGpu, Running};

const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Upper bound on candidates examined per lease request.
const CANDIDATE_LIMIT: u32 = 500;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("job not found")]
    NotFound,
    #[error("lease is not held by this worker")]
    LeaseLost,
    #[error("job is {0} and cannot be changed")]
    WrongState(&'static str),
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct Count {
    pub state: String,
    pub priority: String,
    pub resource: String,
    pub n: u64,
}

pub struct Store {
    conn: Mutex<Connection>,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS jobs (
    id               TEXT PRIMARY KEY,
    job_type         TEXT NOT NULL,
    priority         INTEGER NOT NULL,
    resource_kind    TEXT NOT NULL,
    model            TEXT,
    payload          TEXT NOT NULL,
    state            TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    idempotency_key  TEXT UNIQUE,
    worker_id        TEXT,
    lease_expires_at TEXT,
    result           TEXT,
    review_required  INTEGER NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs (state, priority DESC, created_at);
";

fn ts(t: DateTime<Utc>) -> String {
    // Fixed width so lexical order == chronological order.
    t.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })
}

fn priority_num(p: Priority) -> i64 {
    match p {
        Priority::Background => 0,
        Priority::Normal => 1,
        Priority::Interactive => 2,
    }
}

fn priority_from_num(n: i64) -> Priority {
    match n {
        0 => Priority::Background,
        2 => Priority::Interactive,
        _ => Priority::Normal,
    }
}

fn json_col(s: Option<String>) -> rusqlite::Result<Option<Value>> {
    s.map(|s| {
        serde_json::from_str(&s).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })
    })
    .transpose()
}

fn row_to_job(row: &Row) -> rusqlite::Result<Job> {
    let id: String = row.get("id")?;
    let kind: String = row.get("resource_kind")?;
    let model: Option<String> = row.get("model")?;
    let resource = match (kind.as_str(), model) {
        ("gpu", Some(model)) => Resource::Gpu { model },
        ("neural_engine", _) => Resource::NeuralEngine,
        _ => Resource::Cpu,
    };
    let state: String = row.get("state")?;
    Ok(Job {
        id: Uuid::parse_str(&id).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?,
        job_type: row.get("job_type")?,
        priority: priority_from_num(row.get("priority")?),
        resource,
        payload: json_col(row.get("payload")?)?.unwrap_or(Value::Null),
        state: JobState::parse(&state).unwrap_or(JobState::Failed),
        attempts: row.get("attempts")?,
        max_attempts: row.get("max_attempts")?,
        idempotency_key: row.get("idempotency_key")?,
        worker_id: row.get("worker_id")?,
        lease_expires_at: row
            .get::<_, Option<String>>("lease_expires_at")?
            .map(|s| parse_ts(&s))
            .transpose()?,
        result: json_col(row.get("result")?)?,
        review_required: row.get::<_, i64>("review_required")? != 0,
        error: row.get("error")?,
        created_at: parse_ts(&row.get::<_, String>("created_at")?)?,
        updated_at: parse_ts(&row.get::<_, String>("updated_at")?)?,
    })
}

fn get_job(conn: &Connection, id: Uuid) -> Result<Job> {
    conn.query_row(
        "SELECT * FROM jobs WHERE id = ?1",
        [id.to_string()],
        row_to_job,
    )
    .optional()?
    .ok_or(StoreError::NotFound)
}

/// Distinguish "no such job" from "job exists but the lease isn't yours".
fn lease_error(conn: &Connection, id: Uuid) -> StoreError {
    match get_job(conn, id) {
        Ok(_) => StoreError::LeaseLost,
        Err(e) => e,
    }
}

impl Store {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        // A panic while holding the lock leaves SQLite itself consistent.
        self.conn.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Queue a job. Returns `(job, created)`; `created` is false when an
    /// existing job with the same idempotency key was returned instead.
    pub fn submit(&self, new: &NewJob, now: DateTime<Utc>) -> Result<(Job, bool)> {
        let conn = self.conn();
        if let Some(key) = &new.idempotency_key {
            let existing = conn
                .query_row(
                    "SELECT * FROM jobs WHERE idempotency_key = ?1",
                    [key],
                    row_to_job,
                )
                .optional()?;
            if let Some(job) = existing {
                return Ok((job, false));
            }
        }
        let id = Uuid::new_v4();
        let kind = new.resource.kind();
        conn.execute(
            "INSERT INTO jobs (id, job_type, priority, resource_kind, model, payload, state,
                               max_attempts, idempotency_key, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'queued', ?7, ?8, ?9, ?9)",
            params![
                id.to_string(),
                new.job_type,
                priority_num(new.priority),
                kind.as_str(),
                new.resource.model(),
                serde_json::to_string(&new.payload)?,
                new.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
                new.idempotency_key,
                ts(now),
            ],
        )?;
        Ok((get_job(&conn, id)?, true))
    }

    pub fn get(&self, id: Uuid) -> Result<Job> {
        get_job(&self.conn(), id)
    }

    pub fn list(
        &self,
        state: Option<JobState>,
        job_type: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Job>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT * FROM jobs
             WHERE (?1 IS NULL OR state = ?1) AND (?2 IS NULL OR job_type = ?2)
             ORDER BY created_at DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(
            params![state.map(|s| s.as_str()), job_type, limit],
            row_to_job,
        )?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Atomically choose and lease a job. `choose` sees the queued candidates
    /// this worker can run plus current running counts, and returns the id to
    /// lease (normally via `scheduler::pick`). The lock is held throughout, so
    /// concurrency limits cannot be overshot by racing workers.
    pub fn lease_next<F>(
        &self,
        worker_id: &str,
        job_types: &[String],
        kinds: &[ResourceKind],
        now: DateTime<Utc>,
        ttl: Duration,
        choose: F,
    ) -> Result<Option<Job>>
    where
        F: FnOnce(&[Job], &Running) -> Option<Uuid>,
    {
        let conn = self.conn();
        let running = running_counts(&conn)?;
        let mut stmt = conn.prepare(
            "SELECT * FROM jobs WHERE state = 'queued'
             ORDER BY priority DESC, created_at LIMIT ?1",
        )?;
        let candidates: Vec<Job> = stmt
            .query_map([CANDIDATE_LIMIT], row_to_job)?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|j| {
                (job_types.is_empty() || job_types.contains(&j.job_type))
                    && kinds.contains(&j.resource.kind())
            })
            .collect();
        let Some(id) = choose(&candidates, &running) else {
            return Ok(None);
        };
        let changed = conn.execute(
            "UPDATE jobs SET state = 'leased', worker_id = ?2, lease_expires_at = ?3,
                             attempts = attempts + 1, error = NULL, updated_at = ?4
             WHERE id = ?1 AND state = 'queued'",
            params![id.to_string(), worker_id, ts(now + ttl), ts(now)],
        )?;
        if changed == 0 {
            return Ok(None);
        }
        Ok(Some(get_job(&conn, id)?))
    }

    pub fn heartbeat(
        &self,
        id: Uuid,
        worker_id: &str,
        now: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<DateTime<Utc>> {
        let conn = self.conn();
        let expires = now + ttl;
        let changed = conn.execute(
            "UPDATE jobs SET lease_expires_at = ?3, updated_at = ?4
             WHERE id = ?1 AND state = 'leased' AND worker_id = ?2",
            params![id.to_string(), worker_id, ts(expires), ts(now)],
        )?;
        if changed == 0 {
            return Err(lease_error(&conn, id));
        }
        Ok(expires)
    }

    /// Record a result. Model (GPU) output is always marked `review_required`:
    /// it is a draft for a human, never something to act on automatically.
    pub fn complete(
        &self,
        id: Uuid,
        worker_id: &str,
        result: &Value,
        review_required: bool,
        now: DateTime<Utc>,
    ) -> Result<Job> {
        let conn = self.conn();
        let changed = conn.execute(
            "UPDATE jobs SET state = 'succeeded', result = ?3,
                             review_required = (?4 OR resource_kind = 'gpu'),
                             lease_expires_at = NULL, updated_at = ?5
             WHERE id = ?1 AND state = 'leased' AND worker_id = ?2",
            params![
                id.to_string(),
                worker_id,
                serde_json::to_string(result)?,
                review_required,
                ts(now)
            ],
        )?;
        if changed == 0 {
            return Err(lease_error(&conn, id));
        }
        get_job(&conn, id)
    }

    /// Record a failure. Retryable failures go back to the queue until
    /// `max_attempts` is used up.
    pub fn fail(
        &self,
        id: Uuid,
        worker_id: &str,
        error: &str,
        retryable: bool,
        now: DateTime<Utc>,
    ) -> Result<Job> {
        let conn = self.conn();
        let changed = conn.execute(
            "UPDATE jobs SET
                 state = CASE WHEN ?4 AND attempts < max_attempts THEN 'queued' ELSE 'failed' END,
                 worker_id = NULL, lease_expires_at = NULL, error = ?3, updated_at = ?5
             WHERE id = ?1 AND state = 'leased' AND worker_id = ?2",
            params![id.to_string(), worker_id, error, retryable, ts(now)],
        )?;
        if changed == 0 {
            return Err(lease_error(&conn, id));
        }
        get_job(&conn, id)
    }

    /// Cancel a queued or leased job. A worker holding the lease finds out on
    /// its next heartbeat or completion (409).
    pub fn cancel(&self, id: Uuid, now: DateTime<Utc>) -> Result<Job> {
        let conn = self.conn();
        let job = get_job(&conn, id)?;
        if !matches!(job.state, JobState::Queued | JobState::Leased) {
            return Err(StoreError::WrongState(job.state.as_str()));
        }
        conn.execute(
            "UPDATE jobs SET state = 'cancelled', worker_id = NULL, lease_expires_at = NULL, updated_at = ?2
             WHERE id = ?1 AND state IN ('queued', 'leased')",
            params![id.to_string(), ts(now)],
        )?;
        get_job(&conn, id)
    }

    /// Return expired leases to the queue (or fail them when out of attempts).
    pub fn reap_expired(&self, now: DateTime<Utc>) -> Result<usize> {
        let conn = self.conn();
        Ok(conn.execute(
            "UPDATE jobs SET
                 state = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END,
                 error = 'lease expired', worker_id = NULL, lease_expires_at = NULL, updated_at = ?1
             WHERE state = 'leased' AND lease_expires_at < ?1",
            [ts(now)],
        )?)
    }

    pub fn running(&self) -> Result<Running> {
        running_counts(&self.conn())
    }

    pub fn queued_gpu(&self) -> Result<Vec<QueuedGpu>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT model, priority, created_at FROM jobs
             WHERE state = 'queued' AND resource_kind = 'gpu' AND model IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(QueuedGpu {
                model: r.get(0)?,
                priority: priority_from_num(r.get(1)?),
                created_at: parse_ts(&r.get::<_, String>(2)?)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn counts(&self) -> Result<Vec<Count>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT state, priority, resource_kind, COUNT(*) FROM jobs
             GROUP BY state, priority, resource_kind ORDER BY state, priority DESC, resource_kind",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(Count {
                state: r.get(0)?,
                priority: priority_from_num(r.get(1)?).as_str().to_string(),
                resource: r.get(2)?,
                n: r.get::<_, i64>(3)?.max(0) as u64,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

fn running_counts(conn: &Connection) -> Result<Running> {
    let mut stmt = conn.prepare(
        "SELECT resource_kind, COUNT(*) FROM jobs WHERE state = 'leased' GROUP BY resource_kind",
    )?;
    let mut running = Running::default();
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u32>(1)?)))?;
    for row in rows {
        let (kind, n) = row?;
        match kind.as_str() {
            "cpu" => running.cpu = n,
            "neural_engine" => running.neural_engine = n,
            "gpu" => running.gpu = n,
            _ => {}
        }
    }
    Ok(running)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn store() -> Store {
        Store::open(":memory:").unwrap()
    }

    fn new_job(resource: Resource, key: Option<&str>) -> NewJob {
        NewJob {
            job_type: "extract.pdf_text".into(),
            priority: Priority::Normal,
            resource,
            payload: json!({"sha256": "abc"}),
            idempotency_key: key.map(Into::into),
            max_attempts: Some(2),
        }
    }

    fn lease_any(s: &Store, worker: &str, now: DateTime<Utc>) -> Option<Job> {
        s.lease_next(
            worker,
            &[],
            &[ResourceKind::Cpu, ResourceKind::Gpu],
            now,
            Duration::seconds(60),
            |c, _| c.first().map(|j| j.id),
        )
        .unwrap()
    }

    #[test]
    fn idempotency_key_dedupes_submissions() {
        let s = store();
        let now = Utc::now();
        let (a, created_a) = s
            .submit(&new_job(Resource::Cpu, Some("extract:abc")), now)
            .unwrap();
        let (b, created_b) = s
            .submit(&new_job(Resource::Cpu, Some("extract:abc")), now)
            .unwrap();
        assert!(created_a && !created_b);
        assert_eq!(a.id, b.id);
    }

    #[test]
    fn lease_complete_round_trip_and_stale_worker_rejected() {
        let s = store();
        let now = Utc::now();
        let (job, _) = s.submit(&new_job(Resource::Cpu, None), now).unwrap();
        let leased = lease_any(&s, "w1", now).unwrap();
        assert_eq!(leased.id, job.id);
        assert_eq!(leased.attempts, 1);
        assert!(lease_any(&s, "w2", now).is_none(), "already leased");
        assert!(matches!(
            s.complete(job.id, "w2", &json!({}), false, now),
            Err(StoreError::LeaseLost)
        ));
        let done = s
            .complete(job.id, "w1", &json!({"text": "hi"}), false, now)
            .unwrap();
        assert_eq!(done.state, JobState::Succeeded);
        assert!(!done.review_required);
    }

    #[test]
    fn gpu_results_always_need_review() {
        let s = store();
        let now = Utc::now();
        s.submit(&new_job(Resource::Gpu { model: "m".into() }, None), now)
            .unwrap();
        let j = lease_any(&s, "w", now).unwrap();
        let done = s
            .complete(j.id, "w", &json!({"suggestion": "same doc"}), false, now)
            .unwrap();
        assert!(done.review_required);
    }

    #[test]
    fn expired_leases_requeue_until_attempts_exhausted() {
        let s = store();
        let t0 = Utc::now();
        let (job, _) = s.submit(&new_job(Resource::Cpu, None), t0).unwrap();
        lease_any(&s, "w", t0).unwrap();
        let later = t0 + Duration::seconds(120);
        assert_eq!(s.reap_expired(later).unwrap(), 1);
        assert_eq!(s.get(job.id).unwrap().state, JobState::Queued);
        lease_any(&s, "w", later).unwrap();
        s.reap_expired(later + Duration::seconds(120)).unwrap();
        let j = s.get(job.id).unwrap();
        assert_eq!(j.state, JobState::Failed);
        assert_eq!(j.error.as_deref(), Some("lease expired"));
    }

    #[test]
    fn retryable_failure_requeues() {
        let s = store();
        let now = Utc::now();
        let (job, _) = s.submit(&new_job(Resource::Cpu, None), now).unwrap();
        lease_any(&s, "w", now).unwrap();
        assert_eq!(
            s.fail(job.id, "w", "boom", true, now).unwrap().state,
            JobState::Queued
        );
        lease_any(&s, "w", now).unwrap();
        assert_eq!(
            s.fail(job.id, "w", "boom", true, now).unwrap().state,
            JobState::Failed
        );
    }

    #[test]
    fn cancel_invalidates_lease() {
        let s = store();
        let now = Utc::now();
        let (job, _) = s.submit(&new_job(Resource::Cpu, None), now).unwrap();
        lease_any(&s, "w", now).unwrap();
        s.cancel(job.id, now).unwrap();
        assert!(matches!(
            s.heartbeat(job.id, "w", now, Duration::seconds(60)),
            Err(StoreError::LeaseLost)
        ));
        assert!(matches!(
            s.cancel(job.id, now),
            Err(StoreError::WrongState("cancelled"))
        ));
    }
}
