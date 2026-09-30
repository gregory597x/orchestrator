//! HTTP API v1. See `docs/job-api.md` for the contract workers rely on.

use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::config::Config;
use crate::job::{Job, JobState, NewJob, ResourceKind};
use crate::models::{self, Ollama};
use crate::scheduler::{self, HostStatus, ModelMode};
use crate::store::{Store, StoreError};

/// Mutable scheduler state. Lease decisions and mode switches both take this
/// lock, so a mode switch can never race a GPU lease.
#[derive(Debug)]
pub struct Runtime {
    pub mode: ModelMode,
    pub host: Option<HostStatus>,
    /// Model of the most recently leased GPU job; preferred to avoid swaps.
    pub warm_model: Option<String>,
}

pub struct AppState {
    pub cfg: Config,
    pub store: Store,
    pub runtime: Mutex<Runtime>,
    pub ollama: Option<Ollama>,
    pub token: Option<String>,
}

pub type Shared = Arc<AppState>;

impl AppState {
    pub fn new(cfg: Config, store: Store, ollama: Option<Ollama>, token: Option<String>) -> Shared {
        Arc::new(Self {
            cfg,
            store,
            runtime: Mutex::new(Runtime {
                mode: ModelMode::Normal,
                host: None,
                warm_model: None,
            }),
            ollama,
            token,
        })
    }

    fn runtime(&self) -> std::sync::MutexGuard<'_, Runtime> {
        self.runtime.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn ttl(&self) -> Duration {
        Duration::seconds(self.cfg.lease_ttl_secs as i64)
    }
}

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        let status = match e {
            StoreError::NotFound => StatusCode::NOT_FOUND,
            StoreError::LeaseLost | StoreError::WrongState(_) => StatusCode::CONFLICT,
            StoreError::Db(_) | StoreError::Json(_) => {
                tracing::error!(error = %e, "store error");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        ApiError(status, e.to_string())
    }
}

type ApiResult<T> = Result<T, ApiError>;

pub fn router(state: Shared) -> Router {
    let v1 = Router::new()
        .route("/jobs", post(submit).get(list))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/heartbeat", post(heartbeat))
        .route("/jobs/{id}/complete", post(complete))
        .route("/jobs/{id}/fail", post(fail))
        .route("/leases", post(lease))
        .route("/host/status", put(put_host_status))
        .route("/status", get(status))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .nest("/v1", v1)
        .with_state(state)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn require_token(State(state): State<Shared>, req: Request, next: Next) -> Response {
    if let Some(expected) = &state.token {
        let given = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if !given.is_some_and(|g| constant_time_eq(g.as_bytes(), expected.as_bytes())) {
            return ApiError(
                StatusCode::UNAUTHORIZED,
                "missing or invalid bearer token".into(),
            )
            .into_response();
        }
    }
    next.run(req).await
}

async fn submit(
    State(s): State<Shared>,
    Json(new): Json<NewJob>,
) -> ApiResult<(StatusCode, Json<Job>)> {
    if new.job_type.trim().is_empty() {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "job_type is required".into(),
        ));
    }
    let (job, created) = s.store.submit(&new, Utc::now())?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(job),
    ))
}

#[derive(Deserialize)]
struct ListQuery {
    state: Option<String>,
    job_type: Option<String>,
    limit: Option<u32>,
}

async fn list(State(s): State<Shared>, Query(q): Query<ListQuery>) -> ApiResult<Json<Vec<Job>>> {
    let state =
        match q.state.as_deref() {
            None => None,
            Some(raw) => Some(JobState::parse(raw).ok_or_else(|| {
                ApiError(StatusCode::BAD_REQUEST, format!("unknown state {raw:?}"))
            })?),
        };
    let limit = q.limit.unwrap_or(100).min(1000);
    Ok(Json(s.store.list(state, q.job_type.as_deref(), limit)?))
}

async fn get_job(State(s): State<Shared>, Path(id): Path<Uuid>) -> ApiResult<Json<Job>> {
    Ok(Json(s.store.get(id)?))
}

async fn cancel(State(s): State<Shared>, Path(id): Path<Uuid>) -> ApiResult<Json<Job>> {
    Ok(Json(s.store.cancel(id, Utc::now())?))
}

#[derive(Deserialize)]
pub struct LeaseRequest {
    pub worker_id: String,
    /// Job types this worker handles. Empty means any.
    #[serde(default)]
    pub job_types: Vec<String>,
    pub resources: Vec<ResourceKind>,
}

#[derive(Serialize)]
struct LeaseResponse {
    job: Job,
    lease_expires_at: chrono::DateTime<Utc>,
}

/// 200 with a job, or 204 when nothing is runnable for this worker right now.
async fn lease(State(s): State<Shared>, Json(req): Json<LeaseRequest>) -> ApiResult<Response> {
    if req.worker_id.trim().is_empty() {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "worker_id is required".into(),
        ));
    }
    let now = Utc::now();
    let mut rt = s.runtime();
    let host = scheduler::host_view(rt.host.as_ref(), now, &s.cfg);
    let (mode, warm) = (rt.mode.clone(), rt.warm_model.clone());
    let leased = s.store.lease_next(
        &req.worker_id,
        &req.job_types,
        &req.resources,
        now,
        s.ttl(),
        |cands, running| {
            scheduler::pick(cands, &host, &mode, running, warm.as_deref(), &s.cfg).map(|j| j.id)
        },
    )?;
    Ok(match leased {
        Some(job) => {
            if let Some(model) = job.resource.model() {
                rt.warm_model = Some(model.to_string());
            }
            let lease_expires_at = job.lease_expires_at.unwrap_or(now);
            Json(LeaseResponse {
                job,
                lease_expires_at,
            })
            .into_response()
        }
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[derive(Deserialize)]
struct WorkerBody {
    worker_id: String,
}

async fn heartbeat(
    State(s): State<Shared>,
    Path(id): Path<Uuid>,
    Json(b): Json<WorkerBody>,
) -> ApiResult<Json<Value>> {
    let expires = s.store.heartbeat(id, &b.worker_id, Utc::now(), s.ttl())?;
    Ok(Json(json!({ "lease_expires_at": expires })))
}

#[derive(Deserialize)]
struct CompleteBody {
    worker_id: String,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    review_required: bool,
}

async fn complete(
    State(s): State<Shared>,
    Path(id): Path<Uuid>,
    Json(b): Json<CompleteBody>,
) -> ApiResult<Json<Job>> {
    Ok(Json(s.store.complete(
        id,
        &b.worker_id,
        &b.result,
        b.review_required,
        Utc::now(),
    )?))
}

#[derive(Deserialize)]
struct FailBody {
    worker_id: String,
    error: String,
    #[serde(default)]
    retryable: bool,
}

async fn fail(
    State(s): State<Shared>,
    Path(id): Path<Uuid>,
    Json(b): Json<FailBody>,
) -> ApiResult<Json<Job>> {
    Ok(Json(s.store.fail(
        id,
        &b.worker_id,
        &b.error,
        b.retryable,
        Utc::now(),
    )?))
}

#[derive(Deserialize)]
struct HostStatusBody {
    user_idle_secs: u64,
    memory_pressure: scheduler::MemoryPressure,
}

async fn put_host_status(State(s): State<Shared>, Json(b): Json<HostStatusBody>) -> StatusCode {
    s.runtime().host = Some(HostStatus {
        user_idle_secs: b.user_idle_secs,
        memory_pressure: b.memory_pressure,
        reported_at: Utc::now(),
    });
    StatusCode::NO_CONTENT
}

async fn status(State(s): State<Shared>) -> ApiResult<Json<Value>> {
    let (mode, host, warm) = {
        let rt = s.runtime();
        (rt.mode.clone(), rt.host.clone(), rt.warm_model.clone())
    };
    let view = scheduler::host_view(host.as_ref(), Utc::now(), &s.cfg);
    Ok(Json(json!({
        "mode": mode,
        "warm_model": warm,
        "host": host,
        "host_view": view,
        "running": s.store.running()?,
        "counts": s.store.counts()?,
    })))
}

/// One pass of the background loop: reap expired leases, then re-evaluate the
/// model mode and apply a switch if the policy asks for one.
pub async fn tick(s: &Shared) {
    let now = Utc::now();
    match s.store.reap_expired(now) {
        Ok(0) => {}
        Ok(n) => tracing::warn!(n, "re-queued expired leases"),
        Err(e) => tracing::error!(error = %e, "reaping leases failed"),
    }

    let next = {
        let mut rt = s.runtime();
        let decision = (|| -> Result<Option<ModelMode>, StoreError> {
            let host = scheduler::host_view(rt.host.as_ref(), now, &s.cfg);
            let running = s.store.running()?;
            Ok(scheduler::next_mode(
                &rt.mode,
                &s.store.queued_gpu()?,
                running.gpu,
                &host,
                &s.cfg,
            ))
        })();
        match decision {
            Ok(Some(next)) => {
                // Switch while holding the runtime lock: no GPU lease can slip in
                // under the old mode between the check and the switch.
                tracing::info!(from = ?rt.mode, to = ?next, "model mode switch");
                rt.mode = next.clone();
                rt.warm_model = None;
                Some(next)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::error!(error = %e, "mode evaluation failed");
                None
            }
        }
    };

    if let Some(mode) = next {
        // Best effort: if an unload fails, Ollama's own LRU eviction still
        // applies when the next model loads.
        if let Err(e) = models::enter(s.ollama.as_ref(), &mode, &s.cfg).await {
            tracing::warn!(error = %e, "applying model mode failed");
        }
    }
}
