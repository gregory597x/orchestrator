//! End-to-end tests through the real router, with an in-memory database and
//! no model server.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use orchestrator::api::{self, AppState, Shared};
use orchestrator::config::Config;
use orchestrator::store::Store;
use serde_json::{Value, json};
use tower::ServiceExt;

const TOKEN: &str = "test-token";

fn setup() -> (Shared, Router) {
    let mut cfg = Config {
        database: ":memory:".into(),
        ..Default::default()
    };
    cfg.models.solo = vec!["qwen3-vl-32b-8k".into()];
    let store = Store::open(&cfg.database).unwrap();
    let state = AppState::new(cfg, store, None, Some(TOKEN.into()));
    let router = api::router(state.clone());
    (state, router)
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(body.map_or(Body::empty(), |b| Body::from(b.to_string())))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

fn lease_req(worker: &str, resources: Value) -> Value {
    json!({ "worker_id": worker, "resources": resources })
}

#[tokio::test]
async fn requires_bearer_token_except_healthz() {
    let (_, app) = setup();
    let req = Request::get("/v1/status").body(Body::empty()).unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let req = Request::get("/healthz").body(Body::empty()).unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn submit_lease_complete_with_idempotency() {
    let (_, app) = setup();
    let job = json!({
        "job_type": "extract.pdf_text",
        "resource": { "kind": "cpu" },
        "payload": { "sha256": "abc" },
        "idempotency_key": "extract:abc"
    });
    let (st, created) = call(&app, "POST", "/v1/jobs", Some(job.clone())).await;
    assert_eq!(st, StatusCode::CREATED);
    let (st, again) = call(&app, "POST", "/v1/jobs", Some(job)).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(created["id"], again["id"]);

    let (st, lease) = call(
        &app,
        "POST",
        "/v1/leases",
        Some(lease_req("w1", json!(["cpu"]))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let id = lease["job"]["id"].as_str().unwrap().to_string();

    let (st, _) = call(
        &app,
        "POST",
        "/v1/leases",
        Some(lease_req("w2", json!(["cpu"]))),
    )
    .await;
    assert_eq!(st, StatusCode::NO_CONTENT);

    let (st, _) = call(
        &app,
        "POST",
        &format!("/v1/jobs/{id}/heartbeat"),
        Some(json!({"worker_id": "w1"})),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = call(
        &app,
        "POST",
        &format!("/v1/jobs/{id}/complete"),
        Some(json!({"worker_id": "w2", "result": {}})),
    )
    .await;
    assert_eq!(
        st,
        StatusCode::CONFLICT,
        "only the lease holder may complete"
    );
    let (st, done) = call(
        &app,
        "POST",
        &format!("/v1/jobs/{id}/complete"),
        Some(json!({"worker_id": "w1", "result": {"text": "hello"}})),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(done["state"], "succeeded");
    assert_eq!(done["review_required"], false);
}

#[tokio::test]
async fn background_jobs_wait_for_idle_host() {
    let (_, app) = setup();
    let job = json!({ "job_type": "extract.ocr", "priority": "background", "resource": { "kind": "neural_engine" } });
    call(&app, "POST", "/v1/jobs", Some(job)).await;

    let worker = lease_req("ocr", json!(["neural_engine"]));
    let (st, _) = call(&app, "POST", "/v1/leases", Some(worker.clone())).await;
    assert_eq!(st, StatusCode::NO_CONTENT, "no host status yet: fail safe");

    let busy = json!({ "user_idle_secs": 5, "memory_pressure": "normal" });
    call(&app, "PUT", "/v1/host/status", Some(busy)).await;
    let (st, _) = call(&app, "POST", "/v1/leases", Some(worker.clone())).await;
    assert_eq!(st, StatusCode::NO_CONTENT, "user is active");

    let idle = json!({ "user_idle_secs": 900, "memory_pressure": "normal" });
    call(&app, "PUT", "/v1/host/status", Some(idle)).await;
    let (st, _) = call(&app, "POST", "/v1/leases", Some(worker)).await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn solo_model_runs_after_mode_switch_and_output_needs_review() {
    let (state, app) = setup();
    let job = json!({
        "job_type": "extract.hard_page",
        "priority": "background",
        "resource": { "kind": "gpu", "model": "qwen3-vl-32b-8k" }
    });
    call(&app, "POST", "/v1/jobs", Some(job)).await;
    call(
        &app,
        "PUT",
        "/v1/host/status",
        Some(json!({ "user_idle_secs": 900, "memory_pressure": "normal" })),
    )
    .await;

    let gpu_worker = lease_req("qwen", json!(["gpu"]));
    let (st, _) = call(&app, "POST", "/v1/leases", Some(gpu_worker.clone())).await;
    assert_eq!(
        st,
        StatusCode::NO_CONTENT,
        "solo model is not allowed in normal mode"
    );

    api::tick(&state).await;
    let (_, status) = call(&app, "GET", "/v1/status", None).await;
    assert_eq!(
        status["mode"],
        json!({ "mode": "solo", "model": "qwen3-vl-32b-8k" })
    );

    let (st, lease) = call(&app, "POST", "/v1/leases", Some(gpu_worker)).await;
    assert_eq!(st, StatusCode::OK);
    let id = lease["job"]["id"].as_str().unwrap().to_string();
    let (_, done) = call(
        &app,
        "POST",
        &format!("/v1/jobs/{id}/complete"),
        Some(
            json!({ "worker_id": "qwen", "result": { "text": "draft" }, "review_required": false }),
        ),
    )
    .await;
    assert_eq!(
        done["review_required"], true,
        "model output is always a draft"
    );

    api::tick(&state).await;
    let (_, status) = call(&app, "GET", "/v1/status", None).await;
    assert_eq!(
        status["mode"],
        json!({ "mode": "normal" }),
        "drained: back to normal"
    );
}
