# Job API v1

The contract between the orchestrator and everything else. Workers can be
written in any language; they only need HTTP and JSON.

All `/v1/*` endpoints require `Authorization: Bearer <token>` when the
orchestrator was started with a token (always, unless it is bound to
loopback). `GET /healthz` is unauthenticated.

Errors are `{"error": "<message>"}` with a meaningful status code.

## Concepts

| Field | Values | Meaning |
|---|---|---|
| `job_type` | any string, e.g. `extract.pdf_text` | Opaque to the orchestrator; workers declare which types they handle |
| `priority` | `interactive` > `normal` > `background` | `background` runs only while the host is idle with normal memory pressure |
| `resource` | `{"kind":"cpu"}`, `{"kind":"neural_engine"}`, `{"kind":"gpu","model":"<ollama model>"}` | What the job occupies; concurrency is limited per resource |
| `state` | `queued` → `leased` → `succeeded` / `failed` / `cancelled` | |
| `review_required` | bool | The result is a draft for a human. Always `true` for `gpu` jobs |

Workers **pull** work: they ask for a lease, run the job, heartbeat while it
runs, then complete or fail it. No inbound connections to workers are needed,
so a worker can live in a container, on the host, or on another machine.

## Submitting and inspecting jobs

### `POST /v1/jobs`

```json
{
  "job_type": "extract.pdf_text",
  "priority": "normal",
  "resource": { "kind": "cpu" },
  "payload": { "sha256": "…", "path": "…" },
  "idempotency_key": "extract.pdf_text:<sha256>",
  "max_attempts": 3
}
```

`priority` defaults to `normal`, `max_attempts` to 3. Returns the job with
`201 Created`, or `200 OK` with the *existing* job when `idempotency_key`
was seen before — submitting the same work twice is safe.

### `GET /v1/jobs/{id}` · `GET /v1/jobs?state=&job_type=&limit=`

Returns a job, or the newest jobs matching the filters (limit ≤ 1000,
default 100).

### `POST /v1/jobs/{id}/cancel`

Cancels a queued or leased job (`409` otherwise). A worker holding the lease
learns about it on its next heartbeat or completion (`409`).

## Worker protocol

### `POST /v1/leases`

```json
{ "worker_id": "extract-cpu-1", "job_types": ["extract.pdf_text"], "resources": ["cpu"] }
```

`job_types: []` (or omitted) means any type. Returns `204 No Content` when
nothing is runnable for this worker right now — back off (e.g. 2–10 s) and
ask again. Otherwise `200`:

```json
{ "job": { "id": "…", "payload": { … }, "attempts": 1, … }, "lease_expires_at": "…" }
```

A job is only handed out when the scheduler admits it: priority rules, the
per-resource concurrency limits (smaller while the user is active), and — for
`gpu` jobs — the current model mode.

### `POST /v1/jobs/{id}/heartbeat`

`{"worker_id": "…"}` → `{"lease_expires_at": "…"}`. Call well inside the
lease TTL (default 120 s; every 30 s is fine). `409` means the lease is gone
(expired, cancelled, or re-leased): stop work and discard the result.

### `POST /v1/jobs/{id}/complete`

```json
{ "worker_id": "…", "result": { … }, "review_required": false }
```

`409` if this worker no longer holds the lease. For `gpu` jobs the stored
`review_required` is forced to `true`.

### `POST /v1/jobs/{id}/fail`

```json
{ "worker_id": "…", "error": "…", "retryable": true }
```

Retryable failures go back to the queue until `max_attempts` is used up;
otherwise the job becomes `failed`. Expired leases are treated as retryable
failures with error `lease expired`.

**Workers must make jobs safe to run more than once** (a lease can expire
while work is still in progress). Write outputs keyed by the job's input
(e.g. a content hash), never append blindly.

## Host status

### `PUT /v1/host/status`

```json
{ "user_idle_secs": 420, "memory_pressure": "normal" }
```

`memory_pressure`: `normal` | `warn` | `critical`. Sent every ~15 s by
`host-agent/orch-host-agent.zsh`. If no report arrives within
`host_status_stale_secs`, the orchestrator assumes the user is active and
memory is tight: background work pauses rather than guessing.

### `GET /v1/status`

Current model mode, warm model, host view, running counts per resource, and
job counts by state/priority/resource.

## Model modes (gpu jobs)

`gpu` jobs name an Ollama model. Models listed in `[models].solo` need the
whole GPU budget; every other model is *resident*.

* **normal** — only resident models may run.
* **solo** `{model}` — only that model may run; everything else is unloaded.

The orchestrator switches modes itself, only when no gpu job is running:

* normal → solo when the best queued solo-model job outranks every queued
  resident job, and it is `interactive` or the host is idle with normal
  memory pressure;
* solo → normal when that model's queue is empty, an `interactive` resident
  job is waiting, or the host is no longer idle (unless the solo work is
  itself `interactive`).

Workers call Ollama directly once they hold a lease; they should pass the
model name exactly as submitted.
