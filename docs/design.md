# Design notes

Why the orchestrator looks the way it does. Target: a single Apple Silicon
workstation running containerized agents and a local model server, where
background work should soak up idle capacity without getting in the user's
way.

## Constraints that shaped it

1. **Containers on macOS cannot use the GPU.** Docker Desktop and OrbStack
   run containers in a Linux VM without Metal. A model server in a container
   runs CPU-only, several times slower. So Ollama runs natively on the host,
   and containers reach it at `host.docker.internal:11434`.
2. **Unified memory is the budget.** By default macOS lets the GPU wire
   roughly 75% of RAM. On a 36 GB machine a 32B vision model (~21 GB plus
   context) cannot share the GPU with a 16B coder model at 32k context
   (~18 GB with an unquantized KV cache). Hence *model modes*: resident
   models share the budget; a solo model gets it alone.
3. **The local model server has no priorities.** Ollama serves requests
   first-come-first-served. Priority therefore lives here: workers only call
   Ollama after the orchestrator leases them a `gpu` job, and at most
   `limits.*.gpu` (default 1) run at once.
4. **The Neural Engine is a separate resource.** Apple Vision OCR runs on the
   ANE, not the GPU, so it barely competes with models. It is scheduled as
   its own resource (`neural_engine`) with its own limits.

## Pieces

```
 macOS host                                   containers (VM)
 ┌──────────────────────────────┐            ┌─────────────────────────┐
 │ Ollama (Metal)          ◄────┼── gpu jobs ┤ agent / worker          │
 │ host workers (Vision OCR) ───┼─┐          │ containers              │
 │ host agent (idle, pressure) ─┼─┤          └───────────┬─────────────┘
 └──────────────────────────────┘ │  job API             │ job API
                                  ▼                      ▼
                         ┌──────────────────────────────────┐
                         │ orchestrator (this repo)         │
                         │  SQLite queue · scheduler ·      │
                         │  model-mode control (unloads)    │
                         └──────────────────────────────────┘
```

* **Queue** (`src/store.rs`): SQLite in WAL mode. Every transition is a
  guarded `UPDATE … WHERE state = … AND worker_id = …`, so a worker whose
  lease expired can never overwrite a newer owner's result.
* **Scheduler** (`src/scheduler.rs`): pure functions, no I/O, fully unit
  tested. `admits` (may this job start now?), `pick` (which job next), and
  `next_mode` (which model mode).
* **Model control** (`src/models.rs`): on a mode switch, unloads models via
  Ollama's API (`keep_alive: 0`). Loading is left to the first request.
* **Host agent** (`host-agent/`): a LaunchAgent that reports keyboard/mouse
  idle time (`HIDIdleTime`) and `kern.memorystatus_vm_pressure_level`.

## Policies

* **Fail safe.** No or stale host status ⇒ treat the user as active and
  memory as tight. Background work waits rather than guesses.
* **Switch at job boundaries.** Model modes never change while a gpu job is
  running, and the switch happens under the same lock as leasing, so no
  lease can slip in under the old mode.
* **Batch by model.** Within a priority, jobs for the model that is already
  warm go first to avoid reload costs (10–30 s for large models).
* **Model output is a draft.** Results of `gpu` jobs are stored with
  `review_required = true` regardless of what the worker says. The
  orchestrator never acts on results; downstream tools must treat them as
  suggestions for a human.
* **Deterministic work stays deterministic.** Hashing, parsing, text
  extraction from born-digital files, and verification belong in ordinary
  code (`cpu` jobs), not in a model.

## What lives elsewhere

This repository is public and domain-agnostic. Domain workers (document
extraction, other units) live in separate private repositories and talk to
the orchestrator only through the [job API](job-api.md). In particular,
copyleft-licensed libraries (e.g. AGPL PDF tooling) stay in those workers
and are never linked into this codebase.

## Not yet built

* Preemption of long-running background work beyond "stop at the next job
  boundary" (workers can checkpoint and fail with `retryable: true`).
* Raising the GPU wired-memory limit around solo mode
  (`sysctl iogpu.wired_limit_mb`, needs root on the host).
* Metrics endpoint and a small status UI.
* Candidate selection in SQL rather than in memory (fine up to a few
  thousand queued jobs).
