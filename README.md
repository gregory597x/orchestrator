# orchestrator

A small job orchestrator for one workstation that runs containerized agents
and host-side workers against a shared local model server (Ollama).

It decides **what may run now**:

- **Priorities** — `interactive` > `normal` > `background`. Background work
  runs only while you are away from the keyboard and memory pressure is
  normal; it pauses at the next job boundary when you come back.
- **Resources** — CPU, Apple Neural Engine and GPU are limited separately,
  with smaller limits while you are active.
- **Model modes** — resident models share the GPU; a large "solo" model gets
  it alone. The orchestrator unloads models and switches modes only between
  jobs, and groups work by model to avoid reloads.
- **Drafts, not actions** — anything a model produces is stored as
  `review_required`. Nothing is ever acted on automatically.

Workers in any language pull jobs over a small HTTP API
([docs/job-api.md](docs/job-api.md)). Design rationale is in
[docs/design.md](docs/design.md).

## Run it

Requires Rust (stable). Ollama runs natively on the host (containers on
macOS have no GPU access).

```sh
cargo test
ORCH_TOKEN=dev cargo run            # listens on 127.0.0.1:8780 by default
curl -s -H 'Authorization: Bearer dev' localhost:8780/v1/status
```

In a container:

```sh
cp config.example.toml config.toml
mkdir -p secrets && openssl rand -hex 32 > secrets/orch_token
docker compose -f compose.example.yaml up -d
```

Then install the host agent on the Mac so the orchestrator knows when you
are idle: see [`host-agent/`](host-agent/).

## Configuration

`ORCH_CONFIG` points at a TOML file (all keys optional); see
[`config.example.toml`](config.example.toml). The API token comes from
`ORCH_TOKEN` or `ORCH_TOKEN_FILE`. Without a token the server refuses to
bind to anything but loopback.

## Layout

| Path | What |
|---|---|
| `src/scheduler.rs` | Pure scheduling policy (admission, ordering, model modes) |
| `src/store.rs` | SQLite queue with guarded lease transitions |
| `src/api.rs` | HTTP API and the background tick (lease reaping, mode switches) |
| `src/models.rs` | Ollama control (unload on mode switch) |
| `host-agent/` | macOS LaunchAgent reporting idle time and memory pressure |

## License

Apache-2.0.
