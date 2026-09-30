# CLAUDE.md

Rust (edition 2024) job orchestrator. See README.md and docs/.

## Checks (run before every push; CI runs the same)

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

## Rules

- **This repository is public.** No real paths, hostnames, usernames, tokens,
  drive names or job files. Examples only (`config.example.toml`).
- **Domain-agnostic.** No domain-specific job types, adapters or business
  logic here; those live in private worker repositories and talk to this
  service only through the job API (`docs/job-api.md`).
- **No copyleft dependencies** (GPL/AGPL/LGPL). Check the license of any new
  crate before adding it.
- **Model output is a draft.** Never add a path where a `gpu` job's result
  triggers an action automatically; `review_required` stays forced for gpu jobs.
- Keep `src/scheduler.rs` free of I/O so policy stays unit-testable.
- Job API changes are contract changes: update `docs/job-api.md` and keep v1
  backward compatible (add fields, don't rename or remove).
