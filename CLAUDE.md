# CLAUDE.md

Rust (edition 2024) job orchestrator. See README.md and docs/.

## Checks (run before every push; CI runs the same)

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

## Rules

- **This repository is public** (Apache-2.0, part of a portfolio). No real
  paths, hostnames, usernames, tokens, drive names or job files. Examples
  only (`config.example.toml`).
- **License decides where code goes.** Anything compatible with Apache-2.0
  may live here, workers included. Code that depends on copyleft or otherwise
  incompatibly licensed software (e.g. PyMuPDF, AGPL), or that must not be
  published, goes in the private `orchestrator-units` repository and talks to
  this service only through the job API (`docs/job-api.md`).
- **No copyleft dependencies** (GPL/AGPL/LGPL) in this repository. Check the
  license of any new crate or package before adding it.
- **Model output is a draft.** Never add a path where a `gpu` job's result
  triggers an action automatically; `review_required` stays forced for gpu jobs.
- Keep `src/scheduler.rs` free of I/O so policy stays unit-testable.
- Job API changes are contract changes: update `docs/job-api.md` and keep v1
  backward compatible (add fields, don't rename or remove).
