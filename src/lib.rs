//! Local job orchestrator: a priority queue with idle-aware scheduling for
//! containerized agents and host-side workers sharing one local model server.

pub mod api;
pub mod config;
pub mod job;
pub mod models;
pub mod scheduler;
pub mod store;
