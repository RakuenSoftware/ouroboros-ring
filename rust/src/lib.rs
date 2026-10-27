//! Rust reference client for the Aimee ouroboros ring, and a
//! module-process runtime on top of it.
//!
//! This is the third independent client of the single in-source C host,
//! beside the C client (`c/src/client.c`) and the pure-Go
//! client (`go/ring`). It links, imports and generates nothing from
//! either: the frame codec is held to the committed wire vectors, and the
//! region layouts follow the C public headers that freeze them.
//!
//! Linux only, as the ring is (`memfd` regions passed with `SCM_RIGHTS`).

#![cfg(target_os = "linux")]

pub mod client;
pub mod error;
mod layout;
pub mod module_adapter;
pub mod module_caller;
pub mod module_protocol;
pub mod module_runtime;
mod region;
mod ring;
mod sys;
pub mod wire;

pub use client::{Attachment, Client, Event, KeepAlive};
pub use error::{AttachStatus, RingError};
pub use sys::monotonic_ns;
