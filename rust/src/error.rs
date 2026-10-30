//! Errors returned by the client and the module runtime.

use crate::layout::{
    ATTACH_STATUS_DENIED_NOSLOT, ATTACH_STATUS_DENIED_POLICY, ATTACH_STATUS_DENIED_VERSION,
    ATTACH_STATUS_OK, ATTACH_STATUS_PROTOCOL,
};
use std::fmt;
use std::io;

/// The host's answer to an attach (`ouro_attach_status_t`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachStatus {
    Ok,
    DeniedPolicy,
    DeniedVersion,
    DeniedNoSlot,
    Protocol,
    Unknown(u32),
}

impl AttachStatus {
    pub(crate) fn from_wire(v: u32) -> AttachStatus {
        match v {
            ATTACH_STATUS_OK => AttachStatus::Ok,
            ATTACH_STATUS_DENIED_POLICY => AttachStatus::DeniedPolicy,
            ATTACH_STATUS_DENIED_VERSION => AttachStatus::DeniedVersion,
            ATTACH_STATUS_DENIED_NOSLOT => AttachStatus::DeniedNoSlot,
            ATTACH_STATUS_PROTOCOL => AttachStatus::Protocol,
            other => AttachStatus::Unknown(other),
        }
    }
}

impl fmt::Display for AttachStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Distinct on purpose: each refusal calls for a different fix, and a
        // bare "denied" sends the reader to the grant file even when the
        // grant was never the problem.
        match self {
            AttachStatus::Ok => f.write_str("ok"),
            AttachStatus::DeniedPolicy => f.write_str(
                "denied by grant policy: no grant admits this principal, or the grant's \
                 executable= does not match the running binary",
            ),
            AttachStatus::DeniedVersion => f.write_str("denied on protocol version"),
            AttachStatus::DeniedNoSlot => f.write_str("denied: the host has no free slot"),
            AttachStatus::Protocol => f.write_str("protocol error"),
            AttachStatus::Unknown(v) => write!(f, "unknown attach status {v}"),
        }
    }
}

/// A ring client or module runtime failure.
#[derive(Debug)]
pub enum RingError {
    /// A system call failed.
    Io(io::Error),
    /// The host refused the attach.
    Denied(AttachStatus),
    /// The attach handshake was malformed.
    Protocol(&'static str),
    /// A mapped region failed validation.
    Region(&'static str),
    /// The outbound ring is full; retry after the host drains it.
    WouldBlock,
    /// The payload does not fit the inline budget.
    Payload,
    /// The host restarted; every mapping is stale and the client must re-attach.
    Epoch,
    /// The module process configuration is invalid.
    Config(&'static str),
    /// The host granted no doorbell (it predates D13); the caller keeps its
    /// own backoff.
    NoDoorbell,
}

impl fmt::Display for RingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RingError::Io(e) => write!(f, "bus: {e}"),
            RingError::Denied(s) => write!(f, "bus: attach {s}"),
            RingError::Protocol(m) => write!(f, "bus: attach protocol error: {m}"),
            RingError::Region(m) => write!(f, "bus: region invalid: {m}"),
            RingError::WouldBlock => f.write_str("bus: would block"),
            RingError::Payload => f.write_str("bus: payload too large for inline budget"),
            RingError::Epoch => f.write_str("bus: host restarted (epoch changed)"),
            RingError::Config(m) => write!(f, "bus: invalid module process configuration: {m}"),
            RingError::NoDoorbell => f.write_str("bus: host granted no doorbell"),
        }
    }
}

impl std::error::Error for RingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RingError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for RingError {
    fn from(e: io::Error) -> Self {
        RingError::Io(e)
    }
}
