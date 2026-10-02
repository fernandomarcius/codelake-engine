//! Client and ops listeners: the open-files limit, connection limits, HTTP
//! timeouts and an accept loop that never stalls in silence (ADR 0018).

pub(crate) mod http;
pub(crate) mod limits;
pub(crate) mod listener;
pub(crate) mod pg;
