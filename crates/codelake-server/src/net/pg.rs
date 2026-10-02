//! The PostgreSQL wire listener's accept loop (ADR 0018), in place of
//! `datafusion_postgres::serve_with_handlers`, whose loop retried a failed
//! `accept` at once (a busy loop while descriptors were exhausted) and
//! closed a client over `max_connections` without a word.
//!
//! Sessions follow PostgreSQL's defaults: no idle-session timeout
//! (`idle_session_timeout = 0`); their number is bounded by
//! `server.max_connections`, which the open-files budget accounts for, and
//! TCP keepalive is on so the sessions of vanished clients end. A client
//! that never completes its startup is dropped by pgwire after 60 s.

use std::io;
use std::path::Path;
use std::sync::Arc;

use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::tokio::process_socket;
use datafusion_postgres::pgwire::tokio::tokio_rustls::TlsAcceptor;
use datafusion_postgres::pgwire::tokio::tokio_rustls::rustls;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::debug;

use super::listener::{GuardedListener, Refusal};

/// Accepts PostgreSQL clients on `listener` forever, each session in its own
/// task.
pub(crate) async fn serve<H>(mut listener: GuardedListener, handlers: Arc<H>, tls: Option<TlsAcceptor>)
where
    H: PgWireServerHandlers + Send + Sync + 'static,
{
    loop {
        let admitted = listener.accept().await;
        let handlers = Arc::clone(&handlers);
        let tls = tls.clone();
        tokio::spawn(async move {
            // As PostgreSQL does on every TCP connection, with the system's
            // probe times.
            if let Err(err) = rustix::net::sockopt::set_socket_keepalive(&admitted.stream, true) {
                debug!(error = %err, "cannot enable TCP keepalive");
            }
            if let Err(err) = process_socket(admitted.stream, tls, handlers).await {
                debug!(error = %err, "PostgreSQL session ended with an error");
            }
            drop(admitted.slot);
        });
    }
}

/// The TLS acceptor of `server.tls`. Any error stops startup: never a
/// silent fallback to plaintext.
pub(crate) fn tls_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    let invalid = |what: &Path, err: &dyn std::fmt::Display| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{}: {err}", what.display()))
    };
    let certs = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| invalid(cert_path, &e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(cert_path, &e))?;
    let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| invalid(key_path, &e))?;
    // Several crates may install it; the first one wins, all are the same.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| invalid(cert_path, &e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A PostgreSQL `ErrorResponse` (severity `FATAL`) for a refused client,
/// which libpq and JDBC report as the connection error.
pub(crate) fn refusal(_listener: &'static str, why: Refusal) -> Vec<u8> {
    let (code, message) = match why {
        // PostgreSQL's own words for its connection limit.
        Refusal::Limit(limit) => (
            "53300",
            format!("sorry, too many clients already (server.max_connections = {limit})"),
        ),
        Refusal::Descriptors => (
            "53000",
            "no file descriptor left for a new connection (open files limit); retry later".to_owned(),
        ),
    };
    let mut fields = Vec::new();
    for (tag, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', code),
        (b'M', message.as_str()),
    ] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    let length = u32::try_from(fields.len() + 4).unwrap_or(u32::MAX);
    let mut message = vec![b'E'];
    message.extend_from_slice(&length.to_be_bytes());
    message.extend_from_slice(&fields);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_is_a_fatal_error_response() {
        let bytes = refusal("pg", Refusal::Limit(256));
        assert_eq!(bytes[0], b'E');
        let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
        assert_eq!(length, bytes.len() - 1);
        assert_eq!(bytes.last(), Some(&0));
        let text = String::from_utf8_lossy(&bytes[5..]);
        assert!(
            text.contains("SFATAL\0VFATAL\0C53300\0Msorry, too many clients already"),
            "{text}"
        );
        let text = String::from_utf8_lossy(&refusal("pg", Refusal::Descriptors)).into_owned();
        assert!(text.contains("C53000\0"), "{text}");
    }

    #[test]
    fn tls_files_that_are_not_pem_stop_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cert.pem");
        std::fs::write(&path, "not a certificate").unwrap();
        let Err(err) = tls_acceptor(&path, &path) else {
            panic!("not a certificate, accepted");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("cert.pem"), "{err}");
    }
}
