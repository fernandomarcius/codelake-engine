//! HTTP/1.1 connections of the ops and Trino protocol listeners, with the
//! timeouts `axum::serve` does not have (ADR 0018).
//!
//! A connection with no request in flight is closed:
//!
//! - after the **idle timeout** since its last response (keep-alive clients
//!   that pool connections and leak them; Trino closes them after
//!   `http-server.net.max-idle-time`, 200 s by default);
//! - after the **header-read timeout** when a request head is incomplete:
//!   counted from the opening of the connection for its first request (a
//!   client that connects and sends nothing) and from the first byte of the
//!   next ones (a client that sends its head slowly).
//!
//! A request in flight is never cut by them, however long its handler or its
//! response takes. `codelake_connections_closed_total{listener,reason}` counts
//! the closes.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use hyper::body::{Frame, Incoming, SizeHint};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tower_service::Service as _;
use tracing::debug;

use super::listener::{GuardedListener, Refusal, Slot};

/// The two timeouts of an HTTP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Timeouts {
    pub(crate) idle: Duration,
    pub(crate) header_read: Duration,
}

/// Serves `router` on `listener` until `shutdown`; then stops accepting,
/// lets every connection finish its request in flight and returns when all
/// are closed.
pub(crate) async fn serve(
    mut listener: GuardedListener,
    router: Router,
    timeouts: Timeouts,
    shutdown: CancellationToken,
) {
    let (closed_tx, closed_rx) = watch::channel(());
    loop {
        let admitted = tokio::select! {
            () = shutdown.cancelled() => break,
            admitted = listener.accept() => admitted,
        };
        let connection = Connection {
            router: router.clone(),
            timeouts,
            keep_alive: !admitted.last_resort,
            shutdown: shutdown.clone(),
            slot: admitted.slot,
            _closed: closed_rx.clone(),
        };
        tokio::spawn(connection.serve(admitted.stream));
    }
    drop(listener);
    drop(closed_rx);
    closed_tx.closed().await;
}

/// The answer of an HTTP listener to a refused connection.
pub(crate) fn refusal(listener: &'static str, why: Refusal) -> Vec<u8> {
    let body = match why {
        Refusal::Limit(limit) => format!(
            "CodeLake: the {listener} listener holds its maximum of {limit} open connections; retry later\n"
        ),
        Refusal::Descriptors => {
            "CodeLake: no file descriptor left for a new connection (open files limit); retry later\n"
                .to_owned()
        }
    };
    format!(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: text/plain; charset=utf-8\r\n\
         content-length: {}\r\nretry-after: 1\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

struct Connection {
    router: Router,
    timeouts: Timeouts,
    keep_alive: bool,
    shutdown: CancellationToken,
    slot: Slot,
    /// Dropped with the connection: [`serve`] waits for every one.
    _closed: watch::Receiver<()>,
}

impl Connection {
    async fn serve(self, stream: TcpStream) {
        let activity = Arc::new(Activity::new(Instant::now()));
        let io = TokioIo::new(Watched {
            inner: stream,
            activity: Arc::clone(&activity),
        });
        let router = self.router;
        let tracked = Arc::clone(&activity);
        let service = hyper::service::service_fn(move |request: hyper::Request<Incoming>| {
            let guard = InFlight::begin(&tracked);
            let response = router.clone().call(request.map(Body::new));
            async move {
                let response = response.await.unwrap_or_else(|never: Infallible| match never {});
                Ok::<_, Infallible>(response.map(|body| {
                    Body::new(Tracked {
                        inner: body,
                        _in_flight: guard,
                    })
                }))
            }
        });
        // No hyper timer: the timeouts below replace its header-read timer,
        // which would also cut the wait between requests.
        let mut builder = hyper::server::conn::http1::Builder::new();
        builder.keep_alive(self.keep_alive);
        let connection = builder.serve_connection(io, service);
        tokio::pin!(connection);
        let mut draining = false;
        loop {
            let changed = activity.changed.notified();
            let deadline = activity.deadline(self.timeouts);
            tokio::select! {
                result = connection.as_mut() => {
                    if let Err(err) = result {
                        debug!(listener = self.slot.listener(), error = %err, "HTTP connection ended with an error");
                    }
                    return;
                }
                () = self.shutdown.cancelled(), if !draining => {
                    connection.as_mut().graceful_shutdown();
                    draining = true;
                }
                () = sleep_until(deadline) => {
                    if let Some(reason) = activity.expired(self.timeouts, Instant::now()) {
                        metrics::counter!(
                            "codelake_connections_closed_total",
                            "listener" => self.slot.listener(),
                            "reason" => reason.label()
                        )
                        .increment(1);
                        debug!(listener = self.slot.listener(), reason = reason.label(), "closing an HTTP connection");
                        return;
                    }
                }
                () = changed => {}
            }
        }
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Why a connection was closed by a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expiry {
    Idle,
    HeaderRead,
}

impl Expiry {
    fn label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::HeaderRead => "header_read",
        }
    }
}

/// Where a connection stands between requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct State {
    /// Requests whose response has not been sent in full.
    in_flight: usize,
    /// When the last response ended (or the connection opened).
    idle_since: Instant,
    /// When the head now awaited started: the opening of the connection for
    /// the first request, its first byte for the next ones.
    head_started: Option<Instant>,
}

impl State {
    pub(crate) fn opened(now: Instant) -> Self {
        Self {
            in_flight: 0,
            idle_since: now,
            head_started: Some(now),
        }
    }

    /// Bytes arrived: with no request in flight they start the next head.
    pub(crate) fn read(&mut self, now: Instant) -> bool {
        if self.in_flight == 0 && self.head_started.is_none() {
            self.head_started = Some(now);
            return true;
        }
        false
    }

    /// A request head arrived in full.
    pub(crate) fn begin(&mut self) {
        self.in_flight += 1;
        self.head_started = None;
    }

    /// A response was sent in full (or abandoned).
    pub(crate) fn end(&mut self, now: Instant) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if self.in_flight == 0 {
            self.idle_since = now;
        }
    }

    /// When the connection is closed unless something happens first.
    pub(crate) fn deadline(&self, timeouts: Timeouts) -> Option<Instant> {
        if self.in_flight > 0 {
            return None;
        }
        Some(match self.head_started {
            Some(started) => started + timeouts.header_read,
            None => self.idle_since + timeouts.idle,
        })
    }

    pub(crate) fn expired(&self, timeouts: Timeouts, now: Instant) -> Option<Expiry> {
        let deadline = self.deadline(timeouts)?;
        (now >= deadline).then_some(if self.head_started.is_some() {
            Expiry::HeaderRead
        } else {
            Expiry::Idle
        })
    }
}

/// [`State`] shared by the connection's I/O, its requests and its timer.
#[derive(Debug)]
struct Activity {
    state: Mutex<State>,
    /// Woken when the deadline may have moved.
    changed: Notify,
}

impl Activity {
    fn new(now: Instant) -> Self {
        Self {
            state: Mutex::new(State::opened(now)),
            changed: Notify::new(),
        }
    }

    fn update<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        let result = change(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_one();
        result
    }

    fn deadline(&self, timeouts: Timeouts) -> Option<Instant> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .deadline(timeouts)
    }

    fn expired(&self, timeouts: Timeouts, now: Instant) -> Option<Expiry> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .expired(timeouts, now)
    }

    fn read(&self) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.read(now) {
            drop(state);
            self.changed.notify_one();
        }
    }
}

/// A request in flight, until its response body is sent or dropped.
#[derive(Debug)]
struct InFlight(Arc<Activity>);

impl InFlight {
    fn begin(activity: &Arc<Activity>) -> Self {
        activity.update(State::begin);
        Self(Arc::clone(activity))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.update(|state| state.end(Instant::now()));
    }
}

/// A response body that ends its request's [`InFlight`] when hyper is done
/// with it.
struct Tracked {
    inner: Body,
    _in_flight: InFlight,
}

impl http_body::Body for Tracked {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// The socket, telling [`Activity`] when bytes arrive.
struct Watched {
    inner: TcpStream,
    activity: Arc<Activity>,
}

impl AsyncRead for Watched {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.activity.read();
        }
        polled
    }
}

impl AsyncWrite for Watched {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::super::listener::OnExhausted;
    use super::*;
    use axum::routing::get;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    const TIMEOUTS: Timeouts = Timeouts {
        idle: Duration::from_secs(200),
        header_read: Duration::from_secs(30),
    };

    #[test]
    fn the_first_request_must_arrive_within_the_header_read_timeout() {
        let t0 = Instant::now();
        let state = State::opened(t0);
        assert_eq!(state.deadline(TIMEOUTS), Some(t0 + Duration::from_secs(30)));
        assert_eq!(state.expired(TIMEOUTS, t0 + Duration::from_millis(29_999)), None);
        assert_eq!(
            state.expired(TIMEOUTS, t0 + Duration::from_secs(30)),
            Some(Expiry::HeaderRead)
        );
    }

    #[test]
    fn a_request_in_flight_is_never_cut() {
        let t0 = Instant::now();
        let mut state = State::opened(t0);
        state.begin();
        assert_eq!(state.deadline(TIMEOUTS), None);
        assert_eq!(state.expired(TIMEOUTS, t0 + Duration::from_secs(3600)), None);
        // Pipelined: two in flight, one ends, the other keeps it open.
        state.begin();
        state.end(t0 + Duration::from_secs(1));
        assert_eq!(state.deadline(TIMEOUTS), None);
        // Bytes of the next request while one is in flight start nothing.
        assert!(!state.read(t0 + Duration::from_secs(2)));
        state.end(t0 + Duration::from_secs(5));
        assert_eq!(state.deadline(TIMEOUTS), Some(t0 + Duration::from_secs(205)));
    }

    #[test]
    fn between_requests_the_idle_timeout_applies_then_the_header_read_one() {
        let t0 = Instant::now();
        let mut state = State::opened(t0);
        state.begin();
        state.end(t0 + Duration::from_secs(1));
        let idle_end = t0 + Duration::from_secs(201);
        assert_eq!(state.deadline(TIMEOUTS), Some(idle_end));
        assert_eq!(state.expired(TIMEOUTS, idle_end - Duration::from_millis(1)), None);
        assert_eq!(state.expired(TIMEOUTS, idle_end), Some(Expiry::Idle));
        // The next head starts arriving late in the idle period: it gets the
        // whole header-read timeout from its first byte.
        let first_byte = t0 + Duration::from_secs(190);
        assert!(state.read(first_byte));
        assert!(
            !state.read(first_byte + Duration::from_secs(1)),
            "only the first byte counts"
        );
        assert_eq!(
            state.deadline(TIMEOUTS),
            Some(first_byte + Duration::from_secs(30))
        );
        assert_eq!(state.expired(TIMEOUTS, idle_end), None);
        assert_eq!(
            state.expired(TIMEOUTS, first_byte + Duration::from_secs(30)),
            Some(Expiry::HeaderRead)
        );
        state.begin();
        assert_eq!(state.deadline(TIMEOUTS), None);
    }

    #[test]
    fn refusals_are_complete_http_responses() {
        let answer = String::from_utf8(refusal("trino", Refusal::Limit(7))).unwrap();
        let (head, body) = answer.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 503 Service Unavailable\r\n"), "{head}");
        assert!(
            head.contains(&format!("content-length: {}", body.len())),
            "{head}"
        );
        assert!(head.contains("connection: close"), "{head}");
        assert!(body.contains("trino listener holds its maximum of 7"), "{body}");
        let answer = String::from_utf8(refusal("ops", Refusal::Descriptors)).unwrap();
        assert!(answer.contains("no file descriptor left"), "{answer}");
    }

    async fn start(timeouts: Timeouts, limit: usize) -> (std::net::SocketAddr, CancellationToken) {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let listener = GuardedListener::new(tcp, "test", limit, OnExhausted::Refuse, refusal);
        let router = Router::new().route("/ok", get(|| async { "ok" })).route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                "slow"
            }),
        );
        let shutdown = CancellationToken::new();
        tokio::spawn(serve(listener, router, timeouts, shutdown.clone()));
        (addr, shutdown)
    }

    const GET_OK: &[u8] = b"GET /ok HTTP/1.1\r\nhost: x\r\n\r\n";

    /// Reads one response (headers and the short body) from `stream`.
    async fn response(stream: &mut TcpStream) -> String {
        let mut buf = vec![0_u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    /// Seconds until the server closes `stream` (reads EOF), at most `max`.
    async fn closed_after(stream: &mut TcpStream, max: Duration) -> Option<Duration> {
        let start = Instant::now();
        let mut buf = [0_u8; 64];
        match tokio::time::timeout(max, stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => Some(start.elapsed()),
            Ok(Ok(n)) => panic!("unexpected bytes: {:?}", &buf[..n]),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn idle_and_silent_connections_are_closed_requests_in_flight_are_not() {
        let timeouts = Timeouts {
            idle: Duration::from_millis(800),
            header_read: Duration::from_millis(400),
        };
        let (addr, shutdown) = start(timeouts, 16).await;

        // Keep-alive: served, then closed after the idle timeout.
        let mut idle = TcpStream::connect(addr).await.unwrap();
        idle.write_all(GET_OK).await.unwrap();
        assert!(response(&mut idle).await.starts_with("HTTP/1.1 200 OK"));
        let after = closed_after(&mut idle, Duration::from_secs(5)).await.unwrap();
        assert!(after >= Duration::from_millis(700), "{after:?}");

        // Connected and silent: closed after the header-read timeout.
        let mut silent = TcpStream::connect(addr).await.unwrap();
        let after = closed_after(&mut silent, Duration::from_secs(5)).await.unwrap();
        assert!(
            after >= Duration::from_millis(300) && after < Duration::from_millis(800),
            "{after:?}"
        );

        // A head that never ends: closed after the header-read timeout.
        let mut partial = TcpStream::connect(addr).await.unwrap();
        partial.write_all(b"GET /ok HTTP/1.1\r\n").await.unwrap();
        assert!(closed_after(&mut partial, Duration::from_secs(5)).await.is_some());

        // A handler slower than both timeouts still answers.
        let mut slow = TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        let answer = response(&mut slow).await;
        assert!(
            answer.starts_with("HTTP/1.1 200 OK") && answer.ends_with("slow"),
            "{answer}"
        );
        shutdown.cancel();
    }

    #[tokio::test]
    async fn over_the_limit_new_clients_get_503_at_once_and_service_resumes() {
        let (addr, shutdown) = start(TIMEOUTS, 2).await;
        let mut held = Vec::new();
        for _ in 0..2 {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(GET_OK).await.unwrap();
            assert!(response(&mut stream).await.starts_with("HTTP/1.1 200 OK"));
            held.push(stream);
        }
        let started = Instant::now();
        let mut refused = TcpStream::connect(addr).await.unwrap();
        refused.write_all(GET_OK).await.unwrap();
        let answer = response(&mut refused).await;
        assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );

        drop(held);
        // The server notices the closes; a new client is served again.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(GET_OK).await.unwrap();
            if response(&mut stream).await.starts_with("HTTP/1.1 200 OK") {
                break;
            }
            assert!(Instant::now() < deadline, "service did not resume");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        shutdown.cancel();
    }

    #[tokio::test]
    async fn shutdown_waits_for_the_request_in_flight() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let listener = GuardedListener::new(tcp, "test", 4, OnExhausted::Refuse, refusal);
        let router = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                "slow"
            }),
        );
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(serve(listener, router, TIMEOUTS, shutdown.clone()));
        let mut slow = TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let answer = response(&mut slow).await;
        assert!(answer.ends_with("slow"), "{answer}");
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
