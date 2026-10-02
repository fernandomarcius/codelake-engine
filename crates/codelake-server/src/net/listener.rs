//! An accept loop that never stalls in silence (ADR 0018).
//!
//! - **Connection limit.** Each listener holds at most its share of the
//!   descriptors ([`super::limits::budget`]); past it, a new client gets the
//!   protocol's error at once (HTTP `503`, PostgreSQL `53300`) and the
//!   connection is closed.
//! - **No descriptor left** (`EMFILE`/`ENFILE`, when something else used the
//!   reserve). The listener keeps one spare descriptor: it frees it, accepts
//!   the waiting client with it and either answers the protocol's error and
//!   closes (query listeners) or serves that one request (the ops listener:
//!   probes keep answering). Then it opens the spare again.
//! - **Other `accept` errors** are logged (at most one line per
//!   [`LOG_EVERY`], with the count of those in between) and retried after a
//!   growing pause (5 ms to 1 s), never in a busy loop.
//!
//! Metrics: `codelake_connections_open{listener}`,
//! `codelake_connections_refused_total{listener,reason}` and
//! `codelake_accept_errors_total{listener,error}`.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tracing::{error, warn};

/// At most one log line per listener and kind of event in this period.
pub(crate) const LOG_EVERY: Duration = Duration::from_secs(10);
/// First pause after an `accept` error; doubles up to [`BACKOFF_MAX`].
const BACKOFF_MIN: Duration = Duration::from_millis(5);
const BACKOFF_MAX: Duration = Duration::from_secs(1);
/// A refused client's request is read (so closing does not reset the
/// connection under the answer) and the answer written within this time.
const REFUSAL_TIMEOUT: Duration = Duration::from_secs(2);
/// The same on the spare descriptor, which the next client waits for.
const SPARE_REFUSAL_TIMEOUT: Duration = Duration::from_millis(500);

/// Why a connection is answered with an error and closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The listener holds its maximum of connections.
    Limit(usize),
    /// The process has no descriptor left (`EMFILE`/`ENFILE`).
    Descriptors,
}

impl Refusal {
    fn label(self) -> &'static str {
        match self {
            Self::Limit(_) => "limit",
            Self::Descriptors => "descriptors",
        }
    }
}

/// What became of the client accepted on the spare descriptor.
enum OnSpare {
    Serve(Admitted),
    Refused,
    /// No spare, or no client waiting.
    Nothing,
}

/// What a listener does with the client it accepts on its spare descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OnExhausted {
    /// Answers the refusal and closes (query listeners: the client retries).
    Refuse,
    /// Serves it, one request, then closes (the ops listener: a probe gets
    /// its real answer).
    Serve,
}

/// The bytes a protocol answers a refused client with.
pub(crate) type Refuse = fn(&'static str, Refusal) -> Vec<u8>;

/// A connection a listener admitted; dropping it frees its slot.
#[derive(Debug)]
pub(crate) struct Admitted {
    pub(crate) stream: TcpStream,
    pub(crate) slot: Slot,
    /// Accepted on the spare descriptor: serve one request and close.
    pub(crate) last_resort: bool,
}

/// One open connection of a listener: counted in
/// `codelake_connections_open` while alive.
#[derive(Debug)]
pub(crate) struct Slot {
    listener: &'static str,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Slot {
    fn new(listener: &'static str, permit: Option<OwnedSemaphorePermit>) -> Self {
        metrics::gauge!("codelake_connections_open", "listener" => listener).increment(1.0);
        Self {
            listener,
            _permit: permit,
        }
    }

    pub(crate) fn listener(&self) -> &'static str {
        self.listener
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        metrics::gauge!("codelake_connections_open", "listener" => self.listener).decrement(1.0);
    }
}

/// A [`TcpListener`] with a connection limit, a spare descriptor and a
/// logged, paced retry of `accept` errors.
#[derive(Debug)]
pub(crate) struct GuardedListener {
    listener: TcpListener,
    name: &'static str,
    limit: usize,
    slots: Arc<Semaphore>,
    spare: Option<std::fs::File>,
    on_exhausted: OnExhausted,
    refuse: Refuse,
    errors: LogLimiter,
    refusals: LogLimiter,
    backoff: Backoff,
}

impl GuardedListener {
    pub(crate) fn new(
        listener: TcpListener,
        name: &'static str,
        limit: usize,
        on_exhausted: OnExhausted,
        refuse: Refuse,
    ) -> Self {
        metrics::gauge!("codelake_connections_open", "listener" => name).set(0.0);
        metrics::gauge!("codelake_connections_max", "listener" => name).set(gauge_value(limit));
        Self {
            listener,
            name,
            limit,
            slots: Arc::new(Semaphore::new(limit)),
            spare: open_spare(),
            on_exhausted,
            refuse,
            errors: LogLimiter::new(LOG_EVERY),
            refusals: LogLimiter::new(LOG_EVERY),
            backoff: Backoff::default(),
        }
    }

    /// The next connection to serve. Refused connections are answered and
    /// closed here; errors are logged and retried.
    pub(crate) async fn accept(&mut self) -> Admitted {
        loop {
            if self.spare.is_none() {
                self.spare = open_spare();
            }
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    self.backoff.reset();
                    match Arc::clone(&self.slots).try_acquire_owned() {
                        Ok(permit) => {
                            return Admitted {
                                stream,
                                slot: Slot::new(self.name, Some(permit)),
                                last_resort: false,
                            };
                        }
                        Err(_) => self.refuse_later(stream, Refusal::Limit(self.limit)),
                    }
                }
                // The client gave up before it was accepted: nothing to do.
                Err(err) if is_connection_error(&err) => {}
                Err(err) => {
                    let exhausted = is_descriptor_exhaustion(&err);
                    metrics::counter!(
                        "codelake_accept_errors_total",
                        "listener" => self.name,
                        "error" => if exhausted { "descriptors" } else { "other" }
                    )
                    .increment(1);
                    if let Some(suppressed) = self.errors.allow(Instant::now()) {
                        error!(
                            listener = self.name,
                            error = %err,
                            suppressed,
                            "cannot accept connections"
                        );
                    }
                    if exhausted {
                        match self.accept_on_spare().await {
                            OnSpare::Serve(admitted) => return admitted,
                            // A client is being answered: progress, go on
                            // with the next one.
                            OnSpare::Refused => {
                                self.backoff.reset();
                                continue;
                            }
                            OnSpare::Nothing => {}
                        }
                    }
                    tokio::time::sleep(self.backoff.next_pause()).await;
                }
            }
        }
    }

    /// Frees the spare descriptor and accepts the waiting client with it.
    async fn accept_on_spare(&mut self) -> OnSpare {
        let Some(spare) = self.spare.take() else {
            return OnSpare::Nothing;
        };
        drop(spare);
        let accepted = tokio::time::timeout(BACKOFF_MIN, self.listener.accept()).await;
        let Ok(Ok((stream, _))) = accepted else {
            return OnSpare::Nothing;
        };
        match self.on_exhausted {
            OnExhausted::Serve => OnSpare::Serve(Admitted {
                stream,
                slot: Slot::new(self.name, None),
                last_resort: true,
            }),
            OnExhausted::Refuse => {
                self.count_refusal(Refusal::Descriptors);
                let answer = (self.refuse)(self.name, Refusal::Descriptors);
                tokio::spawn(refuse(stream, answer, SPARE_REFUSAL_TIMEOUT));
                OnSpare::Refused
            }
        }
    }

    /// Answers a client over the limit and closes, in its own task: there
    /// are descriptors, only the listener's share is used.
    fn refuse_later(&mut self, stream: TcpStream, refusal: Refusal) {
        self.count_refusal(refusal);
        let answer = (self.refuse)(self.name, refusal);
        tokio::spawn(refuse(stream, answer, REFUSAL_TIMEOUT));
    }

    fn count_refusal(&mut self, refusal: Refusal) {
        metrics::counter!(
            "codelake_connections_refused_total",
            "listener" => self.name,
            "reason" => refusal.label()
        )
        .increment(1);
        if let Some(suppressed) = self.refusals.allow(Instant::now()) {
            warn!(
                listener = self.name,
                reason = refusal.label(),
                limit = self.limit,
                suppressed,
                "connection refused"
            );
        }
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "gauges are f64 by protocol; connection counts are far below 2^53"
)]
fn gauge_value(n: usize) -> f64 {
    n as f64
}

fn open_spare() -> Option<std::fs::File> {
    std::fs::File::open("/dev/null").ok()
}

/// Errors of one connection, not of the listener (as hyper and axum treat
/// them): the next `accept` is unaffected.
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset
    )
}

fn is_descriptor_exhaustion(err: &io::Error) -> bool {
    use rustix::io::Errno;
    matches!(Errno::from_io_error(err), Some(Errno::MFILE | Errno::NFILE))
}

/// Reads the client's first bytes, writes `answer`, closes the write side
/// and waits for the client to close, all `within`: closing with the
/// client's request unread would reset the connection, and the client could
/// lose the answer.
pub(crate) async fn refuse(mut stream: TcpStream, answer: Vec<u8>, within: Duration) {
    let _ = tokio::time::timeout(within, async {
        let mut buf = [0_u8; 4096];
        // The request (or startup packet) normally arrives with the
        // connection; do not wait long for it.
        let _ = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buf)).await;
        stream.write_all(&answer).await?;
        stream.shutdown().await?;
        while stream.read(&mut buf).await? > 0 {}
        Ok::<_, io::Error>(())
    })
    .await;
}

/// One log line per period; counts the events it suppressed in between.
#[derive(Debug)]
pub(crate) struct LogLimiter {
    every: Duration,
    last: Option<Instant>,
    suppressed: u64,
}

impl LogLimiter {
    pub(crate) fn new(every: Duration) -> Self {
        Self {
            every,
            last: None,
            suppressed: 0,
        }
    }

    /// `Some(suppressed since the last line)` when a line may be logged now.
    pub(crate) fn allow(&mut self, now: Instant) -> Option<u64> {
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < self.every)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Pause before retrying `accept`: doubles from [`BACKOFF_MIN`] to
/// [`BACKOFF_MAX`]; back to the minimum after a success.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    last: Option<Duration>,
}

impl Backoff {
    pub(crate) fn next_pause(&mut self) -> Duration {
        let pause = self.last.map_or(BACKOFF_MIN, |last| (last * 2).min(BACKOFF_MAX));
        self.last = Some(pause);
        pause
    }

    pub(crate) fn reset(&mut self) {
        self.last = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_logged_once_per_period_with_the_count_in_between() {
        let start = Instant::now();
        let mut limiter = LogLimiter::new(Duration::from_secs(10));
        assert_eq!(limiter.allow(start), Some(0));
        assert_eq!(limiter.allow(start + Duration::from_secs(1)), None);
        assert_eq!(limiter.allow(start + Duration::from_millis(9_999)), None);
        assert_eq!(limiter.allow(start + Duration::from_secs(10)), Some(2));
        assert_eq!(limiter.allow(start + Duration::from_secs(11)), None);
        assert_eq!(limiter.allow(start + Duration::from_secs(25)), Some(1));
    }

    #[test]
    fn backoff_doubles_to_a_second_and_resets() {
        let mut backoff = Backoff::default();
        let pauses: Vec<u128> = (0..10).map(|_| backoff.next_pause().as_millis()).collect();
        assert_eq!(pauses, [5, 10, 20, 40, 80, 160, 320, 640, 1000, 1000]);
        backoff.reset();
        assert_eq!(backoff.next_pause(), Duration::from_millis(5));
    }

    #[test]
    fn descriptor_exhaustion_is_told_apart() {
        let process = io::Error::from_raw_os_error(rustix::io::Errno::MFILE.raw_os_error());
        let system = io::Error::from_raw_os_error(rustix::io::Errno::NFILE.raw_os_error());
        let other = io::Error::from_raw_os_error(rustix::io::Errno::NOBUFS.raw_os_error());
        assert!(is_descriptor_exhaustion(&process));
        assert!(is_descriptor_exhaustion(&system));
        assert!(!is_descriptor_exhaustion(&other));
        assert!(!is_connection_error(&process));
        assert!(is_connection_error(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
        assert!(is_connection_error(&io::Error::from(
            io::ErrorKind::ConnectionReset
        )));
        assert!(is_connection_error(&io::Error::from(
            io::ErrorKind::ConnectionRefused
        )));
    }

    fn answer(_listener: &'static str, refusal: Refusal) -> Vec<u8> {
        format!("refused: {}\n", refusal.label()).into_bytes()
    }

    #[tokio::test]
    async fn over_the_limit_a_client_gets_the_answer_at_once() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let mut listener = GuardedListener::new(tcp, "test", 1, OnExhausted::Refuse, answer);
        let first = TcpStream::connect(addr).await.unwrap();
        let admitted = listener.accept().await;
        assert!(!admitted.last_resort);
        assert_eq!(admitted.slot.listener(), "test");

        let mut second = TcpStream::connect(addr).await.unwrap();
        second.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let accepting = tokio::spawn(async move {
            let next = listener.accept().await;
            (listener, next)
        });
        let mut got = String::new();
        tokio::time::timeout(Duration::from_secs(5), second.read_to_string(&mut got))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, "refused: limit\n");

        // The slot frees when the admitted connection ends.
        drop(admitted);
        drop(first);
        let _third = TcpStream::connect(addr).await.unwrap();
        let (_listener, next) = tokio::time::timeout(Duration::from_secs(5), accepting)
            .await
            .unwrap()
            .unwrap();
        assert!(!next.last_resort);
    }
}
