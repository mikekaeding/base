//! Ordered fallback sources for follow-mode reads (opt-in via `--follow.http-sources`).
//!
//! Every read goes to the first healthy source with a hard per-request timeout. A source that
//! times out or returns a transport error is skipped for a cooldown, so a hung primary costs one
//! timeout rather than one per request; it is still tried last while cooling down. A "block not
//! found" answer is not a health failure: the next source is asked, and if every source answers
//! that way the caller keeps its fast availability retry.

use std::{
    fmt::Debug,
    future::Future,
    sync::Mutex,
    time::{Duration, Instant},
};

use tracing::{debug, warn};

use crate::follow::source::RemoteL2ClientError;

/// How long a failed source is moved to the back of the order.
pub(super) const SOURCE_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Source<P> {
    label: String,
    client: P,
    cooling_until: Mutex<Option<Instant>>,
}

/// An ordered set of read sources with per-request timeouts and failure cooldown.
#[derive(Debug)]
pub(super) struct SourceSet<P> {
    sources: Vec<Source<P>>,
    timeout: Duration,
    cooldown: Duration,
}

impl<P: Clone + Debug + Send + Sync> SourceSet<P> {
    /// Creates a source set; `sources` are (label, client) in priority order.
    pub(super) fn new(sources: Vec<(String, P)>, timeout: Duration, cooldown: Duration) -> Self {
        Self {
            sources: sources
                .into_iter()
                .map(|(label, client)| Source { label, client, cooling_until: Mutex::new(None) })
                .collect(),
            timeout,
            cooldown,
        }
    }

    /// Source labels in configured order (hosts only; never full URLs).
    pub(super) fn labels(&self) -> Vec<String> {
        self.sources.iter().map(|s| s.label.clone()).collect()
    }

    /// Indices to try now: healthy sources in configured order, then cooling ones by earliest
    /// recovery. Every source is always tried once per call.
    pub(super) fn order(&self, now: Instant) -> Vec<usize> {
        let mut healthy = Vec::with_capacity(self.sources.len());
        let mut cooling = Vec::new();
        for (i, s) in self.sources.iter().enumerate() {
            match *s.cooling_until.lock().unwrap_or_else(|e| e.into_inner()) {
                Some(until) if until > now => cooling.push((until, i)),
                _ => healthy.push(i),
            }
        }
        cooling.sort();
        healthy.extend(cooling.into_iter().map(|(_, i)| i));
        healthy
    }

    fn set_cooling(&self, i: usize, until: Option<Instant>) {
        *self.sources[i].cooling_until.lock().unwrap_or_else(|e| e.into_inner()) = until;
    }

    /// Runs `op` against the sources in order until one answers.
    ///
    /// Returns the first success. If no source succeeds, returns `BlockNotFound` when any source
    /// answered "not found" (so the caller keeps its prompt availability retry), otherwise the
    /// last transport error or timeout.
    pub(super) async fn call<T, F, Fut>(
        &self,
        what: &'static str,
        op: F,
    ) -> Result<T, RemoteL2ClientError>
    where
        F: Fn(P) -> Fut,
        Fut: Future<Output = Result<T, RemoteL2ClientError>>,
    {
        let mut not_found = None;
        let mut last_error = None;
        for i in self.order(Instant::now()) {
            let source = &self.sources[i];
            let started = Instant::now();
            match tokio::time::timeout(self.timeout, op(source.client.clone())).await {
                Ok(Ok(value)) => {
                    self.set_cooling(i, None);
                    return Ok(value);
                }
                Ok(Err(e @ RemoteL2ClientError::BlockNotFound(_))) => {
                    debug!(target: "follow", source = %source.label, what, "Source has no such block yet");
                    not_found = Some(e);
                }
                Ok(Err(e)) => {
                    self.set_cooling(i, Some(Instant::now() + self.cooldown));
                    warn!(target: "follow", source = %source.label, what, error = %e,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        cooldown_ms = self.cooldown.as_millis() as u64, "Follow source failed; trying next");
                    last_error = Some(e);
                }
                Err(_) => {
                    self.set_cooling(i, Some(Instant::now() + self.cooldown));
                    warn!(target: "follow", source = %source.label, what,
                        timeout_ms = self.timeout.as_millis() as u64,
                        cooldown_ms = self.cooldown.as_millis() as u64, "Follow source timed out; trying next");
                    last_error = Some(RemoteL2ClientError::Timeout {
                        what,
                        endpoint: source.label.clone(),
                        timeout_ms: self.timeout.as_millis() as u64,
                    });
                }
            }
        }
        Err(not_found.or(last_error).unwrap_or(RemoteL2ClientError::NoSources))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use alloy_transport::TransportErrorKind;

    use super::*;

    #[derive(Clone, Debug)]
    enum Behavior {
        Ok(u64),
        NotFound,
        Fail,
        Hang,
    }

    #[derive(Clone, Debug)]
    struct Fake {
        behavior: Arc<Mutex<Behavior>>,
        calls: Arc<AtomicUsize>,
    }

    impl Fake {
        fn new(b: Behavior) -> Self {
            Self { behavior: Arc::new(Mutex::new(b)), calls: Arc::new(AtomicUsize::new(0)) }
        }
        fn set(&self, b: Behavior) {
            *self.behavior.lock().unwrap() = b;
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    async fn run(f: Fake) -> Result<u64, RemoteL2ClientError> {
        f.calls.fetch_add(1, Ordering::SeqCst);
        let b = f.behavior.lock().unwrap().clone();
        match b {
            Behavior::Ok(v) => Ok(v),
            Behavior::NotFound => Err(RemoteL2ClientError::BlockNotFound("7".into())),
            Behavior::Fail => Err(RemoteL2ClientError::FetchBlock {
                tag: "7".into(),
                source: TransportErrorKind::custom_str("fixture transport failure"),
            }),
            Behavior::Hang => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(0)
            }
        }
    }

    fn set(fakes: &[Fake], timeout: Duration) -> SourceSet<Fake> {
        SourceSet::new(
            fakes.iter().enumerate().map(|(i, f)| (format!("s{i}"), f.clone())).collect(),
            timeout,
            Duration::from_millis(300),
        )
    }

    #[tokio::test]
    async fn healthy_primary_is_always_first() {
        let (a, b, c) =
            (Fake::new(Behavior::Ok(1)), Fake::new(Behavior::Ok(2)), Fake::new(Behavior::Ok(3)));
        let s = set(&[a.clone(), b.clone(), c.clone()], Duration::from_millis(200));
        for _ in 0..5 {
            assert_eq!(s.call("t", run).await.unwrap(), 1);
        }
        assert_eq!((a.calls(), b.calls(), c.calls()), (5, 0, 0));
    }

    #[tokio::test]
    async fn failing_primary_falls_back_in_order_and_cools_down() {
        let (a, b, c) =
            (Fake::new(Behavior::Fail), Fake::new(Behavior::Ok(2)), Fake::new(Behavior::Ok(3)));
        let s = set(&[a.clone(), b.clone(), c.clone()], Duration::from_millis(200));
        assert_eq!(s.call("t", run).await.unwrap(), 2);
        assert_eq!(s.order(Instant::now()), vec![1, 2, 0], "failed primary moves to the back");
        // while cooling, the primary is not asked first
        assert_eq!(s.call("t", run).await.unwrap(), 2);
        assert_eq!(a.calls(), 1);
        // after the cooldown the primary is first again and, once healthy, used again
        a.set(Behavior::Ok(1));
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(s.order(Instant::now()), vec![0, 1, 2]);
        assert_eq!(s.call("t", run).await.unwrap(), 1);
        assert_eq!((a.calls(), b.calls(), c.calls()), (2, 2, 0));
    }

    #[tokio::test]
    async fn second_failure_uses_third_source() {
        let (a, b, c) =
            (Fake::new(Behavior::Fail), Fake::new(Behavior::Fail), Fake::new(Behavior::Ok(3)));
        let s = set(&[a.clone(), b.clone(), c.clone()], Duration::from_millis(200));
        assert_eq!(s.call("t", run).await.unwrap(), 3);
        assert_eq!(s.order(Instant::now())[0], 2);
    }

    #[tokio::test]
    async fn hung_primary_times_out_and_falls_back_within_the_timeout() {
        let (a, b) = (Fake::new(Behavior::Hang), Fake::new(Behavior::Ok(2)));
        let s = set(&[a.clone(), b.clone()], Duration::from_millis(150));
        let started = Instant::now();
        assert_eq!(s.call("t", run).await.unwrap(), 2);
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(150) && took < Duration::from_millis(400),
            "{took:?}"
        );
        // the next read does not wait on the hung primary again
        let started = Instant::now();
        assert_eq!(s.call("t", run).await.unwrap(), 2);
        assert!(started.elapsed() < Duration::from_millis(50));
        assert_eq!(a.calls(), 1);
    }

    #[tokio::test]
    async fn every_source_hung_returns_timeout_after_one_timeout_each() {
        let (a, b) = (Fake::new(Behavior::Hang), Fake::new(Behavior::Hang));
        let s = set(&[a, b], Duration::from_millis(100));
        let started = Instant::now();
        let err = s.call("payload", run).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::Timeout { what: "payload", .. }), "{err:?}");
        assert!(started.elapsed() < Duration::from_millis(350));
    }

    #[tokio::test]
    async fn not_found_is_not_a_health_failure_and_asks_the_next_source() {
        let (a, b) = (Fake::new(Behavior::NotFound), Fake::new(Behavior::Ok(2)));
        let s = set(&[a.clone(), b.clone()], Duration::from_millis(200));
        assert_eq!(s.call("t", run).await.unwrap(), 2);
        assert_eq!(s.order(Instant::now()), vec![0, 1], "not-found keeps the primary healthy");
    }

    #[tokio::test]
    async fn not_found_everywhere_is_reported_as_not_found() {
        let (a, b) = (Fake::new(Behavior::NotFound), Fake::new(Behavior::Fail));
        let s = set(&[a, b], Duration::from_millis(200));
        let err = s.call("t", run).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::BlockNotFound(_)), "{err:?}");
    }

    #[tokio::test]
    async fn all_failing_returns_last_transport_error() {
        let (a, b) = (Fake::new(Behavior::Fail), Fake::new(Behavior::Fail));
        let s = set(&[a, b], Duration::from_millis(200));
        let err = s.call("t", run).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::FetchBlock { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn cooling_sources_are_still_tried_as_a_last_resort() {
        let (a, b) = (Fake::new(Behavior::Fail), Fake::new(Behavior::Fail));
        let s = set(&[a.clone(), b.clone()], Duration::from_millis(200));
        let _ = s.call("t", run).await;
        a.set(Behavior::Ok(1));
        // both cooling; the primary (earliest recovery) is still tried and succeeds
        assert_eq!(s.call("t", run).await.unwrap(), 1);
        assert_eq!(s.order(Instant::now())[0], 0, "success clears the cooldown");
    }
}
