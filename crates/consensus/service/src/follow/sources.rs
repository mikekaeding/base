//! Ordered fallback sources for follow-mode reads (opt-in via `--follow.http-sources`).
//!
//! Every read goes to the first healthy source with a hard per-request timeout. A source that
//! times out or returns a transport error is moved to the back for a cooldown that doubles with
//! each consecutive failure (5 s, 10 s, ... capped at 60 s) and resets on its next success, so a
//! hanging source costs one timeout per cooldown rather than one per request. It is still tried
//! last while cooling, so a total outage of the others still reaches it.
//!
//! Block payload reads ([`SourceSet::call_block`]) treat a clean "not found" from the first source
//! as the usual announced-but-not-yet-served race: the caller re-polls that source on its own
//! 10-200 ms schedule, exactly as the single-source follower does. Only after the block has been
//! missing there for [`NOT_FOUND_GRACE`] are the other sources asked, so a lagging primary costs at
//! most the grace, while the common race costs no extra round trips to slower sources.

use std::{
    fmt::Debug,
    future::Future,
    sync::Mutex,
    time::{Duration, Instant},
};

use tracing::{debug, warn};

use crate::follow::source::RemoteL2ClientError;

/// First cooldown after a failure; doubles with each consecutive failure.
pub(super) const SOURCE_COOLDOWN: Duration = Duration::from_secs(5);
/// Longest cooldown.
pub(super) const SOURCE_COOLDOWN_MAX: Duration = Duration::from_secs(60);
/// How long the first source may answer "not found" for one block before others are asked.
pub(super) const NOT_FOUND_GRACE: Duration = Duration::from_millis(400);
/// Consecutive blocks the first source had to be bypassed after the grace before it is treated as
/// lagging and moved to the back like a failed source.
pub(super) const LAG_STRIKES: u32 = 2;

/// Timing policy of a [`SourceSet`].
#[derive(Debug, Clone, Copy)]
pub(super) struct SourcePolicy {
    /// Hard timeout per request.
    pub(super) timeout: Duration,
    /// First cooldown after a failure.
    pub(super) cooldown: Duration,
    /// Cooldown cap.
    pub(super) cooldown_max: Duration,
    /// Block-not-found grace on the first source (see [`SourceSet::call_block`]).
    pub(super) not_found_grace: Duration,
}

impl SourcePolicy {
    /// Production policy with the given request timeout.
    pub(super) const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            cooldown: SOURCE_COOLDOWN,
            cooldown_max: SOURCE_COOLDOWN_MAX,
            not_found_grace: NOT_FOUND_GRACE,
        }
    }

    /// Cooldown after `failures` consecutive failures (`failures >= 1`).
    pub(super) fn cooldown_after(&self, failures: u32) -> Duration {
        let factor = 1u32.checked_shl(failures.saturating_sub(1)).unwrap_or(u32::MAX);
        self.cooldown.saturating_mul(factor).min(self.cooldown_max.max(self.cooldown))
    }
}

#[derive(Debug, Default)]
struct Health {
    failures: u32,
    cooling_until: Option<Instant>,
}

#[derive(Debug)]
struct Source<P> {
    label: String,
    client: P,
    health: Mutex<Health>,
}

/// An ordered set of read sources with per-request timeouts and failure cooldown.
#[derive(Debug)]
pub(super) struct SourceSet<P> {
    sources: Vec<Source<P>>,
    policy: SourcePolicy,
    /// (block, first "not found" from the first source) for the block currently being fetched.
    missing: Mutex<Option<(u64, Instant)>>,
    /// (source, consecutive blocks it was bypassed after the grace).
    lag_strikes: Mutex<(usize, u32)>,
}

enum Outcome<T> {
    Value(T),
    NotFound(RemoteL2ClientError),
    Failed(RemoteL2ClientError),
}

impl<P: Clone + Debug + Send + Sync> SourceSet<P> {
    /// Creates a source set; `sources` are (label, client) in priority order.
    pub(super) fn new(sources: Vec<(String, P)>, policy: SourcePolicy) -> Self {
        Self {
            sources: sources
                .into_iter()
                .map(|(label, client)| Source {
                    label,
                    client,
                    health: Mutex::new(Health::default()),
                })
                .collect(),
            policy,
            missing: Mutex::new(None),
            lag_strikes: Mutex::new((usize::MAX, 0)),
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
            match s.health.lock().unwrap_or_else(|e| e.into_inner()).cooling_until {
                Some(until) if until > now => cooling.push((until, i)),
                _ => healthy.push(i),
            }
        }
        cooling.sort();
        healthy.extend(cooling.into_iter().map(|(_, i)| i));
        healthy
    }

    fn mark_ok(&self, i: usize) {
        *self.sources[i].health.lock().unwrap_or_else(|e| e.into_inner()) = Health::default();
    }

    /// Records a failure; returns the cooldown applied.
    fn mark_failed(&self, i: usize) -> Duration {
        let mut h = self.sources[i].health.lock().unwrap_or_else(|e| e.into_inner());
        h.failures = h.failures.saturating_add(1);
        let cooldown = self.policy.cooldown_after(h.failures);
        h.cooling_until = Some(Instant::now() + cooldown);
        cooldown
    }

    /// Consecutive failures of source `i` (for tests and logs).
    #[cfg(test)]
    pub(super) fn failures(&self, i: usize) -> u32 {
        self.sources[i].health.lock().unwrap_or_else(|e| e.into_inner()).failures
    }

    /// Tracks a first source that keeps lagging: `served_by` answered at position `pos`;
    /// `bypassed` is the first source if it was skipped after the grace for this block.
    ///
    /// A demoted source gets the first cooldown step (5 s) each time: a lagging source still
    /// answers its other reads successfully, and any success resets its failure count, so the
    /// doubling applies to errors and timeouts, not to lag. While it keeps lagging it is demoted
    /// again after two more blocks, so it costs at most the grace twice per 5 s.
    fn note_lag(&self, served_by: usize, pos: usize, bypassed: Option<usize>) {
        let mut strikes = self.lag_strikes.lock().unwrap_or_else(|e| e.into_inner());
        match bypassed {
            Some(first) if served_by != first => {
                let n = if strikes.0 == first { strikes.1 + 1 } else { 1 };
                *strikes = (first, n);
                if n >= LAG_STRIKES {
                    *strikes = (usize::MAX, 0);
                    drop(strikes);
                    let cooldown = self.mark_failed(first);
                    warn!(target: "follow", source = %self.sources[first].label, blocks = n,
                        cooldown_ms = cooldown.as_millis() as u64,
                        "Follow source keeps lagging behind the others; moving it back");
                }
            }
            _ if pos == 0 => *strikes = (usize::MAX, 0),
            _ => {}
        }
    }

    async fn try_one<T, F, Fut>(&self, i: usize, what: &'static str, op: &F) -> Outcome<T>
    where
        F: Fn(P) -> Fut,
        Fut: Future<Output = Result<T, RemoteL2ClientError>>,
    {
        let source = &self.sources[i];
        let started = Instant::now();
        match tokio::time::timeout(self.policy.timeout, op(source.client.clone())).await {
            Ok(Ok(value)) => {
                self.mark_ok(i);
                Outcome::Value(value)
            }
            Ok(Err(e @ RemoteL2ClientError::BlockNotFound(_))) => {
                debug!(target: "follow", source = %source.label, what, "Source has no such block yet");
                Outcome::NotFound(e)
            }
            Ok(Err(e)) => {
                let cooldown = self.mark_failed(i);
                warn!(target: "follow", source = %source.label, what, error = %e,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    cooldown_ms = cooldown.as_millis() as u64, "Follow source failed; trying next");
                Outcome::Failed(e)
            }
            Err(_) => {
                let cooldown = self.mark_failed(i);
                warn!(target: "follow", source = %source.label, what,
                    timeout_ms = self.policy.timeout.as_millis() as u64,
                    cooldown_ms = cooldown.as_millis() as u64, "Follow source timed out; trying next");
                Outcome::Failed(RemoteL2ClientError::Timeout {
                    what,
                    endpoint: source.label.clone(),
                    timeout_ms: self.policy.timeout.as_millis() as u64,
                })
            }
        }
    }

    /// Runs `op` against the sources in order until one answers (heights and labels).
    ///
    /// Returns the first success. If no source succeeds, returns `BlockNotFound` when any source
    /// answered "not found", otherwise the last transport error or timeout.
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
            match self.try_one(i, what, &op).await {
                Outcome::Value(v) => return Ok(v),
                Outcome::NotFound(e) => not_found = Some(e),
                Outcome::Failed(e) => last_error = Some(e),
            }
        }
        Err(not_found.or(last_error).unwrap_or(RemoteL2ClientError::NoSources))
    }

    /// Reads block `number` (payload fetches).
    ///
    /// Errors and timeouts move on to the next source at once. A clean "not found" from the first
    /// source in the current order is returned to the caller immediately (it re-polls on its own
    /// short schedule) until `number` has been missing there for the not-found grace; after that
    /// the remaining sources are asked in order as well.
    pub(super) async fn call_block<T, F, Fut>(
        &self,
        what: &'static str,
        number: u64,
        op: F,
    ) -> Result<T, RemoteL2ClientError>
    where
        F: Fn(P) -> Fut,
        Fut: Future<Output = Result<T, RemoteL2ClientError>>,
    {
        let mut not_found = None;
        let mut last_error = None;
        let mut bypassed_first = None;
        for (pos, i) in self.order(Instant::now()).into_iter().enumerate() {
            match self.try_one(i, what, &op).await {
                Outcome::Value(v) => {
                    {
                        let mut m = self.missing.lock().unwrap_or_else(|e| e.into_inner());
                        if matches!(*m, Some((n, _)) if n <= number) {
                            *m = None;
                        }
                    }
                    self.note_lag(i, pos, bypassed_first);
                    return Ok(v);
                }
                Outcome::NotFound(e) => {
                    if pos == 0 {
                        let first = {
                            let mut m = self.missing.lock().unwrap_or_else(|e| e.into_inner());
                            match *m {
                                Some((n, t)) if n == number => t,
                                _ => {
                                    let now = Instant::now();
                                    *m = Some((number, now));
                                    now
                                }
                            }
                        };
                        if first.elapsed() < self.policy.not_found_grace {
                            return Err(e);
                        }
                        debug!(target: "follow", block = number, missing_ms = first.elapsed().as_millis() as u64,
                            "First source still lacks the block after the grace; asking the others");
                        bypassed_first = Some(i);
                    }
                    not_found = Some(e);
                }
                Outcome::Failed(e) => last_error = Some(e),
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

    fn policy(timeout: Duration) -> SourcePolicy {
        SourcePolicy {
            timeout,
            cooldown: Duration::from_millis(300),
            cooldown_max: Duration::from_millis(1200),
            not_found_grace: Duration::from_millis(150),
        }
    }

    fn set(fakes: &[Fake], timeout: Duration) -> SourceSet<Fake> {
        SourceSet::new(
            fakes.iter().enumerate().map(|(i, f)| (format!("s{i}"), f.clone())).collect(),
            policy(timeout),
        )
    }

    /// A source that serves blocks up to `height` and answers "not found" above it.
    #[derive(Clone, Debug)]
    struct Chain {
        height: Arc<std::sync::atomic::AtomicU64>,
        calls: Arc<AtomicUsize>,
    }

    impl Chain {
        fn at(h: u64) -> Self {
            Self { height: Arc::new(h.into()), calls: Arc::new(AtomicUsize::new(0)) }
        }
        fn to(&self, h: u64) {
            self.height.store(h, Ordering::SeqCst);
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    fn block_set(chains: &[Chain]) -> SourceSet<Chain> {
        SourceSet::new(
            chains.iter().enumerate().map(|(i, c)| (format!("c{i}"), c.clone())).collect(),
            policy(Duration::from_millis(200)),
        )
    }

    async fn fetch(set: &SourceSet<Chain>, n: u64) -> Result<(usize, u64), RemoteL2ClientError> {
        set.call_block("payload", n, |c: Chain| async move {
            c.calls.fetch_add(1, Ordering::SeqCst);
            if n <= c.height.load(Ordering::SeqCst) {
                Ok((c.calls.load(Ordering::SeqCst), n))
            } else {
                Err(RemoteL2ClientError::BlockNotFound(n.to_string()))
            }
        })
        .await
    }

    #[tokio::test]
    async fn not_found_on_the_primary_within_the_grace_does_not_ask_slower_sources() {
        let (p, s2) = (Chain::at(6), Chain::at(7));
        let set = block_set(&[p.clone(), s2.clone()]);
        for _ in 0..3 {
            assert!(matches!(fetch(&set, 7).await, Err(RemoteL2ClientError::BlockNotFound(_))));
        }
        assert_eq!((p.calls(), s2.calls()), (3, 0), "the race is re-polled on the primary only");
    }

    #[tokio::test]
    async fn primary_catching_up_within_the_grace_serves_the_block() {
        let (p, s2) = (Chain::at(6), Chain::at(7));
        let set = block_set(&[p.clone(), s2.clone()]);
        assert!(fetch(&set, 7).await.is_err());
        p.to(7);
        assert!(fetch(&set, 7).await.is_ok());
        assert_eq!(s2.calls(), 0);
    }

    #[tokio::test]
    async fn a_primary_still_behind_after_the_grace_falls_through_to_a_source_that_is_ahead() {
        let (p, s2, s3) = (Chain::at(6), Chain::at(6), Chain::at(9));
        let set = block_set(&[p.clone(), s2.clone(), s3.clone()]);
        assert!(fetch(&set, 7).await.is_err());
        tokio::time::sleep(Duration::from_millis(170)).await;
        assert!(fetch(&set, 7).await.is_ok(), "the source that is ahead serves it after the grace");
        assert_eq!((s2.calls(), s3.calls()), (1, 1), "sources asked in order after the grace");
        // the grace is per block: block 8 starts a new grace on the primary
        assert!(fetch(&set, 8).await.is_err());
        assert_eq!(s3.calls(), 1);
    }

    #[tokio::test]
    async fn a_primary_that_keeps_lagging_is_moved_back_after_two_blocks() {
        let (p, s2) = (Chain::at(0), Chain::at(100));
        let set = block_set(&[p.clone(), s2.clone()]);
        for n in 1..=2u64 {
            assert!(fetch(&set, n).await.is_err(), "grace on block {n}");
            tokio::time::sleep(Duration::from_millis(170)).await;
            assert!(fetch(&set, n).await.is_ok());
        }
        assert_eq!(set.order(Instant::now()), vec![1, 0], "lagging primary demoted after 2 blocks");
        let before = p.calls();
        assert!(fetch(&set, 3).await.is_ok(), "block 3 served at once by the source that is ahead");
        assert_eq!(p.calls(), before, "demoted primary not asked first");
        // after its cooldown, a caught-up primary is first again
        p.to(100);
        tokio::time::sleep(Duration::from_millis(320)).await;
        assert!(fetch(&set, 4).await.is_ok());
        assert_eq!(set.order(Instant::now()), vec![0, 1]);
    }

    #[tokio::test]
    async fn an_error_on_the_primary_moves_on_at_once_without_a_grace() {
        let (p, s2) = (Fake::new(Behavior::Fail), Fake::new(Behavior::Ok(2)));
        let set = set(&[p.clone(), s2.clone()], Duration::from_millis(200));
        let v = set.call_block("payload", 7, run).await.unwrap();
        assert_eq!(v, 2);
    }

    #[tokio::test]
    async fn cooldown_doubles_per_consecutive_failure_up_to_the_cap_and_resets_on_success() {
        let pol = policy(Duration::from_millis(50));
        assert_eq!(pol.cooldown_after(1), Duration::from_millis(300));
        assert_eq!(pol.cooldown_after(2), Duration::from_millis(600));
        assert_eq!(pol.cooldown_after(3), Duration::from_millis(1200));
        assert_eq!(pol.cooldown_after(9), Duration::from_millis(1200), "capped");
        assert_eq!(pol.cooldown_after(u32::MAX), Duration::from_millis(1200), "no overflow");
        let prod = SourcePolicy::with_timeout(Duration::from_secs(1));
        assert_eq!(prod.cooldown_after(1), Duration::from_secs(5));
        assert_eq!(prod.cooldown_after(5), Duration::from_secs(60));

        let (a, b) = (Fake::new(Behavior::Fail), Fake::new(Behavior::Ok(2)));
        let s = set(&[a.clone(), b.clone()], Duration::from_millis(50));
        for expected in 1..=3 {
            // all cooling sources are still tried as a last resort when nothing healthy is left
            a.set(Behavior::Fail);
            b.set(Behavior::Fail);
            let _ = s.call("t", run).await;
            assert_eq!(s.failures(0), expected);
        }
        a.set(Behavior::Ok(1));
        assert_eq!(s.call("t", run).await.unwrap(), 1);
        assert_eq!(s.failures(0), 0, "success resets the failure count");
    }

    #[tokio::test]
    async fn a_hanging_source_is_asked_less_and_less_often() {
        let (a, b) = (Fake::new(Behavior::Hang), Fake::new(Behavior::Ok(2)));
        let s = SourceSet::new(
            vec![("a".into(), a.clone()), ("b".into(), b.clone())],
            SourcePolicy {
                timeout: Duration::from_millis(20),
                cooldown: Duration::from_millis(40),
                cooldown_max: Duration::from_millis(320),
                not_found_grace: Duration::from_millis(150),
            },
        );
        let end = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < end {
            assert_eq!(s.call("t", run).await.unwrap(), 2);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // with a fixed 40 ms cooldown the hung source would be asked ~25 times in 1.5 s;
        // doubling to 320 ms keeps it to about 40+80+160+320+320+320+320 -> ~7
        assert!(a.calls() <= 9, "hung source asked {} times", a.calls());
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
