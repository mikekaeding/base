use std::time::Instant;
use std::{fmt::Debug, sync::Arc, time::Duration};

use alloy_eips::BlockNumberOrTag;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelope;
use tokio::sync::watch;
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::{debug, warn};

use crate::follow::{error::FollowError, source::RemoteClient, source::RemoteL2ClientError};

/// Number of source L2 payloads to keep prefetched ahead of the insert loop.
pub(super) const PREFETCH_WINDOW: usize = 50;
const SOURCE_HEAD_BACKOFF: Duration = Duration::from_millis(200);
const SOURCE_FAILURE_BACKOFF: Duration = Duration::from_secs(1);
const PREFETCH_FAILURE_WARN_INTERVAL: u64 = 5;

/// A fetched source payload.
pub(super) type PrefetchedPayload = BaseExecutionPayloadEnvelope;

/// Fetches source L2 payloads ahead of the insert loop.
#[derive(Debug)]
pub(super) struct PayloadPrefetcher<Remote> {
    source: Arc<Remote>,
    cancellation: CancellationToken,
    blocks_to_insert_tx: mpsc::Sender<PrefetchedPayload>,
}

impl<Remote> PayloadPrefetcher<Remote>
where
    Remote: RemoteClient + 'static,
{
    /// Creates a payload prefetcher.
    pub(super) const fn new(
        source: Arc<Remote>,
        cancellation: CancellationToken,
        blocks_to_insert_tx: mpsc::Sender<PrefetchedPayload>,
    ) -> Self {
        Self { source, cancellation, blocks_to_insert_tx }
    }

    /// Starts fetching from the local node head and pushes payloads through a
    /// bounded channel.
    pub(super) async fn run(
        self,
        start_from_local_head: u64,
        mut head_notifications: Option<watch::Receiver<u64>>,
    ) -> Result<(), FollowError> {
        let mut next_fetch = start_from_local_head.saturating_add(1);
        let mut source_latest = start_from_local_head;
        let mut consecutive_payload_failures = 0;

        loop {
            if self.cancellation.is_cancelled() {
                return Ok(());
            }

            if next_fetch > source_latest {
                // A pushed height is a fetch hint, not authority to skip payload validation.
                // Re-querying latest here adds a round trip and can hit a lagging RPC backend.
                if let Some(notifications) = &head_notifications {
                    source_latest = source_latest.max(*notifications.borrow());
                }
            }
            if next_fetch > source_latest {
                source_latest = tokio::select! {
                    _ = self.cancellation.cancelled() => return Ok(()),
                    latest = self.refresh_source_latest(source_latest) => latest,
                };
                if next_fetch > source_latest {
                    self.wait_at_source_head(&mut head_notifications).await;
                    continue;
                }
            }

            let fetch_started = Instant::now();
            let payload = tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                payload = self.source.get_payload_by_number(next_fetch) => payload,
            };

            match payload {
                Ok(payload) => {
                    let fetch_duration_us = fetch_started.elapsed().as_micros();
                    info!(target: "follow", block = next_fetch, fetch_duration_us,
                        retries = consecutive_payload_failures, "Fetched source payload");
                    let queue_started = Instant::now();
                    let sent = tokio::select! {
                        _ = self.cancellation.cancelled() => return Ok(()),
                        sent = self.blocks_to_insert_tx.send(payload) => sent,
                    };
                    if sent.is_err() {
                        return Ok(());
                    }
                    info!(target: "follow", block = next_fetch,
                        queue_duration_us = queue_started.elapsed().as_micros(), "Queued source payload");
                    consecutive_payload_failures = 0;
                    next_fetch = next_fetch.saturating_add(1);
                }
                Err(e) => {
                    consecutive_payload_failures += 1;
                    let unavailable = matches!(e, RemoteL2ClientError::BlockNotFound(_));
                    let backoff =
                        if unavailable { SOURCE_HEAD_BACKOFF } else { SOURCE_FAILURE_BACKOFF };
                    if consecutive_payload_failures == 1
                        || consecutive_payload_failures % PREFETCH_FAILURE_WARN_INTERVAL == 0
                    {
                        warn!(
                            target: "follow",
                            block = next_fetch,
                            attempts = consecutive_payload_failures,
                            fetch_duration_us = fetch_started.elapsed().as_micros(),
                            maximum_backoff_ms = backoff.as_millis(),
                            unavailable,
                            error = %e,
                            "Failed to prefetch source payload"
                        );
                    } else {
                        debug!(
                            target: "follow",
                            block = next_fetch,
                            attempts = consecutive_payload_failures,
                            error = %e,
                            "Failed to prefetch source payload"
                        );
                    }
                    if unavailable {
                        // The latest-head response can precede payload availability on another backend.
                        self.wait_at_source_head(&mut head_notifications).await;
                    } else {
                        tokio::select! {
                            _ = self.cancellation.cancelled() => {}
                            _ = time::sleep(SOURCE_FAILURE_BACKOFF) => {}
                        }
                    }
                }
            }
        }
    }

    async fn refresh_source_latest(&self, current: u64) -> u64 {
        let started = Instant::now();
        match self.source.get_block_number(BlockNumberOrTag::Latest).await {
            Ok(latest) => {
                info!(target: "follow", block = latest, previous_block = current,
                    fetch_duration_us = started.elapsed().as_micros(), "Fetched source height");
                latest
            }
            Err(e) => {
                warn!(target: "follow", previous_block = current,
                    fetch_duration_us = started.elapsed().as_micros(), error = %e,
                    "Failed to fetch source latest head");
                current
            }
        }
    }

    async fn wait_at_source_head(&self, notifications: &mut Option<watch::Receiver<u64>>) {
        // Watch retains a wake arriving between the latest-head query and this wait.
        // The fallback covers HTTP sources, lost notifications and subscription reconnects.
        tokio::select! {
            _ = self.cancellation.cancelled() => {}
            _ = time::sleep(SOURCE_HEAD_BACKOFF) => {}
            changed = async {
                match notifications.as_mut() {
                    Some(receiver) => receiver.changed().await,
                    None => std::future::pending().await,
                }
            } => {
                if changed.is_err() {
                    *notifications = None;
                    // A closed notification channel must not become a busy-poll loop.
                    tokio::select! {
                        _ = self.cancellation.cancelled() => {}
                        _ = time::sleep(SOURCE_HEAD_BACKOFF) => {}
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::follow::source::MockRemoteClient;

    fn prefetcher() -> PayloadPrefetcher<MockRemoteClient> {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        PayloadPrefetcher::new(Arc::new(MockRemoteClient::new()), CancellationToken::new(), sender)
    }

    #[tokio::test]
    async fn notification_received_before_wait_is_not_lost() {
        let prefetcher = prefetcher();
        let (sender, receiver) = watch::channel(0);
        sender.send(42).expect("test receiver is alive");
        let mut receiver = Some(receiver);
        time::timeout(Duration::from_millis(100), prefetcher.wait_at_source_head(&mut receiver))
            .await
            .expect("queued head wakes without waiting for poll interval");
    }

    #[tokio::test]
    async fn pushed_height_fetches_next_payload_without_latest_rpc()
    -> Result<(), Box<dyn std::error::Error>> {
        for advertised in [42, u64::MAX] {
            let cancellation = CancellationToken::new();
            let stop = cancellation.clone();
            let mut source = MockRemoteClient::new();
            source.expect_get_block_number().times(0);
            source.expect_get_payload_by_number().times(1).returning(move |number| {
                // Even an extreme hint cannot skip a block or authorize its insertion.
                assert_eq!(number, 42);
                stop.cancel();
                Err(RemoteL2ClientError::BlockNotFound(number.to_string()))
            });
            let (head, notifications) = watch::channel(0);
            head.send(advertised).map_err(|e| format!("publish advertised-height fixture: {e}"))?;
            let (output, _receiver) = mpsc::channel(1);
            let prefetcher = PayloadPrefetcher::new(Arc::new(source), cancellation, output);
            time::timeout(Duration::from_millis(100), prefetcher.run(41, Some(notifications)))
                .await
                .map_err(|e| format!("advertised-height fetch exceeded deadline: {e}"))?
                .map_err(|e| format!("advertised-height prefetch failed: {e}"))?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn old_height_hint_keeps_http_fallback_and_sequential_fetch()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let stop = cancellation.clone();
        let mut source = MockRemoteClient::new();
        source.expect_get_block_number().times(1).returning(|_| Ok(42));
        source.expect_get_payload_by_number().times(1).returning(move |number| {
            assert_eq!(number, 42);
            stop.cancel();
            Err(RemoteL2ClientError::BlockNotFound(number.to_string()))
        });
        let (_head, notifications) = watch::channel(40);
        let (output, _receiver) = mpsc::channel(1);
        let prefetcher = PayloadPrefetcher::new(Arc::new(source), cancellation, output);
        time::timeout(Duration::from_millis(100), prefetcher.run(41, Some(notifications)))
            .await
            .map_err(|e| format!("fallback fetch exceeded deadline: {e}"))?
            .map_err(|e| format!("fallback prefetch failed: {e}"))?;
        Ok(())
    }

    #[tokio::test]
    async fn advertised_head_payload_race_wakes_on_push_without_one_second_backoff() {
        for pushed in [true, false] {
            let cancellation = CancellationToken::new();
            let stop = cancellation.clone();
            let mut source = MockRemoteClient::new();
            source.expect_get_block_number().returning(|_| Ok(42));
            let mut calls = 0;
            source.expect_get_payload_by_number().times(2).returning(move |number| {
                assert_eq!(number, 42);
                calls += 1;
                if calls == 2 {
                    stop.cancel();
                }
                Err(RemoteL2ClientError::BlockNotFound("42".to_owned()))
            });
            let (head, notifications) = watch::channel(0);
            if pushed {
                head.send(42).expect("fixture receiver is live");
            }
            let (output, _receiver) = mpsc::channel(1);
            let prefetcher = PayloadPrefetcher::new(Arc::new(source), cancellation, output);
            let started = time::Instant::now();
            let limit = Duration::from_millis(if pushed { 100 } else { 900 });
            time::timeout(limit, prefetcher.run(41, Some(notifications)))
                .await
                .expect("payload availability retry must not wait one second")
                .expect("cancellation terminates the fixture");
            if !pushed {
                assert!(started.elapsed() >= SOURCE_HEAD_BACKOFF);
            }
        }
    }

    #[tokio::test]
    async fn closed_notifications_fall_back_without_busy_loop() {
        let prefetcher = prefetcher();
        let (sender, receiver) = watch::channel(0);
        drop(sender);
        let mut receiver = Some(receiver);
        assert!(
            time::timeout(Duration::from_millis(20), prefetcher.wait_at_source_head(&mut receiver))
                .await
                .is_err()
        );
        assert!(receiver.is_none());
    }

    #[tokio::test]
    async fn cancellation_interrupts_head_wait() {
        let prefetcher = prefetcher();
        prefetcher.cancellation.cancel();
        time::timeout(Duration::from_millis(100), prefetcher.wait_at_source_head(&mut None))
            .await
            .expect("cancellation does not wait for poll interval");
    }

    #[tokio::test]
    async fn polling_still_advances_without_notifications() {
        let prefetcher = prefetcher();
        time::timeout(Duration::from_secs(1), prefetcher.wait_at_source_head(&mut None))
            .await
            .expect("HTTP and disconnected sources retain bounded polling");
    }
}
