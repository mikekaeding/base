use std::{fmt::Debug, sync::Arc, time::Duration};

use alloy_eips::BlockNumberOrTag;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelope;
use tokio::sync::watch;
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
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
        mut head_notifications: Option<watch::Receiver<()>>,
    ) -> Result<(), FollowError> {
        let mut next_fetch = start_from_local_head.saturating_add(1);
        let mut source_latest = start_from_local_head;
        let mut consecutive_payload_failures = 0;

        loop {
            if self.cancellation.is_cancelled() {
                return Ok(());
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

            let payload = tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                payload = self.source.get_payload_by_number(next_fetch) => payload,
            };

            match payload {
                Ok(payload) => {
                    let sent = tokio::select! {
                        _ = self.cancellation.cancelled() => return Ok(()),
                        sent = self.blocks_to_insert_tx.send(payload) => sent,
                    };
                    if sent.is_err() {
                        return Ok(());
                    }
                    consecutive_payload_failures = 0;
                    next_fetch = next_fetch.saturating_add(1);
                }
                Err(e) => {
                    consecutive_payload_failures += 1;
                    if consecutive_payload_failures % PREFETCH_FAILURE_WARN_INTERVAL == 0 {
                        warn!(
                            target: "follow",
                            block = next_fetch,
                            attempts = consecutive_payload_failures,
                            error = %e,
                            "Repeatedly failed to prefetch source payload"
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
                    self.wait_after_payload_error(&e, &mut head_notifications).await;
                }
            }
        }
    }

    async fn wait_after_payload_error(
        &self,
        error: &RemoteL2ClientError,
        notifications: &mut Option<watch::Receiver<()>>,
    ) {
        if matches!(error, RemoteL2ClientError::BlockNotFound(_)) {
            // Latest-head RPC and full-payload availability can race across source backends.
            // A pushed head must wake this wait too, not sit behind transport-failure backoff.
            debug!(target: "follow", "Advertised source head has no payload yet; waiting for head notification or bounded retry");
            self.wait_at_source_head(notifications).await;
        } else {
            tokio::select! {
                _ = self.cancellation.cancelled() => {}
                _ = time::sleep(SOURCE_FAILURE_BACKOFF) => {}
            }
        }
    }

    async fn refresh_source_latest(&self, current: u64) -> u64 {
        match self.source.get_block_number(BlockNumberOrTag::Latest).await {
            Ok(latest) => latest,
            Err(e) => {
                debug!(target: "follow", error = %e, "Failed to fetch source latest head");
                current
            }
        }
    }

    async fn wait_at_source_head(&self, notifications: &mut Option<watch::Receiver<()>>) {
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
        let (sender, receiver) = watch::channel(());
        sender.send(()).expect("test receiver is alive");
        let mut receiver = Some(receiver);
        time::timeout(Duration::from_millis(100), prefetcher.wait_at_source_head(&mut receiver))
            .await
            .expect("queued head wakes without waiting for poll interval");
    }

    #[tokio::test]
    async fn advertised_head_payload_race_wakes_on_push_without_one_second_backoff() {
        let prefetcher = prefetcher();
        let (sender, receiver) = watch::channel(());
        let mut receiver = Some(receiver);
        sender.send(()).expect("fixture receiver is live");
        time::timeout(
            Duration::from_millis(100),
            prefetcher.wait_after_payload_error(
                &RemoteL2ClientError::BlockNotFound("42".to_owned()),
                &mut receiver,
            ),
        )
        .await
        .expect("a new head wakes a not-yet-published payload");
        let started = time::Instant::now();
        prefetcher
            .wait_after_payload_error(
                &RemoteL2ClientError::BlockNotFound("42".to_owned()),
                &mut receiver,
            )
            .await;
        assert!(started.elapsed() >= SOURCE_HEAD_BACKOFF);
        assert!(started.elapsed() < SOURCE_FAILURE_BACKOFF);
    }

    #[tokio::test]
    async fn closed_notifications_fall_back_without_busy_loop() {
        let prefetcher = prefetcher();
        let (sender, receiver) = watch::channel(());
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
