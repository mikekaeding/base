//! Transport-level tests of the follow source against fake RPC servers ([`super::fake_rpc`]).
//!
//! They run the real [`RemoteL2Client`] (alloy WebSocket / HTTP transports) and the real
//! [`PayloadPrefetcher`], and measure, for every block, the time from the fake chain producing it
//! to the follower having its payload.
//!
//! The `#[ignore]` tests are long (legacy alloy keepalive is 10 s + 10 s) and replay the
//! 2026-10-15 trader-va timelines; run them with `cargo test ... -- --ignored --nocapture`.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    fake_rpc::{FakeChain, FakeHttp, FakeWs, HttpMode, WsPlan},
    prefetcher::{PREFETCH_WINDOW, PayloadPrefetcher},
    source::{RemoteClient, RemoteL2Client, RemoteL2ClientOptions},
};

#[derive(Debug)]
struct Run {
    /// (block, delay from production to payload in hand)
    delays: Vec<(u64, Duration)>,
    head_at_end: u64,
    start: u64,
}

impl Run {
    fn max_delay(&self) -> Duration {
        self.delays.iter().map(|d| d.1).max().unwrap_or_default()
    }

    fn worst(&self) -> (u64, Duration) {
        self.delays.iter().copied().max_by_key(|d| d.1).unwrap_or_default()
    }

    /// Every block produced at least `slack` before the end was received, in order.
    fn assert_complete(&self) {
        let got: Vec<u64> = self.delays.iter().map(|d| d.0).collect();
        let want: Vec<u64> = (self.start + 1..=self.start + got.len() as u64).collect();
        assert_eq!(got, want, "blocks out of order or missing");
        assert!(
            got.last().copied().unwrap_or(self.start) + 2 >= self.head_at_end,
            "follower fell behind: last {:?}, head {}",
            got.last(),
            self.head_at_end
        );
    }
}

async fn follow_for(client: RemoteL2Client, chain: Arc<FakeChain>, run_for: Duration) -> Run {
    let start = chain.head();
    let cancel = CancellationToken::new();
    let (head_tx, head_rx) = watch::channel(0);
    let (blocks_tx, mut blocks_rx) = mpsc::channel(PREFETCH_WINDOW);
    let push = tokio::spawn(client.clone().follow_heads(head_tx, cancel.clone()));
    let prefetcher = PayloadPrefetcher::new(Arc::new(client), cancel.clone(), blocks_tx);
    let fetch = tokio::spawn(prefetcher.run(start, Some(head_rx)));
    let mut delays = Vec::new();
    let end = Instant::now() + run_for;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(end) => break,
            p = blocks_rx.recv() => {
                let Some(p) = p else { break };
                let n = p.execution_payload.block_number();
                let produced = chain.produced_at(n).expect("block produced");
                delays.push((n, produced.elapsed()));
            }
        }
    }
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), fetch).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), push).await;
    Run { delays, head_at_end: chain.head(), start }
}

fn opts(sources: Vec<url::Url>, timeout_ms: u64, keepalive_ms: u64) -> RemoteL2ClientOptions {
    RemoteL2ClientOptions {
        http_sources: sources,
        request_timeout: Duration::from_millis(timeout_ms),
        ws_keepalive: Duration::from_millis(keepalive_ms),
    }
}

const FAST_BLOCK: Duration = Duration::from_millis(250);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fake_servers_serve_heights_and_payloads() {
    let chain = FakeChain::spawn(1000, FAST_BLOCK);
    let http = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client = RemoteL2Client::new(http.url()).await.unwrap();
    let head = client.get_block_number(alloy_eips::BlockNumberOrTag::Latest).await.unwrap();
    assert!(head >= 1000);
    let p = client.get_payload_by_number(head).await.unwrap();
    assert_eq!(p.execution_payload.block_number(), head);
    let info = client.get_block_info(alloy_eips::BlockNumberOrTag::Safe).await.unwrap();
    assert!(info.number + 10 <= chain.head(), "safe label is 10 behind the head");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fallback_keeps_importing_through_a_silent_push_socket() {
    let chain = FakeChain::spawn(2000, FAST_BLOCK);
    let ws = FakeWs::spawn(
        Arc::clone(&chain),
        vec![WsPlan::SilentAfter(Duration::from_millis(600))],
        WsPlan::Normal,
    )
    .await;
    let http = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client =
        RemoteL2Client::with_options(ws.url(), opts(vec![http.url()], 1000, 2000)).await.unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(5)).await;
    run.assert_complete();
    // the silent push costs at most the 200 ms poll, never the keepalive
    assert!(run.max_delay() < Duration::from_millis(600), "worst {:?}", run.worst());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fallback_rebuilds_a_silent_push_via_the_short_keepalive() {
    let chain = FakeChain::spawn(2500, FAST_BLOCK);
    let ws = FakeWs::spawn(
        Arc::clone(&chain),
        vec![WsPlan::SilentAfter(Duration::from_millis(300))],
        WsPlan::Normal,
    )
    .await;
    let http = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client =
        RemoteL2Client::with_options(ws.url(), opts(vec![http.url()], 1000, 500)).await.unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(4)).await;
    run.assert_complete();
    assert!(ws.connections() >= 2, "a silent push is detected by the 0.5 s keepalive and replaced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fallback_ignores_bursts_of_1013_closes_on_the_push() {
    let chain = FakeChain::spawn(3000, FAST_BLOCK);
    let ws =
        FakeWs::spawn(Arc::clone(&chain), vec![WsPlan::CloseAfterSubscribe; 12], WsPlan::Normal)
            .await;
    let http = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client =
        RemoteL2Client::with_options(ws.url(), opts(vec![http.url()], 1000, 2000)).await.unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(4)).await;
    run.assert_complete();
    assert!(run.max_delay() < Duration::from_millis(600), "worst {:?}", run.worst());
    assert!(ws.connections() >= 3, "the push saw the close burst");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_primary_http_source_falls_back_within_the_request_timeout() {
    let chain = FakeChain::spawn(4000, FAST_BLOCK);
    let ws = FakeWs::spawn(Arc::clone(&chain), vec![], WsPlan::Normal).await;
    let primary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Hang).await;
    let secondary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client = RemoteL2Client::with_options(
        ws.url(),
        opts(vec![primary.url(), secondary.url()], 300, 2000),
    )
    .await
    .unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(4)).await;
    run.assert_complete();
    // one 300 ms timeout, then the cooled-down primary is skipped
    assert!(run.max_delay() < Duration::from_millis(900), "worst {:?}", run.worst());
    assert!(
        primary.calls() <= 2,
        "hung primary asked {} times within its cooldown",
        primary.calls()
    );
    assert!(secondary.calls() > 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_primary_recovers_after_cooldown() {
    let chain = FakeChain::spawn(5000, FAST_BLOCK);
    let primary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Fail).await;
    let secondary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client = RemoteL2Client::with_options(
        primary.url(),
        opts(vec![primary.url(), secondary.url()], 500, 2000),
    )
    .await
    .unwrap();
    let p = primary.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        p.set_mode(HttpMode::Healthy);
    });
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(8)).await;
    run.assert_complete();
    assert!(run.max_delay() < Duration::from_millis(600), "worst {:?}", run.worst());
    let before = primary.calls();
    assert!(before > 3, "primary used again after its 5 s cooldown ({before} calls)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_sources_down_does_not_busy_loop_and_recovers() {
    let chain = FakeChain::spawn(6000, FAST_BLOCK);
    let a = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Fail).await;
    let b = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Fail).await;
    let client = RemoteL2Client::with_options(a.url(), opts(vec![a.url(), b.url()], 300, 2000))
        .await
        .unwrap();
    let (aa, bb) = (a.clone(), b.clone());
    let outage = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let calls = (aa.calls(), bb.calls());
        bb.set_mode(HttpMode::Healthy);
        calls
    });
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(5)).await;
    let (ca, cb) = outage.await.unwrap();
    // a 2 s total outage: bounded by the 200 ms head poll (~10 polls), no busy loop
    assert!(ca <= 14 && cb <= 14, "calls during outage a {ca} b {cb}");
    run.assert_complete();
    assert!(!run.delays.is_empty(), "recovered once a source came back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_one_block_behind_still_serves_within_the_grace() {
    // the usual announced-but-not-served race: the primary lags ~1 block (250 ms here, below the
    // 400 ms grace); payloads keep coming from the primary, the secondary is not asked for them
    let chain = FakeChain::spawn(11_000, FAST_BLOCK);
    let ws = FakeWs::spawn(Arc::clone(&chain), vec![], WsPlan::Normal).await;
    let primary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let secondary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    primary.set_lag(1);
    let client = RemoteL2Client::with_options(
        ws.url(),
        opts(vec![primary.url(), secondary.url()], 1000, 2000),
    )
    .await
    .unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(4)).await;
    run.assert_complete();
    assert!(
        secondary.payload_calls() <= 1,
        "secondary asked for {} payloads",
        secondary.payload_calls()
    );
    assert!(run.max_delay() < Duration::from_millis(700), "worst {:?}", run.worst());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_far_behind_falls_through_after_the_grace() {
    // the primary lags 4 blocks (1 s): after the 400 ms grace the source that is ahead serves
    let chain = FakeChain::spawn(12_000, FAST_BLOCK);
    let ws = FakeWs::spawn(Arc::clone(&chain), vec![], WsPlan::Normal).await;
    let primary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let secondary = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    primary.set_lag(4);
    let client = RemoteL2Client::with_options(
        ws.url(),
        opts(vec![primary.url(), secondary.url()], 1000, 2000),
    )
    .await
    .unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(4)).await;
    run.assert_complete();
    assert!(
        secondary.payload_calls() >= 5,
        "secondary served {} payloads",
        secondary.payload_calls()
    );
    assert!(run.max_delay() < Duration::from_millis(900), "worst {:?}", run.worst());
    // the grace is paid for at most two blocks, then the lagging primary is moved back
    let slow = run.delays.iter().filter(|d| d.1 > Duration::from_millis(350)).count();
    assert!(slow <= 3, "{slow} blocks paid the grace: {:?}", run.delays);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_only_fallback_without_push_polls() {
    let chain = FakeChain::spawn(7000, FAST_BLOCK);
    let http = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client =
        RemoteL2Client::with_options(http.url(), opts(vec![http.url()], 1000, 2000)).await.unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(3)).await;
    run.assert_complete();
    assert!(run.max_delay() < Duration::from_millis(600), "worst {:?}", run.worst());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_mode_is_unchanged_on_a_healthy_socket() {
    let chain = FakeChain::spawn(8000, FAST_BLOCK);
    let ws = FakeWs::spawn(Arc::clone(&chain), vec![], WsPlan::Normal).await;
    let client =
        RemoteL2Client::with_options(ws.url(), RemoteL2ClientOptions::default()).await.unwrap();
    assert!(!client.is_fallback());
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(3)).await;
    run.assert_complete();
    assert!(run.max_delay() < Duration::from_millis(400), "worst {:?}", run.worst());
}

// ------------------------------------------------------------------------------------------
// Long characterisation and replay tests (real 2 s blocks, alloy's real 10 s keepalive).

/// Legacy follower on one WebSocket: a socket that goes silent mid-stream stalls payloads until
/// alloy's keepalive (10 s idle + 10 s pong) replaces it. This is the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long: ~35 s; run with --ignored"]
async fn legacy_single_socket_stalls_until_the_keepalive_on_a_silent_socket() {
    let chain = FakeChain::spawn(9000, Duration::from_secs(2));
    let ws = FakeWs::spawn(
        Arc::clone(&chain),
        vec![WsPlan::SilentAfter(Duration::from_secs(5))],
        WsPlan::Normal,
    )
    .await;
    let client = RemoteL2Client::new(ws.url()).await.unwrap();
    let run = follow_for(client, Arc::clone(&chain), Duration::from_secs(35)).await;
    println!("legacy silent socket: worst {:?}, connections {}", run.worst(), ws.connections());
    assert!(
        run.max_delay() >= Duration::from_secs(12),
        "expected a keepalive-length stall, worst {:?}",
        run.worst()
    );
}

fn replay_plans(close_burst: usize, silent_sockets: usize, legacy: bool) -> Vec<WsPlan> {
    // connection 0: normal for 10 s, then a 1013 close (the storm starts)
    let mut plans = vec![WsPlan::CloseAt(Duration::from_secs(10))];
    // the storm: every new socket is closed with 1013 as soon as it is used
    let storm = if legacy { WsPlan::CloseOnFirstMessage } else { WsPlan::CloseAfterSubscribe };
    plans.extend(std::iter::repeat_n(storm, close_burst));
    // then sockets that open and go silent (no close, no pong)
    plans.extend(std::iter::repeat_n(WsPlan::SilentAfter(Duration::ZERO), silent_sockets));
    plans
}

async fn replay(
    name: &str,
    close_burst: usize,
    silent_sockets: usize,
    run_for: Duration,
) -> (Run, Run) {
    // legacy: today's configuration (one wss source)
    let chain = FakeChain::spawn(10_000, Duration::from_secs(2));
    let ws = FakeWs::spawn(
        Arc::clone(&chain),
        replay_plans(close_burst, silent_sockets, true),
        WsPlan::Normal,
    )
    .await;
    let legacy =
        follow_for(RemoteL2Client::new(ws.url()).await.unwrap(), Arc::clone(&chain), run_for).await;
    // fallback: same push timeline, HTTP reads from two healthy sources
    let chain = FakeChain::spawn(20_000, Duration::from_secs(2));
    let ws = FakeWs::spawn(
        Arc::clone(&chain),
        replay_plans(close_burst, silent_sockets, false),
        WsPlan::Normal,
    )
    .await;
    let a = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let b = FakeHttp::spawn(Arc::clone(&chain), HttpMode::Healthy).await;
    let client = RemoteL2Client::with_options(ws.url(), opts(vec![a.url(), b.url()], 1000, 2000))
        .await
        .unwrap();
    let fb = follow_for(client, Arc::clone(&chain), run_for).await;
    println!(
        "REPLAY {name}: legacy worst block delay {:?} (block {}), fallback worst {:?} (block {}); blocks legacy {} fallback {}",
        legacy.worst().1,
        legacy.worst().0,
        fb.worst().1,
        fb.worst().0,
        legacy.delays.len(),
        fb.delays.len()
    );
    (legacy, fb)
}

/// 2026-10-15 21:17:47 UTC on trader-va: 4 closes in 0.3 s, then two sockets that went silent
/// (missed pongs at 21:18:10.996 and 21:18:31.084); the node fell 44.3 s behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long: ~2 x 75 s; run with --ignored"]
async fn replay_oct15_2117_four_closes_then_two_silent_sockets() {
    let (legacy, fb) = replay("oct15-21:17", 4, 2, Duration::from_secs(75)).await;
    assert!(
        legacy.max_delay() >= Duration::from_secs(30),
        "legacy should reproduce the ~40 s stall"
    );
    fb.assert_complete();
    assert!(fb.max_delay() < Duration::from_millis(1500), "fallback worst {:?}", fb.worst());
}

/// 2026-10-15 14:08:45 UTC: 28 closes in 3.5 s, then one silent socket; 26.2 s behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long: ~2 x 55 s; run with --ignored"]
async fn replay_oct15_1408_28_closes_then_one_silent_socket() {
    let (legacy, fb) = replay("oct15-14:08", 28, 1, Duration::from_secs(55)).await;
    assert!(
        legacy.max_delay() >= Duration::from_secs(15),
        "legacy should reproduce the ~20-26 s stall"
    );
    fb.assert_complete();
    assert!(fb.max_delay() < Duration::from_millis(1500), "fallback worst {:?}", fb.worst());
}

/// Live measurement against real Base endpoints (real blocks, real TLS): fallback reads over HTTPS
/// with the wss newHeads push. Needs network; set `FOLLOW_LIVE_SMOKE=1`. Optional:
/// `FOLLOW_LIVE_SECS` (default 40), `FOLLOW_LIVE_PUSH` (default wss publicnode),
/// `FOLLOW_LIVE_SOURCES` (comma list; default publicnode, mainnet.base.org) and `FOLLOW_LIVE_OUT`
/// (JSONL path: one `{"n", "ts", "ready"}` line per block, `ready` = wall time the payload was in
/// hand), used for the read-only side-by-side run next to the live node on trader-va.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "network: FOLLOW_LIVE_SMOKE=1 cargo test ... live_public -- --ignored --nocapture"]
async fn live_public_endpoints_smoke() {
    if std::env::var("FOLLOW_LIVE_SMOKE").as_deref() != Ok("1") {
        return;
    }
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
    let secs: u64 = env("FOLLOW_LIVE_SECS", "40").parse().unwrap();
    let push = url::Url::parse(&env("FOLLOW_LIVE_PUSH", "wss://base-rpc.publicnode.com")).unwrap();
    let sources: Vec<url::Url> =
        env("FOLLOW_LIVE_SOURCES", "https://base-rpc.publicnode.com,https://mainnet.base.org")
            .split(',')
            .map(|u| url::Url::parse(u.trim()).unwrap())
            .collect();
    let mut out = std::env::var("FOLLOW_LIVE_OUT").ok().map(|p| std::fs::File::create(p).unwrap());
    let client = RemoteL2Client::with_options(push, opts(sources, 1000, 2000)).await.unwrap();
    let head = client.get_block_number(alloy_eips::BlockNumberOrTag::Latest).await.unwrap();
    let cancel = CancellationToken::new();
    let (head_tx, head_rx) = watch::channel(0);
    let (blocks_tx, mut blocks_rx) = mpsc::channel(PREFETCH_WINDOW);
    let push_task = tokio::spawn(client.clone().follow_heads(head_tx, cancel.clone()));
    let prefetcher = PayloadPrefetcher::new(Arc::new(client), cancel.clone(), blocks_tx);
    let fetch = tokio::spawn(prefetcher.run(head, Some(head_rx)));
    let end = Instant::now() + Duration::from_secs(secs);
    let mut lags = Vec::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(end) => break,
            p = blocks_rx.recv() => {
                let Some(p) = p else { break };
                let ts = p.execution_payload.timestamp();
                let n = p.execution_payload.block_number();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();
                if let Some(f) = out.as_mut() {
                    use std::io::Write;
                    writeln!(f, "{{\"n\":{n},\"ts\":{ts},\"ready\":{now:.6}}}").unwrap();
                }
                lags.push((n, now - ts as f64));
            }
        }
    }
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), fetch).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), push_task).await;
    let mut l: Vec<f64> = lags.iter().map(|x| x.1).collect();
    l.sort_by(f64::total_cmp);
    let q = |p: f64| l[((l.len() - 1) as f64 * p) as usize];
    println!(
        "LIVE: {} blocks from {head}; payload in hand after the block timestamp p50 {:.3} s, p90 {:.3} s, p99 {:.3} s, max {:.3} s",
        l.len(),
        q(0.5),
        q(0.9),
        q(0.99),
        l[l.len() - 1]
    );
    assert!(l.len() as u64 >= (secs / 2).saturating_sub(3), "about one block per 2 s");
    let numbers: Vec<u64> = lags.iter().map(|x| x.0).collect();
    assert_eq!(numbers, (head + 1..=head + numbers.len() as u64).collect::<Vec<_>>());
}
