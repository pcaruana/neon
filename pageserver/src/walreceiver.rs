//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream wal into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * determine the way that a timeline needs WAL streaming.
//! For that, it watches specific keys in etcd broker and pulls the relevant data periodically.
//! The data is produced by safekeepers, that push it periodically and pull it to synchronize between each other.
//! Without this data, no WAL streaming is possible currently.
//!
//! Only one active WAL streaming connection is allowed at a time.
//! The connection is supposed to be updated periodically, based on safekeeper timeline data.
//!
//! * handle the actual connection and WAL streaming
//!
//!
//! ## Implementation details
//!
//! WAL receiver's implementation consists of 3 kinds of nested loops, separately handling the logic from the bullets above:
//!
//! * wal receiver main thread, containing the control async loop: timeline addition/removal and interruption of a whole thread handling.
//! The loop can exit with an error, if broker or safekeeper connection attempt limit is reached, with the backpressure mechanism.
//! All of the code inside the loop is either async or a spawn_blocking wrapper around the sync code.
//!
//! * wal receiver broker task, handling the etcd broker interactions, safekeeper selection logic and backpressure.
//! On every concequent broker/wal streamer connection attempt, the loop steps are forced to wait for some time before running,
//! increasing with the number of attempts (capped with 30s).
//!
//! Apart from the broker management, it keeps the wal streaming connection open, with the safekeeper having the most advanced timeline state.
//! The connection could be closed from safekeeper side (with error or not), could be cancelled from pageserver side from time to time.
//!
//! * wal streaming task, opening the libpq conneciton and reading the data out of it to the end, then reporting the result to the broker thread

mod connection_handler;

use crate::config::PageServerConf;
use crate::repository::Timeline;
use crate::tenant_mgr::{self, LocalTimelineUpdate, TenantState};
use crate::thread_mgr::ThreadKind;
use crate::{thread_mgr, DatadirTimelineImpl};
use anyhow::{ensure, Context};
use chrono::{NaiveDateTime, Utc};
use etcd_broker::{Client, SkTimelineInfo, SkTimelineSubscriptionKind};
use itertools::Itertools;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::thread_local;
use std::time::Duration;
use tokio::select;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::*;
use url::Url;
use utils::pq_proto::ZenithFeedback;
use utils::zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId};

use self::connection_handler::{WalConnectionEvent, WalreceiverConnection};

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

pub fn init_wal_receiver_main_thread(
    conf: &'static PageServerConf,
    mut timeline_updates_receiver: mpsc::UnboundedReceiver<LocalTimelineUpdate>,
) -> anyhow::Result<()> {
    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );
    let broker_prefix = &conf.broker_etcd_prefix;
    info!(
        "Starting wal receiver main thread, etdc endpoints: {}",
        etcd_endpoints.iter().map(Url::to_string).join(", ")
    );

    thread_mgr::spawn(
        ThreadKind::WalReceiverManager,
        None,
        None,
        "WAL receiver manager main thread",
        true,
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to create storage sync runtime")?;
            let mut etcd_client = runtime
                .block_on(etcd_broker::Client::connect(etcd_endpoints, None))
                .context("Failed to connect to etcd")?;
            let mut local_timeline_wal_receivers = HashMap::new();

            loop {
                let loop_step = runtime.block_on(async {
                    select! {
                        // check for shutdown first
                        biased;
                        _ = thread_mgr::shutdown_watcher() => {
                            shutdown_all_wal_connections(&mut local_timeline_wal_receivers).await;
                            ControlFlow::Break(Ok(()))
                        },
                        step = walreceiver_main_thread_loop_step(
                            broker_prefix,
                            &mut etcd_client,
                            &mut timeline_updates_receiver,
                            &mut local_timeline_wal_receivers,
                        ).instrument(info_span!("walreceiver_main_thread_loop_step")) => step,
                    }
                });

                match loop_step {
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(Ok(())) => {
                        info!("Wal receiver main thread stopped successfully");
                        return Ok(());
                    }
                    ControlFlow::Break(Err(e)) => {
                        error!("WAL receiver main thread exited with an error: {e:?}");
                        return Err(e);
                    }
                }
            }
        },
    )
    .map(|_thread_id| ())
    .context("Failed to spawn wal receiver main thread")
}

async fn walreceiver_main_thread_loop_step<'a>(
    broker_prefix: &str,
    etcd_client: &mut Client,
    timeline_updates_receiver: &'a mut mpsc::UnboundedReceiver<LocalTimelineUpdate>,
    local_timeline_wal_receivers: &'a mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalReceiverHandles>,
    >,
    // TODO kb odd type, can it be just Result<()>?
) -> ControlFlow<anyhow::Result<()>, ()> {
    match timeline_updates_receiver.recv().await {
        Some(update) => {
            info!("Processing timeline update: {update:?}");
            match update {
                LocalTimelineUpdate::Remove(id) => {
                    match local_timeline_wal_receivers.get_mut(&id.tenant_id) {
                        Some(timelines) => {
                            if let Some(handles) = timelines.remove(&id.timeline_id) {
                                let wal_receiver_shutdown_result = handles.shutdown(id).await;
                                if wal_receiver_shutdown_result.is_err() {
                                    return ControlFlow::Break(wal_receiver_shutdown_result)
                                }
                            };
                            if timelines.is_empty() {
                                let state_change_result = change_tenant_state(id.tenant_id, TenantState::Idle).await;
                                if state_change_result.is_err() {
                                    return ControlFlow::Break(state_change_result)
                                }
                            }
                        }
                        None => warn!("Timeline {id} does not have a tenant entry in wal receiver main thread"),
                    };
                }
                LocalTimelineUpdate::Add(new_id, new_timeline) => {
                    let timelines = local_timeline_wal_receivers
                        .entry(new_id.tenant_id)
                        .or_default();

                    if timelines.is_empty() {
                        let state_change_result =
                            change_tenant_state(new_id.tenant_id, TenantState::Active).await;
                        if state_change_result.is_err() {
                            return ControlFlow::Break(state_change_result);
                        }
                    } else if let Some(old_entry_handles) = timelines.remove(&new_id.timeline_id) {
                        warn!(
                            "Readding to an existing timeline {new_id}, shutting the old wal receiver down"
                        );
                        let wal_receiver_shutdown_result = old_entry_handles.shutdown(new_id).await;
                        if wal_receiver_shutdown_result.is_err() {
                            return ControlFlow::Break(wal_receiver_shutdown_result);
                        }
                    }

                    let (wal_connect_timeout, _max_retries) =
                        match fetch_tenant_settings(new_id.tenant_id).await {
                            Ok(settings) => settings,
                            Err(e) => return ControlFlow::Break(Err(e)),
                        };

                    for attempt in 0.. {
                        exponential_backoff(attempt, 2.0, 60.0).await;
                        let wal_connection_manager = WalConnectionManager {
                            timeline_id: new_id,
                            timeline: Arc::clone(&new_timeline),
                            wal_connect_timeout,
                            // TODO kb more tenant settings for walreceiver selection
                            no_wal_timeout: Duration::from_secs(30),
                            max_lsn_lag: NonZeroU64::new(300).unwrap(),
                            connection_data: None,
                            wal_connection_attempt: 0,
                        };
                        match start_timeline_wal_broker(
                            broker_prefix,
                            etcd_client,
                            wal_connection_manager,
                        )
                        .await
                        {
                            Ok(new_handles) => {
                                timelines.insert(new_id.timeline_id, new_handles);
                                break;
                            }
                            Err(e) => {
                                warn!("Attempt #{attempt}, failed to start timeline {new_id} wal broker: {e:#}")
                            }
                        }
                    }
                }
            }
        }
        None => {
            info!("Local timeline update channel closed, shutting down all wal connections");
            shutdown_all_wal_connections(local_timeline_wal_receivers).await;
            return ControlFlow::Break(Ok(()));
        }
    }

    ControlFlow::Continue(())
}

async fn fetch_tenant_settings(tenant_id: ZTenantId) -> anyhow::Result<(Duration, NonZeroU32)> {
    tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        Ok::<_, anyhow::Error>((
            repo.get_walreceiver_connect_timeout(),
            repo.get_max_walreceiver_connect_attempts(),
        ))
    })
    .await
    .with_context(|| format!("Failed to join on tenant {tenant_id} settings fetch task"))?
}

async fn change_tenant_state(tenant_id: ZTenantId, new_state: TenantState) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        tenant_mgr::set_tenant_state(tenant_id, new_state)
            .with_context(|| format!("Failed to activate tenant {tenant_id}"))
    })
    .await
    .with_context(|| format!("Failed to spawn activation task for tenant {tenant_id}"))?
}

async fn exponential_backoff(n: u32, base: f64, max_seconds: f64) {
    if n == 0 {
        return;
    }
    let seconds_to_wait = base.powf(f64::from(n) - 1.0).min(max_seconds);
    info!("Backpressure: waiting {seconds_to_wait} seconds before proceeding with the task");
    tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
}

async fn shutdown_all_wal_connections(
    local_timeline_wal_receivers: &mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalReceiverHandles>,
    >,
) {
    let mut broker_join_handles = Vec::new();
    for (tenant_id, timelines) in local_timeline_wal_receivers.drain() {
        for (timeline_id, handles) in timelines {
            handles.cancellation_sender.send(()).ok();
            broker_join_handles.push((
                ZTenantTimelineId::new(tenant_id, timeline_id),
                handles.broker_join_handle,
            ));
        }
    }

    let mut tenants = HashSet::with_capacity(broker_join_handles.len());
    for (id, broker_join_handle) in broker_join_handles {
        tenants.insert(id.tenant_id);
        debug!("Waiting for wal broker for timeline {id} to finish");
        if let Err(e) = broker_join_handle.await {
            error!("Failed to join on wal broker for timeline {id}: {e}");
        }
    }
    if let Err(e) = tokio::task::spawn_blocking(move || {
        for tenant_id in tenants {
            if let Err(e) = tenant_mgr::set_tenant_state(tenant_id, TenantState::Idle) {
                error!("Failed to make tenant {tenant_id} idle: {e:?}");
            }
        }
    })
    .await
    {
        error!("Failed to spawn a task to make all tenants idle: {e:?}");
    }
}

struct TimelineWalReceiverHandles {
    broker_join_handle: JoinHandle<anyhow::Result<()>>,
    cancellation_sender: watch::Sender<()>,
}

impl TimelineWalReceiverHandles {
    async fn shutdown(self, id: ZTenantTimelineId) -> anyhow::Result<()> {
        self.cancellation_sender.send(()).context(
            "Unexpected: cancellation sender is dropped before the receiver in the loop is",
        )?;
        debug!("Waiting for wal receiver for timeline {id} to finish");
        self.broker_join_handle
            .await
            .with_context(|| format!("Failed to join the wal reveiver broker for timeline {id}"))?
            .with_context(|| format!("Wal reveiver broker for timeline {id} failed to finish"))
    }
}

async fn start_timeline_wal_broker(
    broker_prefix: &str,
    etcd_client: &mut Client,
    mut wal_connection_manager: WalConnectionManager,
) -> anyhow::Result<TimelineWalReceiverHandles> {
    let id = wal_connection_manager.timeline_id;
    let (cancellation_sender, mut cancellation_receiver) = watch::channel(());

    let mut subscription = etcd_broker::subscribe_to_safekeeper_timeline_updates(
        etcd_client,
        SkTimelineSubscriptionKind::timeline(broker_prefix.to_owned(), id),
    )
    .await
    .with_context(|| format!("Failed to subscribe for timeline {id} updates in etcd"))?;

    let broker_join_handle = tokio::spawn(async move {
        info!("WAL receiver broker started");

        loop {
            select! {
                biased;
                _ = cancellation_receiver.changed() => {
                    break;
                }

                walreceiver_poll_result = wal_connection_manager.poll_connection_event() => match walreceiver_poll_result {
                    ControlFlow::Break(()) => break,
                    ControlFlow::Continue(()) => {},
                },

                updates = subscription.fetch_data() => match updates {
                    Some(mut all_timeline_updates) => {
                        if let Some(subscribed_timeline_updates) = all_timeline_updates.remove(&id) {
                            match wal_connection_manager.select_connection_candidate(subscribed_timeline_updates) {
                                Some((new_safekeeper_id, new_wal_producer_connstr)) => {
                                    info!(
                                        "Switching to different safekeeper {new_safekeeper_id} for timeline {id}",
                                    );
                                    wal_connection_manager.change_connection(new_safekeeper_id, new_wal_producer_connstr).await;
                                },
                                None => {}
                            }
                        }
                    },
                    None => {
                        info!("Subscription source end was dropped, no more updates are possible, shutting down");
                        break;
                    },
                },
            }
        }

        info!("Loop ended, shutting down");
        wal_connection_manager.close_connection().await;
        subscription
            .cancel()
            .await
            .with_context(|| format!("Failed to cancel timeline {id} subscription in etcd"))?;
        Ok(())
    }.instrument(info_span!("timeline_walreceiver", id = %id)));

    Ok(TimelineWalReceiverHandles {
        broker_join_handle,
        cancellation_sender,
    })
}

struct WalConnectionManager {
    timeline_id: ZTenantTimelineId,
    timeline: Arc<DatadirTimelineImpl>,
    wal_connect_timeout: Duration,
    no_wal_timeout: Duration,
    max_lsn_lag: NonZeroU64,
    wal_connection_attempt: u32,
    connection_data: Option<ConnectionData>,
}

struct ConnectionData {
    wal_producer_connstr: String,
    safekeeper_id: ZNodeId,
    connection: WalreceiverConnection,
    connection_init_time: NaiveDateTime,
    last_walreceiver_data: Option<(ZenithFeedback, NaiveDateTime)>,
}

impl WalConnectionManager {
    async fn poll_connection_event(&mut self) -> ControlFlow<(), ()> {
        let (connection_data, walreceiver_event) = match self.connection_data.as_mut() {
            Some(connection_data) => match connection_data.connection.next_event().await {
                Some(event) => (connection_data, event),
                None => {
                    warn!("WAL receiver event source stopped sending messages, aborting the loop");
                    return ControlFlow::Break(());
                }
            },
            None => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                warn!("WAL receiver without a connection spent sleeping 30s without being interrupted, aborting the loop");
                return ControlFlow::Break(());
            }
        };

        match walreceiver_event {
            WalConnectionEvent::Started => {
                self.wal_connection_attempt = 0;
            }
            WalConnectionEvent::NewWal(new_wal_data) => {
                self.wal_connection_attempt = 0;
                connection_data.last_walreceiver_data =
                    Some((new_wal_data, Utc::now().naive_utc()));
            }
            WalConnectionEvent::End(walreceiver_result) => {
                match walreceiver_result {
                    Ok(()) => {
                        info!("WAL receiver task finished, reconnecting");
                        self.wal_connection_attempt = 0;
                    }
                    Err(e) => {
                        error!("WAL receiver task failed: {e:#}, reconnecting");
                        self.wal_connection_attempt += 1;
                    }
                }
                self.close_connection().await;
            }
        }

        ControlFlow::Continue(())
    }

    async fn close_connection(&mut self) {
        if let Some(data) = self.connection_data.take() {
            if let Err(e) = data.connection.shutdown().await {
                error!("Failed to shutdown walreceiver connection: {e:#}");
            }
        }
    }

    async fn change_connection(
        &mut self,
        new_safekeeper_id: ZNodeId,
        new_wal_producer_connstr: String,
    ) {
        self.close_connection().await;
        self.connection_data = Some(ConnectionData {
            wal_producer_connstr: new_wal_producer_connstr.clone(),
            safekeeper_id: new_safekeeper_id,
            connection: WalreceiverConnection::new(
                self.timeline_id,
                new_wal_producer_connstr,
                self.wal_connect_timeout,
            ),
            connection_init_time: Utc::now().naive_utc(),
            last_walreceiver_data: None,
        });
    }

    // TODO kb unit test this
    fn select_connection_candidate(
        &self,
        safekeeper_timelines: HashMap<ZNodeId, SkTimelineInfo>,
    ) -> Option<(ZNodeId, String)> {
        let (&new_sk_id, new_sk_timeline, new_wal_producer_connstr) = safekeeper_timelines
            .iter()
            .filter(|(_, info)| {
                info.commit_lsn > Some(self.timeline.tline.get_disk_consistent_lsn())
            })
            .filter_map(|(sk_id, info)| {
                match wal_stream_connection_string(
                    self.timeline_id,
                    info.safekeeper_connstr.as_deref()?,
                    info.pageserver_connstr.as_deref()?,
                ) {
                    Ok(connstr) => Some((sk_id, info, connstr)),
                    Err(e) => {
                        error!("Failed to create wal receiver connection string from broker data of safekeeper node {sk_id}: {e:#}");
                        None
                    }
                }
            })
            .max_by_key(|(_, info, _)| info.commit_lsn)?;

        match self.connection_data.as_ref() {
            None => Some((new_sk_id, new_wal_producer_connstr)),
            Some(old_connection) => {
                let same_safekeeper = old_connection.safekeeper_id == new_sk_id;

                if same_safekeeper
                    && old_connection.wal_producer_connstr != new_wal_producer_connstr
                {
                    debug!("WAL producer connection string changed, old: '{}', new: '{new_wal_producer_connstr}', reconnecting",
                            old_connection.wal_producer_connstr);
                    return Some((new_sk_id, new_wal_producer_connstr));
                } else if !same_safekeeper
                    && self.candidate_advanced_over_threshold(old_connection, new_sk_timeline)
                {
                    return Some((new_sk_id, new_wal_producer_connstr));
                }
                None
            }
        }
    }

    fn candidate_advanced_over_threshold(
        &self,
        old_connection: &ConnectionData,
        new_sk_timeline: &SkTimelineInfo,
    ) -> bool {
        let new_commit_lsn = match new_sk_timeline.commit_lsn {
            Some(lsn) => lsn,
            None => return false,
        };

        let last_sk_interaction_time = match old_connection.last_walreceiver_data.as_ref() {
            Some((last_walreceiver_data, data_submission_time)) => {
                match new_commit_lsn
                    .0
                    .checked_sub(last_walreceiver_data.ps_flushlsn)
                {
                    Some(sk_lsn_advantage) => {
                        if sk_lsn_advantage >= self.max_lsn_lag.get() {
                            return true;
                        }
                    }
                    None => debug!("Best SK candidate has its commit Lsn behind the current timeline's disk consistent Lsn"),
                }

                *data_submission_time
            }
            None => old_connection.connection_init_time,
        };

        let now = Utc::now().naive_utc();
        match (now - last_sk_interaction_time).to_std() {
            Ok(last_sk_interaction_time) => last_sk_interaction_time > self.no_wal_timeout,
            Err(_e) => {
                warn!("Last interaction with safekeeper {} happened in the future, ignoring the candidate. Interaction time: {last_sk_interaction_time}, now: {now}",
                    old_connection.safekeeper_id);
                false
            }
        }
    }
}

fn wal_stream_connection_string(
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    listen_pg_addr_str: &str,
    pageserver_connstr: &str,
) -> anyhow::Result<String> {
    let sk_connstr = format!("postgresql://no_user@{listen_pg_addr_str}/no_db");
    let me_conf = sk_connstr
        .parse::<postgres::config::Config>()
        .with_context(|| {
            format!("Failed to parse pageserver connection string '{sk_connstr}' as a postgres one")
        })?;
    let (host, port) = utils::connstring::connection_host_port(&me_conf);
    Ok(format!(
        "host={host} port={port} options='-c ztimelineid={timeline_id} ztenantid={tenant_id} pageserver_connstr={pageserver_connstr}'",
    ))
}
