// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lightning_block_sync::gossip::GossipVerifier;

use crate::chain::ChainSource;
use crate::config::{RGS_SNAPSHOT_MAX_SIZE, RGS_SYNC_TIMEOUT_SECS};
use crate::logger::{log_error, log_trace, LdkLogger, Logger};
use crate::runtime::{Runtime, RuntimeSpawner};
use crate::types::{GossipSync, Graph, P2PGossipSync, RapidGossipSync};
use crate::Error;

pub(crate) enum GossipSource {
	P2PNetwork {
		gossip_sync: Arc<P2PGossipSync>,
	},
	RapidGossipSync {
		gossip_sync: Arc<RapidGossipSync>,
		server_url: String,
		latest_sync_timestamp: AtomicU32,
		logger: Arc<Logger>,
	},
}

impl GossipSource {
	pub fn new_p2p(
		network_graph: Arc<Graph>, chain_source: Arc<ChainSource>, runtime: Arc<Runtime>,
		logger: Arc<Logger>,
	) -> Self {
		let verifier = chain_source.as_utxo_source().map(|utxo_source| {
			Arc::new(GossipVerifier::new(Arc::new(utxo_source), RuntimeSpawner::new(runtime)))
		});

		let gossip_sync = Arc::new(P2PGossipSync::new(network_graph, verifier, logger));
		Self::P2PNetwork { gossip_sync }
	}

	pub fn new_rgs(
		server_url: String, latest_sync_timestamp: u32, network_graph: Arc<Graph>,
		logger: Arc<Logger>,
	) -> Self {
		let gossip_sync = Arc::new(RapidGossipSync::new(network_graph, Arc::clone(&logger)));
		let latest_sync_timestamp = AtomicU32::new(latest_sync_timestamp);
		Self::RapidGossipSync { gossip_sync, server_url, latest_sync_timestamp, logger }
	}

	pub fn is_rgs(&self) -> bool {
		matches!(self, Self::RapidGossipSync { .. })
	}

	pub fn as_gossip_sync(&self) -> GossipSync {
		match self {
			Self::RapidGossipSync { gossip_sync, .. } => GossipSync::Rapid(Arc::clone(gossip_sync)),
			Self::P2PNetwork { gossip_sync, .. } => GossipSync::P2P(Arc::clone(gossip_sync)),
		}
	}

	pub async fn update_rgs_snapshot(&self) -> Result<u32, Error> {
		match self {
			Self::P2PNetwork { gossip_sync: _, .. } => Ok(0),
			Self::RapidGossipSync { gossip_sync, server_url, latest_sync_timestamp, logger } => {
				let query_timestamp = latest_sync_timestamp.load(Ordering::Acquire);
				let query_url = format!("{}/{}", server_url, query_timestamp);

				let query = bitreq::get(query_url)
					.with_max_body_size(Some(RGS_SNAPSHOT_MAX_SIZE))
					.with_timeout(RGS_SYNC_TIMEOUT_SECS);
				let response = query.send_async().await.map_err(|e| {
					log_error!(logger, "Failed to retrieve RGS gossip update: {e}");
					Error::GossipUpdateTimeout
				})?;

				match response.status_code {
					200 => {
						let new_latest_sync_timestamp =
							gossip_sync.update_network_graph(response.as_bytes()).map_err(|e| {
								log_trace!(
									logger,
									"Failed to update network graph with RGS data: {:?}",
									e
								);
								Error::GossipUpdateFailed
							})?;
						// update_network_graph returns the snapshot's internal
						// latest_seen_timestamp (the newest gossip-message ts inside the
						// snapshot). Two problems with persisting that verbatim:
						//
						//   1. The reference RGS server only serves snapshots at multiples of
						//      SYMLINK_GRANULARITY_INTERVAL (3h). A raw gossip-message ts is
						//      almost never aligned, so re-requesting it 404s.
						//
						//   2. Even an aligned ts can be newer than the server's latest
						//      regenerated reference_timestamp (the server's snapshot_interval
						//      can be longer than its symlink granularity in production), so
						//      the symlink for that aligned boundary may not exist yet.
						//
						// Fix: align down to the 3h grid AND cap at `now − SAFE_LAG_SECS` so
						// we never query forward of the server's most recent regen window.
						// Never regress past query_timestamp (the value we just succeeded
						// with) — that boundary is demonstrably served.
						const RGS_SNAPSHOT_GRANULARITY_SECS: u32 = 10800;
						const SAFE_LAG_SECS: u32 = 6 * 3600;
						let now = SystemTime::now()
							.duration_since(UNIX_EPOCH)
							.map(|d| d.as_secs() as u32)
							.unwrap_or(new_latest_sync_timestamp);
						let safe_max = now.saturating_sub(SAFE_LAG_SECS);
						let capped = std::cmp::min(new_latest_sync_timestamp, safe_max);
						let aligned = capped - (capped % RGS_SNAPSHOT_GRANULARITY_SECS);
						let next_sync_timestamp = std::cmp::max(aligned, query_timestamp);
						latest_sync_timestamp
							.store(next_sync_timestamp, Ordering::Release);
						Ok(next_sync_timestamp)
					},
					code => {
						log_trace!(logger, "Failed to retrieve RGS gossip update: HTTP {}", code);
						Err(Error::GossipUpdateFailed)
					},
				}
			},
		}
	}
}
