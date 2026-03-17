// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#[cfg(feature = "cluster-sync")]
mod cluster_sync;

#[cfg(feature = "cluster-sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-sync")))]
pub use cluster_sync::{RedisClusterDataConn, RedisClusterDataSrc, RedisClusterSyncError};

#[cfg(feature = "cluster-async")]
mod cluster_async;

#[cfg(feature = "cluster-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-async")))]
pub use cluster_async::{
    RedisClusterAsyncDataConn, RedisClusterAsyncDataSrc, RedisClusterAsyncError,
};
