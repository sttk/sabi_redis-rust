// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "standalone-sync")]
mod standalone_sync;

#[cfg(feature = "standalone-async")]
mod standalone_async;

#[cfg(feature = "sentinel-sync")]
mod sentinel_sync;

#[cfg(feature = "sentinel-async")]
mod sentinel_async;

#[cfg(feature = "cluster-sync")]
mod cluster_sync;

#[cfg(feature = "cluster-async")]
mod cluster_async;

#[cfg(feature = "standalone-sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-sync")))]
pub use standalone_sync::{RedisDataConn, RedisDataSrc, RedisSyncError};

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_async::{RedisAsyncDataConn, RedisAsyncDataSrc, RedisAsyncError};

#[cfg(any(feature = "sentinel-sync", feature = "sentinel-async"))]
pub mod sentinel {
    #[cfg(feature = "sentinel-sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-sync")))]
    pub use crate::sentinel_sync::{
        RedisSentinelDataConn, RedisSentinelDataSrc, RedisSentinelSyncError,
    };

    #[cfg(feature = "sentinel-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-async")))]
    pub use crate::sentinel_async::{
        RedisSentinelAsyncDataConn, RedisSentinelAsyncDataSrc, RedisSentinelAsyncError,
    };
}

#[cfg(any(feature = "cluster-sync", feature = "cluster-async"))]
pub mod cluster {
    #[cfg(feature = "cluster-sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-sync")))]
    pub use crate::cluster_sync::{
        RedisClusterDataConn, RedisClusterDataSrc, RedisClusterSyncError,
    };

    #[cfg(feature = "cluster-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-async")))]
    pub use crate::cluster_async::{
        RedisClusterAsyncDataConn, RedisClusterAsyncDataSrc, RedisClusterAsyncError,
    };
}
