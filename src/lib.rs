// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

//! The `sabi` data access library for Redis in Rust.
//!
//! `sabi_redis` is a Rust crate that provides a streamlined way to access various Redis configurations
//! within the `sabi` framework. It includes `DataSrc` and `DataConn` implementations designed to make
//! your development process more efficient.
//!
//! This crate supports multiple Redis configurations:
//! - **Standalone**: For a single Redis server.
//! - **Sentinel**: For a Redis Sentinel managed setup, providing automatic failover.
//! - **Cluster**: For a distributed Redis Cluster setup.
//! - **Pub/Sub**: For processing Redis Pub/Sub messages within the `sabi` framework.
//!
//! Each configuration has both **synchronous** and **asynchronous** (compatible with `tokio`)
//! implementations, which can be enabled via features.
//!
//! ## Features
//!
//! To use this crate, enable the features corresponding to your Redis setup and preferred
//! execution model:
//!
//! - `standalone-sync` (default): Synchronous connection to a standalone Redis server.
//! - `standalone-async`: Asynchronous connection to a standalone Redis server.
//! - `sentinel-sync`: Synchronous connection to a Redis Sentinel setup.
//! - `sentinel-async`: Asynchronous connection to a Redis Sentinel setup.
//! - `cluster-sync`: Synchronous connection to a Redis Cluster.
//! - `cluster-async`: Asynchronous connection to a Redis Cluster.

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

#[cfg(any(
    feature = "standalone-sync",
    feature = "sentinel-sync",
    feature = "cluster-sync"
))]
mod pubsub_sync;

#[cfg(any(
    feature = "standalone-async",
    feature = "sentinel-async",
    feature = "cluster-async"
))]
mod pubsub_async;

#[cfg(feature = "standalone-sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-sync")))]
pub use standalone_sync::{RedisDataConn, RedisDataSrc, RedisSyncError};

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_async::{RedisAsyncDataConn, RedisAsyncDataSrc, RedisAsyncError};

#[cfg(any(feature = "sentinel-sync", feature = "sentinel-async"))]
/// This module provides components for connecting to a Redis Sentinel managed setup.
///
/// - **Synchronous**: When the `sentinel-sync` feature is enabled, it provides data sources and
///   connections that utilize a connection pool for synchronous operations.
/// - **Asynchronous**: When the `sentinel-async` feature is enabled, it provides asynchronous
///   versions compatible with the `tokio` runtime.
///
/// These components automatically track the current master node and handle failover transitions
/// transparently, allowing your data access layer to remain resilient.
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
/// This module provides components for connecting to a Redis Cluster.
///
/// - **Synchronous**: If the `cluster-sync` feature is active, it provides synchronous data
///   sources and connections for sharded Redis setups.
/// - **Asynchronous**: If the `cluster-async` feature is active, it provides asynchronous
///   alternatives compatible with `tokio`.
///
/// It allows for seamless interaction with a distributed Redis Cluster, managing node
/// discovery and request routing internally so that the application can treat the cluster
/// as a single data source.
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

/// This module provides components for processing Redis Pub/Sub messages.
///
/// - **Synchronous**: For synchronous processing, it provides components that handle messages
///   in a blocking manner.
/// - **Asynchronous**: For asynchronous processing, it provides components that work with
///   `tokio` and `redis-rs` async Pub/Sub features.
///
/// This module allows received messages to be treated as `sabi` data connections. By wrapping
/// a message in a data connection, it can be passed through the `sabi` data access layer,
/// making it easy to integrate message handling into your business logic consistently
/// across Standalone, Sentinel, and Cluster modes.
pub mod pubsub {
    #[cfg(any(
        feature = "standalone-sync",
        feature = "sentinel-sync",
        feature = "cluster-sync"
    ))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(
            feature = "standalone-sync",
            feature = "sentinel-sync",
            feature = "cluster-sync"
        )))
    )]
    pub use crate::pubsub_sync::{RedisPubSubMsgDataConn, RedisPubSubMsgDataSrc};

    #[cfg(any(
        feature = "standalone-async",
        feature = "sentinel-async",
        feature = "cluster-async"
    ))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(
            feature = "standalone-async",
            feature = "sentinel-async",
            feature = "cluster-async"
        )))
    )]
    pub use crate::pubsub_async::{RedisPubSubMsgAsyncDataConn, RedisPubSubMsgAsyncDataSrc};

    #[cfg(feature = "standalone-sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "standalone-sync")))]
    pub use crate::pubsub_sync::{RedisPubSub, RedisPubSubError};

    #[cfg(feature = "sentinel-sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentinel-sync")))]
    pub use crate::pubsub_sync::{RedisPubSubSentinel, RedisPubSubSentinelError};

    #[cfg(feature = "cluster-sync")]
    #[cfg_attr(docsrs, doc(cfg(feature = "cluster-sync")))]
    pub use crate::pubsub_sync::{RedisPubSubCluster, RedisPubSubClusterError};
}
