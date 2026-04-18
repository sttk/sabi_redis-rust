// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "standalone")]
mod standalone;

#[cfg(feature = "standalone")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone")))]
pub use standalone::{RedisDataConn, RedisDataSrc, RedisError};

#[cfg(feature = "standalone-async")]
mod standalone_async;

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_async::{RedisDataConnAsync, RedisDataSrcAsync, RedisErrorAsync};

#[cfg(feature = "sentinel")]
mod sentinel;

#[cfg(feature = "sentinel")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel")))]
pub use sentinel::{RedisSentinelDataConn, RedisSentinelDataSrc, RedisSentinelError};

#[cfg(feature = "sentinel-async")]
mod sentinel_async;

#[cfg(feature = "sentinel-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-async")))]
pub use sentinel_async::{
    RedisSentinelDataConnAsync, RedisSentinelDataSrcAsync, RedisSentinelErrorAsync,
};

#[cfg(feature = "cluster")]
mod cluster;

#[cfg(feature = "cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster")))]
pub use cluster::{RedisClusterDataConn, RedisClusterDataSrc, RedisClusterError};

#[cfg(feature = "cluster-async")]
mod cluster_async;

#[cfg(feature = "cluster-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "cluster-async")))]
pub use cluster_async::{
    RedisClusterDataConnAsync, RedisClusterDataSrcAsync, RedisClusterErrorAsync,
};
