// Copyright (C) 2025-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "standalone-sync")]
mod standalone_sync;

#[cfg(feature = "standalone-sync")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-sync")))]
pub use standalone_sync::{RedisDataConn, RedisDataSrc, RedisSyncError};

#[cfg(feature = "standalone-async")]
mod standalone_async;

#[cfg(feature = "standalone-async")]
#[cfg_attr(docsrs, doc(cfg(feature = "standalone-async")))]
pub use standalone_async::{RedisAsyncDataConn, RedisAsyncDataSrc, RedisAsyncError};

#[cfg(any(feature = "sentinel-sync", feature = "sentinel-async"))]
pub mod sentinel;

#[cfg(any(feature = "cluster-sync", feature = "cluster-async"))]
pub mod cluster;
